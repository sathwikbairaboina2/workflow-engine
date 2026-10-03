package worker_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sathwikbairaboina2/workflow-engine/internal/testserver"
	"github.com/sathwikbairaboina2/workflow-engine/sdk/activity"
	"github.com/sathwikbairaboina2/workflow-engine/sdk/client"
	"github.com/sathwikbairaboina2/workflow-engine/sdk/worker"
	"github.com/sathwikbairaboina2/workflow-engine/sdk/workflow"
	"github.com/sathwikbairaboina2/workflow-engine/wire"
)

func bg(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// runWorker starts a worker on queue "q" and returns a stop function (also run at cleanup).
func runWorker(t *testing.T, srv *testserver.Server, register func(w *worker.Worker)) (stop func()) {
	t.Helper()
	w := worker.New(client.New(srv.URL), "q", worker.Options{Identity: "test", WorkflowPollers: 2, ActivityPollers: 2})
	register(w)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); w.Run(ctx) }()
	var once atomic.Bool
	stop = func() {
		if once.CompareAndSwap(false, true) {
			cancel()
			<-done
		}
	}
	t.Cleanup(stop)
	return stop
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func history(t *testing.T, c *client.Client, id string) []wire.Event {
	t.Helper()
	h, err := c.History(context.Background(), id, "")
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func hasEvent(evs []wire.Event, ty wire.EventType, pred func(wire.Event) bool) bool {
	for _, e := range evs {
		if e.Type == ty && (pred == nil || pred(e)) {
			return true
		}
	}
	return false
}

func causeIs(cause string) func(wire.Event) bool {
	return func(e wire.Event) bool {
		var a wire.WorkflowTaskFailedAttrs
		e.DecodeAttrs(&a)
		return a.Cause == cause
	}
}

func Hello(ctx workflow.Context, name string) (string, error) {
	var out string
	err := workflow.ExecuteActivity(ctx, workflow.ActivityOptions{StartToClose: 5 * time.Second}, "Greet", name).Get(ctx, &out)
	return out, err
}

func Greet(ctx context.Context, name string) (string, error) { return "hello, " + name, nil }

func TestFlow1EndToEnd(t *testing.T) {
	srv := testserver.Start(t)
	c := client.New(srv.URL)
	runWorker(t, srv, func(w *worker.Worker) { w.RegisterWorkflow(Hello); w.RegisterActivity(Greet) })
	if _, err := c.Start(bg(t), client.StartOptions{ID: "f1", TaskQueue: "q"}, Hello, "ada"); err != nil {
		t.Fatal(err)
	}
	var out string
	if err := c.GetResult(bg(t), "f1", &out); err != nil || out != "hello, ada" {
		t.Fatalf("result %q err %v", out, err)
	}
	want := []wire.EventType{wire.WorkflowExecutionStarted, wire.WorkflowTaskScheduled, wire.WorkflowTaskStarted, wire.WorkflowTaskCompleted,
		wire.ActivityTaskScheduled, wire.ActivityTaskStarted, wire.ActivityTaskCompleted,
		wire.WorkflowTaskScheduled, wire.WorkflowTaskStarted, wire.WorkflowTaskCompleted, wire.WorkflowExecutionCompleted}
	h := history(t, c, "f1")
	if len(h) != len(want) {
		t.Fatalf("history has %d events, want %d", len(h), len(want))
	}
	for i, e := range h {
		if e.Type != want[i] {
			t.Fatalf("event %d is %s, want %s", i+1, e.Type, want[i])
		}
	}
}

func TestActivityRetryEndToEnd(t *testing.T) {
	srv := testserver.Start(t)
	c := client.New(srv.URL)
	var calls atomic.Int32
	flaky := func(ctx context.Context, in int) (int, error) {
		if calls.Add(1) <= 2 {
			return 0, errors.New("transient")
		}
		return in * 2, nil
	}
	wf := func(ctx workflow.Context, in int) (int, error) {
		var out int
		err := workflow.ExecuteActivity(ctx, workflow.ActivityOptions{
			StartToClose: 5 * time.Second,
			Retry:        workflow.RetryPolicy{Initial: 10 * time.Millisecond, Backoff: 1, MaxAttempts: 5},
		}, "Flaky", in).Get(ctx, &out)
		return out, err
	}
	runWorker(t, srv, func(w *worker.Worker) {
		w.RegisterWorkflowWithName("Retry", wf)
		w.RegisterActivityWithName("Flaky", flaky)
	})
	c.Start(bg(t), client.StartOptions{ID: "r1", TaskQueue: "q"}, "Retry", 21)
	var out int
	if err := c.GetResult(bg(t), "r1", &out); err != nil || out != 42 {
		t.Fatalf("result %d err %v", out, err)
	}
	var attempt int
	for _, e := range history(t, c, "r1") {
		if e.Type == wire.ActivityTaskStarted {
			var a wire.ActivityTaskStartedAttrs
			e.DecodeAttrs(&a)
			attempt = a.Attempt
		}
	}
	if attempt != 3 || calls.Load() != 3 {
		t.Fatalf("attempt %d, calls %d", attempt, calls.Load())
	}
}

func TestNonRetryableEndToEnd(t *testing.T) {
	srv := testserver.Start(t)
	c := client.New(srv.URL)
	var calls atomic.Int32
	fatal := func(ctx context.Context, in int) (int, error) {
		calls.Add(1)
		return 0, activity.NewNonRetryableError("Fatal", "no way")
	}
	wf := func(ctx workflow.Context, in int) (int, error) {
		var out int
		err := workflow.ExecuteActivity(ctx, workflow.ActivityOptions{Retry: workflow.RetryPolicy{Initial: 10 * time.Millisecond}}, "Fatal", in).Get(ctx, &out)
		return out, err
	}
	runWorker(t, srv, func(w *worker.Worker) {
		w.RegisterWorkflowWithName("NR", wf)
		w.RegisterActivityWithName("Fatal", fatal)
	})
	c.Start(bg(t), client.StartOptions{ID: "n1", TaskQueue: "q"}, "NR", 1)
	var out int
	err := c.GetResult(bg(t), "n1", &out)
	var wfe *client.WorkflowFailedError
	if !errors.As(err, &wfe) || wfe.Failure.Type != "Fatal" {
		t.Fatalf("err = %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("activity ran %d times", calls.Load())
	}
}

func TestSleepSignalEndToEnd(t *testing.T) {
	srv := testserver.Start(t)
	c := client.New(srv.URL)
	wf := func(ctx workflow.Context, in int) (string, error) {
		var got string
		ch := workflow.GetSignalChannel(ctx, "go")
		timeout := workflow.NewTimer(ctx, 30*time.Second)
		result := "timeout"
		err := workflow.NewSelector(ctx).
			AddReceive(ch, func(c workflow.ReceiveChannel) { c.Receive(ctx, &got); result = got }).
			AddFuture(timeout, func(workflow.Future) {}).
			Select(ctx)
		return result, err
	}
	runWorker(t, srv, func(w *worker.Worker) { w.RegisterWorkflowWithName("Wait", wf) })
	c.Start(bg(t), client.StartOptions{ID: "s1", TaskQueue: "q"}, "Wait", 0)
	waitFor(t, "timer started", func() bool { return hasEvent(history(t, c, "s1"), wire.TimerStarted, nil) })
	if err := c.Signal(bg(t), "s1", "go", "now"); err != nil {
		t.Fatal(err)
	}
	var out string
	if err := c.GetResult(bg(t), "s1", &out); err != nil || out != "now" {
		t.Fatalf("result %q err %v", out, err)
	}
}

func TestCancelEndToEnd(t *testing.T) {
	srv := testserver.Start(t)
	c := client.New(srv.URL)
	wf := func(ctx workflow.Context, in int) error { return workflow.Sleep(ctx, time.Hour) }
	runWorker(t, srv, func(w *worker.Worker) { w.RegisterWorkflowWithName("Sleeper", wf) })
	c.Start(bg(t), client.StartOptions{ID: "x1", TaskQueue: "q"}, "Sleeper", 0)
	waitFor(t, "timer started", func() bool { return hasEvent(history(t, c, "x1"), wire.TimerStarted, nil) })
	if err := c.Cancel(bg(t), "x1", "enough"); err != nil {
		t.Fatal(err)
	}
	if err := c.GetResult(bg(t), "x1", nil); !errors.Is(err, client.ErrWorkflowCanceled) {
		t.Fatalf("err = %v", err)
	}
	d, _ := c.Describe(bg(t), "x1")
	if d.Status != "canceled" || len(d.PendingTimers) != 0 {
		t.Fatalf("describe %+v", d)
	}
}

func TestNondeterminismDetected(t *testing.T) {
	srv := testserver.Start(t)
	c := client.New(srv.URL)
	v1 := func(ctx workflow.Context, in int) error {
		if err := workflow.ExecuteActivity(ctx, workflow.ActivityOptions{}, "A", in).Get(ctx, nil); err != nil {
			return err
		}
		return workflow.Sleep(ctx, time.Hour)
	}
	v2 := func(ctx workflow.Context, in int) error {
		if err := workflow.ExecuteActivity(ctx, workflow.ActivityOptions{}, "B", in).Get(ctx, nil); err != nil {
			return err
		}
		return workflow.Sleep(ctx, time.Hour)
	}
	noop := func(ctx context.Context, in int) error { return nil }
	stopA := runWorker(t, srv, func(w *worker.Worker) { w.RegisterWorkflowWithName("Flow", v1); w.RegisterActivityWithName("A", noop) })
	c.Start(bg(t), client.StartOptions{ID: "d1", TaskQueue: "q"}, "Flow", 1)
	waitFor(t, "timer started", func() bool { return hasEvent(history(t, c, "d1"), wire.TimerStarted, nil) })
	stopA()

	runWorker(t, srv, func(w *worker.Worker) { w.RegisterWorkflowWithName("Flow", v2); w.RegisterActivityWithName("B", noop) })
	if err := c.Signal(bg(t), "d1", "poke", nil); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "WorkflowTaskFailed(nondeterminism)", func() bool {
		return hasEvent(history(t, c, "d1"), wire.WorkflowTaskFailed, causeIs("nondeterminism"))
	})
	h := history(t, c, "d1")
	if hasEvent(h, wire.ActivityTaskScheduled, func(e wire.Event) bool {
		var a wire.ActivityTaskScheduledAttrs
		e.DecodeAttrs(&a)
		return a.ActivityType == "B"
	}) {
		t.Fatal("divergent workflow code appended an activity")
	}
	if d, _ := c.Describe(bg(t), "d1"); d.Status != "running" {
		t.Fatalf("status %s", d.Status)
	}
}

func TestUnknownWorkflowType(t *testing.T) {
	srv := testserver.Start(t)
	c := client.New(srv.URL)
	runWorker(t, srv, func(w *worker.Worker) {})
	c.Start(bg(t), client.StartOptions{ID: "u1", TaskQueue: "q"}, "Nope", nil)
	waitFor(t, "unknown_workflow_type failure", func() bool {
		return hasEvent(history(t, c, "u1"), wire.WorkflowTaskFailed, causeIs("unknown_workflow_type"))
	})
}

func TestPanicInWorkflow(t *testing.T) {
	srv := testserver.Start(t)
	c := client.New(srv.URL)
	wf := func(ctx workflow.Context, in int) error { panic("kaboom") }
	runWorker(t, srv, func(w *worker.Worker) { w.RegisterWorkflowWithName("Panicky", wf) })
	c.Start(bg(t), client.StartOptions{ID: "p1", TaskQueue: "q"}, "Panicky", 0)
	waitFor(t, "panic failure", func() bool {
		return hasEvent(history(t, c, "p1"), wire.WorkflowTaskFailed, causeIs("panic"))
	})
}

func TestActivityInfo(t *testing.T) {
	srv := testserver.Start(t)
	c := client.New(srv.URL)
	echo := func(ctx context.Context, in int) (string, error) {
		info := activity.GetInfo(ctx)
		if info.Attempt != 1 || info.ActivityType != "Echo" || info.Deadline.IsZero() {
			return "", errors.New("incomplete activity info")
		}
		return info.IdempotencyKey, nil
	}
	wf := func(ctx workflow.Context, in int) (string, error) {
		var out string
		err := workflow.ExecuteActivity(ctx, workflow.ActivityOptions{}, "Echo", in).Get(ctx, &out)
		return out, err
	}
	runWorker(t, srv, func(w *worker.Worker) {
		w.RegisterWorkflowWithName("EchoWF", wf)
		w.RegisterActivityWithName("Echo", echo)
	})
	run, _ := c.Start(bg(t), client.StartOptions{ID: "i1", TaskQueue: "q"}, "EchoWF", 0)
	var out string
	if err := c.GetResult(bg(t), "i1", &out); err != nil || out != run+":5" {
		t.Fatalf("key %q err %v, want %q", out, err, run+":5")
	}
}

func TestRegisterRejectsBadSignatures(t *testing.T) {
	w := worker.New(client.New("http://127.0.0.1:1"), "q", worker.Options{})
	for name, f := range map[string]func(){
		"workflow without context": func() { w.RegisterWorkflow(func(in int) error { return nil }) },
		"activity without context": func() { w.RegisterActivity(func(in int) error { return nil }) },
		"activity without error":   func() { w.RegisterActivity(func(ctx context.Context) int { return 0 }) },
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("%s was accepted", name)
				}
			}()
			f()
		}()
	}
}
