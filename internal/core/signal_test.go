package core

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/sathwikbairaboina2/workflow-engine/internal/clock"
	"github.com/sathwikbairaboina2/workflow-engine/internal/store"
	"github.com/sathwikbairaboina2/workflow-engine/wire"
)

func signal(t *testing.T, e *Engine, id, name string) {
	t.Helper()
	if err := e.SignalWorkflow(context.Background(), wire.SignalRequest{WorkflowID: id, Name: name, Payload: wire.MustEncode(name)}); err != nil {
		t.Fatal(err)
	}
}

func TestSignalAppendsWhenIdle(t *testing.T) {
	e, _, _ := newEngine(t)
	run := start(t, e, "w")
	complete(t, e, pollWF(t, e).LeaseToken)
	n := len(history(t, e, run))
	signal(t, e, "w", "go")
	h := history(t, e, run)
	if !eqTypes(types(h[n:]), []wire.EventType{wire.WorkflowExecutionSignaled, wire.WorkflowTaskScheduled}) {
		t.Fatalf("appended %v", types(h[n:]))
	}
	if s := attrsOf[wire.WorkflowExecutionSignaledAttrs](t, h[n]); s.Name != "go" {
		t.Fatalf("signal %+v", s)
	}
	if pollWF(t, e) == nil {
		t.Fatal("no task after signal")
	}
}

func TestSignalWhileTaskScheduledDoesNotCreateSecondTask(t *testing.T) {
	e, _, st := newEngine(t)
	run := start(t, e, "w")
	signal(t, e, "w", "go")
	h := history(t, e, run)
	if !eqTypes(types(h), []wire.EventType{wire.WorkflowExecutionStarted, wire.WorkflowTaskScheduled, wire.WorkflowExecutionSignaled}) {
		t.Fatalf("history %v", types(h))
	}
	if countRows(t, st, "tasks") != 1 {
		t.Fatal("second workflow task created")
	}
	task := pollWF(t, e)
	if len(task.History) != 4 {
		t.Fatalf("task history %v", types(task.History))
	}
}

func TestSignalBufferedDuringInflightTask(t *testing.T) {
	e, _, _ := newEngine(t)
	run := start(t, e, "w")
	task := pollWF(t, e)
	n := len(history(t, e, run))
	signal(t, e, "w", "s1")
	signal(t, e, "w", "s2")
	if len(history(t, e, run)) != n {
		t.Fatal("signal appended while task in flight")
	}
	complete(t, e, task.LeaseToken, wire.Command{Type: wire.ScheduleActivity, Seq: 1, ActivityType: "A"})
	h := history(t, e, run)
	want := []wire.EventType{wire.WorkflowTaskCompleted, wire.ActivityTaskScheduled, wire.WorkflowExecutionSignaled, wire.WorkflowExecutionSignaled, wire.WorkflowTaskScheduled}
	if !eqTypes(types(h[n:]), want) {
		t.Fatalf("appended %v", types(h[n:]))
	}
	if a, b := attrsOf[wire.WorkflowExecutionSignaledAttrs](t, h[n+2]), attrsOf[wire.WorkflowExecutionSignaledAttrs](t, h[n+3]); a.Name != "s1" || b.Name != "s2" {
		t.Fatalf("order %s %s", a.Name, b.Name)
	}
}

func TestActivityResultBufferedDuringInflightTask(t *testing.T) {
	e, _, _ := newEngine(t)
	run := scheduleActivity(t, e, wire.RetryPolicy{})
	act := pollAct(t, e)
	signal(t, e, "w", "wake")
	wft := pollWF(t, e)
	n := len(history(t, e, run))
	if err := completeAct(e, act.LeaseToken, 3); err != nil {
		t.Fatal(err)
	}
	if len(history(t, e, run)) != n {
		t.Fatal("activity result appended while task in flight")
	}
	complete(t, e, wft.LeaseToken)
	h := history(t, e, run)
	want := []wire.EventType{wire.WorkflowTaskCompleted, wire.ActivityTaskStarted, wire.ActivityTaskCompleted, wire.WorkflowTaskScheduled}
	if !eqTypes(types(h[n:]), want) {
		t.Fatalf("appended %v", types(h[n:]))
	}
}

func TestCloseDroppedWhenBuffered(t *testing.T) {
	e, _, _ := newEngine(t)
	run := start(t, e, "w")
	task := pollWF(t, e)
	n := len(history(t, e, run))
	signal(t, e, "w", "late")
	complete(t, e, task.LeaseToken, wire.Command{Type: wire.CompleteWorkflow})
	h := history(t, e, run)
	want := []wire.EventType{wire.WorkflowTaskCompleted, wire.WorkflowExecutionSignaled, wire.WorkflowTaskScheduled}
	if !eqTypes(types(h[n:]), want) {
		t.Fatalf("appended %v", types(h[n:]))
	}
	d, _ := e.Describe(context.Background(), wire.DescribeRequest{WorkflowID: "w"})
	if d.Status != "running" {
		t.Fatalf("status %s", d.Status)
	}
	// the workflow sees the signal on its next task and may close then
	next := pollWF(t, e)
	complete(t, e, next.LeaseToken, wire.Command{Type: wire.CompleteWorkflow})
	d, _ = e.Describe(context.Background(), wire.DescribeRequest{WorkflowID: "w"})
	if d.Status != "completed" {
		t.Fatalf("status %s", d.Status)
	}
}

func TestSignalAfterCloseRejected(t *testing.T) {
	e, _, _ := newEngine(t)
	start(t, e, "w")
	complete(t, e, pollWF(t, e).LeaseToken, wire.Command{Type: wire.CompleteWorkflow})
	err := e.SignalWorkflow(context.Background(), wire.SignalRequest{WorkflowID: "w", Name: "x"})
	wantCode(t, err, CodeRunClosed)
	err = e.SignalWorkflow(context.Background(), wire.SignalRequest{WorkflowID: "nope", Name: "x"})
	wantCode(t, err, CodeNotFound)
}

func TestCancelRequestAndCancelWorkflow(t *testing.T) {
	e, _, st := newEngine(t)
	run := start(t, e, "w")
	complete(t, e, pollWF(t, e).LeaseToken,
		wire.Command{Type: wire.ScheduleActivity, Seq: 1, ActivityType: "A"},
		wire.Command{Type: wire.StartTimer, Seq: 2, DurationMS: 60000})
	n := len(history(t, e, run))
	if err := e.CancelWorkflow(context.Background(), wire.CancelRequest{WorkflowID: "w", Reason: "stop"}); err != nil {
		t.Fatal(err)
	}
	h := history(t, e, run)
	if !eqTypes(types(h[n:]), []wire.EventType{wire.WorkflowExecutionCancelRequested, wire.WorkflowTaskScheduled}) {
		t.Fatalf("appended %v", types(h[n:]))
	}
	complete(t, e, pollWF(t, e).LeaseToken, wire.Command{Type: wire.CancelWorkflow, Reason: "stop"})
	d, _ := e.Describe(context.Background(), wire.DescribeRequest{WorkflowID: "w"})
	if d.Status != "canceled" {
		t.Fatalf("status %s", d.Status)
	}
	if countRows(t, st, "tasks")+countRows(t, st, "timers") != 0 {
		t.Fatal("pending rows left after cancel")
	}
}

func TestHistoryLengthLimit(t *testing.T) {
	e, _, _ := newEngine(t, func(c *Config) { c.MaxHistoryEvents = 12 })
	run := start(t, e, "w")
	for i := 0; i < 20; i++ {
		task, err := e.TryPollWorkflowTask(context.Background(), wire.PollRequest{Queue: "q"})
		if err != nil {
			t.Fatal(err)
		}
		if task == nil {
			break
		}
		if err := tryComplete(e, task.LeaseToken); err != nil {
			t.Fatal(err)
		}
		if err := e.SignalWorkflow(context.Background(), wire.SignalRequest{WorkflowID: "w", Name: "s"}); err != nil {
			wantCode(t, err, CodeRunClosed)
			break
		}
	}
	d, err := e.Describe(context.Background(), wire.DescribeRequest{WorkflowID: "w"})
	if err != nil || d.Status != "failed" || d.Failure == nil || d.Failure.Type != "HistoryLimitExceeded" {
		t.Fatalf("describe %+v %v", d, err)
	}
	h := history(t, e, run)
	if len(h) > 12 || h[len(h)-1].Type != wire.WorkflowExecutionFailed {
		t.Fatalf("history length %d last %s", len(h), h[len(h)-1].Type)
	}
}

func TestRunLoopsFiresTimersAndReaps(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "wf.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	e := New(st, clock.Real{}, Config{PollTimeout: 50 * time.Millisecond, WorkflowTaskTimeout: 100 * time.Millisecond})
	run := start(t, e, "w")
	task := pollWF(t, e)
	complete(t, e, task.LeaseToken, wire.Command{Type: wire.StartTimer, Seq: 1, DurationMS: 50})
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); e.RunLoops(ctx, 10*time.Millisecond, 10*time.Millisecond) }()
	defer func() { cancel(); wg.Wait() }()
	deadline := time.Now().Add(2 * time.Second)
	for {
		for _, ev := range history(t, e, run) {
			if ev.Type == wire.TimerFired {
				goto fired
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("timer did not fire within 2s")
		}
		time.Sleep(10 * time.Millisecond)
	}
fired:
	// the task scheduled by the timer is polled and abandoned; the reaper must fail it with a timeout
	pollWF(t, e)
	deadline = time.Now().Add(2 * time.Second)
	for {
		for _, ev := range history(t, e, run) {
			if ev.Type == wire.WorkflowTaskFailed {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("reaper did not fail the abandoned task within 2s")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
