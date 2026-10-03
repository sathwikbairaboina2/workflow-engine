package worker_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sathwikbairaboina2/workflow-engine/internal/testserver"
	"github.com/sathwikbairaboina2/workflow-engine/sdk/client"
	"github.com/sathwikbairaboina2/workflow-engine/sdk/worker"
	"github.com/sathwikbairaboina2/workflow-engine/sdk/workflow"
	"github.com/sathwikbairaboina2/workflow-engine/wire"
)

var update = flag.Bool("update", false, "rewrite golden histories")

var goldenFlakyCalls atomic.Int32

func goldenFlaky(ctx context.Context, in int) (int, error) {
	if goldenFlakyCalls.Add(1) <= 1 {
		return 0, errors.New("transient")
	}
	return in * 2, nil
}

func goldenRetry(ctx workflow.Context, in int) (int, error) {
	var out int
	err := workflow.ExecuteActivity(ctx, workflow.ActivityOptions{
		StartToClose: 5 * time.Second,
		Retry:        workflow.RetryPolicy{Initial: 10 * time.Millisecond, Backoff: 1, MaxAttempts: 3},
	}, "goldenFlaky", in).Get(ctx, &out)
	return out, err
}

func goldenSignal(ctx workflow.Context, in int) (string, error) {
	var got string
	if !workflow.GetSignalChannel(ctx, "go").Receive(ctx, &got) {
		return "", workflow.ErrCanceled
	}
	return got, nil
}

func goldenCancel(ctx workflow.Context, in int) error { return workflow.Sleep(ctx, time.Hour) }

func goldenSideEffect(ctx workflow.Context, in int) (string, error) {
	var id string
	workflow.SideEffect(ctx, func() any {
		b := make([]byte, 8)
		rand.Read(b)
		return hex.EncodeToString(b)
	}).Get(&id)
	if err := workflow.Sleep(ctx, 10*time.Millisecond); err != nil {
		return "", err
	}
	return id, nil
}

type scenario struct {
	name     string
	fn       any
	register func(w *worker.Worker)
	input    any
	drive    func(t *testing.T, c *client.Client, id string)
}

func waitHistory(t *testing.T, c *client.Client, id, what string, ty wire.EventType) {
	t.Helper()
	waitFor(t, what, func() bool { return hasEvent(history(t, c, id), ty, nil) })
}

func scenarios() []scenario {
	return []scenario{
		{name: "hello", fn: Hello, input: "ada",
			register: func(w *worker.Worker) { w.RegisterWorkflow(Hello); w.RegisterActivity(Greet) }},
		{name: "retry", fn: goldenRetry, input: 21,
			register: func(w *worker.Worker) { w.RegisterWorkflow(goldenRetry); w.RegisterActivity(goldenFlaky) }},
		{name: "signal", fn: goldenSignal, input: 0,
			register: func(w *worker.Worker) { w.RegisterWorkflow(goldenSignal) },
			drive: func(t *testing.T, c *client.Client, id string) {
				waitHistory(t, c, id, "first workflow task", wire.WorkflowTaskCompleted)
				if err := c.Signal(context.Background(), id, "go", "payload"); err != nil {
					t.Fatal(err)
				}
			}},
		{name: "cancel", fn: goldenCancel, input: 0,
			register: func(w *worker.Worker) { w.RegisterWorkflow(goldenCancel) },
			drive: func(t *testing.T, c *client.Client, id string) {
				waitHistory(t, c, id, "timer started", wire.TimerStarted)
				if err := c.Cancel(context.Background(), id, "golden"); err != nil {
					t.Fatal(err)
				}
			}},
		{name: "sideeffect", fn: goldenSideEffect, input: 0,
			register: func(w *worker.Worker) { w.RegisterWorkflow(goldenSideEffect) }},
	}
}

func goldenPath(name string) string { return filepath.Join("testdata", "histories", name+".json") }

func isClose(ty wire.EventType) bool {
	return ty == wire.WorkflowExecutionCompleted || ty == wire.WorkflowExecutionFailed || ty == wire.WorkflowExecutionCanceled
}

func regenerate(t *testing.T, s scenario) {
	srv := testserver.Start(t)
	c := client.New(srv.URL)
	runWorker(t, srv, s.register)
	const id = "golden"
	if _, err := c.Start(context.Background(), client.StartOptions{ID: id, TaskQueue: "q"}, s.name, s.input); err != nil {
		t.Fatal(err)
	}
	if s.drive != nil {
		s.drive(t, c, id)
	}
	waitFor(t, "run to close", func() bool {
		d, err := c.Describe(context.Background(), id)
		return err == nil && d.Status != "running"
	})
	h := history(t, c, id)
	b, err := json.MarshalIndent(h, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(goldenPath(s.name)), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(goldenPath(s.name), append(b, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestReplayGoldenHistories(t *testing.T) {
	for _, s := range scenarios() {
		t.Run(s.name, func(t *testing.T) {
			if *update {
				// The scenario is registered under s.name so the history's workflow type matches.
				regenerateAs(t, s)
			}
			events, err := worker.ReadHistoryFile(goldenPath(s.name))
			if err != nil {
				t.Fatal(err)
			}
			if len(events) == 0 || !isClose(events[len(events)-1].Type) {
				t.Fatalf("golden history must end in a close event, got %d events", len(events))
			}
			if err := worker.ReplayHistory(events, s.fn); err != nil {
				t.Fatalf("replay of %s diverged: %v", s.name, err)
			}
		})
	}
}

// regenerateAs runs the scenario with its workflow function registered under the scenario name.
func regenerateAs(t *testing.T, s scenario) {
	inner := s.register
	s.register = func(w *worker.Worker) {
		inner(w)
		w.RegisterWorkflowWithName(s.name, s.fn)
	}
	regenerate(t, s)
}

func TestGoldenDetectsChange(t *testing.T) {
	changed := func(ctx workflow.Context, name string) (string, error) {
		var out string
		err := workflow.ExecuteActivity(ctx, workflow.ActivityOptions{}, "SomethingElse", name).Get(ctx, &out)
		return out, err
	}
	err := worker.ReplayHistoryFile(goldenPath("hello"), changed)
	var nd *worker.NondeterminismError
	if !errors.As(err, &nd) {
		t.Fatalf("expected a nondeterminism error, got %v", err)
	}
}
