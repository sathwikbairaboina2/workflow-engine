package core

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/sathwikbairaboina2/workflow-engine/wire"
)

func pollWF(t *testing.T, e *Engine) *wire.WorkflowTask {
	t.Helper()
	task, err := e.TryPollWorkflowTask(context.Background(), wire.PollRequest{Queue: "q", Identity: "id1"})
	if err != nil {
		t.Fatal(err)
	}
	if task == nil {
		t.Fatal("no workflow task available")
	}
	return task
}

func tryComplete(e *Engine, tok string, cmds ...wire.Command) error {
	return e.CompleteWorkflowTask(context.Background(), wire.CompleteWorkflowTaskRequest{LeaseToken: tok, Commands: cmds})
}

func complete(t *testing.T, e *Engine, tok string, cmds ...wire.Command) {
	t.Helper()
	if err := tryComplete(e, tok, cmds...); err != nil {
		t.Fatalf("complete: %v", err)
	}
}

func attrsOf[T any](t *testing.T, ev wire.Event) T {
	t.Helper()
	var a T
	if err := ev.DecodeAttrs(&a); err != nil {
		t.Fatal(err)
	}
	return a
}

func wantCode(t *testing.T, err error, code string) {
	t.Helper()
	var ce *Error
	if !errors.As(err, &ce) || ce.Code != code {
		t.Fatalf("want code %s, got %v", code, err)
	}
}

func TestPollAppendsStartedAndReturnsFullHistory(t *testing.T) {
	e, _, _ := newEngine(t)
	start(t, e, "w")
	task := pollWF(t, e)
	if !eqTypes(types(task.History), []wire.EventType{wire.WorkflowExecutionStarted, wire.WorkflowTaskScheduled, wire.WorkflowTaskStarted}) {
		t.Fatalf("types %v", types(task.History))
	}
	st := attrsOf[wire.WorkflowTaskStartedAttrs](t, task.History[2])
	if st.ScheduledEventID != 2 || st.Identity != "id1" {
		t.Fatalf("started attrs %+v", st)
	}
	if task.Attempt != 1 || task.WorkflowType != "W" || task.TaskQueue != "q" {
		t.Fatalf("task %+v", task)
	}
	got, err := e.TryPollWorkflowTask(context.Background(), wire.PollRequest{Queue: "q"})
	if err != nil || got != nil {
		t.Fatalf("second poll %v %v", got, err)
	}
}

func TestCompleteScheduleActivity(t *testing.T) {
	e, _, st := newEngine(t)
	run := start(t, e, "w")
	task := pollWF(t, e)
	complete(t, e, task.LeaseToken, wire.Command{Type: wire.ScheduleActivity, Seq: 1, ActivityType: "A", Input: wire.MustEncode(5)})
	h := history(t, e, run)
	if !eqTypes(types(h[3:]), []wire.EventType{wire.WorkflowTaskCompleted, wire.ActivityTaskScheduled}) {
		t.Fatalf("types %v", types(h))
	}
	if h[3].EventID != 4 || h[4].EventID != 5 {
		t.Fatalf("ids %d %d", h[3].EventID, h[4].EventID)
	}
	c := attrsOf[wire.WorkflowTaskCompletedAttrs](t, h[3])
	if c.ScheduledEventID != 2 || c.StartedEventID != 3 {
		t.Fatalf("completed attrs %+v", c)
	}
	var sched, timeout int64
	var queue string
	if err := st.DB.QueryRow(`SELECT scheduled_event_id, timeout_ms, queue FROM tasks WHERE kind='activity' AND run_id=?`, run).Scan(&sched, &timeout, &queue); err != nil {
		t.Fatal(err)
	}
	if sched != 5 || timeout != 10000 || queue != "q" {
		t.Fatalf("activity task %d %d %s", sched, timeout, queue)
	}
	if n := countRows(t, st, "tasks"); n != 1 {
		t.Fatalf("task rows %d (workflow task should be gone)", n)
	}
}

func TestCompleteWorkflowCloses(t *testing.T) {
	e, _, st := newEngine(t)
	run := start(t, e, "w")
	task := pollWF(t, e)
	complete(t, e, task.LeaseToken,
		wire.Command{Type: wire.StartTimer, Seq: 1, DurationMS: 1000},
		wire.Command{Type: wire.CompleteWorkflow, Result: wire.MustEncode(map[string]bool{"ok": true})})
	h := history(t, e, run)
	if h[len(h)-1].Type != wire.WorkflowExecutionCompleted {
		t.Fatalf("last %s", h[len(h)-1].Type)
	}
	d, err := e.Describe(context.Background(), wire.DescribeRequest{WorkflowID: "w"})
	if err != nil || d.Status != "completed" {
		t.Fatalf("describe %+v %v", d, err)
	}
	var res map[string]bool
	if err := d.Result.Decode(&res); err != nil || !res["ok"] {
		t.Fatalf("result %v %v", res, err)
	}
	if countRows(t, st, "tasks")+countRows(t, st, "timers") != 0 {
		t.Fatal("rows left after close")
	}
}

func TestStaleLeaseRejected(t *testing.T) {
	e, clk, _ := newEngine(t)
	run := start(t, e, "w")
	task := pollWF(t, e)
	n := len(history(t, e, run))
	wantCode(t, tryComplete(e, "bogus"), CodeStaleLease)
	clk.Advance(e.cfg.WorkflowTaskTimeout + time.Millisecond)
	wantCode(t, tryComplete(e, task.LeaseToken), CodeStaleLease)
	if got := len(history(t, e, run)); got != n {
		t.Fatalf("history grew %d -> %d", n, got)
	}
}

func TestOptimisticConcurrencyConflict(t *testing.T) {
	e, _, _ := newEngine(t)
	run := start(t, e, "w")
	task := pollWF(t, e)
	n := len(history(t, e, run))
	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i := range errs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = tryComplete(e, task.LeaseToken, wire.Command{Type: wire.ScheduleActivity, Seq: 1, ActivityType: "A" + string(rune('0'+i))})
		}()
	}
	wg.Wait()
	ok := 0
	for _, err := range errs {
		if err == nil {
			ok++
			continue
		}
		var ce *Error
		if !errors.As(err, &ce) || (ce.Code != CodeStaleLease && ce.Code != CodeConflict) {
			t.Fatalf("loser error %v", err)
		}
	}
	if ok != 1 {
		t.Fatalf("%d completions succeeded", ok)
	}
	if got := len(history(t, e, run)); got != n+2 {
		t.Fatalf("history grew by %d, want 2", got-n)
	}
}

func TestFailWorkflowTaskReschedulesWithBackoff(t *testing.T) {
	e, clk, st := newEngine(t)
	run := start(t, e, "w")
	for attempt, wantDelay := range []int64{1000, 2000} {
		task := pollWF(t, e)
		if task.Attempt != attempt+1 {
			t.Fatalf("attempt %d, want %d", task.Attempt, attempt+1)
		}
		before := len(history(t, e, run))
		if err := e.FailWorkflowTask(context.Background(), wire.FailWorkflowTaskRequest{LeaseToken: task.LeaseToken, Cause: "nondeterminism", Message: "x"}); err != nil {
			t.Fatal(err)
		}
		h := history(t, e, run)
		if !eqTypes(types(h[before:]), []wire.EventType{wire.WorkflowTaskFailed, wire.WorkflowTaskScheduled}) {
			t.Fatalf("appended %v", types(h[before:]))
		}
		if a := attrsOf[wire.WorkflowTaskScheduledAttrs](t, h[len(h)-1]); a.Attempt != attempt+2 {
			t.Fatalf("scheduled attempt %d", a.Attempt)
		}
		var vis int64
		var att int
		st.DB.QueryRow(`SELECT visible_at, attempt FROM tasks WHERE run_id=? AND kind='workflow'`, run).Scan(&vis, &att)
		if want := clk.Now().UnixMilli() + wantDelay; vis != want || att != attempt+2 {
			t.Fatalf("visible_at %d want %d, attempt %d", vis, want, att)
		}
		if got, _ := e.TryPollWorkflowTask(context.Background(), wire.PollRequest{Queue: "q"}); got != nil {
			t.Fatal("task visible before backoff elapsed")
		}
		clk.Advance(time.Duration(wantDelay) * time.Millisecond)
	}
}

func TestFailWorkflowTaskBackoffCaps(t *testing.T) {
	if d := wftBackoff(1); d != time.Second {
		t.Fatalf("attempt 1: %v", d)
	}
	if d := wftBackoff(10); d != 30*time.Second {
		t.Fatalf("attempt 10: %v", d)
	}
	if d := wftBackoff(200); d != 30*time.Second {
		t.Fatalf("attempt 200: %v", d)
	}
}

func TestFailWorkflowTaskRejectsUnknownCause(t *testing.T) {
	e, _, _ := newEngine(t)
	start(t, e, "w")
	task := pollWF(t, e)
	err := e.FailWorkflowTask(context.Background(), wire.FailWorkflowTaskRequest{LeaseToken: task.LeaseToken, Cause: "whatever"})
	wantCode(t, err, CodeInvalid)
}

func TestPollWorkflowTaskWakesOnStart(t *testing.T) {
	e, _, _ := newEngine(t, func(c *Config) { c.PollTimeout = 5 * time.Second })
	done := make(chan *wire.WorkflowTask, 1)
	go func() {
		task, _ := e.PollWorkflowTask(context.Background(), wire.PollRequest{Queue: "q", Identity: "i"})
		done <- task
	}()
	time.Sleep(50 * time.Millisecond)
	start(t, e, "w")
	select {
	case task := <-done:
		if task == nil {
			t.Fatal("poll returned nil")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("poll did not wake")
	}
}

func TestPollTimeoutReturnsNil(t *testing.T) {
	e, _, _ := newEngine(t)
	task, err := e.PollWorkflowTask(context.Background(), wire.PollRequest{Queue: "q"})
	if task != nil || err != nil {
		t.Fatalf("%v %v", task, err)
	}
}
