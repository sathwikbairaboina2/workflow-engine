package core

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/sathwikbairaboina2/workflow-engine/internal/store"
	"github.com/sathwikbairaboina2/workflow-engine/wire"
)

func scheduleActivity(t *testing.T, e *Engine, policy wire.RetryPolicy) string {
	t.Helper()
	run := start(t, e, "w")
	task := pollWF(t, e)
	complete(t, e, task.LeaseToken, wire.Command{Type: wire.ScheduleActivity, Seq: 1, ActivityType: "A",
		Input: wire.MustEncode(map[string]int{"x": 1}), RetryPolicy: &policy, StartToCloseMS: 5000})
	return run
}

func tryPollAct(t *testing.T, e *Engine) *wire.ActivityTask {
	t.Helper()
	a, err := e.TryPollActivityTask(context.Background(), wire.PollRequest{Queue: "q", Identity: "worker-1"})
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func pollAct(t *testing.T, e *Engine) *wire.ActivityTask {
	t.Helper()
	a := tryPollAct(t, e)
	if a == nil {
		t.Fatal("no activity task available")
	}
	return a
}

func failAct(e *Engine, tok string, f wire.Failure) error {
	return e.FailActivityTask(context.Background(), wire.FailActivityTaskRequest{LeaseToken: tok, Failure: f})
}

func completeAct(e *Engine, tok string, result any) error {
	return e.CompleteActivityTask(context.Background(), wire.CompleteActivityTaskRequest{LeaseToken: tok, Identity: "worker-1", Result: wire.MustEncode(result)})
}

func activityRow(t *testing.T, st *store.Store) (attempt int, visible int64, token *string) {
	t.Helper()
	var tok *string
	if err := st.DB.QueryRow(`SELECT attempt, visible_at, lease_token FROM tasks WHERE kind='activity'`).Scan(&attempt, &visible, &tok); err != nil {
		t.Fatal(err)
	}
	return attempt, visible, tok
}

func TestActivityCompleteAppendsAndSchedulesWFT(t *testing.T) {
	e, clk, st := newEngine(t)
	run := scheduleActivity(t, e, wire.RetryPolicy{})
	a := tryPollAct(t, e)
	if a == nil || a.ScheduledEventID != 5 || a.Attempt != 1 || a.DeadlineMS != clk.Now().UnixMilli()+5000 || a.ActivityType != "A" || a.RunID != run || a.WorkflowID != "w" {
		t.Fatalf("task %+v", a)
	}
	var in map[string]int
	if err := a.Input.Decode(&in); err != nil || in["x"] != 1 {
		t.Fatalf("input %v %v", in, err)
	}
	before := len(history(t, e, run))
	if err := completeAct(e, a.LeaseToken, map[string]int{"v": 7}); err != nil {
		t.Fatal(err)
	}
	h := history(t, e, run)
	if !eqTypes(types(h[before:]), []wire.EventType{wire.ActivityTaskStarted, wire.ActivityTaskCompleted, wire.WorkflowTaskScheduled}) {
		t.Fatalf("appended %v", types(h[before:]))
	}
	s := attrsOf[wire.ActivityTaskStartedAttrs](t, h[before])
	if s.ScheduledEventID != 5 || s.Attempt != 1 || s.Identity != "worker-1" {
		t.Fatalf("started %+v", s)
	}
	c := attrsOf[wire.ActivityTaskCompletedAttrs](t, h[before+1])
	var res map[string]int
	c.Result.Decode(&res)
	if c.ScheduledEventID != 5 || res["v"] != 7 {
		t.Fatalf("completed %+v", c)
	}
	var wf, act int
	st.DB.QueryRow(`SELECT count(*) FROM tasks WHERE kind='workflow'`).Scan(&wf)
	st.DB.QueryRow(`SELECT count(*) FROM tasks WHERE kind='activity'`).Scan(&act)
	if wf != 1 || act != 0 {
		t.Fatalf("workflow tasks %d activity tasks %d", wf, act)
	}
}

func TestActivityRetryThenComplete(t *testing.T) {
	e, clk, st := newEngine(t)
	run := scheduleActivity(t, e, wire.RetryPolicy{InitialMS: 100, Backoff: 2, MaxAttempts: 3})
	n := len(history(t, e, run))
	a := pollAct(t, e)
	if err := failAct(e, a.LeaseToken, wire.Failure{Type: "Boom"}); err != nil {
		t.Fatal(err)
	}
	if len(history(t, e, run)) != n {
		t.Fatal("retry changed history")
	}
	att, vis, tok := activityRow(t, st)
	if att != 2 || vis != clk.Now().UnixMilli()+100 || tok != nil {
		t.Fatalf("after fail 1: attempt=%d visible=%d token=%v", att, vis, tok)
	}
	if tryPollAct(t, e) != nil {
		t.Fatal("retry visible too early")
	}
	clk.Advance(100 * time.Millisecond)
	a = pollAct(t, e)
	if a.Attempt != 2 {
		t.Fatalf("attempt %d", a.Attempt)
	}
	failAct(e, a.LeaseToken, wire.Failure{Type: "Boom"})
	att, vis, _ = activityRow(t, st)
	if att != 3 || vis != clk.Now().UnixMilli()+200 {
		t.Fatalf("after fail 2: attempt=%d visible=%d", att, vis)
	}
	clk.Advance(200 * time.Millisecond)
	a = pollAct(t, e)
	if err := completeAct(e, a.LeaseToken, 1); err != nil {
		t.Fatal(err)
	}
	h := history(t, e, run)
	s := attrsOf[wire.ActivityTaskStartedAttrs](t, h[n])
	if h[n].Type != wire.ActivityTaskStarted || s.Attempt != 3 {
		t.Fatalf("started %+v", s)
	}
}

func TestActivityRetriesExhausted(t *testing.T) {
	e, clk, _ := newEngine(t)
	run := scheduleActivity(t, e, wire.RetryPolicy{InitialMS: 10, MaxAttempts: 2})
	n := len(history(t, e, run))
	a := pollAct(t, e)
	failAct(e, a.LeaseToken, wire.Failure{Type: "Boom"})
	clk.Advance(10 * time.Millisecond)
	a = pollAct(t, e)
	if err := failAct(e, a.LeaseToken, wire.Failure{Type: "Boom", Message: "second"}); err != nil {
		t.Fatal(err)
	}
	h := history(t, e, run)
	if !eqTypes(types(h[n:]), []wire.EventType{wire.ActivityTaskStarted, wire.ActivityTaskFailed, wire.WorkflowTaskScheduled}) {
		t.Fatalf("appended %v", types(h[n:]))
	}
	f := attrsOf[wire.ActivityTaskFailedAttrs](t, h[n+1])
	if f.Attempts != 2 || f.Failure.Type != "Boom" || f.Failure.Message != "second" || f.ScheduledEventID != 5 {
		t.Fatalf("failed %+v", f)
	}
}

func TestActivityNonRetryable(t *testing.T) {
	e, _, _ := newEngine(t)
	run := scheduleActivity(t, e, wire.RetryPolicy{InitialMS: 10})
	n := len(history(t, e, run))
	a := pollAct(t, e)
	failAct(e, a.LeaseToken, wire.Failure{Type: "Fatal", NonRetryable: true})
	h := history(t, e, run)
	f := attrsOf[wire.ActivityTaskFailedAttrs](t, h[n+1])
	if h[n+1].Type != wire.ActivityTaskFailed || f.Attempts != 1 {
		t.Fatalf("appended %v %+v", types(h[n:]), f)
	}
}

func TestActivityNonRetryableByPolicyType(t *testing.T) {
	e, _, _ := newEngine(t)
	run := scheduleActivity(t, e, wire.RetryPolicy{InitialMS: 10, NonRetryable: []string{"Fatal"}})
	n := len(history(t, e, run))
	a := pollAct(t, e)
	failAct(e, a.LeaseToken, wire.Failure{Type: "Fatal"})
	if h := history(t, e, run); h[n+1].Type != wire.ActivityTaskFailed {
		t.Fatalf("appended %v", types(h[n:]))
	}
}

func TestActivityLeaseExpiryRetries(t *testing.T) {
	e, clk, st := newEngine(t)
	run := scheduleActivity(t, e, wire.RetryPolicy{InitialMS: 10})
	n := len(history(t, e, run))
	pollAct(t, e)
	clk.Advance(5001 * time.Millisecond)
	got, err := e.ReapExpiredLeases(context.Background())
	if err != nil || got != 1 {
		t.Fatalf("reaped %d err %v", got, err)
	}
	att, _, tok := activityRow(t, st)
	if att != 2 || tok != nil {
		t.Fatalf("attempt=%d token=%v", att, tok)
	}
	if len(history(t, e, run)) != n {
		t.Fatal("history changed")
	}
	if got, _ := e.ReapExpiredLeases(context.Background()); got != 0 {
		t.Fatalf("second reap %d", got)
	}
}

func TestActivityLeaseExpiryFinalTimesOut(t *testing.T) {
	e, clk, _ := newEngine(t)
	run := scheduleActivity(t, e, wire.RetryPolicy{InitialMS: 10, MaxAttempts: 1})
	n := len(history(t, e, run))
	pollAct(t, e)
	clk.Advance(5001 * time.Millisecond)
	if got, err := e.ReapExpiredLeases(context.Background()); err != nil || got != 1 {
		t.Fatalf("reaped %d err %v", got, err)
	}
	h := history(t, e, run)
	if !eqTypes(types(h[n:]), []wire.EventType{wire.ActivityTaskStarted, wire.ActivityTaskTimedOut, wire.WorkflowTaskScheduled}) {
		t.Fatalf("appended %v", types(h[n:]))
	}
	if a := attrsOf[wire.ActivityTaskTimedOutAttrs](t, h[n+1]); a.Attempts != 1 || a.ScheduledEventID != 5 {
		t.Fatalf("timed out %+v", a)
	}
}

func TestZombieActivityCompletionRejected(t *testing.T) {
	e, clk, _ := newEngine(t)
	run := scheduleActivity(t, e, wire.RetryPolicy{InitialMS: 10})
	n := len(history(t, e, run))
	a := pollAct(t, e)
	clk.Advance(5001 * time.Millisecond)
	e.ReapExpiredLeases(context.Background())
	clk.Advance(10 * time.Millisecond)
	b := pollAct(t, e)
	if b.LeaseToken == a.LeaseToken || b.Attempt != 2 {
		t.Fatalf("second lease %+v", b)
	}
	wantCode(t, completeAct(e, a.LeaseToken, 1), CodeStaleLease)
	wantCode(t, failAct(e, a.LeaseToken, wire.Failure{Type: "X"}), CodeStaleLease)
	if len(history(t, e, run)) != n {
		t.Fatal("zombie changed history")
	}
	if err := completeAct(e, b.LeaseToken, 2); err != nil {
		t.Fatal(err)
	}
}

func TestWorkflowTaskTimeout(t *testing.T) {
	e, clk, st := newEngine(t)
	run := start(t, e, "w")
	task := pollWF(t, e)
	n := len(history(t, e, run))
	clk.Advance(e.cfg.WorkflowTaskTimeout + time.Millisecond)
	if got, err := e.ReapExpiredLeases(context.Background()); err != nil || got != 1 {
		t.Fatalf("reaped %d err %v", got, err)
	}
	h := history(t, e, run)
	if !eqTypes(types(h[n:]), []wire.EventType{wire.WorkflowTaskFailed, wire.WorkflowTaskScheduled}) {
		t.Fatalf("appended %v", types(h[n:]))
	}
	if f := attrsOf[wire.WorkflowTaskFailedAttrs](t, h[n]); f.Cause != "timeout" {
		t.Fatalf("cause %q", f.Cause)
	}
	if s := attrsOf[wire.WorkflowTaskScheduledAttrs](t, h[n+1]); s.Attempt != 2 {
		t.Fatalf("attempt %d", s.Attempt)
	}
	var vis int64
	st.DB.QueryRow(`SELECT visible_at FROM tasks WHERE kind='workflow'`).Scan(&vis)
	if vis != clk.Now().UnixMilli() {
		t.Fatalf("visible_at %d, want now", vis)
	}
	wantCode(t, tryComplete(e, task.LeaseToken), CodeStaleLease)
	if again := pollWF(t, e); again.Attempt != 2 {
		t.Fatalf("attempt %d", again.Attempt)
	}
}

func TestActivityResultTooLarge(t *testing.T) {
	e, _, _ := newEngine(t, func(c *Config) { c.MaxPayloadBytes = 1024 })
	run := scheduleActivity(t, e, wire.RetryPolicy{InitialMS: 10})
	n := len(history(t, e, run))
	a := pollAct(t, e)
	if err := completeAct(e, a.LeaseToken, strings.Repeat("x", 2000)); err != nil {
		t.Fatal(err)
	}
	h := history(t, e, run)
	f := attrsOf[wire.ActivityTaskFailedAttrs](t, h[n+1])
	if h[n+1].Type != wire.ActivityTaskFailed || f.Failure.Type != "PayloadTooLarge" || !f.Failure.NonRetryable {
		t.Fatalf("appended %v %+v", types(h[n:]), f)
	}
}

func TestActivityCompletionAfterRunClosed(t *testing.T) {
	e, _, st := newEngine(t)
	run := scheduleActivity(t, e, wire.RetryPolicy{})
	a := pollAct(t, e)
	if err := e.SignalWorkflow(context.Background(), wire.SignalRequest{WorkflowID: "w", Name: "s"}); err != nil {
		t.Fatal(err)
	}
	task := pollWF(t, e)
	complete(t, e, task.LeaseToken, wire.Command{Type: wire.CompleteWorkflow})
	if countRows(t, st, "tasks") != 0 {
		t.Fatal("tasks left after close")
	}
	n := len(history(t, e, run))
	wantCode(t, completeAct(e, a.LeaseToken, 1), CodeStaleLease)
	if len(history(t, e, run)) != n {
		t.Fatal("history changed after close")
	}
}
