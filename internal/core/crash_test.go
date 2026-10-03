package core

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/sathwikbairaboina2/workflow-engine/internal/clock"
	"github.com/sathwikbairaboina2/workflow-engine/internal/store"
	"github.com/sathwikbairaboina2/workflow-engine/wire"
)

// crashEnv is an engine on a file-backed store that can be "restarted": the store is closed and reopened
// at the same path, exactly what a kill -9 followed by a restart looks like to the database.
type crashEnv struct {
	t    *testing.T
	path string
	clk  *clock.Fake
	st   *store.Store
	e    *Engine
}

func newCrashEnv(t *testing.T) *crashEnv {
	c := &crashEnv{t: t, path: filepath.Join(t.TempDir(), "wf.db"), clk: clock.NewFake(t0)}
	c.open()
	t.Cleanup(func() { c.st.Close() })
	return c
}

func (c *crashEnv) open() {
	st, err := store.Open(c.path)
	if err != nil {
		c.t.Fatal(err)
	}
	c.st = st
	c.e = New(st, c.clk, Config{PollTimeout: time.Millisecond, WorkflowTaskTimeout: 3 * time.Second})
}

func (c *crashEnv) restart() {
	c.st.Close()
	c.open()
}

func (c *crashEnv) mustConsistent(when string) {
	c.t.Helper()
	v, err := store.CheckConsistency(context.Background(), c.st.DB)
	if err != nil {
		c.t.Fatal(err)
	}
	if len(v) > 0 {
		c.t.Fatalf("%s: inconsistent: %v", when, v)
	}
}

func (c *crashEnv) startRun() {
	c.t.Helper()
	_, err := c.e.StartWorkflow(context.Background(), wire.StartRequest{WorkflowID: "w", WorkflowType: "W", TaskQueue: "q"})
	if err != nil {
		c.t.Fatal(err)
	}
}

func (c *crashEnv) pollWF() *wire.WorkflowTask {
	c.t.Helper()
	task, err := c.e.TryPollWorkflowTask(context.Background(), wire.PollRequest{Queue: "q", Identity: "c"})
	if err != nil || task == nil {
		c.t.Fatalf("poll workflow task: %v %v", task, err)
	}
	return task
}

func (c *crashEnv) completeWF(tok string, cmds ...wire.Command) {
	c.t.Helper()
	if err := c.e.CompleteWorkflowTask(context.Background(), wire.CompleteWorkflowTaskRequest{LeaseToken: tok, Commands: cmds}); err != nil {
		c.t.Fatal(err)
	}
}

func (c *crashEnv) pollAct() *wire.ActivityTask {
	c.t.Helper()
	task, err := c.e.TryPollActivityTask(context.Background(), wire.PollRequest{Queue: "q", Identity: "c"})
	if err != nil || task == nil {
		c.t.Fatalf("poll activity task: %v %v", task, err)
	}
	return task
}

func (c *crashEnv) scheduleActivity() {
	c.startRun()
	c.completeWF(c.pollWF().LeaseToken, wire.Command{Type: wire.ScheduleActivity, Seq: 1, ActivityType: "A", StartToCloseMS: 2000,
		RetryPolicy: &wire.RetryPolicy{InitialMS: 10, MaxAttempts: 2}})
}

// finish drives the run to completion regardless of where the crash happened, using only the public
// protocol: restart the run if it never started, then keep reaping, firing, working and completing.
func (c *crashEnv) finish() {
	c.t.Helper()
	ctx := context.Background()
	if _, err := c.e.StartWorkflow(ctx, wire.StartRequest{WorkflowID: "w", WorkflowType: "W", TaskQueue: "q"}); err != nil {
		var ce *Error
		if !errors.As(err, &ce) || ce.Code != CodeAlreadyStarted {
			c.t.Fatal(err)
		}
	}
	for i := 0; i < 40; i++ {
		d, err := c.e.Describe(ctx, wire.DescribeRequest{WorkflowID: "w"})
		if err != nil {
			c.t.Fatal(err)
		}
		if d.Status == "completed" {
			return
		}
		c.clk.Advance(11 * time.Second)
		// retry of an interrupted signal/cancel: an idle run needs a nudge to get its next workflow task
		c.e.SignalWorkflow(ctx, wire.SignalRequest{WorkflowID: "w", Name: "poke"})
		if _, err := c.e.ReapExpiredLeases(ctx); err != nil {
			c.t.Fatal(err)
		}
		if _, err := c.e.FireDueTimers(ctx); err != nil {
			c.t.Fatal(err)
		}
		if a, _ := c.e.TryPollActivityTask(ctx, wire.PollRequest{Queue: "q"}); a != nil {
			c.e.CompleteActivityTask(ctx, wire.CompleteActivityTaskRequest{LeaseToken: a.LeaseToken, Result: wire.MustEncode(1)})
		}
		if w, _ := c.e.TryPollWorkflowTask(ctx, wire.PollRequest{Queue: "q"}); w != nil {
			c.e.CompleteWorkflowTask(ctx, wire.CompleteWorkflowTaskRequest{LeaseToken: w.LeaseToken, Commands: []wire.Command{{Type: wire.CompleteWorkflow}}})
		}
	}
	c.t.Fatal("run did not complete after recovery")
}

func TestCrashPointConsistency(t *testing.T) {
	scenarios := []struct {
		tx    string
		setup func(c *crashEnv)
		act   func(c *crashEnv)
	}{
		{"start", func(c *crashEnv) {}, func(c *crashEnv) { c.startRun() }},
		{"wft_start", func(c *crashEnv) { c.startRun() }, func(c *crashEnv) { c.e.TryPollWorkflowTask(context.Background(), wire.PollRequest{Queue: "q"}) }},
		{"wft_complete", func(c *crashEnv) { c.startRun() }, func(c *crashEnv) {
			task := c.pollWF()
			c.completeWF(task.LeaseToken, wire.Command{Type: wire.ScheduleActivity, Seq: 1, ActivityType: "A"}, wire.Command{Type: wire.StartTimer, Seq: 2, DurationMS: 500})
		}},
		{"wft_fail", func(c *crashEnv) { c.startRun() }, func(c *crashEnv) {
			task := c.pollWF()
			c.e.FailWorkflowTask(context.Background(), wire.FailWorkflowTaskRequest{LeaseToken: task.LeaseToken, Cause: "panic"})
		}},
		{"act_start", func(c *crashEnv) { c.scheduleActivity() }, func(c *crashEnv) {
			c.e.TryPollActivityTask(context.Background(), wire.PollRequest{Queue: "q"})
		}},
		{"act_complete", func(c *crashEnv) { c.scheduleActivity() }, func(c *crashEnv) {
			a := c.pollAct()
			c.e.CompleteActivityTask(context.Background(), wire.CompleteActivityTaskRequest{LeaseToken: a.LeaseToken, Result: wire.MustEncode(1)})
		}},
		{"act_fail", func(c *crashEnv) { c.scheduleActivity() }, func(c *crashEnv) {
			a := c.pollAct()
			c.e.FailActivityTask(context.Background(), wire.FailActivityTaskRequest{LeaseToken: a.LeaseToken, Failure: wire.Failure{Type: "E", NonRetryable: true}})
		}},
		{"timer_fire", func(c *crashEnv) {
			c.startRun()
			c.completeWF(c.pollWF().LeaseToken, wire.Command{Type: wire.StartTimer, Seq: 1, DurationMS: 100})
			c.clk.Advance(time.Second)
		}, func(c *crashEnv) { c.e.FireDueTimers(context.Background()) }},
		{"reap", func(c *crashEnv) {
			c.startRun()
			c.pollWF()
			c.clk.Advance(11 * time.Second)
		}, func(c *crashEnv) { c.e.ReapExpiredLeases(context.Background()) }},
		{"signal", func(c *crashEnv) {
			c.startRun()
			c.completeWF(c.pollWF().LeaseToken)
		}, func(c *crashEnv) {
			c.e.SignalWorkflow(context.Background(), wire.SignalRequest{WorkflowID: "w", Name: "s"})
		}},
		{"cancel", func(c *crashEnv) {
			c.startRun()
			c.completeWF(c.pollWF().LeaseToken)
		}, func(c *crashEnv) {
			c.e.CancelWorkflow(context.Background(), wire.CancelRequest{WorkflowID: "w"})
		}},
	}
	for _, sc := range scenarios {
		for _, point := range []string{"before_commit", "after_commit"} {
			t.Run(sc.tx+"/"+point, func(t *testing.T) {
				c := newCrashEnv(t)
				sc.setup(c)
				c.mustConsistent("before crash")

				hits := 0
				old := store.Failpoint
				store.Failpoint = func(p string) {
					if p == sc.tx+"/"+point && hits == 0 {
						hits++
						panic("crash")
					}
				}
				var recovered any
				func() {
					defer func() { recovered = recover() }()
					sc.act(c)
				}()
				store.Failpoint = old
				if recovered != "crash" || hits != 1 {
					t.Fatalf("failpoint %s/%s did not fire (recovered=%v hits=%d)", sc.tx, point, recovered, hits)
				}

				c.restart()
				c.mustConsistent("after crash and restart")
				c.finish()
				c.mustConsistent("after recovery")
			})
		}
	}
}
