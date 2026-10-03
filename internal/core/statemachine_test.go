package core

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/sathwikbairaboina2/workflow-engine/internal/clock"
	"github.com/sathwikbairaboina2/workflow-engine/internal/store"
	"github.com/sathwikbairaboina2/workflow-engine/wire"
	"pgregory.net/rapid"
)

// machine is the model for TestHistoryDenseAppendOnly: it drives the engine through random legal and
// illegal-but-expected operations and checks after every step that history only ever grows by appending.
type machine struct {
	t    *rapid.T
	e    *Engine
	clk  *clock.Fake
	st   *store.Store
	seq  map[string]int64        // next command seq per run
	seen map[string][]wire.Event // history observed so far per run
}

func (m *machine) okErr(err error, allowed ...string) {
	if err == nil {
		return
	}
	var ce *Error
	if errors.As(err, &ce) {
		for _, c := range allowed {
			if ce.Code == c {
				return
			}
		}
	}
	m.t.Fatalf("unexpected error: %v", err)
}

func (m *machine) start(t *rapid.T) {
	id := fmt.Sprintf("wf%d", rapid.IntRange(0, 4).Draw(t, "id"))
	_, err := m.e.StartWorkflow(context.Background(), wire.StartRequest{WorkflowID: id, WorkflowType: "W", TaskQueue: "q"})
	m.okErr(err, CodeAlreadyStarted)
}

func (m *machine) genCommands(t *rapid.T, runID string) []wire.Command {
	var cmds []wire.Command
	next := func() int64 { m.seq[runID]++; return m.seq[runID] }
	for i := rapid.IntRange(0, 2).Draw(t, "activities"); i > 0; i-- {
		cmds = append(cmds, wire.Command{Type: wire.ScheduleActivity, Seq: next(), ActivityType: "A", StartToCloseMS: 2000,
			RetryPolicy: &wire.RetryPolicy{InitialMS: 50, MaxAttempts: rapid.IntRange(0, 3).Draw(t, "maxAttempts")}})
	}
	if rapid.Bool().Draw(t, "timer") {
		cmds = append(cmds, wire.Command{Type: wire.StartTimer, Seq: next(), DurationMS: rapid.Int64Range(0, 2000).Draw(t, "dur")})
	}
	if rapid.Bool().Draw(t, "marker") {
		cmds = append(cmds, wire.Command{Type: wire.RecordMarker, Seq: next(), MarkerKind: "side_effect", Value: wire.MustEncode(1)})
	}
	switch rapid.IntRange(0, 5).Draw(t, "close") {
	case 0:
		cmds = append(cmds, wire.Command{Type: wire.CompleteWorkflow})
	case 1:
		cmds = append(cmds, wire.Command{Type: wire.FailWorkflow, Failure: &wire.Failure{Type: "X"}})
	case 2:
		cmds = append(cmds, wire.Command{Type: wire.CancelWorkflow})
	}
	return cmds
}

func (m *machine) pollAndComplete(t *rapid.T) {
	task, err := m.e.TryPollWorkflowTask(context.Background(), wire.PollRequest{Queue: "q", Identity: "m"})
	m.okErr(err)
	if task == nil {
		return
	}
	err = m.e.CompleteWorkflowTask(context.Background(), wire.CompleteWorkflowTaskRequest{LeaseToken: task.LeaseToken, Commands: m.genCommands(t, task.RunID)})
	m.okErr(err, CodeStaleLease, CodeRunClosed)
}

func (m *machine) pollAndFailWF(t *rapid.T) {
	task, err := m.e.TryPollWorkflowTask(context.Background(), wire.PollRequest{Queue: "q", Identity: "m"})
	m.okErr(err)
	if task == nil {
		return
	}
	cause := rapid.SampledFrom(failCauses).Draw(t, "cause")
	m.okErr(m.e.FailWorkflowTask(context.Background(), wire.FailWorkflowTaskRequest{LeaseToken: task.LeaseToken, Cause: cause}), CodeStaleLease, CodeRunClosed)
}

func (m *machine) pollAbandonWF(t *rapid.T) {
	_, err := m.e.TryPollWorkflowTask(context.Background(), wire.PollRequest{Queue: "q", Identity: "m"})
	m.okErr(err)
}

func (m *machine) pollActivity(t *rapid.T) {
	task, err := m.e.TryPollActivityTask(context.Background(), wire.PollRequest{Queue: "q", Identity: "m"})
	m.okErr(err)
	if task == nil {
		return
	}
	switch rapid.IntRange(0, 3).Draw(t, "outcome") {
	case 0:
		m.okErr(m.e.CompleteActivityTask(context.Background(), wire.CompleteActivityTaskRequest{LeaseToken: task.LeaseToken, Result: wire.MustEncode(1)}), CodeStaleLease, CodeRunClosed)
	case 1:
		m.okErr(m.e.FailActivityTask(context.Background(), wire.FailActivityTaskRequest{LeaseToken: task.LeaseToken, Failure: wire.Failure{Type: "E"}}), CodeStaleLease, CodeRunClosed)
	case 2:
		m.okErr(m.e.FailActivityTask(context.Background(), wire.FailActivityTaskRequest{LeaseToken: task.LeaseToken, Failure: wire.Failure{Type: "E", NonRetryable: true}}), CodeStaleLease, CodeRunClosed)
	default: // abandoned: the reaper has to deal with it
	}
}

func (m *machine) signal(t *rapid.T) {
	id := fmt.Sprintf("wf%d", rapid.IntRange(0, 4).Draw(t, "id"))
	m.okErr(m.e.SignalWorkflow(context.Background(), wire.SignalRequest{WorkflowID: id, Name: "s"}), CodeNotFound, CodeRunClosed)
}

func (m *machine) cancel(t *rapid.T) {
	id := fmt.Sprintf("wf%d", rapid.IntRange(0, 4).Draw(t, "id"))
	m.okErr(m.e.CancelWorkflow(context.Background(), wire.CancelRequest{WorkflowID: id}), CodeNotFound, CodeRunClosed)
}

func (m *machine) advance(t *rapid.T) {
	m.clk.Advance(time.Duration(rapid.IntRange(0, 3000).Draw(t, "ms")) * time.Millisecond)
	_, err := m.e.FireDueTimers(context.Background())
	m.okErr(err)
	_, err = m.e.ReapExpiredLeases(context.Background())
	m.okErr(err)
}

// check runs after every action.
func (m *machine) check(t *rapid.T) {
	v, err := store.CheckConsistency(context.Background(), m.st.DB)
	if err != nil {
		t.Fatal(err)
	}
	if len(v) > 0 {
		t.Fatalf("consistency violations: %v", v)
	}
	rows, err := m.st.DB.Query(`SELECT run_id FROM workflows`)
	if err != nil {
		t.Fatal(err)
	}
	var runs []string
	for rows.Next() {
		var id string
		rows.Scan(&id)
		runs = append(runs, id)
	}
	rows.Close()
	for _, run := range runs {
		var h []wire.Event
		if err := m.st.View(context.Background(), func(tx *store.Tx) error {
			var err error
			h, err = tx.LoadHistory(run, 1, 1<<20)
			return err
		}); err != nil {
			t.Fatal(err)
		}
		for i, ev := range h {
			if ev.EventID != int64(i+1) {
				t.Fatalf("run %s: history not dense at %d", run, i)
			}
		}
		prev := m.seen[run]
		if len(h) < len(prev) {
			t.Fatalf("run %s: history shrank from %d to %d", run, len(prev), len(h))
		}
		for i, old := range prev {
			if h[i].Type != old.Type || h[i].Time != old.Time || !bytes.Equal(h[i].Attrs, old.Attrs) {
				t.Fatalf("run %s: event %d changed after being observed", run, i+1)
			}
		}
		m.seen[run] = h
	}
}

// limitRapidChecks caps the number of generated runs unless -rapid.checks was given explicitly: each run
// opens a database and drives dozens of transitions, so the default 100 takes about a minute under -race.
func limitRapidChecks(n string) {
	explicit := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == "rapid.checks" {
			explicit = true
		}
	})
	if !explicit {
		flag.Set("rapid.checks", n)
	}
}

func TestHistoryDenseAppendOnly(t *testing.T) {
	limitRapidChecks("40")
	rapid.Check(t, func(rt *rapid.T) {
		st, err := store.Open(filepath.Join(t.TempDir(), fmt.Sprintf("sm-%d.db", time.Now().UnixNano())))
		if err != nil {
			rt.Fatal(err)
		}
		defer st.Close()
		// The model only needs logical atomicity, not durability: one connection with fsync off keeps
		// thousands of generated transitions fast. Durable commits are exercised by TestCrashPointConsistency.
		st.DB.SetMaxOpenConns(1)
		if _, err := st.DB.Exec(`PRAGMA synchronous=OFF`); err != nil {
			rt.Fatal(err)
		}
		clk := clock.NewFake(t0)
		m := &machine{t: rt, clk: clk, st: st, e: New(st, clk, Config{PollTimeout: time.Millisecond, WorkflowTaskTimeout: 3 * time.Second}),
			seq: map[string]int64{}, seen: map[string][]wire.Event{}}
		rt.Repeat(map[string]func(*rapid.T){
			"start":           m.start,
			"pollAndComplete": m.pollAndComplete,
			"pollAndFailWF":   m.pollAndFailWF,
			"pollAbandonWF":   m.pollAbandonWF,
			"pollActivity":    m.pollActivity,
			"signal":          m.signal,
			"cancel":          m.cancel,
			"advance":         m.advance,
			"":                m.check,
		})
	})
}
