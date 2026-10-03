package core

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/sathwikbairaboina2/workflow-engine/internal/clock"
	"github.com/sathwikbairaboina2/workflow-engine/internal/store"
	"github.com/sathwikbairaboina2/workflow-engine/wire"
)

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func newEngine(t *testing.T, cfg ...func(*Config)) (*Engine, *clock.Fake, *store.Store) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "wf.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	clk := clock.NewFake(t0)
	c := Config{PollTimeout: 50 * time.Millisecond, PollInterval: 10 * time.Millisecond}
	for _, f := range cfg {
		f(&c)
	}
	return New(st, clk, c), clk, st
}

func start(t *testing.T, e *Engine, id string) string {
	t.Helper()
	run, err := e.StartWorkflow(context.Background(), wire.StartRequest{
		WorkflowID: id, WorkflowType: "W", TaskQueue: "q", Input: wire.MustEncode(map[string]int{"n": 1}),
	})
	if err != nil {
		t.Fatal(err)
	}
	return run
}

func history(t *testing.T, e *Engine, runID string) []wire.Event {
	t.Helper()
	var out []wire.Event
	err := e.st.View(context.Background(), func(tx *store.Tx) error {
		var err error
		out, err = tx.LoadHistory(runID, 1, 100000)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func types(evs []wire.Event) []wire.EventType {
	out := make([]wire.EventType, len(evs))
	for i, e := range evs {
		out[i] = e.Type
	}
	return out
}

func countRows(t *testing.T, st *store.Store, table string) int {
	t.Helper()
	var n int
	if err := st.DB.QueryRow(`SELECT count(*) FROM ` + table).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func eqTypes(a, b []wire.EventType) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
