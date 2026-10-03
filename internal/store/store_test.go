package store

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
)

func openTemp(t *testing.T) *Store {
	t.Helper()
	s, err := Open(filepath.Join(t.TempDir(), "wf.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func pragma(t *testing.T, s *Store, name string) string {
	t.Helper()
	var v string
	if err := s.DB.QueryRow("PRAGMA " + name).Scan(&v); err != nil {
		t.Fatal(err)
	}
	return v
}

func insertRun(tx *Tx, run, wid, status string) error {
	_, err := tx.tx.Exec(`INSERT INTO workflows(run_id,workflow_id,workflow_type,task_queue,status,next_event_id,created_at)
		VALUES(?,?,?,?,?,1,0)`, run, wid, "W", "q", status)
	return err
}

func TestOpenAppliesPragmas(t *testing.T) {
	s := openTemp(t)
	if v := pragma(t, s, "journal_mode"); v != "wal" {
		t.Fatalf("journal_mode %q", v)
	}
	if v := pragma(t, s, "synchronous"); v != "2" {
		t.Fatalf("synchronous %q", v)
	}
	if v := pragma(t, s, "user_version"); v != "1" {
		t.Fatalf("user_version %q", v)
	}
}

func TestOpenIsIdempotent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wf.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	s.Close()
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if v := pragma(t, s, "user_version"); v != "1" {
		t.Fatalf("user_version %q", v)
	}
}

func TestHistoryTriggerBlocksMutation(t *testing.T) {
	s := openTemp(t)
	_, err := s.DB.Exec(`INSERT INTO history_events(run_id,event_id,event_type,attrs,ts) VALUES('r',1,'X','{}',0)`)
	if err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{
		`UPDATE history_events SET attrs='{"a":1}' WHERE run_id='r'`,
		`DELETE FROM history_events WHERE run_id='r'`,
	} {
		_, err := s.DB.Exec(q)
		if err == nil || !strings.Contains(err.Error(), "history is append-only") {
			t.Fatalf("%s: err %v", q, err)
		}
	}
}

func TestOneOpenRunIndex(t *testing.T) {
	s := openTemp(t)
	ctx := context.Background()
	if err := s.WithTx(ctx, "t", func(tx *Tx) error { return insertRun(tx, "r1", "w", "running") }); err != nil {
		t.Fatal(err)
	}
	if err := s.WithTx(ctx, "t", func(tx *Tx) error { return insertRun(tx, "r2", "w", "running") }); err == nil {
		t.Fatal("second open run accepted")
	}
	if _, err := s.DB.Exec(`UPDATE workflows SET status='completed' WHERE run_id='r1'`); err != nil {
		t.Fatal(err)
	}
	if err := s.WithTx(ctx, "t", func(tx *Tx) error { return insertRun(tx, "r2", "w", "running") }); err != nil {
		t.Fatalf("insert after close: %v", err)
	}
}

func TestOneWorkflowTaskPerRun(t *testing.T) {
	s := openTemp(t)
	ins := `INSERT INTO tasks(queue,kind,run_id,scheduled_event_id,timeout_ms,visible_at) VALUES('q','workflow','r',?,1000,0)`
	if _, err := s.DB.Exec(ins, 2); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DB.Exec(ins, 5); err == nil {
		t.Fatal("second workflow task accepted")
	}
}

func TestWithTxRollsBackOnError(t *testing.T) {
	s := openTemp(t)
	err := s.WithTx(context.Background(), "t", func(tx *Tx) error {
		if err := insertRun(tx, "r1", "w", "running"); err != nil {
			return err
		}
		return context.Canceled
	})
	if err == nil {
		t.Fatal("expected error")
	}
	var n int
	s.DB.QueryRow(`SELECT count(*) FROM workflows`).Scan(&n)
	if n != 0 {
		t.Fatalf("row survived rollback: %d", n)
	}
}

func recoverPanic(fn func()) (r any) {
	defer func() { r = recover() }()
	fn()
	return nil
}

func armFailpoint(t *testing.T, point string) {
	t.Helper()
	old := Failpoint
	Failpoint = func(p string) {
		if p == point {
			panic("boom")
		}
	}
	t.Cleanup(func() { Failpoint = old })
}

func countRuns(s *Store) int {
	var n int
	s.DB.QueryRow(`SELECT count(*) FROM workflows`).Scan(&n)
	return n
}

func TestFailpointBeforeCommitRollsBack(t *testing.T) {
	s := openTemp(t)
	armFailpoint(t, "t1/before_commit")
	r := recoverPanic(func() {
		s.WithTx(context.Background(), "t1", func(tx *Tx) error { return insertRun(tx, "r1", "w", "running") })
	})
	if r != "boom" {
		t.Fatalf("recovered %v", r)
	}
	if n := countRuns(s); n != 0 {
		t.Fatalf("rows %d", n)
	}
}

func TestFailpointAfterCommitKeepsWrite(t *testing.T) {
	s := openTemp(t)
	armFailpoint(t, "t1/after_commit")
	r := recoverPanic(func() {
		s.WithTx(context.Background(), "t1", func(tx *Tx) error { return insertRun(tx, "r1", "w", "running") })
	})
	if r != "boom" {
		t.Fatalf("recovered %v", r)
	}
	if n := countRuns(s); n != 1 {
		t.Fatalf("rows %d", n)
	}
}
