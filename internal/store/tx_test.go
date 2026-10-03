package store

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/sathwikbairaboina2/workflow-engine/wire"
)

func newRun(t *testing.T, s *Store, run, wid string) {
	t.Helper()
	err := s.WithTx(context.Background(), "t", func(tx *Tx) error {
		return tx.InsertWorkflow(Workflow{RunID: run, WorkflowID: wid, WorkflowType: "W", TaskQueue: "q",
			Status: "running", NextEventID: 1, CreatedAt: 10})
	})
	if err != nil {
		t.Fatal(err)
	}
}

func evs(n int) []NewEvent {
	out := make([]NewEvent, n)
	for i := range out {
		out[i] = NewEvent{Type: wire.MarkerRecorded, Attrs: map[string]int{"i": i}, Time: 5}
	}
	return out
}

func TestAppendEventsDense(t *testing.T) {
	s := openTemp(t)
	ctx := context.Background()
	newRun(t, s, "r", "w")
	err := s.WithTx(ctx, "t", func(tx *Tx) error {
		first, err := tx.AppendEvents("r", 1, evs(2))
		if err != nil || first != 1 {
			t.Fatalf("first=%d err=%v", first, err)
		}
		first, err = tx.AppendEvents("r", 3, evs(1))
		if err != nil || first != 3 {
			t.Fatalf("first=%d err=%v", first, err)
		}
		h, err := tx.LoadHistory("r", 1, 100)
		if err != nil {
			t.Fatal(err)
		}
		if len(h) != 3 || h[0].EventID != 1 || h[1].EventID != 2 || h[2].EventID != 3 {
			t.Fatalf("history %+v", h)
		}
		w, _, _ := tx.GetRun("r")
		if w.NextEventID != 4 {
			t.Fatalf("next %d", w.NextEventID)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestAppendEventsConflict(t *testing.T) {
	s := openTemp(t)
	ctx := context.Background()
	newRun(t, s, "r", "w")
	app := func(tx *Tx) error { _, err := tx.AppendEvents("r", 1, evs(1)); return err }
	if err := s.WithTx(ctx, "t", app); err != nil {
		t.Fatal(err)
	}
	if err := s.WithTx(ctx, "t", app); !errors.Is(err, ErrConflict) {
		t.Fatalf("err %v", err)
	}
	var n int
	s.DB.QueryRow(`SELECT count(*) FROM history_events`).Scan(&n)
	if n != 1 {
		t.Fatalf("history length %d", n)
	}
}

func TestAppendEventsConcurrent(t *testing.T) {
	s := openTemp(t)
	ctx := context.Background()
	newRun(t, s, "r", "w")
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				err := s.WithTx(ctx, "t", func(tx *Tx) error {
					w, _, err := tx.GetRun("r")
					if err != nil {
						return err
					}
					_, err = tx.AppendEvents("r", w.NextEventID, evs(1))
					return err
				})
				if errors.Is(err, ErrConflict) {
					continue
				}
				if err != nil {
					t.Error(err)
				}
				return
			}
		}()
	}
	wg.Wait()
	s.WithTx(ctx, "t", func(tx *Tx) error {
		h, _ := tx.LoadHistory("r", 1, 100)
		if len(h) != 20 {
			t.Errorf("len %d", len(h))
		}
		for i, e := range h {
			if e.EventID != int64(i+1) {
				t.Errorf("gap at %d: %d", i, e.EventID)
			}
		}
		return nil
	})
}

func TestInsertWorkflowDuplicateOpen(t *testing.T) {
	s := openTemp(t)
	newRun(t, s, "r1", "w")
	err := s.WithTx(context.Background(), "t", func(tx *Tx) error {
		return tx.InsertWorkflow(Workflow{RunID: "r2", WorkflowID: "w", WorkflowType: "W", TaskQueue: "q",
			Status: "running", NextEventID: 1, CreatedAt: 11})
	})
	if !errors.Is(err, ErrDuplicateOpenRun) {
		t.Fatalf("err %v", err)
	}
}

func TestLeaseNextTaskOrderAndVisibility(t *testing.T) {
	s := openTemp(t)
	ctx := context.Background()
	newRun(t, s, "r", "w")
	err := s.WithTx(ctx, "t", func(tx *Tx) error {
		for i, vis := range []int64{100, 50, 500} {
			if _, err := tx.InsertTask(Task{Queue: "q", Kind: "activity", RunID: "r", ScheduledEventID: int64(i + 1),
				Attempt: 1, TimeoutMS: 1000, VisibleAt: vis}); err != nil {
				return err
			}
		}
		a, ok, err := tx.LeaseNextTask("q", "activity", 200, "tokA")
		if err != nil || !ok || a.ScheduledEventID != 2 || a.LeaseToken != "tokA" || a.LeaseExpiresAt != 1200 {
			t.Fatalf("a=%+v ok=%v err=%v", a, ok, err)
		}
		b, ok, err := tx.LeaseNextTask("q", "activity", 200, "tokB")
		if err != nil || !ok || b.ScheduledEventID != 1 {
			t.Fatalf("b=%+v ok=%v err=%v", b, ok, err)
		}
		_, ok, err = tx.LeaseNextTask("q", "activity", 200, "tokC")
		if err != nil || ok {
			t.Fatalf("third lease ok=%v err=%v", ok, err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestLeaseSkipsLeased(t *testing.T) {
	s := openTemp(t)
	newRun(t, s, "r", "w")
	s.WithTx(context.Background(), "t", func(tx *Tx) error {
		tx.InsertTask(Task{Queue: "q", Kind: "activity", RunID: "r", ScheduledEventID: 1, Attempt: 1, TimeoutMS: 10, VisibleAt: 0})
		if _, ok, _ := tx.LeaseNextTask("q", "activity", 1, "a"); !ok {
			t.Fatal("first lease failed")
		}
		if _, ok, _ := tx.LeaseNextTask("q", "activity", 1, "b"); ok {
			t.Fatal("leased task returned again")
		}
		return nil
	})
}

func TestRetryTaskClearsLease(t *testing.T) {
	s := openTemp(t)
	newRun(t, s, "r", "w")
	s.WithTx(context.Background(), "t", func(tx *Tx) error {
		id, _ := tx.InsertTask(Task{Queue: "q", Kind: "activity", RunID: "r", ScheduledEventID: 1, Attempt: 1, TimeoutMS: 10, VisibleAt: 0})
		tx.LeaseNextTask("q", "activity", 1, "a")
		if err := tx.RetryTask(id, 2, 99); err != nil {
			t.Fatal(err)
		}
		task, ok, _ := tx.TaskByLease("a")
		if ok {
			t.Fatalf("lease survived: %+v", task)
		}
		got, ok, _ := tx.LeaseNextTask("q", "activity", 99, "b")
		if !ok || got.Attempt != 2 || got.VisibleAt != 99 {
			t.Fatalf("got %+v ok=%v", got, ok)
		}
		return nil
	})
}

func TestDeleteTimerOnce(t *testing.T) {
	s := openTemp(t)
	newRun(t, s, "r", "w")
	s.WithTx(context.Background(), "t", func(tx *Tx) error {
		if err := tx.InsertTimer(Timer{RunID: "r", StartedEventID: 3, FireAt: 10}); err != nil {
			t.Fatal(err)
		}
		if ok, _ := tx.DeleteTimer("r", 3); !ok {
			t.Fatal("first delete false")
		}
		if ok, _ := tx.DeleteTimer("r", 3); ok {
			t.Fatal("second delete true")
		}
		return nil
	})
}

func TestBufferedEventsFIFO(t *testing.T) {
	s := openTemp(t)
	newRun(t, s, "r", "w")
	s.WithTx(context.Background(), "t", func(tx *Tx) error {
		for i := 0; i < 3; i++ {
			if err := tx.BufferEvent("r", NewEvent{Type: wire.WorkflowExecutionSignaled, Attrs: map[string]int{"i": i}, Time: int64(i)}); err != nil {
				t.Fatal(err)
			}
		}
		got, err := tx.TakeBufferedEvents("r")
		if err != nil || len(got) != 3 {
			t.Fatalf("got %d err %v", len(got), err)
		}
		for i, e := range got {
			if e.Time != int64(i) || e.Type != wire.WorkflowExecutionSignaled {
				t.Fatalf("order broken: %+v", got)
			}
		}
		again, _ := tx.TakeBufferedEvents("r")
		if len(again) != 0 {
			t.Fatal("buffer not drained")
		}
		return nil
	})
}

func TestCloseRunAndCleanup(t *testing.T) {
	s := openTemp(t)
	newRun(t, s, "r", "w")
	s.WithTx(context.Background(), "t", func(tx *Tx) error {
		tx.InsertTask(Task{Queue: "q", Kind: "activity", RunID: "r", ScheduledEventID: 1, Attempt: 1, TimeoutMS: 10})
		tx.InsertTimer(Timer{RunID: "r", StartedEventID: 2, FireAt: 5})
		tx.BufferEvent("r", NewEvent{Type: wire.TimerFired, Attrs: map[string]int{}, Time: 1})
		if err := tx.CloseRun("r", "completed", wire.MustEncode(7), nil, 77); err != nil {
			t.Fatal(err)
		}
		if err := tx.DeleteRunTasksTimersBuffer("r"); err != nil {
			t.Fatal(err)
		}
		w, _, _ := tx.GetRun("r")
		if w.Status != "completed" || w.ClosedAt != 77 || w.Result == nil {
			t.Fatalf("run %+v", w)
		}
		if _, ok, _ := tx.GetOpenRun("w"); ok {
			t.Fatal("closed run still open")
		}
		if l, ok, _ := tx.GetLatestRun("w"); !ok || l.RunID != "r" {
			t.Fatal("latest run missing")
		}
		ts, _ := tx.TimersForRun("r")
		pa, _ := tx.PendingActivityTasks("r")
		if len(ts) != 0 || len(pa) != 0 {
			t.Fatal("rows left")
		}
		if n, _ := tx.CountRuns("completed"); n != 1 {
			t.Fatalf("count %d", n)
		}
		return nil
	})
}
