package core

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/sathwikbairaboina2/workflow-engine/wire"
)

func TestStartAppendsStartedAndScheduled(t *testing.T) {
	e, _, st := newEngine(t)
	run := start(t, e, "w1")
	h := history(t, e, run)
	if !eqTypes(types(h), []wire.EventType{wire.WorkflowExecutionStarted, wire.WorkflowTaskScheduled}) {
		t.Fatalf("types %v", types(h))
	}
	if h[0].EventID != 1 || h[1].EventID != 2 {
		t.Fatalf("ids %d %d", h[0].EventID, h[1].EventID)
	}
	var sched int64
	st.DB.QueryRow(`SELECT scheduled_event_id FROM tasks WHERE run_id=? AND kind='workflow'`, run).Scan(&sched)
	if sched != 2 || countRows(t, st, "tasks") != 1 {
		t.Fatalf("task row: scheduled=%d", sched)
	}
	d, err := e.Describe(context.Background(), wire.DescribeRequest{WorkflowID: "w1"})
	if err != nil {
		t.Fatal(err)
	}
	if d.Status != "running" || d.HistoryLength != 2 || d.RunID != run {
		t.Fatalf("describe %+v", d)
	}
}

func TestSingleOpenRunPerWorkflowID(t *testing.T) {
	e, _, st := newEngine(t)
	var wg sync.WaitGroup
	var ok atomic.Int32
	var mu sync.Mutex
	var winner string
	var losers []*Error
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			run, err := e.StartWorkflow(context.Background(), wire.StartRequest{WorkflowID: "same", WorkflowType: "W", TaskQueue: "q"})
			mu.Lock()
			defer mu.Unlock()
			if err == nil {
				ok.Add(1)
				winner = run
				return
			}
			var ce *Error
			if !errors.As(err, &ce) {
				t.Errorf("unexpected error %v", err)
				return
			}
			losers = append(losers, ce)
		}()
	}
	wg.Wait()
	if ok.Load() != 1 || len(losers) != 49 {
		t.Fatalf("ok=%d losers=%d", ok.Load(), len(losers))
	}
	for _, l := range losers {
		if l.Code != CodeAlreadyStarted || l.RunID != winner {
			t.Fatalf("loser %+v winner %s", l, winner)
		}
	}
	if n := countRows(t, st, "workflows"); n != 1 {
		t.Fatalf("runs %d", n)
	}
}

func TestStartValidation(t *testing.T) {
	e, _, _ := newEngine(t)
	for _, r := range []wire.StartRequest{
		{WorkflowType: "W", TaskQueue: "q"},
		{WorkflowID: "a", TaskQueue: "q"},
		{WorkflowID: "a", WorkflowType: "W"},
	} {
		_, err := e.StartWorkflow(context.Background(), r)
		var ce *Error
		if !errors.As(err, &ce) || ce.Code != CodeInvalid {
			t.Errorf("%+v: err %v", r, err)
		}
	}
}

func TestPayloadLimit(t *testing.T) {
	e, _, st := newEngine(t, func(c *Config) { c.MaxPayloadBytes = 1024 })
	_, err := e.StartWorkflow(context.Background(), wire.StartRequest{WorkflowID: "a", WorkflowType: "W", TaskQueue: "q",
		Input: wire.MustEncode(strings.Repeat("x", 2000))})
	var ce *Error
	if !errors.As(err, &ce) || ce.Code != CodeTooLarge {
		t.Fatalf("err %v", err)
	}
	if countRows(t, st, "workflows") != 0 {
		t.Fatal("run created")
	}
}

func TestDescribeNotFound(t *testing.T) {
	e, _, _ := newEngine(t)
	_, err := e.Describe(context.Background(), wire.DescribeRequest{WorkflowID: "nope"})
	var ce *Error
	if !errors.As(err, &ce) || ce.Code != CodeNotFound {
		t.Fatalf("err %v", err)
	}
}

func TestHistoryPaging(t *testing.T) {
	e, _, _ := newEngine(t)
	start(t, e, "w")
	r1, err := e.History(context.Background(), wire.HistoryRequest{WorkflowID: "w", PageSize: 1})
	if err != nil || len(r1.Events) != 1 || r1.Events[0].EventID != 1 || r1.NextEventID != 2 {
		t.Fatalf("page 1: %+v %v", r1, err)
	}
	r2, err := e.History(context.Background(), wire.HistoryRequest{WorkflowID: "w", PageSize: 1, FromEventID: r1.NextEventID})
	if err != nil || len(r2.Events) != 1 || r2.Events[0].EventID != 2 || r2.NextEventID != 0 {
		t.Fatalf("page 2: %+v %v", r2, err)
	}
}
