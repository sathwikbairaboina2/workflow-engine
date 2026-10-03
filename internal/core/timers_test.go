package core

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/sathwikbairaboina2/workflow-engine/wire"
)

func TestTimerNotEarly(t *testing.T) {
	e, clk, st := newEngine(t)
	run := start(t, e, "w")
	complete(t, e, pollWF(t, e).LeaseToken, wire.Command{Type: wire.StartTimer, Seq: 1, DurationMS: 1000})
	h := history(t, e, run)
	ts := h[len(h)-1]
	started := attrsOf[wire.TimerStartedAttrs](t, ts)
	if ts.Type != wire.TimerStarted || started.FireAt != clk.Now().UnixMilli()+1000 {
		t.Fatalf("timer started %+v", started)
	}
	if countRows(t, st, "timers") != 1 {
		t.Fatal("no timer row")
	}
	n := len(h)
	clk.Advance(999 * time.Millisecond)
	if got, err := e.FireDueTimers(context.Background()); err != nil || got != 0 {
		t.Fatalf("early fire %d %v", got, err)
	}
	clk.Advance(time.Millisecond)
	if got, err := e.FireDueTimers(context.Background()); err != nil || got != 1 {
		t.Fatalf("fire %d %v", got, err)
	}
	h = history(t, e, run)
	if !eqTypes(types(h[n:]), []wire.EventType{wire.TimerFired, wire.WorkflowTaskScheduled}) {
		t.Fatalf("appended %v", types(h[n:]))
	}
	if f := attrsOf[wire.TimerFiredAttrs](t, h[n]); f.StartedEventID != ts.EventID {
		t.Fatalf("fired %+v", f)
	}
	if countRows(t, st, "timers") != 0 {
		t.Fatal("timer row left")
	}
}

func TestTimerFiresOnce(t *testing.T) {
	e, clk, _ := newEngine(t)
	var runs []string
	for i := 0; i < 20; i++ {
		id := fmt.Sprintf("w%d", i)
		runs = append(runs, start(t, e, id))
		complete(t, e, pollWF(t, e).LeaseToken, wire.Command{Type: wire.StartTimer, Seq: 1, DurationMS: 100})
	}
	clk.Advance(time.Second)
	var wg sync.WaitGroup
	total := make([]int, 2)
	for g := 0; g < 2; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				n, err := e.FireDueTimers(context.Background())
				if err != nil {
					t.Error(err)
					return
				}
				if n == 0 {
					return
				}
				total[g] += n
			}
		}()
	}
	wg.Wait()
	if total[0]+total[1] != 20 {
		t.Fatalf("fired %d+%d", total[0], total[1])
	}
	fired := 0
	for _, run := range runs {
		h := history(t, e, run)
		for i, ev := range h {
			if ev.EventID != int64(i+1) {
				t.Fatalf("history not dense in %s", run)
			}
			if ev.Type == wire.TimerFired {
				fired++
			}
		}
	}
	if fired != 20 {
		t.Fatalf("%d TimerFired events", fired)
	}
}
