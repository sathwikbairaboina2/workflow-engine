package core

import (
	"context"

	"github.com/sathwikbairaboina2/workflow-engine/internal/clock"
	"github.com/sathwikbairaboina2/workflow-engine/internal/store"
	"github.com/sathwikbairaboina2/workflow-engine/wire"
)

// FireDueTimers fires every timer that is due and returns how many it fired. Deleting the timer row and
// delivering TimerFired happen in one transaction, so concurrent callers fire each timer exactly once.
func (e *Engine) FireDueTimers(ctx context.Context) (int, error) {
	var due []store.Timer
	if err := e.st.View(ctx, func(tx *store.Tx) error {
		var err error
		due, err = tx.DueTimers(clock.NowMS(e.clk), 100)
		return err
	}); err != nil {
		return 0, err
	}
	fired := 0
	for _, tm := range due {
		var did bool
		var notify string
		err := e.transition(ctx, "timer_fire", func(tx *store.Tx, now int64) (bool, error) {
			did, notify = false, ""
			deleted, err := tx.DeleteTimer(tm.RunID, tm.StartedEventID)
			if err != nil || !deleted {
				return false, err // another loop fired it first
			}
			run, ok, err := tx.GetRun(tm.RunID)
			if err != nil || !ok || run.Status != "running" {
				return deleted, err
			}
			sched, err := e.deliver(tx, run, now, []store.NewEvent{
				{Type: wire.TimerFired, Time: now, Attrs: wire.TimerFiredAttrs{StartedEventID: tm.StartedEventID}}})
			if err != nil {
				return false, err
			}
			did = true
			if sched {
				notify = wfKey(run.TaskQueue)
			}
			return true, nil
		})
		if err != nil {
			return fired, err
		}
		if did {
			fired++
		}
		if notify != "" {
			e.notify.broadcast(notify)
		}
	}
	return fired, nil
}
