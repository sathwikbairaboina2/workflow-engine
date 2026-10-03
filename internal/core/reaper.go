package core

import (
	"context"

	"github.com/sathwikbairaboina2/workflow-engine/internal/clock"
	"github.com/sathwikbairaboina2/workflow-engine/internal/store"
	"github.com/sathwikbairaboina2/workflow-engine/wire"
)

// ReapExpiredLeases turns expired leases into failed attempts and returns how many it handled.
// A workflow task lease expiry appends WorkflowTaskFailed{timeout} and reschedules at once; an activity
// lease expiry counts as a failed attempt of the activity.
func (e *Engine) ReapExpiredLeases(ctx context.Context) (int, error) {
	var expired []store.Task
	if err := e.st.View(ctx, func(tx *store.Tx) error {
		var err error
		expired, err = tx.ExpiredTasks(clock.NowMS(e.clk), 100)
		return err
	}); err != nil {
		return 0, err
	}
	reaped := 0
	for _, t := range expired {
		var did bool
		var notify string
		err := e.transition(ctx, "reap", func(tx *store.Tx, now int64) (bool, error) {
			did, notify = false, ""
			cur, ok, err := tx.TaskByLease(t.LeaseToken)
			if err != nil || !ok || cur.TaskID != t.TaskID || cur.LeaseExpiresAt > now {
				return false, err
			}
			run, ok, err := tx.GetRun(cur.RunID)
			if err != nil {
				return false, err
			}
			did = true
			if !ok || run.Status != "running" {
				return true, tx.DeleteTask(cur.TaskID)
			}
			switch cur.Kind {
			case "workflow":
				if err := e.failWorkflowTask(tx, run, cur, now, "timeout", "workflow task lease expired", 0); err != nil {
					return false, err
				}
				notify = wfKey(run.TaskQueue)
			default:
				sched, err := e.failActivity(tx, run, cur, now, wire.Failure{Type: "StartToCloseTimeout", Message: "activity lease expired"}, "", true)
				if err != nil {
					return false, err
				}
				if sched {
					notify = wfKey(run.TaskQueue)
				}
			}
			return true, nil
		})
		if err != nil {
			return reaped, err
		}
		if did {
			reaped++
		}
		if notify != "" {
			e.notify.broadcast(notify)
		}
	}
	return reaped, nil
}
