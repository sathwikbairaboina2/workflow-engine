package core

import (
	"context"
	"time"

	"github.com/sathwikbairaboina2/workflow-engine/internal/retry"
	"github.com/sathwikbairaboina2/workflow-engine/internal/store"
	"github.com/sathwikbairaboina2/workflow-engine/wire"
)

// TryPollActivityTask leases one visible activity task without waiting; nil when none is available.
func (e *Engine) TryPollActivityTask(ctx context.Context, req wire.PollRequest) (*wire.ActivityTask, error) {
	if req.Queue == "" {
		return nil, invalid("queue is required")
	}
	var out *wire.ActivityTask
	err := e.transition(ctx, "act_start", func(tx *store.Tx, now int64) (bool, error) {
		out = nil
		task, ok, err := tx.LeaseNextTask(req.Queue, "activity", now, newID())
		if err != nil || !ok {
			return false, err
		}
		run, ok, err := tx.GetRun(task.RunID)
		if err != nil {
			return false, err
		}
		if !ok || run.Status != "running" {
			return true, tx.DeleteTask(task.TaskID)
		}
		ev, ok, err := tx.GetEvent(run.RunID, task.ScheduledEventID)
		if err != nil || !ok {
			return false, err
		}
		var sched wire.ActivityTaskScheduledAttrs
		if err := ev.DecodeAttrs(&sched); err != nil {
			return false, err
		}
		out = &wire.ActivityTask{LeaseToken: task.LeaseToken, RunID: run.RunID, WorkflowID: run.WorkflowID,
			ScheduledEventID: task.ScheduledEventID, ActivityType: sched.ActivityType, Input: sched.Input,
			Attempt: task.Attempt, DeadlineMS: task.LeaseExpiresAt}
		return false, nil // leasing writes no history, so it is not counted as a transition
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// PollActivityTask long-polls for an activity task; (nil, nil) on timeout.
func (e *Engine) PollActivityTask(ctx context.Context, req wire.PollRequest) (*wire.ActivityTask, error) {
	deadline := time.NewTimer(e.cfg.PollTimeout)
	defer deadline.Stop()
	for {
		ch := e.notify.wait(actKey(req.Queue))
		task, err := e.TryPollActivityTask(ctx, req)
		if err != nil || task != nil {
			return task, err
		}
		select {
		case <-ch:
		case <-time.After(e.cfg.PollInterval):
		case <-deadline.C:
			return nil, nil
		case <-ctx.Done():
			return nil, nil
		}
	}
}

// CompleteActivityTask records a successful activity attempt.
func (e *Engine) CompleteActivityTask(ctx context.Context, req wire.CompleteActivityTaskRequest) error {
	var notify string
	var postErr error
	err := e.transition(ctx, "act_complete", func(tx *store.Tx, now int64) (bool, error) {
		notify, postErr = "", nil
		task, err := leased(tx, req.LeaseToken, "activity", now)
		if err != nil {
			return false, err
		}
		run, ok, err := tx.GetRun(task.RunID)
		if err != nil {
			return false, err
		}
		if !ok || run.Status != "running" {
			postErr = &Error{Code: CodeRunClosed, Message: "run is closed", RunID: task.RunID}
			return true, tx.DeleteTask(task.TaskID)
		}
		if req.Result.Size() > e.cfg.MaxPayloadBytes {
			f := wire.Failure{Type: "PayloadTooLarge", Message: "activity result exceeds the payload limit", NonRetryable: true}
			sched, err := e.failActivity(tx, run, task, now, f, req.Identity, false)
			if sched {
				notify = wfKey(run.TaskQueue)
			}
			return true, err
		}
		if err := tx.DeleteTask(task.TaskID); err != nil {
			return false, err
		}
		sched, err := e.deliver(tx, run, now, []store.NewEvent{
			{Type: wire.ActivityTaskStarted, Time: now, Attrs: wire.ActivityTaskStartedAttrs{ScheduledEventID: task.ScheduledEventID, Attempt: task.Attempt, Identity: req.Identity}},
			{Type: wire.ActivityTaskCompleted, Time: now, Attrs: wire.ActivityTaskCompletedAttrs{ScheduledEventID: task.ScheduledEventID, Result: req.Result}},
		})
		if sched {
			notify = wfKey(run.TaskQueue)
		}
		return true, err
	})
	if err != nil {
		return err
	}
	if notify != "" {
		e.notify.broadcast(notify)
	}
	return postErr
}

// FailActivityTask records a failed activity attempt; the retry policy decides whether it is retried.
func (e *Engine) FailActivityTask(ctx context.Context, req wire.FailActivityTaskRequest) error {
	var notify string
	var postErr error
	err := e.transition(ctx, "act_fail", func(tx *store.Tx, now int64) (bool, error) {
		notify, postErr = "", nil
		task, err := leased(tx, req.LeaseToken, "activity", now)
		if err != nil {
			return false, err
		}
		run, ok, err := tx.GetRun(task.RunID)
		if err != nil {
			return false, err
		}
		if !ok || run.Status != "running" {
			postErr = &Error{Code: CodeRunClosed, Message: "run is closed", RunID: task.RunID}
			return true, tx.DeleteTask(task.TaskID)
		}
		sched, err := e.failActivity(tx, run, task, now, req.Failure, req.Identity, false)
		if sched {
			notify = wfKey(run.TaskQueue)
		}
		return true, err
	})
	if err != nil {
		return err
	}
	if notify != "" {
		e.notify.broadcast(notify)
	}
	return postErr
}

// failActivity applies the retry policy to a failed or timed-out attempt. A retry only reschedules the
// task row (no history events, ADR 0003); a final outcome appends ActivityTaskStarted plus
// ActivityTaskFailed or ActivityTaskTimedOut through deliver.
func (e *Engine) failActivity(tx *store.Tx, run store.Workflow, task store.Task, now int64, f wire.Failure, identity string, timedOut bool) (scheduledWFT bool, err error) {
	ev, ok, err := tx.GetEvent(run.RunID, task.ScheduledEventID)
	if err != nil {
		return false, err
	}
	var sched wire.ActivityTaskScheduledAttrs
	if ok {
		if err := ev.DecodeAttrs(&sched); err != nil {
			return false, err
		}
	}
	if again, delay := retry.Next(sched.RetryPolicy, task.Attempt, f); again {
		e.m.ActivityRetry()
		return false, tx.RetryTask(task.TaskID, task.Attempt+1, now+delay.Milliseconds())
	}
	if err := tx.DeleteTask(task.TaskID); err != nil {
		return false, err
	}
	final := store.NewEvent{Time: now}
	if timedOut {
		final.Type = wire.ActivityTaskTimedOut
		final.Attrs = wire.ActivityTaskTimedOutAttrs{ScheduledEventID: task.ScheduledEventID, Attempts: task.Attempt}
	} else {
		final.Type = wire.ActivityTaskFailed
		final.Attrs = wire.ActivityTaskFailedAttrs{ScheduledEventID: task.ScheduledEventID, Failure: f, Attempts: task.Attempt}
	}
	return e.deliver(tx, run, now, []store.NewEvent{
		{Type: wire.ActivityTaskStarted, Time: now, Attrs: wire.ActivityTaskStartedAttrs{ScheduledEventID: task.ScheduledEventID, Attempt: task.Attempt, Identity: identity}},
		final,
	})
}
