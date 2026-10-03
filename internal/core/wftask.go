package core

import (
	"context"
	"slices"
	"time"

	"github.com/sathwikbairaboina2/workflow-engine/internal/store"
	"github.com/sathwikbairaboina2/workflow-engine/wire"
)

// leased returns the task holding token if it is of the wanted kind and its lease has not expired.
func leased(tx *store.Tx, token, kind string, now int64) (store.Task, error) {
	t, ok, err := tx.TaskByLease(token)
	if err != nil {
		return store.Task{}, err
	}
	if !ok || t.Kind != kind || t.LeaseExpiresAt <= now {
		return store.Task{}, staleLease()
	}
	return t, nil
}

// wftBackoff is how long a failed workflow task waits before it becomes visible again.
func wftBackoff(failedAttempt int) time.Duration {
	if failedAttempt < 1 {
		failedAttempt = 1
	}
	if failedAttempt > 6 {
		return 30 * time.Second
	}
	return min(time.Second<<(failedAttempt-1), 30*time.Second)
}

// TryPollWorkflowTask leases one visible workflow task without waiting; nil when none is available.
func (e *Engine) TryPollWorkflowTask(ctx context.Context, req wire.PollRequest) (*wire.WorkflowTask, error) {
	if req.Queue == "" {
		return nil, invalid("queue is required")
	}
	var out *wire.WorkflowTask
	err := e.transition(ctx, "wft_start", func(tx *store.Tx, now int64) (bool, error) {
		out = nil
		task, ok, err := tx.LeaseNextTask(req.Queue, "workflow", now, newID())
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
		first, closed, err := e.appendEvents(tx, run, now, []store.NewEvent{
			{Type: wire.WorkflowTaskStarted, Time: now, Attrs: wire.WorkflowTaskStartedAttrs{ScheduledEventID: task.ScheduledEventID, Identity: req.Identity}}})
		if err != nil || closed {
			return closed, err
		}
		if err := tx.SetTaskStarted(task.TaskID, first); err != nil {
			return false, err
		}
		hist, err := tx.LoadHistory(run.RunID, 1, int(e.cfg.MaxHistoryEvents))
		if err != nil {
			return false, err
		}
		out = &wire.WorkflowTask{LeaseToken: task.LeaseToken, RunID: run.RunID, WorkflowID: run.WorkflowID,
			WorkflowType: run.WorkflowType, TaskQueue: run.TaskQueue, Attempt: task.Attempt, History: hist}
		return true, nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// PollWorkflowTask long-polls for a workflow task; (nil, nil) on timeout.
func (e *Engine) PollWorkflowTask(ctx context.Context, req wire.PollRequest) (*wire.WorkflowTask, error) {
	deadline := time.NewTimer(e.cfg.PollTimeout)
	defer deadline.Stop()
	for {
		ch := e.notify.wait(wfKey(req.Queue))
		task, err := e.TryPollWorkflowTask(ctx, req)
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

// CompleteWorkflowTask applies the commands a workflow step produced (spec Semantics 2 and 4).
func (e *Engine) CompleteWorkflowTask(ctx context.Context, req wire.CompleteWorkflowTaskRequest) error {
	var notifs []string
	var postErr error
	err := e.transition(ctx, "wft_complete", func(tx *store.Tx, now int64) (bool, error) {
		notifs, postErr = nil, nil
		task, err := leased(tx, req.LeaseToken, "workflow", now)
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
		if err := ValidateCommands(req.Commands, e.cfg); err != nil {
			return false, err
		}
		buffered, err := tx.TakeBufferedEvents(run.RunID)
		if err != nil {
			return false, err
		}
		cmds := req.Commands
		if len(buffered) > 0 && len(cmds) > 0 && cmds[len(cmds)-1].IsClose() {
			cmds = cmds[:len(cmds)-1] // the workflow has not seen the buffered events yet
		}
		closing := len(cmds) > 0 && cmds[len(cmds)-1].IsClose()
		scheduleNext := !closing && len(buffered) > 0

		evs := []store.NewEvent{{Type: wire.WorkflowTaskCompleted, Time: now,
			Attrs: wire.WorkflowTaskCompletedAttrs{ScheduledEventID: task.ScheduledEventID, StartedEventID: task.StartedEventID}}}
		for _, c := range cmds {
			evs = append(evs, commandEvent(c, run, now))
		}
		evs = append(evs, buffered...)
		if scheduleNext {
			evs = append(evs, store.NewEvent{Type: wire.WorkflowTaskScheduled, Time: now, Attrs: wire.WorkflowTaskScheduledAttrs{Attempt: 1}})
		}
		first, limited, err := e.appendEvents(tx, run, now, evs)
		if err != nil || limited {
			return limited, err
		}
		if err := tx.DeleteTask(task.TaskID); err != nil {
			return false, err
		}
		if closing {
			status, result, failure := closeOutcome(cmds[len(cmds)-1])
			if err := tx.CloseRun(run.RunID, status, result, failure, now); err != nil {
				return false, err
			}
			e.m.WorkflowClosed(status)
			return true, tx.DeleteRunTasksTimersBuffer(run.RunID)
		}
		for i, c := range cmds {
			id := first + 1 + int64(i)
			switch c.Type {
			case wire.ScheduleActivity:
				stc := c.StartToCloseMS
				if stc == 0 {
					stc = defaultStartToCloseMS
				}
				q := commandQueue(c, run)
				if _, err := tx.InsertTask(store.Task{Queue: q, Kind: "activity", RunID: run.RunID, ScheduledEventID: id,
					Attempt: 1, TimeoutMS: stc, VisibleAt: now}); err != nil {
					return false, err
				}
				if !slices.Contains(notifs, actKey(q)) {
					notifs = append(notifs, actKey(q))
				}
			case wire.StartTimer:
				if err := tx.InsertTimer(store.Timer{RunID: run.RunID, StartedEventID: id, FireAt: now + c.DurationMS}); err != nil {
					return false, err
				}
			}
		}
		if scheduleNext {
			_, err := tx.InsertTask(store.Task{Queue: run.TaskQueue, Kind: "workflow", RunID: run.RunID,
				ScheduledEventID: first + int64(len(evs)) - 1, Attempt: 1, TimeoutMS: e.cfg.WorkflowTaskTimeout.Milliseconds(), VisibleAt: now})
			if err != nil {
				return false, err
			}
			notifs = append(notifs, wfKey(run.TaskQueue))
		}
		return true, nil
	})
	if err != nil {
		return err
	}
	for _, k := range notifs {
		e.notify.broadcast(k)
	}
	return postErr
}

var failCauses = []string{"nondeterminism", "panic", "unknown_workflow_type", "invalid_commands"}

// FailWorkflowTask records that a worker could not run a step and reschedules it with backoff.
func (e *Engine) FailWorkflowTask(ctx context.Context, req wire.FailWorkflowTaskRequest) error {
	if !slices.Contains(failCauses, req.Cause) {
		return invalid("unknown failure cause " + req.Cause)
	}
	var notify string
	err := e.transition(ctx, "wft_fail", func(tx *store.Tx, now int64) (bool, error) {
		notify = ""
		task, err := leased(tx, req.LeaseToken, "workflow", now)
		if err != nil {
			return false, err
		}
		run, ok, err := tx.GetRun(task.RunID)
		if err != nil {
			return false, err
		}
		if !ok || run.Status != "running" {
			return true, tx.DeleteTask(task.TaskID)
		}
		if err := e.failWorkflowTask(tx, run, task, now, req.Cause, req.Message, wftBackoff(task.Attempt)); err != nil {
			return false, err
		}
		notify = wfKey(run.TaskQueue)
		return true, nil
	})
	if err == nil && notify != "" {
		e.notify.broadcast(notify)
	}
	return err
}

// failWorkflowTask appends WorkflowTaskFailed, the buffered events and WorkflowTaskScheduled{attempt+1},
// then replaces the task row so it becomes visible after delay. No command events are appended.
func (e *Engine) failWorkflowTask(tx *store.Tx, run store.Workflow, task store.Task, now int64, cause, message string, delay time.Duration) error {
	e.m.WorkflowTaskFailed(cause)
	buffered, err := tx.TakeBufferedEvents(run.RunID)
	if err != nil {
		return err
	}
	evs := []store.NewEvent{{Type: wire.WorkflowTaskFailed, Time: now, Attrs: wire.WorkflowTaskFailedAttrs{
		ScheduledEventID: task.ScheduledEventID, StartedEventID: task.StartedEventID, Cause: cause, Message: message}}}
	evs = append(evs, buffered...)
	evs = append(evs, store.NewEvent{Type: wire.WorkflowTaskScheduled, Time: now, Attrs: wire.WorkflowTaskScheduledAttrs{Attempt: task.Attempt + 1}})
	first, closed, err := e.appendEvents(tx, run, now, evs)
	if err != nil || closed {
		return err
	}
	if err := tx.DeleteTask(task.TaskID); err != nil {
		return err
	}
	_, err = tx.InsertTask(store.Task{Queue: run.TaskQueue, Kind: "workflow", RunID: run.RunID,
		ScheduledEventID: first + int64(len(evs)) - 1, Attempt: task.Attempt + 1,
		TimeoutMS: e.cfg.WorkflowTaskTimeout.Milliseconds(), VisibleAt: now + delay.Milliseconds()})
	return err
}
