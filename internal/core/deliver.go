package core

import (
	"github.com/sathwikbairaboina2/workflow-engine/internal/store"
	"github.com/sathwikbairaboina2/workflow-engine/wire"
)

// appendEvents is the only way the engine appends to a run. If the append would reach the history limit it
// appends a single WorkflowExecutionFailed{HistoryLimitExceeded} instead (the last slot is reserved for it),
// closes the run and reports closed=true so the caller skips its own row changes.
func (e *Engine) appendEvents(tx *store.Tx, run store.Workflow, now int64, evs []store.NewEvent) (firstID int64, closed bool, err error) {
	if run.NextEventID-1+int64(len(evs)) > e.cfg.MaxHistoryEvents-1 {
		f := wire.Failure{Type: "HistoryLimitExceeded", Message: "workflow history reached the configured event limit"}
		id, err := tx.AppendEvents(run.RunID, run.NextEventID, []store.NewEvent{
			{Type: wire.WorkflowExecutionFailed, Time: now, Attrs: wire.WorkflowExecutionFailedAttrs{Failure: f}}})
		if err != nil {
			return 0, false, err
		}
		if err := tx.CloseRun(run.RunID, "failed", nil, &f, now); err != nil {
			return 0, false, err
		}
		if err := tx.DeleteRunTasksTimersBuffer(run.RunID); err != nil {
			return 0, false, err
		}
		return id, true, nil
	}
	first, err := tx.AppendEvents(run.RunID, run.NextEventID, evs)
	return first, false, err
}

// deliver hands events that need the workflow's attention to the run, following the buffering rule:
// while a workflow task is in flight (leased) the events are buffered; otherwise they are appended,
// followed by a WorkflowTaskScheduled and a task row when the run has no workflow task yet.
// It reports whether a new workflow task was scheduled, so the caller can wake pollers after commit.
func (e *Engine) deliver(tx *store.Tx, run store.Workflow, now int64, evs []store.NewEvent) (scheduledWFT bool, err error) {
	task, has, err := tx.WorkflowTaskForRun(run.RunID)
	if err != nil {
		return false, err
	}
	if has && task.LeaseToken != "" {
		for _, ev := range evs {
			if err := tx.BufferEvent(run.RunID, ev); err != nil {
				return false, err
			}
		}
		return false, nil
	}
	all := evs
	if !has {
		all = append(append([]store.NewEvent{}, evs...),
			store.NewEvent{Type: wire.WorkflowTaskScheduled, Time: now, Attrs: wire.WorkflowTaskScheduledAttrs{Attempt: 1}})
	}
	first, closed, err := e.appendEvents(tx, run, now, all)
	if err != nil || closed {
		return false, err
	}
	if has {
		return false, nil
	}
	_, err = tx.InsertTask(store.Task{Queue: run.TaskQueue, Kind: "workflow", RunID: run.RunID,
		ScheduledEventID: first + int64(len(all)) - 1, Attempt: 1, TimeoutMS: e.cfg.WorkflowTaskTimeout.Milliseconds(), VisibleAt: now})
	return err == nil, err
}
