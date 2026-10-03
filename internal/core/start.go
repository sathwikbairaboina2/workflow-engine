package core

import (
	"context"
	"errors"

	"github.com/sathwikbairaboina2/workflow-engine/internal/store"
	"github.com/sathwikbairaboina2/workflow-engine/wire"
)

// StartWorkflow creates a run, its first events and its first workflow task in one transition.
func (e *Engine) StartWorkflow(ctx context.Context, req wire.StartRequest) (string, error) {
	if req.WorkflowID == "" || req.WorkflowType == "" || req.TaskQueue == "" {
		return "", invalid("workflow_id, workflow_type and task_queue are required")
	}
	if req.Input.Size() > e.cfg.MaxPayloadBytes {
		return "", tooLarge()
	}
	for attempt := 0; ; attempt++ {
		runID := newID()
		err := e.transition(ctx, "start", func(tx *store.Tx, now int64) (bool, error) {
			if err := tx.InsertWorkflow(store.Workflow{RunID: runID, WorkflowID: req.WorkflowID, WorkflowType: req.WorkflowType,
				TaskQueue: req.TaskQueue, Status: "running", NextEventID: 1, Input: req.Input, CreatedAt: now}); err != nil {
				return false, err
			}
			first, err := tx.AppendEvents(runID, 1, []store.NewEvent{
				{Type: wire.WorkflowExecutionStarted, Time: now, Attrs: wire.WorkflowExecutionStartedAttrs{
					WorkflowID: req.WorkflowID, WorkflowType: req.WorkflowType, TaskQueue: req.TaskQueue, Input: req.Input}},
				{Type: wire.WorkflowTaskScheduled, Time: now, Attrs: wire.WorkflowTaskScheduledAttrs{Attempt: 1}},
			})
			if err != nil {
				return false, err
			}
			_, err = tx.InsertTask(store.Task{Queue: req.TaskQueue, Kind: "workflow", RunID: runID, ScheduledEventID: first + 1,
				Attempt: 1, TimeoutMS: e.cfg.WorkflowTaskTimeout.Milliseconds(), VisibleAt: now})
			return true, err
		})
		if err == nil {
			e.notify.broadcast(wfKey(req.TaskQueue))
			return runID, nil
		}
		if !errors.Is(err, store.ErrDuplicateOpenRun) {
			return "", err
		}
		var open store.Workflow
		var found bool
		if verr := e.st.View(ctx, func(tx *store.Tx) error {
			var err error
			open, found, err = tx.GetOpenRun(req.WorkflowID)
			return err
		}); verr != nil {
			return "", verr
		}
		if found {
			return "", &Error{Code: CodeAlreadyStarted, Message: "workflow " + req.WorkflowID + " already has an open run", RunID: open.RunID}
		}
		if attempt >= 3 { // the open run closed between our insert and our read; try again
			return "", &Error{Code: CodeConflict, Message: "could not start workflow, retry"}
		}
	}
}

// resolveRun finds a run by explicit id, or the latest run of a workflow id.
func resolveRun(tx *store.Tx, workflowID, runID string) (store.Workflow, error) {
	var w store.Workflow
	var ok bool
	var err error
	if runID != "" {
		w, ok, err = tx.GetRun(runID)
		if ok && workflowID != "" && w.WorkflowID != workflowID {
			ok = false
		}
	} else {
		w, ok, err = tx.GetLatestRun(workflowID)
	}
	if err != nil {
		return store.Workflow{}, err
	}
	if !ok {
		return store.Workflow{}, &Error{Code: CodeNotFound, Message: "workflow not found"}
	}
	return w, nil
}

// Describe summarises a run and its pending work.
func (e *Engine) Describe(ctx context.Context, req wire.DescribeRequest) (wire.DescribeResponse, error) {
	var out wire.DescribeResponse
	err := e.st.View(ctx, func(tx *store.Tx) error {
		w, err := resolveRun(tx, req.WorkflowID, req.RunID)
		if err != nil {
			return err
		}
		out = wire.DescribeResponse{WorkflowID: w.WorkflowID, RunID: w.RunID, WorkflowType: w.WorkflowType, TaskQueue: w.TaskQueue,
			Status: w.Status, Result: w.Result, Failure: w.Failure, CreatedAt: w.CreatedAt, ClosedAt: w.ClosedAt,
			HistoryLength: w.NextEventID - 1, PendingActivities: []wire.PendingActivity{}, PendingTimers: []wire.PendingTimer{}}
		acts, err := tx.PendingActivityTasks(w.RunID)
		if err != nil {
			return err
		}
		for _, a := range acts {
			ev, ok, err := tx.GetEvent(w.RunID, a.ScheduledEventID)
			if err != nil {
				return err
			}
			var attrs wire.ActivityTaskScheduledAttrs
			if ok {
				if err := ev.DecodeAttrs(&attrs); err != nil {
					return err
				}
			}
			out.PendingActivities = append(out.PendingActivities, wire.PendingActivity{ScheduledEventID: a.ScheduledEventID,
				ActivityType: attrs.ActivityType, Attempt: a.Attempt, Leased: a.LeaseToken != ""})
		}
		timers, err := tx.TimersForRun(w.RunID)
		if err != nil {
			return err
		}
		for _, tm := range timers {
			out.PendingTimers = append(out.PendingTimers, wire.PendingTimer{StartedEventID: tm.StartedEventID, FireAt: tm.FireAt})
		}
		return nil
	})
	return out, err
}

// History returns one page of a run's history.
func (e *Engine) History(ctx context.Context, req wire.HistoryRequest) (wire.HistoryResponse, error) {
	size := req.PageSize
	if size <= 0 || size > 1000 {
		size = 1000
	}
	from := req.FromEventID
	if from < 1 {
		from = 1
	}
	var out wire.HistoryResponse
	err := e.st.View(ctx, func(tx *store.Tx) error {
		w, err := resolveRun(tx, req.WorkflowID, req.RunID)
		if err != nil {
			return err
		}
		evs, err := tx.LoadHistory(w.RunID, from, size+1)
		if err != nil {
			return err
		}
		out.RunID = w.RunID
		if len(evs) > size {
			out.NextEventID = evs[size].EventID
			evs = evs[:size]
		}
		out.Events = evs
		if out.Events == nil {
			out.Events = []wire.Event{}
		}
		return nil
	})
	return out, err
}

// SignalWorkflow delivers a signal to the open run of a workflow id.
func (e *Engine) SignalWorkflow(ctx context.Context, req wire.SignalRequest) error {
	if req.WorkflowID == "" || req.Name == "" {
		return invalid("workflow_id and name are required")
	}
	if req.Payload.Size() > e.cfg.MaxPayloadBytes {
		return tooLarge()
	}
	return e.deliverToOpenRun(ctx, "signal", req.WorkflowID, func(now int64) store.NewEvent {
		return store.NewEvent{Type: wire.WorkflowExecutionSignaled, Time: now,
			Attrs: wire.WorkflowExecutionSignaledAttrs{Name: req.Name, Payload: req.Payload}}
	})
}

// CancelWorkflow records a cancellation request; the workflow decides when to close as canceled.
func (e *Engine) CancelWorkflow(ctx context.Context, req wire.CancelRequest) error {
	if req.WorkflowID == "" {
		return invalid("workflow_id is required")
	}
	return e.deliverToOpenRun(ctx, "cancel", req.WorkflowID, func(now int64) store.NewEvent {
		return store.NewEvent{Type: wire.WorkflowExecutionCancelRequested, Time: now,
			Attrs: wire.WorkflowExecutionCancelRequestedAttrs{Reason: req.Reason}}
	})
}

func (e *Engine) deliverToOpenRun(ctx context.Context, kind, workflowID string, mk func(now int64) store.NewEvent) error {
	var notify string
	var postErr error
	err := e.transition(ctx, kind, func(tx *store.Tx, now int64) (bool, error) {
		notify, postErr = "", nil
		run, ok, err := tx.GetOpenRun(workflowID)
		if err != nil {
			return false, err
		}
		if !ok {
			latest, found, err := tx.GetLatestRun(workflowID)
			if err != nil {
				return false, err
			}
			if found {
				return false, &Error{Code: CodeRunClosed, Message: "workflow run is closed", RunID: latest.RunID}
			}
			return false, &Error{Code: CodeNotFound, Message: "workflow not found"}
		}
		sched, err := e.deliver(tx, run, now, []store.NewEvent{mk(now)})
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
