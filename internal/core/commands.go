package core

import (
	"fmt"

	"github.com/sathwikbairaboina2/workflow-engine/internal/store"
	"github.com/sathwikbairaboina2/workflow-engine/wire"
)

const defaultStartToCloseMS = 10000

// ValidateCommands checks a command list before it touches history.
func ValidateCommands(cmds []wire.Command, cfg Config) error {
	for i, c := range cmds {
		switch c.Type {
		case wire.ScheduleActivity:
			if c.Seq <= 0 || c.ActivityType == "" {
				return invalid(fmt.Sprintf("command %d: ScheduleActivity needs seq > 0 and activity_type", i))
			}
			if c.StartToCloseMS < 0 {
				return invalid(fmt.Sprintf("command %d: negative start_to_close_ms", i))
			}
		case wire.StartTimer:
			if c.Seq <= 0 || c.DurationMS < 0 {
				return invalid(fmt.Sprintf("command %d: StartTimer needs seq > 0 and a non-negative duration", i))
			}
		case wire.RecordMarker:
			if c.Seq <= 0 {
				return invalid(fmt.Sprintf("command %d: RecordMarker needs seq > 0", i))
			}
		case wire.CompleteWorkflow, wire.FailWorkflow, wire.CancelWorkflow:
			if i != len(cmds)-1 {
				return invalid(fmt.Sprintf("command %d: close command must be last", i))
			}
		default:
			return invalid(fmt.Sprintf("command %d: unknown type %q", i, c.Type))
		}
		for _, p := range []*wire.Payload{c.Input, c.Value, c.Result} {
			if p.Size() > cfg.MaxPayloadBytes {
				return invalid(fmt.Sprintf("command %d: payload exceeds %d bytes", i, cfg.MaxPayloadBytes))
			}
		}
	}
	return nil
}

func commandQueue(c wire.Command, run store.Workflow) string {
	if c.TaskQueue != "" {
		return c.TaskQueue
	}
	return run.TaskQueue
}

// commandEvent turns a validated command into the event it produces.
func commandEvent(c wire.Command, run store.Workflow, now int64) store.NewEvent {
	ev := store.NewEvent{Type: wire.EventTypeFor(c.Type), Time: now}
	switch c.Type {
	case wire.ScheduleActivity:
		rp := wire.RetryPolicy{}
		if c.RetryPolicy != nil {
			rp = *c.RetryPolicy
		}
		stc := c.StartToCloseMS
		if stc == 0 {
			stc = defaultStartToCloseMS
		}
		ev.Attrs = wire.ActivityTaskScheduledAttrs{Seq: c.Seq, ActivityType: c.ActivityType, TaskQueue: commandQueue(c, run),
			Input: c.Input, RetryPolicy: rp, StartToCloseMS: stc}
	case wire.StartTimer:
		ev.Attrs = wire.TimerStartedAttrs{Seq: c.Seq, DurationMS: c.DurationMS, FireAt: now + c.DurationMS}
	case wire.RecordMarker:
		ev.Attrs = wire.MarkerRecordedAttrs{Seq: c.Seq, Kind: c.MarkerKind, Value: c.Value}
	case wire.CompleteWorkflow:
		ev.Attrs = wire.WorkflowExecutionCompletedAttrs{Result: c.Result}
	case wire.FailWorkflow:
		f := wire.Failure{Type: "WorkflowError"}
		if c.Failure != nil {
			f = *c.Failure
		}
		ev.Attrs = wire.WorkflowExecutionFailedAttrs{Failure: f}
	case wire.CancelWorkflow:
		ev.Attrs = wire.WorkflowExecutionCanceledAttrs{Reason: c.Reason}
	}
	return ev
}

// closeOutcome returns the run status and outcome produced by a close command.
func closeOutcome(c wire.Command) (status string, result *wire.Payload, failure *wire.Failure) {
	switch c.Type {
	case wire.CompleteWorkflow:
		return "completed", c.Result, nil
	case wire.FailWorkflow:
		f := wire.Failure{Type: "WorkflowError"}
		if c.Failure != nil {
			f = *c.Failure
		}
		return "failed", nil, &f
	default:
		return "canceled", nil, nil
	}
}
