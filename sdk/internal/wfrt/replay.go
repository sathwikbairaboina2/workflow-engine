package wfrt

import (
	"fmt"
	"time"

	"github.com/sathwikbairaboina2/workflow-engine/wire"
)

// NondeterminismError means workflow code produced commands that disagree with recorded history.
type NondeterminismError struct{ Msg string }

func (e *NondeterminismError) Error() string { return "nondeterminism: " + e.Msg }

func nondet(format string, args ...any) error {
	return &NondeterminismError{Msg: fmt.Sprintf(format, args...)}
}

// describeCmd and describeEvent render a command or event for error messages.
func describeCmd(c wire.Command) string {
	switch c.Type {
	case wire.ScheduleActivity:
		return fmt.Sprintf("%s(seq=%d, activity=%q)", c.Type, c.Seq, c.ActivityType)
	case wire.StartTimer, wire.RecordMarker:
		return fmt.Sprintf("%s(seq=%d)", c.Type, c.Seq)
	}
	return string(c.Type)
}

func describeEvent(ev wire.Event) string {
	switch ev.Type {
	case wire.ActivityTaskScheduled:
		var a wire.ActivityTaskScheduledAttrs
		_ = ev.DecodeAttrs(&a)
		return fmt.Sprintf("event %d %s(seq=%d, activity=%q)", ev.EventID, ev.Type, a.Seq, a.ActivityType)
	case wire.TimerStarted:
		var a wire.TimerStartedAttrs
		_ = ev.DecodeAttrs(&a)
		return fmt.Sprintf("event %d %s(seq=%d)", ev.EventID, ev.Type, a.Seq)
	case wire.MarkerRecorded:
		var a wire.MarkerRecordedAttrs
		_ = ev.DecodeAttrs(&a)
		return fmt.Sprintf("event %d %s(seq=%d)", ev.EventID, ev.Type, a.Seq)
	}
	return fmt.Sprintf("event %d %s", ev.EventID, ev.Type)
}

// matchCommand checks one produced command against the next command event. It returns the event's seq.
func matchCommand(c wire.Command, ev wire.Event) (seq int64, err error) {
	if wire.EventTypeFor(c.Type) != ev.Type {
		return 0, nondet("workflow code produced %s but history has %s", describeCmd(c), describeEvent(ev))
	}
	switch ev.Type {
	case wire.ActivityTaskScheduled:
		var a wire.ActivityTaskScheduledAttrs
		if err := ev.DecodeAttrs(&a); err != nil {
			return 0, err
		}
		if a.Seq != c.Seq || a.ActivityType != c.ActivityType {
			return 0, nondet("workflow code produced %s but history has %s", describeCmd(c), describeEvent(ev))
		}
		return a.Seq, nil
	case wire.TimerStarted:
		var a wire.TimerStartedAttrs
		if err := ev.DecodeAttrs(&a); err != nil {
			return 0, err
		}
		if a.Seq != c.Seq {
			return 0, nondet("workflow code produced %s but history has %s", describeCmd(c), describeEvent(ev))
		}
		return a.Seq, nil
	case wire.MarkerRecorded:
		var a wire.MarkerRecordedAttrs
		if err := ev.DecodeAttrs(&a); err != nil {
			return 0, err
		}
		if a.Seq != c.Seq {
			return 0, nondet("workflow code produced %s but history has %s", describeCmd(c), describeEvent(ev))
		}
		return a.Seq, nil
	}
	return 0, nil
}

// Replay runs fn against history. If the last event is WorkflowTaskStarted it returns the new
// commands for that task; otherwise it validates the whole history and returns nil commands.
//
// Code runs only at a WorkflowTaskStarted that is the last event or is followed by WorkflowTaskCompleted;
// a started task that failed or timed out never ran to completion and is skipped (spec Semantics 6).
func Replay(history []wire.Event, fn any) ([]wire.Command, error) {
	if len(history) == 0 || history[0].Type != wire.WorkflowExecutionStarted {
		return nil, fmt.Errorf("wfrt: history must begin with WorkflowExecutionStarted")
	}
	var started wire.WorkflowExecutionStartedAttrs
	if err := history[0].DecodeAttrs(&started); err != nil {
		return nil, fmt.Errorf("wfrt: decode start event: %w", err)
	}
	if err := ValidateWorkflowFunc(fn); err != nil {
		return nil, err
	}
	e := newEnv()
	e.taskQueue = started.TaskQueue
	for _, ev := range history {
		if ev.Type != wire.MarkerRecorded {
			continue
		}
		var m wire.MarkerRecordedAttrs
		if err := ev.DecodeAttrs(&m); err != nil {
			return nil, err
		}
		e.markers[m.Seq] = m.Value
	}
	defer e.disp.close()
	ctx := wctx{e}
	e.disp.spawn(func() {
		res, err := invokeWorkflow(fn, ctx, started.Input)
		c := closeCommand(e, res, err)
		e.closeCmd = &c
		e.emit(c)
		e.rootDone = true
	})

	for i := 0; i < len(history); i++ {
		ev := history[i]
		switch ev.Type {
		case wire.WorkflowTaskStarted:
			last := i == len(history)-1
			if !last && history[i+1].Type != wire.WorkflowTaskCompleted {
				continue
			}
			e.now = time.UnixMilli(ev.Time)
			e.cmds = nil
			if e.rootDone {
				e.cmds = []wire.Command{*e.closeCmd}
			} else if err := e.disp.runUntilBlocked(func() bool { return e.rootDone }); err != nil {
				return nil, err
			}
			if last {
				return e.cmds, nil
			}
			k := i + 2 // history[i+1] is WorkflowTaskCompleted; its command events follow
			for ci, c := range e.cmds {
				if k < len(history) && wire.IsCommandEvent(history[k].Type) {
					seq, err := matchCommand(c, history[k])
					if err != nil {
						return nil, err
					}
					if history[k].Type == wire.ActivityTaskScheduled || history[k].Type == wire.TimerStarted {
						e.eventToSeq[history[k].EventID] = seq
					}
					k++
					continue
				}
				// History has no event for this command: only a trailing close command may be missing,
				// because the server drops it when events were buffered (spec Semantics 4).
				if !(c.IsClose() && ci == len(e.cmds)-1) {
					return nil, nondet("workflow code produced %s but history has no matching event after event %d", describeCmd(c), history[i+1].EventID)
				}
			}
			if k < len(history) && wire.IsCommandEvent(history[k].Type) {
				return nil, nondet("history has %s that workflow code did not produce", describeEvent(history[k]))
			}
			i = k - 1
		case wire.ActivityTaskCompleted, wire.ActivityTaskFailed, wire.ActivityTaskTimedOut:
			e.resolveActivity(ev)
		case wire.TimerFired:
			var a wire.TimerFiredAttrs
			if err := ev.DecodeAttrs(&a); err != nil {
				return nil, err
			}
			if f := e.timers[e.eventToSeq[a.StartedEventID]]; f != nil {
				f.ready = true
			}
		case wire.WorkflowExecutionSignaled:
			var a wire.WorkflowExecutionSignaledAttrs
			if err := ev.DecodeAttrs(&a); err != nil {
				return nil, err
			}
			ch := e.channel(a.Name)
			ch.q = append(ch.q, a.Payload)
		case wire.WorkflowExecutionCancelRequested:
			e.canceled = true
		case wire.ActivityTaskScheduled, wire.TimerStarted, wire.MarkerRecorded,
			wire.WorkflowExecutionCompleted, wire.WorkflowExecutionFailed, wire.WorkflowExecutionCanceled:
			return nil, fmt.Errorf("wfrt: corrupt history at event %d: %s outside a workflow task batch", ev.EventID, ev.Type)
		}
	}
	return nil, nil
}

func (e *env) resolveActivity(ev wire.Event) {
	var schedID int64
	var result *wire.Payload
	var failure wire.Failure
	timedOut := false
	switch ev.Type {
	case wire.ActivityTaskCompleted:
		var a wire.ActivityTaskCompletedAttrs
		_ = ev.DecodeAttrs(&a)
		schedID, result = a.ScheduledEventID, a.Result
	case wire.ActivityTaskFailed:
		var a wire.ActivityTaskFailedAttrs
		_ = ev.DecodeAttrs(&a)
		schedID, failure = a.ScheduledEventID, a.Failure
	case wire.ActivityTaskTimedOut:
		var a wire.ActivityTaskTimedOutAttrs
		_ = ev.DecodeAttrs(&a)
		schedID, timedOut = a.ScheduledEventID, true
	}
	f := e.activities[e.eventToSeq[schedID]]
	if f == nil {
		return
	}
	f.ready = true
	switch ev.Type {
	case wire.ActivityTaskCompleted:
		f.val = result
	default:
		f.err = &ActivityError{ActivityType: f.activityType, Failure: failure, TimedOut: timedOut}
	}
}
