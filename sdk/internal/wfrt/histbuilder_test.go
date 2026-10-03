package wfrt

import (
	"github.com/sathwikbairaboina2/workflow-engine/wire"
)

// hist builds workflow histories the way the server would append them.
type hist struct {
	evs       []wire.Event
	ts        int64
	attempt   int
	schedID   int64
	startedID int64
}

func newHist(wfType string, input any) *hist {
	b := &hist{ts: 1_700_000_000_000, attempt: 1}
	b.add(wire.WorkflowExecutionStarted, wire.WorkflowExecutionStartedAttrs{
		WorkflowID: "w", WorkflowType: wfType, TaskQueue: "q", Input: wire.MustEncode(input)})
	return b
}

func (b *hist) add(t wire.EventType, attrs any) int64 {
	b.ts += 10
	id := int64(len(b.evs) + 1)
	ev, err := wire.NewEvent(id, t, b.ts, attrs)
	if err != nil {
		panic(err)
	}
	b.evs = append(b.evs, ev)
	return id
}

// task appends WorkflowTaskScheduled and WorkflowTaskStarted.
func (b *hist) task() *hist {
	b.schedID = b.add(wire.WorkflowTaskScheduled, wire.WorkflowTaskScheduledAttrs{Attempt: b.attempt})
	b.startedID = b.add(wire.WorkflowTaskStarted, wire.WorkflowTaskStartedAttrs{ScheduledEventID: b.schedID, Identity: "t"})
	return b
}

// taskAt is task with a chosen WorkflowTaskStarted timestamp (ms).
func (b *hist) taskAt(ms int64) *hist {
	b.task()
	b.evs[len(b.evs)-1].Time = ms
	return b
}

// failedTask closes the current task as failed; the next task() is its retry.
func (b *hist) failedTask(cause string) *hist {
	b.add(wire.WorkflowTaskFailed, wire.WorkflowTaskFailedAttrs{ScheduledEventID: b.schedID, StartedEventID: b.startedID, Cause: cause})
	b.attempt++
	return b
}

// completed appends WorkflowTaskCompleted followed by one event per command and returns their event ids.
func (b *hist) completed(cmds ...wire.Command) []int64 {
	b.add(wire.WorkflowTaskCompleted, wire.WorkflowTaskCompletedAttrs{ScheduledEventID: b.schedID, StartedEventID: b.startedID})
	b.attempt = 1
	var ids []int64
	for _, c := range cmds {
		var attrs any
		switch c.Type {
		case wire.ScheduleActivity:
			attrs = wire.ActivityTaskScheduledAttrs{Seq: c.Seq, ActivityType: c.ActivityType, TaskQueue: "q", Input: c.Input}
		case wire.StartTimer:
			attrs = wire.TimerStartedAttrs{Seq: c.Seq, DurationMS: c.DurationMS, FireAt: b.ts + c.DurationMS}
		case wire.RecordMarker:
			attrs = wire.MarkerRecordedAttrs{Seq: c.Seq, Kind: "side_effect", Value: c.Value}
		case wire.CompleteWorkflow:
			attrs = wire.WorkflowExecutionCompletedAttrs{Result: c.Result}
		case wire.FailWorkflow:
			attrs = wire.WorkflowExecutionFailedAttrs{Failure: *c.Failure}
		case wire.CancelWorkflow:
			attrs = wire.WorkflowExecutionCanceledAttrs{}
		}
		ids = append(ids, b.add(wire.EventTypeFor(c.Type), attrs))
	}
	return ids
}

func (b *hist) activityDone(schedID int64, result any) *hist {
	b.add(wire.ActivityTaskStarted, wire.ActivityTaskStartedAttrs{ScheduledEventID: schedID, Attempt: 1})
	b.add(wire.ActivityTaskCompleted, wire.ActivityTaskCompletedAttrs{ScheduledEventID: schedID, Result: wire.MustEncode(result)})
	return b
}

func (b *hist) activityFailed(schedID int64, f wire.Failure) *hist {
	b.add(wire.ActivityTaskStarted, wire.ActivityTaskStartedAttrs{ScheduledEventID: schedID, Attempt: 1})
	b.add(wire.ActivityTaskFailed, wire.ActivityTaskFailedAttrs{ScheduledEventID: schedID, Failure: f, Attempts: 1})
	return b
}

func (b *hist) timerFired(startedID int64) *hist {
	b.add(wire.TimerFired, wire.TimerFiredAttrs{StartedEventID: startedID})
	return b
}

func (b *hist) signal(name string, v any) *hist {
	b.add(wire.WorkflowExecutionSignaled, wire.WorkflowExecutionSignaledAttrs{Name: name, Payload: wire.MustEncode(v)})
	return b
}

func (b *hist) cancel() *hist {
	b.add(wire.WorkflowExecutionCancelRequested, wire.WorkflowExecutionCancelRequestedAttrs{})
	return b
}
