package wire

import "encoding/json"

// EventType names a history event.
type EventType string

// History event types.
const (
	WorkflowExecutionStarted         EventType = "WorkflowExecutionStarted"
	WorkflowTaskScheduled            EventType = "WorkflowTaskScheduled"
	WorkflowTaskStarted              EventType = "WorkflowTaskStarted"
	WorkflowTaskCompleted            EventType = "WorkflowTaskCompleted"
	WorkflowTaskFailed               EventType = "WorkflowTaskFailed"
	ActivityTaskScheduled            EventType = "ActivityTaskScheduled"
	ActivityTaskStarted              EventType = "ActivityTaskStarted"
	ActivityTaskCompleted            EventType = "ActivityTaskCompleted"
	ActivityTaskFailed               EventType = "ActivityTaskFailed"
	ActivityTaskTimedOut             EventType = "ActivityTaskTimedOut"
	TimerStarted                     EventType = "TimerStarted"
	TimerFired                       EventType = "TimerFired"
	MarkerRecorded                   EventType = "MarkerRecorded"
	WorkflowExecutionSignaled        EventType = "WorkflowExecutionSignaled"
	WorkflowExecutionCancelRequested EventType = "WorkflowExecutionCancelRequested"
	WorkflowExecutionCompleted       EventType = "WorkflowExecutionCompleted"
	WorkflowExecutionFailed          EventType = "WorkflowExecutionFailed"
	WorkflowExecutionCanceled        EventType = "WorkflowExecutionCanceled"
)

// Event is one immutable history entry. Time is Unix milliseconds.
type Event struct {
	EventID int64           `json:"event_id"`
	Type    EventType       `json:"type"`
	Time    int64           `json:"ts"`
	Attrs   json.RawMessage `json:"attrs"`
}

// NewEvent builds an event, marshalling attrs.
func NewEvent(id int64, t EventType, ts int64, attrs any) (Event, error) {
	b, err := json.Marshal(attrs)
	if err != nil {
		return Event{}, err
	}
	return Event{EventID: id, Type: t, Time: ts, Attrs: b}, nil
}

// DecodeAttrs unmarshals the event attributes into v.
func (e Event) DecodeAttrs(v any) error {
	if len(e.Attrs) == 0 {
		return nil
	}
	return json.Unmarshal(e.Attrs, v)
}

// IsCommandEvent reports whether the event type is produced by a workflow command.
func IsCommandEvent(t EventType) bool {
	switch t {
	case ActivityTaskScheduled, TimerStarted, MarkerRecorded,
		WorkflowExecutionCompleted, WorkflowExecutionFailed, WorkflowExecutionCanceled:
		return true
	}
	return false
}

// WorkflowExecutionStartedAttrs are the attributes of WorkflowExecutionStarted.
type WorkflowExecutionStartedAttrs struct {
	WorkflowID   string   `json:"workflow_id"`
	WorkflowType string   `json:"workflow_type"`
	TaskQueue    string   `json:"task_queue"`
	Input        *Payload `json:"input,omitempty"`
}

// WorkflowTaskScheduledAttrs are the attributes of WorkflowTaskScheduled.
type WorkflowTaskScheduledAttrs struct {
	Attempt int `json:"attempt"`
}

// WorkflowTaskStartedAttrs are the attributes of WorkflowTaskStarted.
type WorkflowTaskStartedAttrs struct {
	ScheduledEventID int64  `json:"scheduled_event_id"`
	Identity         string `json:"identity"`
}

// WorkflowTaskCompletedAttrs are the attributes of WorkflowTaskCompleted.
type WorkflowTaskCompletedAttrs struct {
	ScheduledEventID int64 `json:"scheduled_event_id"`
	StartedEventID   int64 `json:"started_event_id"`
}

// WorkflowTaskFailedAttrs are the attributes of WorkflowTaskFailed.
type WorkflowTaskFailedAttrs struct {
	ScheduledEventID int64  `json:"scheduled_event_id"`
	StartedEventID   int64  `json:"started_event_id"`
	Cause            string `json:"cause"` // nondeterminism | panic | unknown_workflow_type | invalid_commands | timeout
	Message          string `json:"message,omitempty"`
}

// ActivityTaskScheduledAttrs are the attributes of ActivityTaskScheduled.
type ActivityTaskScheduledAttrs struct {
	Seq            int64       `json:"seq"`
	ActivityType   string      `json:"activity_type"`
	TaskQueue      string      `json:"task_queue"`
	Input          *Payload    `json:"input,omitempty"`
	RetryPolicy    RetryPolicy `json:"retry_policy"`
	StartToCloseMS int64       `json:"start_to_close_ms"`
}

// ActivityTaskStartedAttrs are the attributes of ActivityTaskStarted.
type ActivityTaskStartedAttrs struct {
	ScheduledEventID int64  `json:"scheduled_event_id"`
	Attempt          int    `json:"attempt"`
	Identity         string `json:"identity,omitempty"`
}

// ActivityTaskCompletedAttrs are the attributes of ActivityTaskCompleted.
type ActivityTaskCompletedAttrs struct {
	ScheduledEventID int64    `json:"scheduled_event_id"`
	Result           *Payload `json:"result,omitempty"`
}

// ActivityTaskFailedAttrs are the attributes of ActivityTaskFailed.
type ActivityTaskFailedAttrs struct {
	ScheduledEventID int64   `json:"scheduled_event_id"`
	Failure          Failure `json:"failure"`
	Attempts         int     `json:"attempts"`
}

// ActivityTaskTimedOutAttrs are the attributes of ActivityTaskTimedOut.
type ActivityTaskTimedOutAttrs struct {
	ScheduledEventID int64 `json:"scheduled_event_id"`
	Attempts         int   `json:"attempts"`
}

// TimerStartedAttrs are the attributes of TimerStarted.
type TimerStartedAttrs struct {
	Seq        int64 `json:"seq"`
	DurationMS int64 `json:"duration_ms"`
	FireAt     int64 `json:"fire_at"`
}

// TimerFiredAttrs are the attributes of TimerFired.
type TimerFiredAttrs struct {
	StartedEventID int64 `json:"started_event_id"`
}

// MarkerRecordedAttrs are the attributes of MarkerRecorded.
type MarkerRecordedAttrs struct {
	Seq   int64    `json:"seq"`
	Kind  string   `json:"kind"` // "side_effect"
	Value *Payload `json:"value,omitempty"`
}

// WorkflowExecutionSignaledAttrs are the attributes of WorkflowExecutionSignaled.
type WorkflowExecutionSignaledAttrs struct {
	Name    string   `json:"name"`
	Payload *Payload `json:"payload,omitempty"`
}

// WorkflowExecutionCancelRequestedAttrs are the attributes of WorkflowExecutionCancelRequested.
type WorkflowExecutionCancelRequestedAttrs struct {
	Reason string `json:"reason,omitempty"`
}

// WorkflowExecutionCompletedAttrs are the attributes of WorkflowExecutionCompleted.
type WorkflowExecutionCompletedAttrs struct {
	Result *Payload `json:"result,omitempty"`
}

// WorkflowExecutionFailedAttrs are the attributes of WorkflowExecutionFailed.
type WorkflowExecutionFailedAttrs struct {
	Failure Failure `json:"failure"`
}

// WorkflowExecutionCanceledAttrs are the attributes of WorkflowExecutionCanceled.
type WorkflowExecutionCanceledAttrs struct {
	Reason string `json:"reason,omitempty"`
}
