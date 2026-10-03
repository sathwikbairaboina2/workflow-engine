package wire

// CommandType names a command emitted by workflow code.
type CommandType string

// Command types.
const (
	ScheduleActivity CommandType = "ScheduleActivity"
	StartTimer       CommandType = "StartTimer"
	RecordMarker     CommandType = "RecordMarker"
	CompleteWorkflow CommandType = "CompleteWorkflow"
	FailWorkflow     CommandType = "FailWorkflow"
	CancelWorkflow   CommandType = "CancelWorkflow"
)

// Command is what workflow code proposes; the server core decides.
type Command struct {
	Type           CommandType  `json:"type"`
	Seq            int64        `json:"seq,omitempty"`
	ActivityType   string       `json:"activity_type,omitempty"`
	TaskQueue      string       `json:"task_queue,omitempty"` // default: the workflow's queue
	Input          *Payload     `json:"input,omitempty"`
	RetryPolicy    *RetryPolicy `json:"retry_policy,omitempty"`
	StartToCloseMS int64        `json:"start_to_close_ms,omitempty"`
	DurationMS     int64        `json:"duration_ms,omitempty"`
	MarkerKind     string       `json:"marker_kind,omitempty"`
	Value          *Payload     `json:"value,omitempty"`
	Result         *Payload     `json:"result,omitempty"`
	Failure        *Failure     `json:"failure,omitempty"`
	Reason         string       `json:"reason,omitempty"`
}

// IsClose reports whether the command closes the run.
func (c Command) IsClose() bool {
	switch c.Type {
	case CompleteWorkflow, FailWorkflow, CancelWorkflow:
		return true
	}
	return false
}

// EventTypeFor maps a command type to the event type it produces.
func EventTypeFor(t CommandType) EventType {
	switch t {
	case ScheduleActivity:
		return ActivityTaskScheduled
	case StartTimer:
		return TimerStarted
	case RecordMarker:
		return MarkerRecorded
	case CompleteWorkflow:
		return WorkflowExecutionCompleted
	case FailWorkflow:
		return WorkflowExecutionFailed
	case CancelWorkflow:
		return WorkflowExecutionCanceled
	}
	return ""
}
