package wire

// StartRequest starts a workflow run.
type StartRequest struct {
	WorkflowID   string   `json:"workflow_id"`
	WorkflowType string   `json:"workflow_type"`
	TaskQueue    string   `json:"task_queue"`
	Input        *Payload `json:"input,omitempty"`
}

// StartResponse carries the new run id.
type StartResponse struct {
	RunID string `json:"run_id"`
}

// SignalRequest delivers a signal to the open run of a workflow id.
type SignalRequest struct {
	WorkflowID string   `json:"workflow_id"`
	Name       string   `json:"name"`
	Payload    *Payload `json:"payload,omitempty"`
}

// CancelRequest asks a workflow to cancel.
type CancelRequest struct {
	WorkflowID string `json:"workflow_id"`
	Reason     string `json:"reason,omitempty"`
}

// DescribeRequest selects a run (latest when RunID is empty).
type DescribeRequest struct {
	WorkflowID string `json:"workflow_id"`
	RunID      string `json:"run_id,omitempty"`
}

// PendingActivity is an activity that has not reached a terminal event.
type PendingActivity struct {
	ScheduledEventID int64  `json:"scheduled_event_id"`
	ActivityType     string `json:"activity_type"`
	Attempt          int    `json:"attempt"`
	Leased           bool   `json:"leased"`
}

// PendingTimer is a timer that has not fired.
type PendingTimer struct {
	StartedEventID int64 `json:"started_event_id"`
	FireAt         int64 `json:"fire_at"`
}

// DescribeResponse summarises a run.
type DescribeResponse struct {
	WorkflowID        string            `json:"workflow_id"`
	RunID             string            `json:"run_id"`
	WorkflowType      string            `json:"workflow_type"`
	TaskQueue         string            `json:"task_queue"`
	Status            string            `json:"status"` // running | completed | failed | canceled
	Result            *Payload          `json:"result,omitempty"`
	Failure           *Failure          `json:"failure,omitempty"`
	CreatedAt         int64             `json:"created_at"`
	ClosedAt          int64             `json:"closed_at,omitempty"`
	HistoryLength     int64             `json:"history_length"`
	PendingActivities []PendingActivity `json:"pending_activities"`
	PendingTimers     []PendingTimer    `json:"pending_timers"`
}

// HistoryRequest pages through a run's history.
type HistoryRequest struct {
	WorkflowID  string `json:"workflow_id"`
	RunID       string `json:"run_id,omitempty"`
	FromEventID int64  `json:"from_event_id,omitempty"` // default 1
	PageSize    int    `json:"page_size,omitempty"`     // default and max 1000
}

// HistoryResponse is one page of history.
type HistoryResponse struct {
	RunID       string  `json:"run_id"`
	Events      []Event `json:"events"`
	NextEventID int64   `json:"next_event_id,omitempty"` // 0 when there are no more pages
}

// PollRequest long-polls a task queue.
type PollRequest struct {
	Queue    string `json:"queue"`
	Identity string `json:"identity"`
}

// WorkflowTask is leased to a worker together with the full history.
type WorkflowTask struct {
	LeaseToken   string  `json:"lease_token"`
	RunID        string  `json:"run_id"`
	WorkflowID   string  `json:"workflow_id"`
	WorkflowType string  `json:"workflow_type"`
	TaskQueue    string  `json:"task_queue"`
	Attempt      int     `json:"attempt"`
	History      []Event `json:"history"`
}

// CompleteWorkflowTaskRequest returns the commands a workflow step produced.
type CompleteWorkflowTaskRequest struct {
	LeaseToken string    `json:"lease_token"`
	Commands   []Command `json:"commands"`
}

// FailWorkflowTaskRequest reports that the worker could not run the step.
type FailWorkflowTaskRequest struct {
	LeaseToken string `json:"lease_token"`
	Cause      string `json:"cause"`
	Message    string `json:"message,omitempty"`
}

// ActivityTask is leased to a worker.
type ActivityTask struct {
	LeaseToken       string   `json:"lease_token"`
	RunID            string   `json:"run_id"`
	WorkflowID       string   `json:"workflow_id"`
	ScheduledEventID int64    `json:"scheduled_event_id"`
	ActivityType     string   `json:"activity_type"`
	Input            *Payload `json:"input,omitempty"`
	Attempt          int      `json:"attempt"`
	DeadlineMS       int64    `json:"deadline_ms"`
}

// CompleteActivityTaskRequest reports a successful activity.
type CompleteActivityTaskRequest struct {
	LeaseToken string   `json:"lease_token"`
	Identity   string   `json:"identity,omitempty"`
	Result     *Payload `json:"result,omitempty"`
}

// FailActivityTaskRequest reports a failed activity attempt.
type FailActivityTaskRequest struct {
	LeaseToken string  `json:"lease_token"`
	Identity   string  `json:"identity,omitempty"`
	Failure    Failure `json:"failure"`
}

// ErrorBody is the JSON error envelope.
type ErrorBody struct {
	Error ErrorDetail `json:"error"`
}

// ErrorDetail is the content of an error response.
type ErrorDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	RunID   string `json:"run_id,omitempty"`
}
