package client

import (
	"errors"
	"fmt"

	"github.com/sathwikbairaboina2/workflow-engine/wire"
)

// APIError is a non-2xx reply from the server.
type APIError struct {
	Status  int
	Code    string
	Message string
	RunID   string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("wfd: %d %s: %s", e.Status, e.Code, e.Message)
}

// IsAlreadyStarted reports whether err means the workflow id already has an open run, and returns that run.
func IsAlreadyStarted(err error) (runID string, ok bool) {
	var ae *APIError
	if errors.As(err, &ae) && ae.Code == "workflow_already_started" {
		return ae.RunID, true
	}
	return "", false
}

// IsStaleLease reports whether err means the task lease expired or was already used.
func IsStaleLease(err error) bool {
	var ae *APIError
	return errors.As(err, &ae) && ae.Code == "stale_lease"
}

// WorkflowFailedError is returned by GetResult for a workflow that closed as failed.
type WorkflowFailedError struct{ Failure wire.Failure }

func (e *WorkflowFailedError) Error() string {
	return fmt.Sprintf("workflow failed: %s: %s", e.Failure.Type, e.Failure.Message)
}

// ErrWorkflowCanceled is returned by GetResult for a workflow that closed as canceled.
var ErrWorkflowCanceled = errors.New("workflow canceled")
