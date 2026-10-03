// Package activity is the API available inside activity code.
package activity

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/sathwikbairaboina2/workflow-engine/wire"
)

// Info describes the running activity attempt.
type Info struct {
	WorkflowID, RunID, ActivityType string
	ScheduledEventID                int64
	Attempt                         int
	// IdempotencyKey is stable across retries of the same scheduled activity. Side effects that honour it
	// (for example a unique-keyed insert) become effectively-once even though activities run at-least-once.
	IdempotencyKey string
	Deadline       time.Time
}

// Key builds the idempotency key for a scheduled activity: RunID + ":" + ScheduledEventID.
func Key(runID string, scheduledEventID int64) string {
	return runID + ":" + strconv.FormatInt(scheduledEventID, 10)
}

type infoKey struct{}

// WithInfo attaches info to ctx; the worker calls it before invoking an activity.
func WithInfo(ctx context.Context, info Info) context.Context {
	return context.WithValue(ctx, infoKey{}, info)
}

// GetInfo returns the Info of the running activity, or a zero Info outside an activity.
func GetInfo(ctx context.Context) Info {
	info, _ := ctx.Value(infoKey{}).(Info)
	return info
}

// Error is an activity failure with an explicit type and retry behaviour.
type Error struct {
	Type         string
	Message      string
	NonRetryable bool
}

func (e *Error) Error() string { return e.Type + ": " + e.Message }

// NewError returns a retryable failure.
func NewError(typ, msg string) error { return &Error{Type: typ, Message: msg} }

// NewNonRetryableError returns a failure that stops retries at once.
func NewNonRetryableError(typ, msg string) error {
	return &Error{Type: typ, Message: msg, NonRetryable: true}
}

// ToFailure converts any error to a wire.Failure (type "GenericError" unless it wraps an *Error).
func ToFailure(err error) wire.Failure {
	var ae *Error
	if errors.As(err, &ae) {
		return wire.Failure{Type: ae.Type, Message: ae.Message, NonRetryable: ae.NonRetryable}
	}
	return wire.Failure{Type: "GenericError", Message: err.Error()}
}
