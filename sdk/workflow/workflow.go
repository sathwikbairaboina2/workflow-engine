// Package workflow is the API available inside workflow code. Everything here is deterministic: the
// same history always produces the same commands. Use cmd/wfcheck to catch accidental nondeterminism.
package workflow

import (
	"time"

	"github.com/sathwikbairaboina2/workflow-engine/sdk/internal/wfrt"
)

// Context is the deterministic workflow context; it is the first parameter of every workflow function.
type Context = wfrt.Context

// Future is the eventual result of an activity or timer.
type Future = wfrt.Future

// ReceiveChannel delivers the payloads of one named signal.
type ReceiveChannel = wfrt.ReceiveChannel

// Selector waits for the first of several futures or channels.
type Selector = wfrt.Selector

// RetryPolicy controls server-side activity retries.
type RetryPolicy = wfrt.RetryPolicy

// ActivityOptions configure one activity call.
type ActivityOptions = wfrt.ActivityOptions

// Encoded is a recorded value that can be decoded later.
type Encoded = wfrt.Encoded

// ActivityError is returned by Future.Get when an activity failed or timed out.
type ActivityError = wfrt.ActivityError

// ErrCanceled is returned by blocking calls once cancellation has been requested.
var ErrCanceled = wfrt.ErrCanceled

// ExecuteActivity schedules an activity, given by registered name or by function.
func ExecuteActivity(ctx Context, opts ActivityOptions, activity any, input any) Future {
	return wfrt.ExecuteActivity(ctx, opts, activity, input)
}

// NewTimer starts a durable timer.
func NewTimer(ctx Context, d time.Duration) Future { return wfrt.NewTimer(ctx, d) }

// Sleep waits on a durable timer.
func Sleep(ctx Context, d time.Duration) error { return wfrt.Sleep(ctx, d) }

// Now returns the deterministic time of the current workflow step.
func Now(ctx Context) time.Time { return wfrt.Now(ctx) }

// SideEffect runs fn once, records its value in history and returns the recorded value on replay.
func SideEffect(ctx Context, fn func() any) Encoded { return wfrt.SideEffect(ctx, fn) }

// GetSignalChannel returns the channel receiving the named signal.
func GetSignalChannel(ctx Context, name string) ReceiveChannel {
	return wfrt.GetSignalChannel(ctx, name)
}

// Go starts a deterministic coroutine; use it instead of the go statement.
func Go(ctx Context, fn func(ctx Context)) { wfrt.Go(ctx, fn) }

// NewSelector returns an empty selector; use it instead of the select statement.
func NewSelector(ctx Context) Selector { return wfrt.NewSelector(ctx) }

// IsCanceled reports whether cancellation has been requested.
func IsCanceled(ctx Context) bool { return wfrt.IsCanceled(ctx) }
