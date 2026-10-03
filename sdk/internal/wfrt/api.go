package wfrt

import (
	"errors"
	"fmt"
	"time"

	"github.com/sathwikbairaboina2/workflow-engine/sdk/internal/naming"
	"github.com/sathwikbairaboina2/workflow-engine/wire"
)

// Context is the deterministic workflow context. Only this runtime implements it.
type Context interface{ env() *env }

// Future is the eventual result of an activity or timer.
type Future interface {
	Get(ctx Context, valuePtr any) error
	IsReady() bool
}

// ReceiveChannel delivers the payloads of one named signal in arrival order.
type ReceiveChannel interface {
	// Receive blocks for the next payload; false only when the workflow is canceled and the queue is empty.
	Receive(ctx Context, valuePtr any) (ok bool)
	// ReceiveAsync takes a payload if one is queued.
	ReceiveAsync(valuePtr any) (ok bool)
}

// Selector waits for the first of several futures or channels.
type Selector interface {
	AddFuture(f Future, fn func(Future)) Selector
	AddReceive(ch ReceiveChannel, fn func(ReceiveChannel)) Selector
	// Select runs the first ready case in the order added. ErrCanceled if canceled and none is ready.
	Select(ctx Context) error
}

// RetryPolicy controls server-side activity retries.
type RetryPolicy struct {
	Initial      time.Duration
	Backoff      float64
	Max          time.Duration
	MaxAttempts  int
	NonRetryable []string
}

// ActivityOptions configure one activity call.
type ActivityOptions struct {
	TaskQueue    string
	StartToClose time.Duration // default 10s
	Retry        RetryPolicy
}

// Encoded is a recorded value that can be decoded later.
type Encoded struct{ p *wire.Payload }

// Get decodes the value into valuePtr.
func (e Encoded) Get(valuePtr any) error { return e.p.Decode(valuePtr) }

// ActivityError is returned by Future.Get when the activity ended in failure or timeout.
type ActivityError struct {
	ActivityType string
	Failure      wire.Failure
	TimedOut     bool
}

func (e *ActivityError) Error() string {
	if e.TimedOut {
		return fmt.Sprintf("activity %s timed out", e.ActivityType)
	}
	return fmt.Sprintf("activity %s failed: %s: %s", e.ActivityType, e.Failure.Type, e.Failure.Message)
}

// ErrCanceled is returned by blocking calls once cancellation has been requested.
var ErrCanceled = errors.New("workflow: canceled")

type future struct {
	e            *env
	activityType string
	ready        bool
	val          *wire.Payload
	err          error
}

func readyFuture(e *env, err error) *future { return &future{e: e, ready: true, err: err} }

func (f *future) IsReady() bool { return f.ready }

func (f *future) Get(ctx Context, valuePtr any) error {
	e := ctx.env()
	e.disp.block(func() bool { return f.ready || e.canceled })
	if !f.ready {
		return ErrCanceled
	}
	if f.err != nil {
		return f.err
	}
	if valuePtr == nil {
		return nil
	}
	return f.val.Decode(valuePtr)
}

type channel struct {
	e *env
	q []*wire.Payload
}

func (c *channel) pop(valuePtr any) bool {
	p := c.q[0]
	c.q = c.q[1:]
	if valuePtr != nil {
		_ = p.Decode(valuePtr)
	}
	return true
}

func (c *channel) Receive(ctx Context, valuePtr any) bool {
	e := ctx.env()
	e.disp.block(func() bool { return len(c.q) > 0 || e.canceled })
	if len(c.q) == 0 {
		return false
	}
	return c.pop(valuePtr)
}

func (c *channel) ReceiveAsync(valuePtr any) bool {
	if len(c.q) == 0 {
		return false
	}
	return c.pop(valuePtr)
}

type selectCase struct {
	fut   Future
	futFn func(Future)
	ch    ReceiveChannel
	chFn  func(ReceiveChannel)
	fired bool
}

type selector struct{ cases []*selectCase }

// NewSelector returns an empty selector.
func NewSelector(ctx Context) Selector { return &selector{} }

func (s *selector) AddFuture(f Future, fn func(Future)) Selector {
	s.cases = append(s.cases, &selectCase{fut: f, futFn: fn})
	return s
}

func (s *selector) AddReceive(ch ReceiveChannel, fn func(ReceiveChannel)) Selector {
	s.cases = append(s.cases, &selectCase{ch: ch, chFn: fn})
	return s
}

func (c *selectCase) ready() bool {
	if c.fut != nil {
		return !c.fired && c.fut.IsReady()
	}
	return len(c.ch.(*channel).q) > 0
}

func (s *selector) first() *selectCase {
	for _, c := range s.cases {
		if c.ready() {
			return c
		}
	}
	return nil
}

func (s *selector) Select(ctx Context) error {
	e := ctx.env()
	e.disp.block(func() bool { return s.first() != nil || e.canceled })
	c := s.first()
	if c == nil {
		return ErrCanceled
	}
	if c.fut != nil {
		c.fired = true
		c.futFn(c.fut)
	} else {
		c.chFn(c.ch)
	}
	return nil
}

// ExecuteActivity schedules an activity. activity is a registered name or the function itself.
func ExecuteActivity(ctx Context, opts ActivityOptions, activity any, input any) Future {
	e := ctx.env()
	name := naming.Name(activity)
	if e.canceled {
		return readyFuture(e, ErrCanceled)
	}
	p, err := wire.Encode(input)
	if err != nil {
		return readyFuture(e, err)
	}
	stc := opts.StartToClose
	if stc <= 0 {
		stc = 10 * time.Second
	}
	seq := e.nextSeq()
	e.emit(wire.Command{Type: wire.ScheduleActivity, Seq: seq, ActivityType: name, TaskQueue: opts.TaskQueue, Input: p,
		StartToCloseMS: stc.Milliseconds(),
		RetryPolicy: &wire.RetryPolicy{InitialMS: opts.Retry.Initial.Milliseconds(), Backoff: opts.Retry.Backoff,
			MaxMS: opts.Retry.Max.Milliseconds(), MaxAttempts: opts.Retry.MaxAttempts, NonRetryable: opts.Retry.NonRetryable}})
	f := &future{e: e, activityType: name}
	e.activities[seq] = f
	return f
}

// NewTimer starts a durable timer. A non-positive duration is ready immediately.
func NewTimer(ctx Context, d time.Duration) Future {
	e := ctx.env()
	if d <= 0 {
		return readyFuture(e, nil)
	}
	seq := e.nextSeq()
	e.emit(wire.Command{Type: wire.StartTimer, Seq: seq, DurationMS: d.Milliseconds()})
	f := &future{e: e}
	e.timers[seq] = f
	return f
}

// Sleep waits on a durable timer.
func Sleep(ctx Context, d time.Duration) error { return NewTimer(ctx, d).Get(ctx, nil) }

// Now returns the time of the current step: the WorkflowTaskStarted timestamp, identical on every replay.
func Now(ctx Context) time.Time { return ctx.env().now }

// SideEffect runs fn once and records the value; replays return the recorded value without calling fn.
func SideEffect(ctx Context, fn func() any) Encoded {
	e := ctx.env()
	seq := e.nextSeq()
	v, ok := e.markers[seq]
	if !ok {
		v = wire.MustEncode(fn())
	}
	e.emit(wire.Command{Type: wire.RecordMarker, Seq: seq, MarkerKind: "side_effect", Value: v})
	return Encoded{p: v}
}

// GetSignalChannel returns the channel for the named signal.
func GetSignalChannel(ctx Context, name string) ReceiveChannel { return ctx.env().channel(name) }

// Go starts a deterministic coroutine.
func Go(ctx Context, fn func(ctx Context)) {
	ctx.env().disp.spawn(func() { fn(ctx) })
}

// IsCanceled reports whether cancellation has been requested.
func IsCanceled(ctx Context) bool { return ctx.env().canceled }
