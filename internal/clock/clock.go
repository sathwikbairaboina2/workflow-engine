// Package clock lets every time-dependent component take an injectable clock.
package clock

import (
	"sync"
	"time"
)

// Clock returns the current time.
type Clock interface{ Now() time.Time }

// NowMS returns c.Now() in Unix milliseconds, the unit stored everywhere in the engine.
func NowMS(c Clock) int64 { return c.Now().UnixMilli() }

// Real is the wall clock.
type Real struct{}

// Now returns the current wall-clock time.
func (Real) Now() time.Time { return time.Now() }

// Fake is a manually advanced clock, safe for concurrent use.
type Fake struct {
	mu sync.Mutex
	t  time.Time
}

// NewFake returns a fake clock starting at t.
func NewFake(t time.Time) *Fake { return &Fake{t: t} }

// Now returns the fake time.
func (f *Fake) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.t
}

// Advance moves the fake time forward by d.
func (f *Fake) Advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.t = f.t.Add(d)
}

// Set replaces the fake time.
func (f *Fake) Set(t time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.t = t
}
