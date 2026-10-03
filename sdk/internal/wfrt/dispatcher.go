// Package wfrt is the deterministic workflow runtime: a cooperative coroutine dispatcher plus the replay engine.
package wfrt

import (
	"fmt"
	"runtime/debug"
)

type resumeMsg int

const (
	resumeRun resumeMsg = iota
	resumeExit
)

type yieldMsg struct {
	progressed bool
	done       bool
	panicVal   any
	stack      []byte
}

type coroutine struct {
	resume chan resumeMsg
	yield  chan yieldMsg
	done   bool
}

// exitSignal unwinds a parked coroutine when the dispatcher closes.
type exitSignal struct{}

// PanicError reports a panic raised by workflow code.
type PanicError struct {
	Value any
	Stack string
}

func (e *PanicError) Error() string { return fmt.Sprintf("workflow panic: %v", e.Value) }

// dispatcher runs coroutines strictly one at a time, in creation order, so workflow code that uses
// several coroutines behaves identically on every replay. Only one goroutine ever runs at once, and the
// channel hand-offs give the happens-before edges, so workflow state needs no locks.
type dispatcher struct {
	coros []*coroutine
	cur   *coroutine
}

func (d *dispatcher) spawn(fn func()) {
	c := &coroutine{resume: make(chan resumeMsg), yield: make(chan yieldMsg)}
	d.coros = append(d.coros, c)
	go func() {
		defer func() {
			r := recover()
			switch r.(type) {
			case nil:
				c.yield <- yieldMsg{done: true, progressed: true}
			case exitSignal:
				c.yield <- yieldMsg{done: true}
			default:
				c.yield <- yieldMsg{done: true, panicVal: r, stack: debug.Stack()}
			}
		}()
		if <-c.resume == resumeExit {
			panic(exitSignal{})
		}
		fn()
	}()
}

// block parks the running coroutine until cond holds. Only SDK blocking calls use it.
func (d *dispatcher) block(cond func() bool) {
	c := d.cur
	progressed := true
	for !cond() {
		c.yield <- yieldMsg{progressed: progressed}
		if <-c.resume == resumeExit {
			panic(exitSignal{})
		}
		progressed = false
	}
}

// runUntilBlocked resumes coroutines in creation order (including ones spawned during the pass)
// until a full pass makes no progress or stop() is true.
func (d *dispatcher) runUntilBlocked(stop func() bool) error {
	for {
		progressed := false
		for i := 0; i < len(d.coros); i++ {
			c := d.coros[i]
			if c.done {
				continue
			}
			d.cur = c
			c.resume <- resumeRun
			m := <-c.yield
			d.cur = nil
			c.done = m.done
			if m.panicVal != nil {
				return &PanicError{Value: m.panicVal, Stack: string(m.stack)}
			}
			if m.progressed {
				progressed = true
			}
			if stop() {
				return nil
			}
		}
		if !progressed {
			return nil
		}
	}
}

// close unwinds every unfinished coroutine and waits for each goroutine to exit.
func (d *dispatcher) close() {
	for _, c := range d.coros {
		if !c.done {
			c.resume <- resumeExit
			<-c.yield
			c.done = true
		}
	}
}
