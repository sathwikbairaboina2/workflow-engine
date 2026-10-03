package wfrt

import (
	"time"

	"github.com/sathwikbairaboina2/workflow-engine/wire"
)

// env is the state of one replay: everything workflow code can observe or emit. It is only ever touched by
// the coroutine the dispatcher is currently running, so it needs no locking.
type env struct {
	disp       *dispatcher
	seq        int64
	cmds       []wire.Command
	now        time.Time
	markers    map[int64]*wire.Payload
	activities map[int64]*future
	timers     map[int64]*future
	eventToSeq map[int64]int64
	signals    map[string]*channel
	canceled   bool
	rootDone   bool
	closeCmd   *wire.Command
	taskQueue  string
}

func newEnv() *env {
	return &env{
		disp:       &dispatcher{},
		markers:    map[int64]*wire.Payload{},
		activities: map[int64]*future{},
		timers:     map[int64]*future{},
		eventToSeq: map[int64]int64{},
		signals:    map[string]*channel{},
	}
}

func (e *env) nextSeq() int64 {
	e.seq++
	return e.seq
}

func (e *env) emit(c wire.Command) { e.cmds = append(e.cmds, c) }

func (e *env) channel(name string) *channel {
	ch, ok := e.signals[name]
	if !ok {
		ch = &channel{e: e}
		e.signals[name] = ch
	}
	return ch
}

// wctx is the only implementation of Context.
type wctx struct{ e *env }

func (c wctx) env() *env { return c.e }
