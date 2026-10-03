// Package core is the deterministic server core: every state transition is one store transaction.
package core

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"time"

	"github.com/sathwikbairaboina2/workflow-engine/internal/clock"
	"github.com/sathwikbairaboina2/workflow-engine/internal/store"
)

// Config tunes the engine. Zero fields take the defaults noted below.
type Config struct {
	WorkflowTaskTimeout time.Duration // default 10s
	PollTimeout         time.Duration // default 20s
	PollInterval        time.Duration // default 250ms; re-check for tasks that became visible by time
	MaxPayloadBytes     int           // default 2 << 20
	MaxHistoryEvents    int64         // default 50000
}

func (c Config) withDefaults() Config {
	if c.WorkflowTaskTimeout <= 0 {
		c.WorkflowTaskTimeout = 10 * time.Second
	}
	if c.PollTimeout <= 0 {
		c.PollTimeout = 20 * time.Second
	}
	if c.PollInterval <= 0 {
		c.PollInterval = 250 * time.Millisecond
	}
	if c.MaxPayloadBytes <= 0 {
		c.MaxPayloadBytes = 2 << 20
	}
	if c.MaxHistoryEvents <= 0 {
		c.MaxHistoryEvents = 50000
	}
	return c
}

// Option customises an Engine.
type Option func(*Engine)

// WithObserver registers a callback invoked after every committed state-changing transition.
func WithObserver(f func(kind string, d time.Duration)) Option {
	return func(e *Engine) { e.obs = f }
}

// Engine executes state transitions against the store.
type Engine struct {
	st     *store.Store
	clk    clock.Clock
	cfg    Config
	notify *notifier
	obs    func(string, time.Duration)
}

// New builds an engine.
func New(st *store.Store, clk clock.Clock, cfg Config, opts ...Option) *Engine {
	e := &Engine{st: st, clk: clk, cfg: cfg.withDefaults(), notify: newNotifier()}
	for _, o := range opts {
		o(e)
	}
	return e
}

// Error codes returned in Error.Code.
const (
	CodeInvalid        = "invalid_request"
	CodeNotFound       = "not_found"
	CodeAlreadyStarted = "workflow_already_started"
	CodeStaleLease     = "stale_lease"
	CodeRunClosed      = "run_closed"
	CodeConflict       = "conflict"
	CodeTooLarge       = "payload_too_large"
)

// Error is returned for every client-visible failure; the API maps Code to an HTTP status.
type Error struct{ Code, Message, RunID string }

func (e *Error) Error() string { return e.Code + ": " + e.Message }

func invalid(msg string) *Error { return &Error{Code: CodeInvalid, Message: msg} }
func staleLease() *Error {
	return &Error{Code: CodeStaleLease, Message: "lease token is unknown, expired or already used"}
}

// transition runs fn in one store transaction named kind. When fn reports changed=true and the commit
// succeeds, it calls the observer with the elapsed time. store.ErrConflict becomes CodeConflict.
func (e *Engine) transition(ctx context.Context, kind string, fn func(tx *store.Tx, now int64) (changed bool, err error)) error {
	begin := time.Now()
	changed := false
	err := e.st.WithTx(ctx, kind, func(tx *store.Tx) error {
		c, err := fn(tx, clock.NowMS(e.clk))
		changed = c
		return err
	})
	if errors.Is(err, store.ErrConflict) {
		return &Error{Code: CodeConflict, Message: "concurrent modification, retry"}
	}
	if err != nil {
		return err
	}
	if changed && e.obs != nil {
		e.obs(kind, time.Since(begin))
	}
	return nil
}

func newID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}

func tooLarge() *Error {
	return &Error{Code: CodeTooLarge, Message: "payload exceeds the configured limit"}
}
