package core

import (
	"context"
	"log/slog"
	"time"
)

// RunLoops runs the timer and lease-reaper loops until ctx ends.
func (e *Engine) RunLoops(ctx context.Context, timerEvery, reapEvery time.Duration) {
	timers := time.NewTicker(timerEvery)
	reaper := time.NewTicker(reapEvery)
	defer timers.Stop()
	defer reaper.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timers.C:
			if _, err := e.FireDueTimers(ctx); err != nil && ctx.Err() == nil {
				slog.Error("fire timers", "err", err)
			}
		case <-reaper.C:
			if _, err := e.ReapExpiredLeases(ctx); err != nil && ctx.Err() == nil {
				slog.Error("reap leases", "err", err)
			}
		}
	}
}
