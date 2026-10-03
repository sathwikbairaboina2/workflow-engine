// Package retry holds the pure activity retry policy.
package retry

import (
	"math"
	"slices"
	"time"

	"github.com/sathwikbairaboina2/workflow-engine/wire"
)

// Normalize fills defaults: InitialMS 1000, Backoff 2.0 (when < 1), MaxMS 100*InitialMS, MaxAttempts 0 = unlimited.
func Normalize(p wire.RetryPolicy) wire.RetryPolicy {
	if p.InitialMS <= 0 {
		p.InitialMS = 1000
	}
	if p.Backoff < 1 {
		p.Backoff = 2
	}
	if p.MaxMS <= 0 {
		p.MaxMS = 100 * p.InitialMS
	}
	return p
}

// Delay returns min(InitialMS * Backoff^(attempt-1), MaxMS) for the attempt that just failed (1-based).
func Delay(p wire.RetryPolicy, attempt int) time.Duration {
	p = Normalize(p)
	if attempt < 1 {
		attempt = 1
	}
	ms := float64(p.InitialMS) * math.Pow(p.Backoff, float64(attempt-1))
	ms = math.Min(ms, float64(p.MaxMS))
	return time.Duration(math.Round(ms)) * time.Millisecond
}

// Next decides whether the failed attempt is retried and after how long.
func Next(p wire.RetryPolicy, attempt int, f wire.Failure) (retry bool, delay time.Duration) {
	p = Normalize(p)
	if f.NonRetryable || slices.Contains(p.NonRetryable, f.Type) {
		return false, 0
	}
	if p.MaxAttempts > 0 && attempt >= p.MaxAttempts {
		return false, 0
	}
	return true, Delay(p, attempt)
}
