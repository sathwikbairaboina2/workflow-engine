package retry

import (
	"math"
	"testing"
	"time"

	"github.com/sathwikbairaboina2/workflow-engine/wire"
	"pgregory.net/rapid"
)

func TestNormalizeDefaults(t *testing.T) {
	p := Normalize(wire.RetryPolicy{})
	if p.InitialMS != 1000 || p.Backoff != 2 || p.MaxMS != 100000 || p.MaxAttempts != 0 {
		t.Fatalf("%+v", p)
	}
}

func TestBackoffClosedForm(t *testing.T) {
	p := wire.RetryPolicy{InitialMS: 100, Backoff: 2, MaxMS: 1000}
	want := []int64{100, 200, 400, 800, 1000, 1000}
	for i, w := range want {
		if got := Delay(p, i+1); got != time.Duration(w)*time.Millisecond {
			t.Errorf("attempt %d: %v want %dms", i+1, got, w)
		}
	}
}

func TestNonRetryableByTypeAndFlag(t *testing.T) {
	p := wire.RetryPolicy{InitialMS: 10, NonRetryable: []string{"Fatal"}}
	if r, _ := Next(p, 1, wire.Failure{Type: "Fatal"}); r {
		t.Error("listed type retried")
	}
	if r, _ := Next(p, 1, wire.Failure{Type: "Other", NonRetryable: true}); r {
		t.Error("flagged failure retried")
	}
	if r, d := Next(p, 1, wire.Failure{Type: "Other"}); !r || d != 10*time.Millisecond {
		t.Errorf("retry=%v delay=%v", r, d)
	}
}

func closedForm(p wire.RetryPolicy, attempt int) time.Duration {
	p = Normalize(p)
	ms := float64(p.InitialMS) * math.Pow(p.Backoff, float64(attempt-1))
	ms = math.Min(ms, float64(p.MaxMS))
	return time.Duration(math.Round(ms)) * time.Millisecond
}

func TestRetryPolicyBounds(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		initial := rapid.Int64Range(1, 5000).Draw(t, "initial")
		p := wire.RetryPolicy{
			InitialMS:    initial,
			Backoff:      rapid.Float64Range(1, 4).Draw(t, "backoff"),
			MaxMS:        rapid.Int64Range(initial, 600000).Draw(t, "max"),
			MaxAttempts:  rapid.IntRange(0, 12).Draw(t, "maxAttempts"),
			NonRetryable: []string{"Fatal"},
		}
		type fail struct {
			Type         string
			NonRetryable bool
		}
		fails := rapid.SliceOfN(rapid.Custom(func(t *rapid.T) fail {
			return fail{Type: rapid.SampledFrom([]string{"A", "B", "Fatal"}).Draw(t, "type"), NonRetryable: rapid.Bool().Draw(t, "nr")}
		}), 0, 30).Draw(t, "failures")

		attempt := 1
		var prev time.Duration
		for _, f := range fails {
			retry, delay := Next(p, attempt, wire.Failure{Type: f.Type, NonRetryable: f.NonRetryable})
			stop := f.Type == "Fatal" || f.NonRetryable || (p.MaxAttempts > 0 && attempt >= p.MaxAttempts)
			if retry == stop {
				t.Fatalf("attempt %d fail %+v: retry=%v but stop=%v", attempt, f, retry, stop)
			}
			if !retry {
				return
			}
			if delay != closedForm(p, attempt) {
				t.Fatalf("attempt %d: delay %v != closed form %v", attempt, delay, closedForm(p, attempt))
			}
			if delay > time.Duration(p.MaxMS)*time.Millisecond {
				t.Fatalf("delay %v over max", delay)
			}
			if delay < prev {
				t.Fatalf("delay decreased %v -> %v", prev, delay)
			}
			prev = delay
			attempt++
			if p.MaxAttempts > 0 && attempt > p.MaxAttempts {
				t.Fatalf("attempt %d exceeds max %d", attempt, p.MaxAttempts)
			}
		}
	})
}
