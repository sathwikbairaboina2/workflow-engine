package bench

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestBenchSmall(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	res, err := Run(ctx, Config{Workflows: 20, Concurrency: 4, WorkflowPollers: 2, ActivityPollers: 4,
		DBPath: filepath.Join(t.TempDir(), "wf.db")})
	if err != nil {
		t.Fatal(err)
	}
	if res.Completed != 20 {
		t.Fatalf("completed %d of 20", res.Completed)
	}
	if res.Transitions < 20*6 {
		t.Fatalf("transitions %d, want at least %d", res.Transitions, 20*6)
	}
	if res.TransitionsPerSec <= 0 || res.WorkflowsPerSec <= 0 {
		t.Fatalf("rates %+v", res)
	}
	if res.TransitionP99MS < res.TransitionP50MS || res.WorkflowP99MS < res.WorkflowP50MS {
		t.Fatalf("percentiles out of order: %+v", res)
	}
}

func TestPercentileNearestRank(t *testing.T) {
	xs := []float64{10, 20, 30, 40, 50, 60, 70, 80, 90, 100}
	if p := percentile(xs, 50); p != 50 {
		t.Errorf("p50 = %v", p)
	}
	if p := percentile(xs, 99); p != 100 {
		t.Errorf("p99 = %v", p)
	}
	if p := percentile(nil, 99); p != 0 {
		t.Errorf("empty = %v", p)
	}
}
