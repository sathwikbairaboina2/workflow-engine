// Package bench is the load generator behind cmd/wfbench: an in-process server and SDK worker talking
// over real HTTP to a real SQLite file (synchronous=FULL), so every transition pays a durable commit.
package bench

import (
	"context"
	"fmt"
	"net/http/httptest"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/sathwikbairaboina2/workflow-engine/internal/api"
	"github.com/sathwikbairaboina2/workflow-engine/internal/clock"
	"github.com/sathwikbairaboina2/workflow-engine/internal/core"
	"github.com/sathwikbairaboina2/workflow-engine/internal/metrics"
	"github.com/sathwikbairaboina2/workflow-engine/internal/store"
	"github.com/sathwikbairaboina2/workflow-engine/sdk/client"
	"github.com/sathwikbairaboina2/workflow-engine/sdk/worker"
	"github.com/sathwikbairaboina2/workflow-engine/sdk/workflow"
)

// Config sizes a benchmark run.
type Config struct {
	Workflows       int
	Concurrency     int // concurrent starters
	WorkflowPollers int
	ActivityPollers int
	DBPath          string
}

// Result is what a run measured. Everything in it comes from the run; nothing is estimated.
type Result struct {
	Workflows         int     `json:"workflows"`
	Completed         int     `json:"completed"`
	WallS             float64 `json:"wall_s"`
	WorkflowsPerSec   float64 `json:"workflows_per_s"`
	Transitions       int     `json:"transitions"`
	TransitionsPerSec float64 `json:"transitions_per_s"`
	TransitionP50MS   float64 `json:"transition_p50_ms"`
	TransitionP99MS   float64 `json:"transition_p99_ms"`
	WorkflowP50MS     float64 `json:"workflow_e2e_p50_ms"`
	WorkflowP99MS     float64 `json:"workflow_e2e_p99_ms"`
	GoVersion         string  `json:"go_version"`
	GOOS              string  `json:"goos"`
	GOARCH            string  `json:"goarch"`
	NumCPU            int     `json:"num_cpu"`
	Store             string  `json:"store"` // includes the filesystem that held the database
	Note              string  `json:"note"`
	StartedAt         string  `json:"started_at"`
}

// Bench runs one activity and returns its result plus one.
func Bench(ctx workflow.Context, n int) (int, error) {
	var out int
	err := workflow.ExecuteActivity(ctx, workflow.ActivityOptions{StartToClose: 10 * time.Second}, "Noop", n).Get(ctx, &out)
	return out, err
}

// Noop is the benchmark activity.
func Noop(ctx context.Context, n int) (int, error) { return n + 1, nil }

// percentile is the nearest-rank percentile (0-100) of an unsorted slice; 0 when empty.
func percentile(xs []float64, p float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	s := append([]float64(nil), xs...)
	sort.Float64s(s)
	rank := int(p/100*float64(len(s)) + 0.9999999)
	rank = max(1, min(rank, len(s)))
	return s[rank-1]
}

// Run executes the benchmark.
func Run(ctx context.Context, cfg Config) (Result, error) {
	began := time.Now()
	res := Result{Workflows: cfg.Workflows, GoVersion: runtime.Version(), GOOS: runtime.GOOS, GOARCH: runtime.GOARCH,
		NumCPU: runtime.NumCPU(), Store: "sqlite WAL synchronous=FULL, database on " + describeFS(filepath.Dir(cfg.DBPath)),
		Note:      "Docker Desktop on Windows 11; server, workers and load generator in one container",
		StartedAt: began.UTC().Format(time.RFC3339)}
	if cfg.Workflows <= 0 || cfg.Concurrency <= 0 {
		return res, fmt.Errorf("bench: Workflows and Concurrency must be positive")
	}
	st, err := store.Open(cfg.DBPath)
	if err != nil {
		return res, err
	}
	defer st.Close()

	var mu sync.Mutex
	var samples []float64 // transition durations in ms
	engine := core.New(st, clock.Real{}, core.Config{PollTimeout: 2 * time.Second, WorkflowTaskTimeout: 30 * time.Second},
		core.WithMetrics(metrics.New()),
		core.WithObserver(func(_ string, d time.Duration) {
			mu.Lock()
			samples = append(samples, float64(d.Microseconds())/1000)
			mu.Unlock()
		}))
	loopCtx, stopLoops := context.WithCancel(ctx)
	var loops sync.WaitGroup
	loops.Add(1)
	go func() { defer loops.Done(); engine.RunLoops(loopCtx, 20*time.Millisecond, 100*time.Millisecond) }()
	srv := httptest.NewServer(api.New(engine, metrics.New(), 4<<20).Handler())
	defer func() {
		stopLoops()
		loops.Wait()
		srv.CloseClientConnections()
		srv.Close()
	}()

	c := client.New(srv.URL)
	w := worker.New(c, "bench", worker.Options{Identity: "bench", WorkflowPollers: cfg.WorkflowPollers, ActivityPollers: cfg.ActivityPollers})
	w.RegisterWorkflow(Bench)
	w.RegisterActivity(Noop)
	workerCtx, stopWorker := context.WithCancel(ctx)
	workerDone := make(chan struct{})
	go func() { defer close(workerDone); _ = w.Run(workerCtx) }()
	defer func() { stopWorker(); <-workerDone }()

	jobs := make(chan int)
	var wg sync.WaitGroup
	var startErr error
	var errOnce sync.Once
	for i := 0; i < cfg.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := range jobs {
				_, err := c.Start(ctx, client.StartOptions{ID: fmt.Sprintf("bench-%d", n), TaskQueue: "bench"}, Bench, n)
				if err != nil {
					errOnce.Do(func() { startErr = err })
				}
			}
		}()
	}
	for n := 0; n < cfg.Workflows; n++ {
		jobs <- n
	}
	close(jobs)
	wg.Wait()
	if startErr != nil {
		return res, fmt.Errorf("bench: start: %w", startErr)
	}

	deadline := time.Now().Add(5 * time.Minute)
	for {
		var running int
		if err := st.View(ctx, func(tx *store.Tx) error {
			var err error
			running, err = tx.CountRuns("running")
			return err
		}); err != nil {
			return res, err
		}
		if running == 0 {
			break
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			return res, fmt.Errorf("bench: %d workflows still running after the deadline", running)
		}
		time.Sleep(20 * time.Millisecond)
	}

	// End-to-end timings come from the database so they are exact to the millisecond.
	rows, err := st.DB.QueryContext(ctx, `SELECT status, created_at, closed_at FROM workflows`)
	if err != nil {
		return res, err
	}
	defer rows.Close()
	var e2e []float64
	var first, last int64
	for rows.Next() {
		var status string
		var created, closed int64
		if err := rows.Scan(&status, &created, &closed); err != nil {
			return res, err
		}
		if status == "completed" {
			res.Completed++
		}
		e2e = append(e2e, float64(closed-created))
		if first == 0 || created < first {
			first = created
		}
		last = max(last, closed)
	}
	if err := rows.Err(); err != nil {
		return res, err
	}
	res.WallS = float64(last-first) / 1000
	if res.WallS <= 0 {
		res.WallS = 0.001
	}
	mu.Lock()
	defer mu.Unlock()
	res.Transitions = len(samples)
	res.WorkflowsPerSec = float64(res.Completed) / res.WallS
	res.TransitionsPerSec = float64(res.Transitions) / res.WallS
	res.TransitionP50MS, res.TransitionP99MS = percentile(samples, 50), percentile(samples, 99)
	res.WorkflowP50MS, res.WorkflowP99MS = percentile(e2e, 50), percentile(e2e, 99)
	return res, nil
}
