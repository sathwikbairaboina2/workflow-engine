package chaos

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sathwikbairaboina2/workflow-engine/examples/transfer"
	"github.com/sathwikbairaboina2/workflow-engine/sdk/client"
)

type harness struct {
	cfg     Config
	url     string
	server  *proc
	workers []*proc
	gens    []int
	genMu   sync.Mutex

	recovery    []float64
	unexpected  []string // processes found dead that the harness did not kill
	killsServer atomic.Int32
	killsWorker atomic.Int32
	closed      atomic.Int32
}

func (h *harness) logf(format string, args ...any) {
	if h.cfg.Progress != nil {
		fmt.Fprintf(h.cfg.Progress, format+"\n", args...)
	}
}

// reviveDead restarts any process that exited on its own. That is a bug in the system under test (or the
// harness), so it is recorded and fails the run, but reviving keeps the rest of the run meaningful.
func (h *harness) reviveDead(ctx context.Context) error {
	if !h.server.alive() {
		h.unexpected = append(h.unexpected, "wfd exited on its own; see wfd.log")
		s, err := h.startServer()
		if err != nil {
			return err
		}
		h.server = s
		if _, err := waitHealthy(ctx, h.url, 30*time.Second); err != nil {
			return err
		}
	}
	for i, w := range h.workers {
		if !w.alive() {
			h.unexpected = append(h.unexpected, fmt.Sprintf("%s exited on its own; see %s.log", w.name, w.name))
			nw, err := h.startWorker(i)
			if err != nil {
				return err
			}
			h.workers[i] = nw
		}
	}
	return nil
}

func (h *harness) startServer() (*proc, error) {
	return startProc(h.cfg.WorkDir, "wfd", filepath.Join(h.cfg.BinDir, "wfd"),
		"--listen", fmt.Sprintf("127.0.0.1:%d", h.cfg.Port), "--db", filepath.Join(h.cfg.WorkDir, "wf.db"),
		"--wft-timeout", "2s", "--poll-timeout", "2s", "--timer-interval", "50ms", "--reaper-interval", "100ms")
}

func (h *harness) startWorker(i int) (*proc, error) {
	h.genMu.Lock()
	gen := h.gens[i]
	h.gens[i]++
	h.genMu.Unlock()
	args := []string{"run", "--server", h.url, "--ledger", filepath.Join(h.cfg.WorkDir, "ledger.db"),
		"--queue", "transfers", "--activity-delay", h.cfg.ActivityDelay.String(),
		"--identity", fmt.Sprintf("w%d-g%d", i, gen)}
	if h.cfg.UnsafeLedger {
		args = append(args, "--unsafe-ledger")
	}
	return startProc(h.cfg.WorkDir, fmt.Sprintf("worker-%d", i), filepath.Join(h.cfg.BinDir, "transfer-worker"), args...)
}

// Run executes one chaos run and returns its report. When cfg.Out is set the report is also written there.
func Run(ctx context.Context, cfg Config) (Report, error) {
	cfg = cfg.withDefaults()
	began := time.Now()
	rep := Report{Seed: cfg.Seed, UnsafeLedger: cfg.UnsafeLedger, Workflows: cfg.Workflows, GoVersion: runtime.Version(),
		NumCPU: runtime.NumCPU(), StartedAt: began.UTC().Format(time.RFC3339)}
	if cfg.WorkDir == "" || cfg.BinDir == "" {
		return rep, fmt.Errorf("chaos: BinDir and WorkDir are required")
	}
	if err := os.RemoveAll(cfg.WorkDir); err != nil {
		return rep, err
	}
	if err := os.MkdirAll(cfg.WorkDir, 0o755); err != nil {
		return rep, err
	}

	h := &harness{cfg: cfg, url: fmt.Sprintf("http://127.0.0.1:%d", cfg.Port), gens: make([]int, cfg.Workers)}
	var err error
	if h.server, err = h.startServer(); err != nil {
		return rep, err
	}
	defer func() {
		h.server.kill()
		for _, w := range h.workers {
			w.kill()
		}
	}()
	if _, err := waitHealthy(ctx, h.url, 30*time.Second); err != nil {
		return rep, err
	}
	for i := 0; i < cfg.Workers; i++ {
		w, err := h.startWorker(i)
		if err != nil {
			return rep, err
		}
		h.workers = append(h.workers, w)
	}

	c := client.New(h.url, client.WithRetry(2*time.Minute))
	ids := make([]string, cfg.Workflows)
	for i := range ids {
		ids[i] = "chaos-" + strconv.FormatInt(cfg.Seed, 10) + "-" + strconv.Itoa(i)
	}

	var violations []string
	var vMu sync.Mutex
	addViolation := func(s string) {
		vMu.Lock()
		violations = append(violations, s)
		vMu.Unlock()
	}

	var wg sync.WaitGroup
	startFailed := make([]bool, cfg.Workflows)
	wg.Add(2)
	go func() { // starter
		defer wg.Done()
		rng := rand.New(rand.NewPCG(uint64(cfg.Seed), 1))
		gap := time.Duration(int64(cfg.Kills) * int64(cfg.KillEvery) / int64(cfg.Workflows))
		for i, id := range ids {
			from := rng.IntN(10)
			to := (from + 1 + rng.IntN(9)) % 10
			in := transfer.Input{From: fmt.Sprintf("acct-%d", from), To: fmt.Sprintf("acct-%d", to), AmountCents: int64(1 + rng.IntN(10000))}
			_, err := c.Start(ctx, client.StartOptions{ID: id, TaskQueue: "transfers"}, "Transfer", in)
			if _, dup := client.IsAlreadyStarted(err); err != nil && !dup {
				startFailed[i] = true
				addViolation(fmt.Sprintf("%s: start failed: %v", id, err))
			}
			select {
			case <-time.After(gap):
			case <-ctx.Done():
				return
			}
		}
	}()
	go func() { // killer
		defer wg.Done()
		h.killLoop(ctx)
	}()
	progressDone := make(chan struct{})
	go h.progressLoop(ctx, progressDone, cfg.Workflows)
	wg.Wait()
	if err := h.reviveDead(ctx); err != nil {
		return rep, err
	}
	rep.UnexpectedExits = len(h.unexpected)
	for _, u := range h.unexpected {
		addViolation(u)
	}

	outcomes := h.drain(ctx, c, ids)
	close(progressDone)
	for i, o := range outcomes {
		switch o.status {
		case "completed":
			rep.Completed++
		case "failed", "canceled":
			rep.Failed++
			addViolation(fmt.Sprintf("%s: closed as %s", o.id, o.status))
		default:
			rep.Lost++
			if !startFailed[i] {
				addViolation(fmt.Sprintf("%s: not closed within %s (status %s)", o.id, cfg.DrainTimeout, o.status))
			}
		}
	}

	failures, msgs := replayAll(ctx, c, ids)
	rep.ReplayFailures = failures
	for _, m := range msgs {
		addViolation(m)
	}

	// Stop everything gracefully before touching the database files.
	for _, w := range h.workers {
		w.stop(10 * time.Second)
	}
	h.server.stop(10 * time.Second)
	more, err := storeChecks(ctx, cfg.WorkDir, outcomes, &rep)
	if err != nil {
		return rep, err
	}
	for _, m := range more {
		addViolation(m)
	}

	rep.KillsServer, rep.KillsWorker = int(h.killsServer.Load()), int(h.killsWorker.Load())
	rep.KillsTotal = rep.KillsServer + rep.KillsWorker
	rep.ServerRecoveryMSP50 = percentile(h.recovery, 50)
	rep.ServerRecoveryMSMax = percentile(h.recovery, 100)
	rep.DurationS = time.Since(began).Seconds()
	if len(violations) > maxViolations {
		violations = violations[:maxViolations]
	}
	rep.Violations = violations
	rep.Pass = rep.computePass()
	if cfg.Out != "" {
		if err := os.MkdirAll(filepath.Dir(cfg.Out), 0o755); err != nil {
			return rep, err
		}
		if err := rep.WriteJSON(cfg.Out); err != nil {
			return rep, err
		}
	}
	return rep, nil
}

// killLoop runs cfg.Kills rounds of: wait a jittered gap, SIGKILL the server or a random worker, wait
// RestartDelay, restart it. Server restarts are timed from spawn until /healthz answers.
func (h *harness) killLoop(ctx context.Context) {
	rng := rand.New(rand.NewPCG(uint64(h.cfg.Seed), 2))
	for round := 0; round < h.cfg.Kills; round++ {
		gap := time.Duration(float64(h.cfg.KillEvery) * (0.5 + rng.Float64()))
		killServer := rng.Float64() < h.cfg.ServerKillShare
		victim := rng.IntN(len(h.workers))
		select {
		case <-time.After(gap):
		case <-ctx.Done():
			return
		}
		if err := h.reviveDead(ctx); err != nil {
			h.logf("revive: %v", err)
			return
		}
		if killServer {
			if h.server.kill() {
				h.killsServer.Add(1)
			}
			time.Sleep(h.cfg.RestartDelay)
			s, err := h.startServer()
			if err != nil {
				h.logf("restart wfd: %v", err)
				return
			}
			h.server = s
			d, err := waitHealthy(ctx, h.url, 30*time.Second)
			if err != nil {
				h.logf("wfd unhealthy after restart: %v", err)
				return
			}
			h.recovery = append(h.recovery, float64(d.Microseconds())/1000)
			continue
		}
		if h.workers[victim].kill() {
			h.killsWorker.Add(1)
		}
		time.Sleep(h.cfg.RestartDelay)
		w, err := h.startWorker(victim)
		if err != nil {
			h.logf("restart worker: %v", err)
			return
		}
		h.workers[victim] = w
	}
}

func (h *harness) progressLoop(ctx context.Context, stop <-chan struct{}, total int) {
	t := time.NewTicker(10 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			h.logf("chaos: kills %d (server %d), workflows closed %d/%d", h.killsServer.Load()+h.killsWorker.Load(), h.killsServer.Load(), h.closed.Load(), total)
		case <-stop:
			return
		case <-ctx.Done():
			return
		}
	}
}

// drain waits until every workflow is closed or the drain timeout passes, and returns one outcome per id.
func (h *harness) drain(ctx context.Context, c *client.Client, ids []string) []outcome {
	out := make([]outcome, len(ids))
	for i, id := range ids {
		out[i] = outcome{id: id, status: "unknown"}
	}
	deadline := time.Now().Add(h.cfg.DrainTimeout)
	for {
		pending := 0
		for i := range out {
			if out[i].status == "completed" || out[i].status == "failed" || out[i].status == "canceled" {
				continue
			}
			d, err := c.Describe(ctx, out[i].id)
			if err != nil {
				pending++
				continue
			}
			out[i].status, out[i].runID = d.Status, d.RunID
			if d.Status == "running" {
				pending++
				continue
			}
			h.closed.Add(1)
			if d.Status == "completed" && d.Result != nil {
				var s string
				if json.Unmarshal(d.Result.Data, &s) == nil {
					out[i].result = s
				}
			}
		}
		if pending == 0 || time.Now().After(deadline) || ctx.Err() != nil {
			return out
		}
		time.Sleep(200 * time.Millisecond)
	}
}
