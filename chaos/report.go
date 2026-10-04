// Package chaos is the kill -9 harness: it runs a real wfd and real worker processes, SIGKILLs them on a
// seeded schedule and checks that no workflow is lost and no side effect is applied twice.
package chaos

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"
)

// Config describes one chaos run. Zero fields take the defaults noted below.
type Config struct {
	BinDir, WorkDir, Out string
	Seed                 int64
	Workflows, Kills     int           // defaults 100, 50
	Workers              int           // default 3
	KillEvery            time.Duration // mean gap, default 300ms; each gap uniform in [0.5x, 1.5x]
	ServerKillShare      float64       // default 0.25
	RestartDelay         time.Duration // default 200ms
	ActivityDelay        time.Duration // default 30ms, passed to workers
	DrainTimeout         time.Duration // default 3m
	UnsafeLedger         bool
	Port                 int // default 7233, inside the container only

	// Progress, when set, receives a status line every few seconds.
	Progress io.Writer
}

func (c Config) withDefaults() Config {
	if c.Workflows <= 0 {
		c.Workflows = 100
	}
	if c.Kills < 0 {
		c.Kills = 0
	} else if c.Kills == 0 {
		c.Kills = 50
	}
	if c.Workers <= 0 {
		c.Workers = 3
	}
	if c.KillEvery <= 0 {
		c.KillEvery = 300 * time.Millisecond
	}
	if c.ServerKillShare <= 0 {
		c.ServerKillShare = 0.25
	}
	if c.RestartDelay <= 0 {
		c.RestartDelay = 200 * time.Millisecond
	}
	if c.ActivityDelay <= 0 {
		c.ActivityDelay = 30 * time.Millisecond
	}
	if c.DrainTimeout <= 0 {
		c.DrainTimeout = 3 * time.Minute
	}
	if c.Port <= 0 {
		c.Port = 7233
	}
	return c
}

// Report is the outcome of a run, written as JSON and quoted by the README.
type Report struct {
	Seed                  int64    `json:"seed"`
	UnsafeLedger          bool     `json:"unsafe_ledger"`
	Workflows             int      `json:"workflows"`
	Completed             int      `json:"completed"`
	Failed                int      `json:"failed"`
	Lost                  int      `json:"lost"`
	KillsTotal            int      `json:"kills_total"`
	KillsServer           int      `json:"kills_server"`
	KillsWorker           int      `json:"kills_worker"`
	ActivityExecutions    int      `json:"activity_executions"`
	ActivityReExecutions  int      `json:"activity_re_executions"`
	LedgerRows            int      `json:"ledger_rows"`
	DoubleApplied         int      `json:"double_applied"`
	MissingApplies        int      `json:"missing_applies"`
	ReplayFailures        int      `json:"replay_failures"`
	ConsistencyViolations int      `json:"consistency_violations"`
	UnexpectedExits       int      `json:"unexpected_exits"`
	Violations            []string `json:"violations,omitempty"` // first 20
	ServerRecoveryMSP50   float64  `json:"server_recovery_ms_p50"`
	ServerRecoveryMSMax   float64  `json:"server_recovery_ms_max"`
	DurationS             float64  `json:"duration_s"`
	GoVersion             string   `json:"go_version"`
	NumCPU                int      `json:"num_cpu"`
	StartedAt             string   `json:"started_at"`
	Pass                  bool     `json:"pass"`
}

// computePass applies the pass rule: nothing lost or failed, every replay and consistency check clean,
// every completed workflow applied both legs, and (unless idempotency keys are disabled) nothing applied twice.
func (r Report) computePass() bool {
	return r.Lost == 0 && r.Failed == 0 && r.ReplayFailures == 0 && r.ConsistencyViolations == 0 && r.UnexpectedExits == 0 &&
		r.MissingApplies == 0 && (r.UnsafeLedger || r.DoubleApplied == 0)
}

// Summary is the human-readable block printed at the end of a run.
func (r Report) Summary() string {
	var b strings.Builder
	row := func(label, format string, args ...any) {
		fmt.Fprintf(&b, "  %-24s %s\n", label, fmt.Sprintf(format, args...))
	}
	title := fmt.Sprintf("workflow-engine chaos report (seed %d)", r.Seed)
	reexec := "(absorbed by idempotency keys)"
	if r.UnsafeLedger {
		title = fmt.Sprintf("workflow-engine chaos report (seed %d, CONTROL: idempotency keys disabled)", r.Seed)
		reexec = "(idempotency keys disabled)"
	}
	fmt.Fprintln(&b, title)
	row("workflows started", "%d", r.Workflows)
	row("kill -9 injected", "%d  (server %d, workers %d)", r.KillsTotal, r.KillsServer, r.KillsWorker)
	row("lost workflows", "%d", r.Lost)
	row("double-applied effects", "%d", r.DoubleApplied)
	row("activity re-executions", "%d  %s", r.ActivityReExecutions, reexec)
	row("replay failures", "%d", r.ReplayFailures)
	row("consistency violations", "%d", r.ConsistencyViolations)
	row("server recovery p50", "%.0f ms  (max %.0f ms)", r.ServerRecoveryMSP50, r.ServerRecoveryMSMax)
	result := "PASS"
	switch {
	case !r.Pass:
		result = "FAIL"
	case r.UnsafeLedger:
		result = "PASS  (duplicates are expected in this control run)"
	}
	row("result", "%s", result)
	return b.String()
}

// WriteJSON writes the report, indented, to path.
func (r Report) WriteJSON(path string) error {
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0o644)
}

// percentile returns the nearest-rank percentile (0-100) of xs; 0 for an empty slice.
func percentile(xs []float64, p float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	s := append([]float64(nil), xs...)
	sort.Float64s(s)
	rank := int(p/100*float64(len(s)) + 0.9999999)
	if rank < 1 {
		rank = 1
	}
	if rank > len(s) {
		rank = len(s)
	}
	return s[rank-1]
}
