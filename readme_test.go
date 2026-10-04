package workflowengine_test

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
)

func mustJSON(t *testing.T, path string, v any) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("%s: %v (run scripts/chaos.sh and scripts/bench.sh to produce it)", path, err)
	}
	if err := json.Unmarshal(b, v); err != nil {
		t.Fatalf("%s: %v", path, err)
	}
}

// TestREADMEHeadline keeps the README headline tied to measured results.
func TestREADMEHeadline(t *testing.T) {
	var safe, control struct {
		Workflows            int  `json:"workflows"`
		KillsTotal           int  `json:"kills_total"`
		KillsServer          int  `json:"kills_server"`
		Lost                 int  `json:"lost"`
		DoubleApplied        int  `json:"double_applied"`
		ActivityReExecutions int  `json:"activity_re_executions"`
		UnsafeLedger         bool `json:"unsafe_ledger"`
		Pass                 bool `json:"pass"`
		Seed                 int  `json:"seed"`
	}
	var bench struct {
		TransitionsPerSec float64 `json:"transitions_per_s"`
		TransitionP99MS   float64 `json:"transition_p99_ms"`
	}
	mustJSON(t, "bench/results/chaos-latest.json", &safe)
	mustJSON(t, "bench/results/chaos-control.json", &control)
	mustJSON(t, "bench/results/bench-latest.json", &bench)
	if safe.UnsafeLedger || !control.UnsafeLedger {
		t.Fatal("chaos-latest.json must be the safe run and chaos-control.json the --unsafe-ledger run")
	}
	if !safe.Pass {
		t.Fatal("chaos-latest.json records a failed run")
	}
	if safe.Seed != control.Seed || safe.Workflows != control.Workflows {
		t.Fatalf("control run must use the same seed and size: safe seed %d/%d workflows, control seed %d/%d workflows",
			safe.Seed, safe.Workflows, control.Seed, control.Workflows)
	}
	want := fmt.Sprintf("**%d lost workflows and %d double-applied side effects across %d `kill -9`s (%d of the server) over %d workflows; %d activity re-executions were absorbed by idempotency keys. With the keys disabled, the same seed double-applied %d. %.0f transitions/s on SQLite (WAL, synchronous=FULL, database on tmpfs), p99 %.1f ms per transition.**",
		safe.Lost, safe.DoubleApplied, safe.KillsTotal, safe.KillsServer, safe.Workflows, safe.ActivityReExecutions,
		control.DoubleApplied, bench.TransitionsPerSec, bench.TransitionP99MS)
	readme, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(readme), want) {
		t.Fatalf("README headline is not the measured one; put this line in README.md:\n%s", want)
	}
}
