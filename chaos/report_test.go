package chaos

import "testing"

func TestPassRule(t *testing.T) {
	ok := Report{Workflows: 10, Completed: 10}
	cases := []struct {
		name string
		mut  func(r *Report)
		want bool
	}{
		{"clean", func(r *Report) {}, true},
		{"lost", func(r *Report) { r.Lost = 1 }, false},
		{"failed", func(r *Report) { r.Failed = 1 }, false},
		{"replay failure", func(r *Report) { r.ReplayFailures = 1 }, false},
		{"consistency violation", func(r *Report) { r.ConsistencyViolations = 1 }, false},
		{"unexpected exit", func(r *Report) { r.UnexpectedExits = 1 }, false},
		{"missing apply", func(r *Report) { r.MissingApplies = 1 }, false},
		{"double applied", func(r *Report) { r.DoubleApplied = 1 }, false},
		{"double applied in control run", func(r *Report) { r.DoubleApplied = 5; r.UnsafeLedger = true }, true},
		{"lost in control run", func(r *Report) { r.Lost = 1; r.UnsafeLedger = true }, false},
	}
	for _, c := range cases {
		r := ok
		c.mut(&r)
		if got := r.computePass(); got != c.want {
			t.Errorf("%s: pass = %v, want %v", c.name, got, c.want)
		}
	}
}

func TestSummaryFormat(t *testing.T) {
	r := Report{Seed: 42, Workflows: 500, KillsTotal: 200, KillsServer: 52, KillsWorker: 148, ActivityReExecutions: 37,
		ServerRecoveryMSP50: 41, ServerRecoveryMSMax: 88, Pass: true}
	want := `workflow-engine chaos report (seed 42)
  workflows started        500
  kill -9 injected         200  (server 52, workers 148)
  lost workflows           0
  double-applied effects   0
  activity re-executions   37  (absorbed by idempotency keys)
  replay failures          0
  consistency violations   0
  server recovery p50      41 ms  (max 88 ms)
  result                   PASS
`
	if got := r.Summary(); got != want {
		t.Fatalf("summary mismatch:\n--- got ---\n%s--- want ---\n%s", got, want)
	}
	r.Pass = false
	if got := r.Summary(); got[len(got)-len("FAIL\n"):] != "FAIL\n" {
		t.Fatalf("failing summary should end with FAIL:\n%s", got)
	}
}

func TestPercentile(t *testing.T) {
	xs := []float64{5, 1, 3, 2, 4}
	if p := percentile(xs, 50); p != 3 {
		t.Errorf("p50 = %v", p)
	}
	if p := percentile(xs, 100); p != 5 {
		t.Errorf("p100 = %v", p)
	}
	if p := percentile(nil, 50); p != 0 {
		t.Errorf("empty = %v", p)
	}
	if p := percentile([]float64{7}, 99); p != 7 {
		t.Errorf("single = %v", p)
	}
}
