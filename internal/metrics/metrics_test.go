package metrics

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestMetricsExposed(t *testing.T) {
	m := New()
	m.ObserveTransition("start", 2*time.Millisecond)
	m.LeaseExpired("activity")
	m.ActivityRetry()
	m.WorkflowTaskFailed("panic")
	m.WorkflowClosed("completed")
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	body := rec.Body.String()
	for _, want := range []string{
		`wf_transitions_total{kind="start"} 1`,
		`wf_transition_seconds_count{kind="start"} 1`,
		`wf_lease_expiries_total{kind="activity"} 1`,
		`wf_activity_retries_total 1`,
		`wf_workflow_task_failures_total{cause="panic"} 1`,
		`wf_workflows_closed_total{status="completed"} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("missing %q in:\n%s", want, body)
		}
	}
}

func TestNilMetricsAreNoOps(t *testing.T) {
	var m *Metrics
	m.ObserveTransition("x", time.Second)
	m.LeaseExpired("x")
	m.ActivityRetry()
	m.WorkflowTaskFailed("x")
	m.WorkflowClosed("x")
}
