// Package metrics exposes the engine's Prometheus metrics. Every method is safe on a nil *Metrics.
package metrics

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics holds the registry and the collectors.
type Metrics struct {
	reg             *prometheus.Registry
	transitions     *prometheus.CounterVec
	transitionTime  *prometheus.HistogramVec
	leaseExpiries   *prometheus.CounterVec
	activityRetries prometheus.Counter
	wftFailures     *prometheus.CounterVec
	workflowsClosed *prometheus.CounterVec
}

// New builds a Metrics with its own registry.
func New() *Metrics {
	m := &Metrics{reg: prometheus.NewRegistry()}
	m.transitions = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "wf_transitions_total", Help: "Committed state transitions by kind."}, []string{"kind"})
	m.transitionTime = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "wf_transition_seconds", Help: "Wall time of committed transitions by kind.",
		Buckets: []float64{0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 1}}, []string{"kind"})
	m.leaseExpiries = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "wf_lease_expiries_total", Help: "Expired task leases reaped, by task kind."}, []string{"kind"})
	m.activityRetries = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "wf_activity_retries_total", Help: "Activity attempts that were retried by the server."})
	m.wftFailures = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "wf_workflow_task_failures_total", Help: "Failed workflow tasks by cause."}, []string{"cause"})
	m.workflowsClosed = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "wf_workflows_closed_total", Help: "Closed workflow runs by final status."}, []string{"status"})
	m.reg.MustRegister(m.transitions, m.transitionTime, m.leaseExpiries, m.activityRetries, m.wftFailures, m.workflowsClosed)
	return m
}

// ObserveTransition records one committed transition.
func (m *Metrics) ObserveTransition(kind string, d time.Duration) {
	if m == nil {
		return
	}
	m.transitions.WithLabelValues(kind).Inc()
	m.transitionTime.WithLabelValues(kind).Observe(d.Seconds())
}

// LeaseExpired counts a reaped lease of the given task kind.
func (m *Metrics) LeaseExpired(kind string) {
	if m != nil {
		m.leaseExpiries.WithLabelValues(kind).Inc()
	}
}

// ActivityRetry counts a server-side activity retry.
func (m *Metrics) ActivityRetry() {
	if m != nil {
		m.activityRetries.Inc()
	}
}

// WorkflowTaskFailed counts a failed workflow task.
func (m *Metrics) WorkflowTaskFailed(cause string) {
	if m != nil {
		m.wftFailures.WithLabelValues(cause).Inc()
	}
}

// WorkflowClosed counts a closed run.
func (m *Metrics) WorkflowClosed(status string) {
	if m != nil {
		m.workflowsClosed.WithLabelValues(status).Inc()
	}
}

// Handler serves the registry in the Prometheus text format.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{})
}
