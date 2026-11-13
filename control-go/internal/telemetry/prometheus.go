package telemetry

import (
	"net/http"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics owns the isolated Prometheus registry and all control-plane metric collectors.
type Metrics struct {
	registry *prometheus.Registry

	ExecutionsTotal       *prometheus.CounterVec
	SetupDurationMs       *prometheus.HistogramVec
	ExecutionDurationMs   *prometheus.HistogramVec
	ActiveSandboxes       *prometheus.GaugeVec
	HTTPRequestsTotal     *prometheus.CounterVec
	IdempotencyOutcomes   *prometheus.CounterVec
	HITLApprovalsTotal    *prometheus.CounterVec
	WorkerFailoversTotal  prometheus.Counter
}

// NewMetrics constructs and registers all AegisRun control-plane Prometheus collectors.
func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	m := &Metrics{
		registry: reg,
		ExecutionsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "aegis_executions_total",
				Help: "Total sandbox executions partitioned by terminal status and runtime.",
			},
			[]string{"status", "runtime"},
		),
		SetupDurationMs: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "aegis_execution_setup_duration_ms",
				Help:    "Sandbox cold-start setup overhead reported by workers in milliseconds.",
				Buckets: []float64{1, 2.5, 5, 10, 15, 20, 25, 35, 50, 100, 250},
			},
			[]string{"runtime"},
		),
		ExecutionDurationMs: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "aegis_execution_duration_ms",
				Help:    "User payload execution wall-clock duration in milliseconds.",
				Buckets: []float64{5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 30000},
			},
			[]string{"runtime"},
		),
		ActiveSandboxes: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "aegis_control_active_sandboxes",
				Help: "Current active sandbox count reported by each worker node heartbeat.",
			},
			[]string{"worker_node_id"},
		),
		HTTPRequestsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "aegis_http_requests_total",
				Help: "Total HTTP API requests handled by route and status code.",
			},
			[]string{"method", "route", "status_code"},
		),
		IdempotencyOutcomes: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "aegis_idempotency_outcomes_total",
				Help: "Idempotency lock outcomes (fresh, piggyback, cached_replay, error).",
			},
			[]string{"outcome"},
		),
		HITLApprovalsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "aegis_hitl_approvals_total",
				Help: "HITL approval transitions (required, approved, rejected, expired).",
			},
			[]string{"outcome"},
		),
		WorkerFailoversTotal: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "aegis_worker_failovers_total",
				Help: "Total pre-STARTED worker failover retries performed by the orchestrator.",
			},
		),
	}

	reg.MustRegister(
		m.ExecutionsTotal,
		m.SetupDurationMs,
		m.ExecutionDurationMs,
		m.ActiveSandboxes,
		m.HTTPRequestsTotal,
		m.IdempotencyOutcomes,
		m.HITLApprovalsTotal,
		m.WorkerFailoversTotal,
	)

	return m
}

// Handler returns the Prometheus HTTP scrape handler for `/metrics`.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
	})
}

// Registry exposes the underlying Prometheus registry for assertions or custom scraping.
func (m *Metrics) Registry() *prometheus.Registry {
	return m.registry
}

// RecordExecution observes execution completion metrics in Prometheus.
func (m *Metrics) RecordExecution(status, runtime string, setupUs, execUs uint64) {
	if m == nil {
		return
	}
	m.ExecutionsTotal.WithLabelValues(status, runtime).Inc()
	if setupUs > 0 {
		m.SetupDurationMs.WithLabelValues(runtime).Observe(float64(setupUs) / 1000.0)
	}
	if execUs > 0 {
		m.ExecutionDurationMs.WithLabelValues(runtime).Observe(float64(execUs) / 1000.0)
	}
}

// SetWorkerActiveSandboxes updates the active sandbox gauge for a worker node.
func (m *Metrics) SetWorkerActiveSandboxes(workerNodeID string, active uint32) {
	if m == nil {
		return
	}
	m.ActiveSandboxes.WithLabelValues(workerNodeID).Set(float64(active))
}

// RecordHTTPRequest increments the HTTP request counter.
func (m *Metrics) RecordHTTPRequest(method, route string, statusCode int) {
	if m == nil {
		return
	}
	m.HTTPRequestsTotal.WithLabelValues(method, route, strconv.Itoa(statusCode)).Inc()
}

// RecordIdempotency increments the idempotency outcome counter.
func (m *Metrics) RecordIdempotency(outcome string) {
	if m == nil {
		return
	}
	m.IdempotencyOutcomes.WithLabelValues(outcome).Inc()
}

// RecordHITL increments the HITL outcome counter.
func (m *Metrics) RecordHITL(outcome string) {
	if m == nil {
		return
	}
	m.HITLApprovalsTotal.WithLabelValues(outcome).Inc()
}
