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

	ExecutionsTotal        *prometheus.CounterVec
	SetupDurationUs        *prometheus.HistogramVec
	ExecutionDurationUs    *prometheus.HistogramVec
	TeardownDurationUs     *prometheus.HistogramVec
	ActiveSandboxes        *prometheus.GaugeVec
	WorkerMemorySaturation *prometheus.GaugeVec
	HTTPRequestsTotal      *prometheus.CounterVec
	IdempotencyOutcomes    *prometheus.CounterVec
	HITLApprovalsTotal     *prometheus.CounterVec
	WorkerFailoversTotal   prometheus.Counter
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
				Help: "Total sandbox executions partitioned by runtime, terminal status, and tenant_id.",
			},
			[]string{"runtime", "status", "tenant_id"},
		),
		SetupDurationUs: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "aegis_sandbox_setup_duration_microseconds",
				Help:    "OverlayFS + cgroup v2 + namespace initialization overhead in microseconds.",
				Buckets: []float64{1000, 2500, 5000, 10000, 20000, 35000, 50000, 100000},
			},
			[]string{"runtime"},
		),
		ExecutionDurationUs: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "aegis_sandbox_exec_duration_microseconds",
				Help:    "User payload runtime duration inside the sandbox in microseconds.",
				Buckets: []float64{1000, 5000, 10000, 50000, 100000, 500000, 1000000, 5000000},
			},
			[]string{"runtime"},
		),
		TeardownDurationUs: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "aegis_sandbox_teardown_duration_microseconds",
				Help:    "Cgroup kill + OverlayFS/tmpfs unmount latency in microseconds.",
				Buckets: []float64{500, 1000, 2500, 5000, 10000, 25000},
			},
			[]string{"runtime"},
		),
		ActiveSandboxes: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "aegis_worker_active_sandboxes",
				Help: "Real-time concurrent active sandboxes reported per worker node.",
			},
			[]string{"worker_node_id"},
		),
		WorkerMemorySaturation: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "aegis_worker_memory_saturation_ratio",
				Help: "Worker host memory saturation ratio [0.0, 1.0] reported per worker node.",
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
		m.SetupDurationUs,
		m.ExecutionDurationUs,
		m.TeardownDurationUs,
		m.ActiveSandboxes,
		m.WorkerMemorySaturation,
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
func (m *Metrics) RecordExecution(status, runtime, tenantID string, setupUs, execUs, teardownUs uint64) {
	if m == nil {
		return
	}
	if tenantID == "" {
		tenantID = "default"
	}
	m.ExecutionsTotal.WithLabelValues(runtime, status, tenantID).Inc()
	if setupUs > 0 {
		m.SetupDurationUs.WithLabelValues(runtime).Observe(float64(setupUs))
	}
	if execUs > 0 {
		m.ExecutionDurationUs.WithLabelValues(runtime).Observe(float64(execUs))
	}
	if teardownUs > 0 {
		m.TeardownDurationUs.WithLabelValues(runtime).Observe(float64(teardownUs))
	}
}

// RecordIdempotentReplay increments the IDEMPOTENT_REPLAY status counter.
func (m *Metrics) RecordIdempotentReplay(runtime, tenantID string) {
	if m == nil {
		return
	}
	if tenantID == "" {
		tenantID = "default"
	}
	m.ExecutionsTotal.WithLabelValues(runtime, "IDEMPOTENT_REPLAY", tenantID).Inc()
}

// SetWorkerHeartbeat updates active sandbox count and memory saturation ratio gauges for a worker node.
func (m *Metrics) SetWorkerHeartbeat(workerNodeID string, active uint32, memUsedPct float64) {
	if m == nil {
		return
	}
	m.ActiveSandboxes.WithLabelValues(workerNodeID).Set(float64(active))
	ratio := memUsedPct / 100.0
	if ratio < 0 {
		ratio = 0
	} else if ratio > 1 {
		ratio = 1
	}
	m.WorkerMemorySaturation.WithLabelValues(workerNodeID).Set(ratio)
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
