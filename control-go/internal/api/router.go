package api

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/aegisrun/aegisrun/control-go/internal/orchestrator"
	"github.com/aegisrun/aegisrun/control-go/internal/store"
	"github.com/aegisrun/aegisrun/control-go/internal/telemetry"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

// RouterDeps groups runtime dependencies required to assemble the HTTP gateway.
type RouterDeps struct {
	Engine      *orchestrator.Engine
	Broker      *store.StreamBroker
	Pool        *orchestrator.WorkerPool
	Metrics     *telemetry.Metrics
	Logger      *slog.Logger
	Postgres    *pgxpool.Pool
	Redis       *redis.Client
	APIKeys     map[string]string
	RateLimiter RateLimiterConfig
}

// NewRouter builds the Go 1.22 net/http ServeMux with auth, rate-limiting, recovery, and metrics middleware.
func NewRouter(deps RouterDeps) http.Handler {
	if deps.Logger == nil {
		deps.Logger = slog.Default()
	}
	if deps.Metrics == nil {
		deps.Metrics = telemetry.NewMetrics()
	}

	mux := http.NewServeMux()
	execHandler := NewExecHandler(deps.Engine, deps.Broker)
	hitlHandler := NewHITLHandler(deps.Engine)

	mux.Handle("GET /healthz", wrapBase("healthz", deps, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if deps.Engine != nil && deps.Engine.IsDraining() {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "draining"})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})))

	mux.Handle("GET /readyz", wrapBase("readyz", deps, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if deps.Engine != nil && deps.Engine.IsDraining() {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "draining"})
			return
		}
		if deps.Postgres != nil {
			if err := deps.Postgres.Ping(r.Context()); err != nil {
				writeJSONError(w, http.StatusServiceUnavailable, "postgres unreachable")
				return
			}
		}
		if deps.Redis != nil {
			if err := deps.Redis.Ping(r.Context()).Err(); err != nil {
				writeJSONError(w, http.StatusServiceUnavailable, "redis unreachable")
				return
			}
		}
		if deps.Pool != nil {
			healthy := 0
			for _, wk := range deps.Pool.Snapshot(time.Now()) {
				if wk.Healthy {
					healthy++
				}
			}
			if healthy == 0 {
				writeJSONError(w, http.StatusServiceUnavailable, "no healthy sandbox workers")
				return
			}
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
	})))

	mux.Handle("GET /metrics", deps.Metrics.Handler())

	mux.Handle("POST /v1/execute", wrapBase("execute", deps,
		BearerAuthMiddleware(deps.APIKeys,
			RateLimitAndConcurrencyMiddleware(deps.RateLimiter, http.HandlerFunc(execHandler.HandleExecute)),
		),
	))

	mux.Handle("GET /v1/executions/{execution_id}", wrapBase("get_execution", deps,
		BearerAuthMiddleware(deps.APIKeys, http.HandlerFunc(execHandler.HandleGetExecution)),
	))

	mux.Handle("GET /v1/executions/{execution_id}/stream", wrapBase("stream_execution", deps,
		BearerAuthMiddleware(deps.APIKeys, http.HandlerFunc(execHandler.HandleStreamExecution)),
	))

	mux.Handle("POST /v1/approvals/{approval_token}/decide", wrapBase("decide_approval", deps,
		http.HandlerFunc(hitlHandler.HandleDecideApproval),
	))

	return mux
}

func wrapBase(routeName string, deps RouterDeps, h http.Handler) http.Handler {
	return RequestIDMiddleware(
		RecoveryMiddleware(deps.Logger,
			MetricsAndLoggingMiddleware(routeName, deps.Metrics, deps.Logger, h),
		),
	)
}

