# `control-go/` — Control Plane API Gateway & Orchestrator

`control-go` is AegisRun's Tier 2 orchestration service built with Go (`net/http`, `pgx/v5`, `go-redis/v9`, gRPC, Prometheus). It authenticates callers, enforces atomic per-tenant rate/concurrency limits and idempotency, manages Human-in-the-Loop (HITL) approvals, persists audit records in PostgreSQL, and schedules sandboxes across `daemon-rs` workers.

---

## HTTP API Endpoints

| Method & Path | Auth | Description |
| :--- | :--- | :--- |
| `GET /healthz` | Public | Liveness check (`503` while draining on shutdown). |
| `GET /readyz` | Public | Readiness check verifying PostgreSQL, Redis, and at least 1 healthy worker. |
| `GET /metrics` | Public | Prometheus scrape endpoint (`aegis_*` and Go runtime metrics). |
| `POST /v1/execute` | Bearer | Execute a tool synchronously (`X-Execution-Mode: sync`) or via SSE (`stream` / `Accept: text/event-stream`). Supports idempotency piggybacking/replay (`X-Aegis-Idempotent-Replay`). |
| `GET /v1/executions/{execution_id}` | Bearer | Fetch stored execution state and metrics. |
| `GET /v1/executions/{execution_id}/stream` | Bearer | Subscribe to live/replayed SSE execution frames via Redis Pub/Sub. |
| `POST /v1/approvals/{approval_token}/decide` | Token | Approve or reject an `AWAITING_APPROVAL` execution via its hashed HITL token. |

---

## Internal Packages

- `internal/config`: Loads `Config` via `LoadFromEnv()`.
- `internal/idempotency`: SHA-256 request hashing (`tenant_id`, `session_id`, `tool_name`, `runtime`, `entrypoint_code`, canonical `args`) and atomic Redis Lua lock state machine (`ACQUIRED`, `IN_FLIGHT`, `COMPLETED`).
- `internal/api`: Go 1.22 `ServeMux` routing (`router.go`), Bearer API-key auth + fused atomic Lua token-bucket & concurrency middleware (`middleware.go`), and sync/SSE handlers (`handlers_exec.go`, `handlers_hitl.go`).
- `internal/hitl`: Generates cryptographic HITL approval tokens, dispatches signed JSON webhooks with retries (`Notifier`), and expires timed-out approvals on a 30s ticker.
- `internal/orchestrator`: Tracks live worker health from `StreamWorkerHeartbeat` (`WorkerPool`), selects least-loaded workers using `(ActiveSandboxes/MaxSandboxes)*0.7 + (MemoryUsedPct/100)*0.3` (`Scheduler`), and drives gRPC failover retries (`Engine`).
- `internal/store`: PostgreSQL persistence (`pgx/v5`) and Redis Pub/Sub SSE frame broker (`StreamBroker`).
- `internal/telemetry`: Registers `aegis_executions_total`, `aegis_sandbox_*_duration_microseconds`, `aegis_worker_*`, `aegis_http_requests_total`, `aegis_idempotency_outcomes_total`, `aegis_hitl_approvals_total`, and `aegis_worker_failovers_total`.

---

## Configuration

| Environment Variable | Default | Description |
| :--- | :--- | :--- |
| `AEGIS_HTTP_ADDR` | `:8080` | HTTP gateway listen address |
| `AEGIS_POSTGRES_DSN` | `postgres://aegis:aegis@localhost:5432/aegisrun?sslmode=disable` | PostgreSQL connection string |
| `AEGIS_REDIS_ADDR` | `localhost:6379` | Redis / Valkey address |
| `AEGIS_REDIS_PASSWORD` | `""` | Optional Redis password |
| `AEGIS_REDIS_DB` | `0` | Redis database index |
| `AEGIS_WORKERS` | `worker-1=localhost:50051` | Comma-separated `worker_id=host:port` gRPC targets |
| `AEGIS_API_KEYS` | `aegis-dev-token:default` | Comma-separated `token:tenant_id` entries |
| `AEGIS_RATE_LIMIT_RPS` | `50` | Token-bucket refill rate per tenant |
| `AEGIS_RATE_LIMIT_BURST` | `100` | Token-bucket burst capacity per tenant |
| `AEGIS_MAX_ACTIVE_PER_TENANT` | `32` | Max concurrent in-flight executions per tenant |
| `AEGIS_HEARTBEAT_TIMEOUT` | `5s` | Worker heartbeat staleness threshold |
| `AEGIS_APPROVAL_TTL` | `5m` | HITL approval token lifetime |
| `AEGIS_APPROVAL_REAP_INTERVAL` | `30s` | Background HITL reaper interval |
| `AEGIS_SHUTDOWN_TIMEOUT` | `30s` | Graceful HTTP + worker drain timeout |

---

## Build, Vet & Race Tests

```bash
cd control-go
go vet ./...
go test -v -race ./...
go build -o bin/aegis-control ./cmd/control
```
