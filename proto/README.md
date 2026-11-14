# `proto/` — Shared Protobuf v3 & gRPC Contracts

Single source of truth for the wire contract between the Go Control Plane ([`control-go`](../control-go/README.md)) and the Rust Sandbox Execution Daemon ([`daemon-rs`](../daemon-rs/README.md)), defined in [`aegis/v1/sandbox.proto`](aegis/v1/sandbox.proto).

---

## Service: `aegis.v1.SandboxWorker`

| RPC | Kind | Description |
| :--- | :--- | :--- |
| `ExecuteTool(ExecutionRequest)` | Server Streaming | Spawns an isolated sandbox and streams `ExecutionEvent` frames (`STARTED`, stdout/stderr chunks, terminal lifecycle event + metrics). |
| `CancelExecution(CancelRequest)` | Unary | Immediately kills the target execution cgroup (`cgroup.kill = 1`) and cleans up its mounts. |
| `StreamWorkerHeartbeat(Empty)` | Server Streaming | Periodic worker capacity and health heartbeat (`active_sandboxes`, `max_sandboxes`, memory pressure, `WorkerStatus`). |

---

## Core Enums & Limits

### `RuntimeType`
- `PYTHON_3_12` -> `/var/lib/aegis/rootfs/python3.12` (`/usr/bin/python3`)
- `NODE_20` -> `/var/lib/aegis/rootfs/node20` (`/usr/bin/node`)
- `BASH_5` -> `/var/lib/aegis/rootfs/bash5` (`/bin/bash`)

### `EventType` Lifecycle State Machine
- **Active**: `STARTED`, `STDOUT_CHUNK`, `STDERR_CHUNK`
- **Terminal**: `COMPLETED`, `OOM_KILLED`, `TIMEOUT_KILLED`, `SYSCALL_VIOLATION`, `INTERNAL_ERROR`

### `ResourceLimits`
- `wall_timeout_ms` (default: `5000`, cap: `60000`)
- `memory_max_bytes` (default: `134217728` / 128 MiB, cap: `1073741824` / 1 GiB)
- `cpu_quota_us` (default: `50000` per 100,000 us period)
- `pids_max` (default: `16`, cap: `64`)
- `tmpfs_size_bytes` (default: `33554432` / 32 MiB)

Every terminal `ExecutionEvent` attaches `ExecutionMetrics`: `exit_code`, `signaled`, `setup_duration_us`, `exec_duration_us`, `teardown_duration_us`, `peak_memory_bytes`, and `cpu_usage_us`.

---

## Code Generation

```bash
# From the repository root (regenerates control-go/pkg/pb/*.pb.go and checks daemon-rs)
make proto
```

- **Go**: Generated into [`control-go/pkg/pb/`](../control-go/pkg/pb) (`package pb`).
- **Rust**: Compiled automatically at build time by [`daemon-rs/build.rs`](../daemon-rs/build.rs) via `tonic-build` and `protoc-bin-vendored`.
