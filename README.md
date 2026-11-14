# AegisRun

**AegisRun** is a distributed AI agent sandbox and tool-calling orchestrator engineered to execute untrusted LLM-generated code and tool invocations inside isolated Linux micro-environments without Docker runtime overhead.

AegisRun achieves a **21.45 ms** end-to-end sequential P50 latency (**28.0x faster** than `docker run` with `runc` at `600.52 ms`), **555 us** P50 kernel isolation setup latency, and **3.24 MiB** P50 peak per-sandbox memory footprint.

---

## Architecture Overview

AegisRun is structured as a strict polyglot monorepo driven by a single Protobuf v3 contract (`proto/aegis/v1/sandbox.proto`):

1. **`sdk-py/` (Python 3.11+)**: Developer SDK featuring `@sandbox_tool`, AST decorator stripping so sandbox code runs with zero `aegis_sdk` dependency inside rootfs, OpenAI/Anthropic JSON schema generators (`to_openai_tool()`, `to_anthropic_tool()`), `AegisClient` / `AsyncAegisClient`, and LLM self-correction `.to_llm_observation()` exceptions.
2. **`control-go/` (Go 1.22+)**: High-concurrency REST/SSE API gateway (`POST /v1/execute`), atomic Redis Lua token-bucket rate/concurrency limiter and idempotency engine (`X-Aegis-Idempotent-Replay`), Human-in-the-Loop (HITL) approval state machine with signed webhook resume tokens, PostgreSQL audit store (`pgx/v5`), and least-loaded gRPC worker failover router.
3. **`daemon-rs/` (Rust 2021)**: Privileged Linux worker daemon (`tonic` gRPC streaming) orchestrating raw kernel primitives: `CLONE_NEWPID | CLONE_NEWNS | CLONE_NEWIPC | CLONE_NEWUTS | CLONE_NEWCGROUP | CLONE_NEWNET`, `pivot_root`, OverlayFS over read-only base rootfs, per-execution `tmpfs`, `cgroups v2` resource ceilings, and `seccomp-bpf` filtering.

```mermaid
flowchart LR
    Agent["LLM Agent / sdk-py<br/>(@sandbox_tool, AegisClient)"] -->|"HTTPS / REST + SSE<br/>Bearer Auth"| Gateway["control-go<br/>API Gateway & Orchestrator"]
    Gateway -->|"Atomic Lua Scripts<br/>Idempotency & Rate Limit"| Redis[("Redis 7<br/>Cache & Pub/Sub")]
    Gateway -->|"pgx/v5 Audit & HITL"| Postgres[("PostgreSQL 16<br/>Executions & Approvals")]
    Gateway -->|"gRPC Stream<br/>Least-Loaded Router"| Daemon["daemon-rs<br/>Linux Sandbox Worker"]
    Daemon -->|"Namespaces + OverlayFS<br/>cgroups v2 + seccomp-bpf"| Sandbox["Isolated Sandbox<br/>(UID 10000 / MS_RDONLY)"]
    Prom["Prometheus :9090"] -->|"Scrape /metrics"| Gateway
    Prom -->|"Scrape /metrics"| Daemon
    Grafana["Grafana :3000"] -->|"Dashboards"| Prom
```

---

## Non-Negotiable Security Invariants

1. **Unprivileged Execution**: Payloads run as `UID 10000` / `GID 10000` with `PR_SET_NO_NEW_PRIVS = 1`, zero supplementary groups (`SYS_setgroups`), all 64 bounding capabilities dropped (`PR_CAPBSET_DROP`), `RLIMIT_CORE = 0`, `RLIMIT_NOFILE = 64`, and a `seccomp-bpf` filter killing disallowed syscalls (`mount`, `umount2`, `pivot_root`, `chroot`, `ptrace`, `bpf`, etc.) with `SIGSYS`.
2. **Host Filesystem Isolation**: Base rootfs trees under `/var/lib/aegis/rootfs/*` remain strictly read-only (`MS_RDONLY` via OverlayFS + `pivot_root`). Only `/tmp` is writable, backed by a bounded per-execution `tmpfs` (`size=32m,nosuid,nodev`).
3. **Output Flood Protection**: The Rust `io_streamer` enforces a hard `4 MiB` cumulative `stdout + stderr` cap per execution and terminates the cgroup immediately (`cgroup.kill = 1`) on overrun.
4. **Network Isolation**: With `enable_loopback_only = true` (default), each sandbox enters an isolated `CLONE_NEWNET` namespace with only loopback (`lo`) enabled and zero external interfaces.
5. **Deterministic Zero-Leak Teardown**: Every sandbox runs with `memory.swap.max = 0` inside `/sys/fs/cgroup/aegis.slice/exec-{id}` and tears down via RAII in invariant order: `cgroup.kill` -> read `memory.peak` / `memory.events` / `cpu.stat` -> `umount2(MNT_DETACH)` OverlayFS `merged` -> `umount2(MNT_DETACH)` `tmpfs` workspace -> `rmdir` workspace & cgroup directory.

---

## Quickstart

### 1. Start Infrastructure & Observability Stack

```bash
make up
# Starts PostgreSQL (:5432), Redis (:6379), Prometheus (:9090), and Grafana (:3000)
# Apply schema migration:
psql "postgres://aegis:aegis@localhost:5432/aegisrun?sslmode=disable" -f migrations/0001_init_executions.up.sql
```

### 2. Build Rootfs, Protobuf Contracts & Services (Linux / WSL2)

```bash
make proto
make rootfs
make build
```

### 3. Start Worker Daemon & Control Plane

```bash
# Terminal 1 (root / CAP_SYS_ADMIN for namespaces, cgroups v2, OverlayFS)
sudo WORKER_NODE_ID=worker-1 \
  GRPC_LISTEN_ADDR=0.0.0.0:50051 \
  METRICS_LISTEN_ADDR=0.0.0.0:9091 \
  ./daemon-rs/target/release/daemon-rs

# Terminal 2
AEGIS_HTTP_ADDR=0.0.0.0:8080 \
  AEGIS_POSTGRES_DSN="postgres://aegis:aegis@localhost:5432/aegisrun?sslmode=disable" \
  AEGIS_REDIS_ADDR="localhost:6379" \
  AEGIS_WORKERS="worker-1=localhost:50051" \
  AEGIS_API_KEYS="aegis-dev-token:default" \
  go run ./control-go/cmd/control
```

### 4. Decorate & Invoke Agent Tools in Python (`sdk-py`)

```python
from aegis_sdk import AegisClient, sandbox_tool

@sandbox_tool(runtime="PYTHON_3_12", memory_mb=128, timeout_ms=5000)
def compute_factorial(n: int) -> int:
    """Compute factorial inside an isolated AegisRun micro-sandbox."""
    import math
    return math.factorial(n)

# Export OpenAI / Anthropic tool definitions
openai_schema = compute_factorial.to_openai_tool()
anthropic_schema = compute_factorial.to_anthropic_tool()

# Execute safely via control-go -> daemon-rs
with AegisClient(base_url="http://localhost:8080", api_key="aegis-dev-token") as client:
    res = compute_factorial.run(20, client=client)
    print(res.return_value, res.metrics)
```

---

## AI Agent Integration & One-Liner Install

### 1. One-Liner Python SDK Install

```bash
# uv:
uv pip install "aegis-sdk @ git+https://github.com/Rayrsn/AegisRun.git#subdirectory=sdk-py"

# pip:
pip install "git+https://github.com/Rayrsn/AegisRun.git#subdirectory=sdk-py"
```

### 2. Install the AegisRun Agent Skill (`SKILL.md`)

Workspace-local discovery is already built into this repository for **Antigravity / `agy`** ([`.agents/skills/aegisrun/SKILL.md`](.agents/skills/aegisrun/SKILL.md)) and **OpenCode** ([`.opencode/skills/aegisrun/SKILL.md`](.opencode/skills/aegisrun/SKILL.md)). To install globally across all workspaces:

```bash
# Universal skills CLI (OpenCode, Claude Code, Cursor, Codex, etc.):
npx skills add Rayrsn/AegisRun

# Antigravity (agy CLI & Antigravity IDE/App) — Linux / WSL2 / macOS:
mkdir -p ~/.gemini/config/skills/aegisrun && curl -fsSL https://raw.githubusercontent.com/Rayrsn/AegisRun/main/skills/aegisrun/SKILL.md -o ~/.gemini/config/skills/aegisrun/SKILL.md

# OpenCode — Linux / WSL2 / macOS:
mkdir -p ~/.config/opencode/skills/aegisrun && curl -fsSL https://raw.githubusercontent.com/Rayrsn/AegisRun/main/skills/aegisrun/SKILL.md -o ~/.config/opencode/skills/aegisrun/SKILL.md

# Claude Code — Linux / WSL2 / macOS:
mkdir -p ~/.claude/skills/aegisrun && curl -fsSL https://raw.githubusercontent.com/Rayrsn/AegisRun/main/skills/aegisrun/SKILL.md -o ~/.claude/skills/aegisrun/SKILL.md
```

Windows PowerShell global install for **Antigravity (`agy`)** & **OpenCode**:

```powershell
# Antigravity (agy CLI & Antigravity IDE/App):
New-Item -ItemType Directory -Force "$HOME\.gemini\config\skills\aegisrun" | Out-Null; irm https://raw.githubusercontent.com/Rayrsn/AegisRun/main/skills/aegisrun/SKILL.md -OutFile "$HOME\.gemini\config\skills\aegisrun\SKILL.md"

# OpenCode:
New-Item -ItemType Directory -Force "$HOME\.config\opencode\skills\aegisrun" | Out-Null; irm https://raw.githubusercontent.com/Rayrsn/AegisRun/main/skills/aegisrun/SKILL.md -OutFile "$HOME\.config\opencode\skills\aegisrun\SKILL.md"
```

### 3. One-Liner Backend Stack Launch (Linux / WSL2 Root)

```bash
git clone https://github.com/Rayrsn/AegisRun.git && cd AegisRun && sudo bash deploy/scripts/quickstart.sh
# Verify readiness: curl -fsS http://127.0.0.1:8080/readyz
# Stop stack: sudo bash deploy/scripts/quickstart.sh --stop
```

### 4. Copy-Paste Prompt for AI Coding Agents

Give this prompt to **Antigravity (`agy`)**, **OpenCode**, Claude Code, or Cursor:

> Install `aegis-sdk` (`pip install "git+https://github.com/Rayrsn/AegisRun.git#subdirectory=sdk-py"`), clone `https://github.com/Rayrsn/AegisRun.git` on a Linux/WSL2 host, start the sandbox backend with `sudo bash deploy/scripts/quickstart.sh`, verify `http://127.0.0.1:8080/readyz` reports `workers_healthy >= 1`, and follow the `aegisrun` skill (`skills/aegisrun/SKILL.md`) for how to define `@sandbox_tool` functions, export OpenAI/Anthropic schemas, and handle `.to_llm_observation()` self-correction errors.

---

## Adversarial Red-Team Security Audit (`benchmarks/security_redteam.py`)

Verified live (`make redteam`) against `daemon-rs` and `control-go` across all 7 containment vectors with zero leaked mounts or cgroups (`mounts=0`, `cgroups=0`):

| Attack Vector | Hostile Payload / Action | Verified Sandbox Outcome | Status |
| :--- | :--- | :--- | :--- |
| **1. Fork Bomb DoS** | Repeated `os.fork()` loop | Blocked by cgroup `pids.max` (`FORK_BLOCKED:11:15`); all tasks reaped via `cgroup.kill` | **PASS** |
| **2. Memory Exhaustion** | 512 MiB allocation under 128 MiB cap (`memory.swap.max=0`) | Immediate kernel OOM kill (`OOM_KILLED peak=134217728B`) | **PASS** |
| **3. Host Credential Exfiltration** | Read `/etc/shadow` & inspect `/proc/1/cmdline` | `UID=10000`, `/etc/shadow` -> `PermissionError`, PID 1 is isolated sandbox `/usr/bin/python3 /tmp/main.py` | **PASS** |
| **4. Container Escape (`mount`)** | Raw `libc.mount(...)` syscall via `ctypes` | Terminated by `seccomp-bpf` with `SIGSYS` (`SYSCALL_VIOLATION`) | **PASS** |
| **5. Filesystem Fill / RO Bypass** | Write to `/` and exhaust `/tmp` | Read-only rootfs fails `EROFS` (`RO_ERRNO=30`); `32 MiB` `tmpfs` caps write with `ENOSPC` (`TMPFS_FILL_ERRNO=28 WRITTEN_MIB=31`) | **PASS** |
| **6. Outbound Raw/TCP Sockets** | TCP connect to `1.1.1.1:80` | Blocked immediately by `CLONE_NEWNET` (`NET_BLOCKED:errno=101`, `ENETUNREACH`) | **PASS** |
| **7. Output Flood DoS** | Continuous `64 KiB` stdout writes | Killed by `io_streamer` flood guard at `4 MiB` (`FLOOD_KILLED`) | **PASS** |

---

## Performance & Load Benchmarks

### AegisRun vs. Docker (`runc`) Cold-Start (`benchmarks/vs_docker_bench.py`)

Payload: `import math; print(math.factorial(20))` ([full report](benchmarks/results/vs_docker_summary.md))

- **Sequential P50 Speedup**: **28.0x faster** (`21.45 ms` vs `600.52 ms`)
- **Kernel Setup Overhead (P50 / P95)**: `555 us` (`0.56 ms`) / `829 us` (`0.83 ms`)
- **Per-Sandbox Peak Cgroup Memory (P50)**: `3.24 MiB`

| Runtime | Mode | Samples | P50 Latency (ms) | P95 Latency (ms) | P99 Latency (ms) | Mean (ms) | Setup P50 (us) |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: |
| **AegisRun** | Sequential | 500 | 21.45 | 26.70 | 54.71 | 22.84 | 555 |
| **AegisRun** | Concurrent (8w) | 500 | 33.32 | 64.86 | 114.07 | 36.93 | 776 |
| **Docker (runc)** | Sequential | 20 | 600.52 | 724.86 | 783.77 | 616.88 | n/a |
| **Docker (runc)** | Concurrent (8w) | 20 | 1819.97 | 2196.43 | 2265.31 | 1808.39 | n/a |

![Cold Start Comparison](benchmarks/results/cold_start_comparison.svg)

### High-Concurrency Orchestrator Load Test (`benchmarks/load_test.js` via `k6`)

Workload: **200 concurrent VUs**, 15s mixed workload (70% unique compute payloads, 20% duplicate idempotent retries, 10% OOM probes) against `POST /v1/execute`:

- **Checks Passed**: **100.00%** (`8271 / 8271`) over `2757` iterations (`2761` HTTP requests)
- **HTTP 5xx Error Rate / Request Failures**: **0.00%** (`0 / 2757` 5xx; `0 / 2761` failed)
- **Redis Idempotent Replay Latency (`idempotent_replay_duration_ms`)**: med **`855.7 us`**, avg **`936.87 us`**, P90 **`1.14 ms`**, P95 **`1.29 ms`** (`< 3 ms` threshold)
- **Post-Benchmark Leak Audit**: `0` leaked OverlayFS/tmpfs mounts and `0` leaked cgroup v2 directories

---

## Verification & Leak Audit

```bash
make proto
make lint
make test
sudo -E cargo test --manifest-path daemon-rs/Cargo.toml --test sandbox_security_tests -- --test-threads=1 --nocapture
make sdk-lint
make sdk-test

# Post-test leak audit (must return 0 for both):
mount | grep "/run/aegis/sandboxes" | wc -l
ls -1 /sys/fs/cgroup/aegis.slice/ 2>/dev/null | grep "^exec-" | wc -l
```

---

## Repository Layout & Component Guides

| Directory / File | Description |
| :--- | :--- |
| [`proto/`](proto/README.md) | Protobuf v3 contract (`aegis/v1/sandbox.proto`) and `make proto` codegen guide |
| [`daemon-rs/`](daemon-rs/README.md) | Privileged Rust Linux sandbox worker daemon (`OverlayFS`, `cgroups v2`, namespaces, `seccomp-bpf`) |
| [`control-go/`](control-go/README.md) | Go REST/SSE API gateway, atomic Redis Lua idempotency/rate limiting, HITL state machine, worker router |
| [`sdk-py/`](sdk-py/README.md) | Python 3.11+ developer SDK (`@sandbox_tool`, OpenAI/Anthropic schema generator, sync/async SSE clients) |
| [`benchmarks/`](benchmarks/README.md) | 7-vector red-team suite, AegisRun vs Docker cold-start benchmark, and `k6` load test |
| [`deploy/`](deploy/README.md) | Docker Compose backing stack (`Postgres`, `Redis`, `Prometheus`, `Grafana`), rootfs builder, and [`quickstart.sh`](deploy/scripts/quickstart.sh) |
| [`skills/aegisrun/SKILL.md`](skills/aegisrun/SKILL.md) | Reusable AI agent skill for `npx skills`, OpenCode, Claude Code, Codex, and Cursor |
| [`.agents/skills/aegisrun/SKILL.md`](.agents/skills/aegisrun/SKILL.md) | Workspace-scoped agent skill auto-discovered by Antigravity (`agy` CLI & IDE) |
| [`.opencode/skills/aegisrun/SKILL.md`](.opencode/skills/aegisrun/SKILL.md) | Workspace-scoped agent skill auto-discovered by OpenCode |
| [`migrations/`](migrations/README.md) | PostgreSQL execution & HITL approval schema migrations (`0001_init_executions`) |
| [`Makefile`](Makefile) | Unified build, codegen, lint, test, rootfs, red-team, and benchmark targets |
| [`.env.example`](.env.example) | Environment variable template matching `daemon-rs`, `control-go`, and `sdk-py` defaults |
| [`LICENSE`](LICENSE) | Apache License 2.0 |
