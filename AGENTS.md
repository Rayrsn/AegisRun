# AGENTS.md — AegisRun Polyglot Monorepo Guidelines

## 1. Project Identity & Core Architecture
You are an autonomous systems & backend engineering agent building **AegisRun**, a distributed AI agent sandbox and tool-calling orchestrator. 
The system executes untrusted LLM-generated code and tool calls in isolated Linux micro-environments with `< 35ms` cold-start overhead.

The repository is a strict 3-tier polyglot monorepo:
1. **`proto/`**: Single source of truth for Protobuf v3 contracts (`sandbox.proto`).
2. **`daemon-rs/` (Rust)**: Privileged Linux worker daemon utilizing raw kernel primitives (`namespaces`, `cgroups v2`, `OverlayFS`, `pivot_root`, `seccomp-bpf`) and `tonic` gRPC streaming. Zero Docker dependency at runtime.
3. **`control-go/` (Go)**: High-concurrency API gateway, Redis idempotency engine, Human-in-the-Loop (HITL) webhook state machine, PostgreSQL audit store, and least-loaded gRPC worker router.
4. **`sdk-py/` (Python)**: Developer-facing SDK (`@sandbox_tool`), AST source extractor, OpenAI/Anthropic JSON schema generator, and async `httpx` streaming client.

---

## 2. Mandatory Execution Order
Never build features out of order. Always read and complete the specification files sequentially:
1. **`01_plan_contracts_and_topology.md`** (Monorepo layout, `sandbox.proto`, SQL migrations, `build-rootfs.sh`)
2. **`02_plan_rust_sandbox_daemon.md`** (`daemon-rs` kernel isolation, cgroup v2 lifecycle, seccomp filter, gRPC server)
3. **`03_plan_go_control_plane.md`** (`control-go` REST/SSE gateway, Redis Lua scripts, HITL state machine, worker pool)
4. **`04_plan_python_agent_sdk.md`** (`sdk-py` AST decorator, schema generator, sync/async clients)
5. **`05_plan_observability_and_benchmarks.md`** (Prometheus/Grafana provisioning, red-team suite, `k6` and Docker comparison benchmarks)

Do not advance to the next plan until 100% of the verification criteria in the current plan pass.

---

## 3. Language-Specific Engineering Standards

### 3.1 Rust (`daemon-rs/`)
* **Edition & Toolchain**: Rust 2021 edition (stable).
* **Error Handling**: 
  * Library/internal modules MUST use `thiserror` with explicit, strongly-typed error enums (`SandboxError::MountFailed`, `SandboxError::CgroupWriteFailed`, etc.).
  * **FORBIDDEN**: Never use `.unwrap()` or `.expect()` in request-handling paths, gRPC stream tasks, or teardown routines. A single panic in the worker daemon can leak kernel mounts or cgroups.
* **Deterministic Resource Cleanup (RAII)**:
  * Every sandbox execution MUST be wrapped in a guard struct implementing `Drop` (or an explicit async teardown block that runs even on task cancellation).
  * Teardown order is invariant:
    1. Write `"1"` to `/sys/fs/cgroup/aegis.slice/exec-{id}/cgroup.kill`
    2. Read final `memory.peak`, `memory.events`, and `cpu.stat`
    3. `umount2` the OverlayFS `merged` directory with `MNT_DETACH`
    4. `umount2` the `tmpfs` workspace directory and `rmdir` the mount point
    5. `rmdir` the `/sys/fs/cgroup/aegis.slice/exec-{id}` directory
* **Syscall & File Descriptor Hygiene**:
  * Use `nix` and `libc` for low-level Linux calls.
  * Ensure all host file descriptors opened by the daemon have `FD_CLOEXEC` set so zero file handles leak into the child process across `execve`.
  * Always set `memory.swap.max = 0` inside every child cgroup so memory overruns trigger immediate OOM-kill rather than host disk thrashing.

### 3.2 Go (`control-go/`)
* **Version**: Go 1.22+.
* **Concurrency & Contexts**:
  * Every I/O function (PostgreSQL `pgx/v5`, Redis `go-redis/v9`, gRPC client calls) MUST accept `ctx context.Context` as its first parameter and respect cancellation.
  * Never spawn an untracked goroutine (`go func()`) without binding it to an `errgroup.Group` or `sync.WaitGroup` tied to graceful shutdown.
* **Atomicity**:
  * Idempotency checks (`SETNX` + status inspection) and token-bucket rate limiting MUST be executed via atomic Redis Lua scripts (`redis.NewScript`), never via multi-step `GET` then `SET` roundtrips in Go.
* **Code Style**:
  * Standard library `net/http` (Go 1.22 routing) or `go-chi/chi/v5`. Avoid heavy reflection-based web frameworks.
  * Wrap errors with context: `fmt.Errorf("dispatching execution %s to worker %s: %w", execID, workerID, err)`.

### 3.3 Python (`sdk-py/`)
* **Version & Typing**: Python 3.11+ with strict type annotations (`mypy --strict` compliant) and `pydantic >= 2.0`.
* **Zero Runtime Pollution**:
  * When extracting a user function via `ast` in `@sandbox_tool`, strip the `@sandbox_tool` decorator node before calling `ast.unparse()` so the isolated sandbox environment does not need `aegis_sdk` installed inside its rootfs.
* **LLM Self-Correction Ergonomics**:
  * Every custom exception (`SandboxOOMError`, `SandboxTimeoutError`, `SandboxSecurityViolationError`) must implement `.to_llm_observation() -> str` returning a concise, structured explanation that an LLM agent can read to self-correct its code or arguments on the next turn.

---

## 4. Non-Negotiable Security Invariants
When implementing `daemon-rs`, verify these invariants in every test:
1. **Unprivileged Execution**: User payloads must execute as `UID 10000` / `GID 10000` with `PR_SET_NO_NEW_PRIVS = 1` and all Linux capabilities dropped.
2. **Host Filesystem Isolation**: Base rootfs (`/var/lib/aegis/rootfs/*`) must remain strictly read-only (`MS_RDONLY`). All writable paths inside the sandbox must reside on the per-execution `tmpfs` (`size=32m,nosuid,nodev`).
3. **Output Flood Protection**: Cap cumulative `stdout + stderr` per execution at `4 MiB`. If exceeded, kill the cgroup immediately to prevent OOM on the daemon or Go control plane.
4. **Network Isolation**: By default (`enable_loopback_only = true`), `CLONE_NEWNET` must be active with zero external `veth` interfaces configured.

---

## 5. Build, Lint & Verification Commands
Before marking any task or plan step complete, run the corresponding verification commands:

```bash
# 1. Regenerate & verify Protobuf bindings (Go + Rust)
make proto

# 2. Rust Daemon: format, clippy lint, and build
cd daemon-rs && cargo fmt --check && cargo clippy -- -D warnings && cargo test

# 3. Rust Kernel Isolation Tests (requires root / CAP_SYS_ADMIN on Linux)
sudo -E cargo test --manifest-path daemon-rs/Cargo.toml --test sandbox_security_tests -- --nocapture

# 4. Go Control Plane: vet, race-detector tests, and build
cd control-go && go vet ./... && go test -v -race ./...

# 5. Python SDK: ruff lint, mypy typecheck, and pytest suite
cd sdk-py && ruff check . && mypy aegis_sdk && pytest -v

# 6. Post-Test Leak Audit (MUST return 0 mounts and 0 cgroup dirs)
mount | grep "/run/aegis/sandboxes" | wc -l
ls -1 /sys/fs/cgroup/aegis.slice/ 2>/dev/null | grep "^exec-" | wc -l
```

---

## 6. Agent Workflow Protocol
For every subtask inside the 5 plan files:
1. **Plan & Inspect**: State which file(s) from the current plan you are implementing.
2. **Implement**: Write clean, production-grade code without placeholder comments (`// TODO: implement later` is forbidden).
3. **Verify**: Run the compiler, linter, and unit/integration tests for that module.
4. **Leak Check**: After running any test that spawns a sandbox, run the Post-Test Leak Audit commands in Section 5 to confirm zero leaked `tmpfs`/OverlayFS mounts or orphaned cgroup directories.