# `daemon-rs/` — Privileged Linux Sandbox Worker Daemon

`daemon-rs` is AegisRun's Tier 1 execution engine written in Rust (2021 edition). It exposes the `aegis.v1.SandboxWorker` gRPC service via `tonic` and isolates untrusted code directly using Linux kernel primitives with zero Docker runtime dependency.

---

## Isolation & Execution Pipeline

Each `ExecuteTool` request runs inside `execute_in_sandbox` (`src/sandbox/mod.rs`) protected by an execution semaphore (`AEGIS_EXEC_CONCURRENCY`), `ActiveExecutionRegistry`, and deterministic `SandboxGuard` RAII cleanup:

1. **OverlayFS & Per-Execution `tmpfs` (`fs_mount.rs`)**: Mounts read-only base rootfs (`/var/lib/aegis/rootfs/<runtime>`) with `MS_RDONLY` and a bounded writable `/tmp` backed by `tmpfs` (`nosuid,nodev`).
2. **cgroups v2 Ceilings (`cgroup_v2.rs`)**: Creates `/sys/fs/cgroup/aegis.slice/exec-{id}`, configures `memory.max`, `memory.swap.max = 0` (immediate OOM kill without host swap thrashing), `cpu.max`, and `pids.max`.
3. **Namespaces & `pivot_root` (`namespace.rs`)**: Clones an isolated child via raw `SYS_clone` (`CLONE_NEWPID | CLONE_NEWNS | CLONE_NEWIPC | CLONE_NEWUTS | CLONE_NEWCGROUP` plus `CLONE_NEWNET` when `enable_loopback_only = true`), pivots into the OverlayFS view, sets `FD_CLOEXEC` / `RLIMIT_CORE = 0` / `RLIMIT_NOFILE = 64`, drops all capabilities, sets `PR_SET_NO_NEW_PRIVS = 1`, and drops privileges to `UID 10000` / `GID 10000` using raw `SYS_setgroups` / `SYS_setresgid` / `SYS_setresuid` syscalls.
4. **Syscall Filtering (`seccomp.rs`)**: Applies a `seccomp-bpf` filter killing dangerous host/container-escape syscalls with `SIGSYS`.
5. **Streaming & Flood Guard (`io_streamer.rs`)**: Streams `STDOUT_CHUNK` / `STDERR_CHUNK` frames, enforces the `wall_timeout_ms` deadline, and aborts the cgroup (`cgroup.kill = 1`) if cumulative `stdout + stderr` exceeds `4 MiB`.
6. **Invariant RAII Teardown (`SandboxGuard`)**: Always runs (even on task cancellation):
   1. Write `1` to `/sys/fs/cgroup/aegis.slice/exec-{id}/cgroup.kill`
   2. Read final `memory.peak`, `memory.events`, and `cpu.stat`
   3. `umount2(MNT_DETACH)` OverlayFS `merged` directory
   4. `umount2(MNT_DETACH)` `tmpfs` workspace directory and `rmdir` mount point
   5. `rmdir` `/sys/fs/cgroup/aegis.slice/exec-{id}`

---

## Configuration

| Environment Variable | Default | Description |
| :--- | :--- | :--- |
| `WORKER_NODE_ID` | `aegis-worker-1` | Worker identifier reported in heartbeats and Prometheus labels |
| `GRPC_LISTEN_ADDR` | `0.0.0.0:50051` | `tonic` gRPC listen address |
| `METRICS_LISTEN_ADDR` | `0.0.0.0:9091` | HTTP `/metrics` Prometheus exporter listen address |
| `AEGIS_CGROUP_ROOT` | `/sys/fs/cgroup` | Host cgroups v2 mount root |
| `AEGIS_CGROUP_SLICE` | `/sys/fs/cgroup/aegis.slice` | Parent slice for `exec-*` child cgroups |
| `AEGIS_SANDBOX_DIR` | `/run/aegis/sandboxes` | Base directory for per-execution ephemeral mounts |
| `AEGIS_ROOTFS_DIR` | `/var/lib/aegis/rootfs` | Read-only base rootfs directory (`python3.12`, `node20`, `bash5`) |
| `AEGIS_MAX_SANDBOXES` | `64` | Advertised worker sandbox capacity |
| `AEGIS_EXEC_CONCURRENCY` | `8` | Max concurrent sandboxes guarded by internal semaphore |

### Exported Prometheus Metrics (`:9091/metrics`)
- `aegis_worker_active_sandboxes`
- `aegis_worker_executions_total` (`COMPLETED`, `OOM_KILLED`, `TIMEOUT_KILLED`, `SYSCALL_VIOLATION`, `INTERNAL_ERROR`)
- `aegis_sandbox_setup_duration_microseconds`
- `aegis_sandbox_exec_duration_microseconds`
- `aegis_sandbox_teardown_duration_microseconds`

---

## Build, Lint & Kernel Tests (Linux / WSL2)

```bash
# Requires provisioned rootfs under /var/lib/aegis/rootfs (see deploy/scripts/build-rootfs.sh)
cd daemon-rs
cargo fmt --check
cargo clippy -- -D warnings
cargo build --release

# Kernel isolation security tests (requires root / CAP_SYS_ADMIN)
sudo -E cargo test --test sandbox_security_tests -- --test-threads=1 --nocapture

# Post-test leak audit (must return 0 for both)
mount | grep "/run/aegis/sandboxes" | wc -l
ls -1 /sys/fs/cgroup/aegis.slice/ 2>/dev/null | grep "^exec-" | wc -l
```
