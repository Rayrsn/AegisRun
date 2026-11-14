pub mod cgroup_v2;
pub mod fs_mount;
pub mod io_streamer;
pub mod namespace;
pub mod seccomp;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use tokio::sync::mpsc;

use crate::config::DaemonConfig;
use crate::error::SandboxError;
use crate::metrics::DaemonMetrics;
use crate::pb::{EventType, ExecutionEvent, ExecutionMetrics, ExecutionRequest, RuntimeType};
use crate::sandbox::cgroup_v2::{CgroupManager, CgroupStats};
use crate::sandbox::fs_mount::EphemeralMounts;

/// Registry tracking active executions so `CancelExecution` can kill their cgroup immediately.
#[derive(Debug, Default)]
pub struct ActiveExecutionRegistry {
    cgroups: Mutex<HashMap<String, PathBuf>>,
}

impl ActiveExecutionRegistry {
    pub fn register(&self, execution_id: &str, cgroup_dir: PathBuf) {
        if let Ok(mut guard) = self.cgroups.lock() {
            guard.insert(execution_id.to_string(), cgroup_dir);
        }
    }

    pub fn unregister(&self, execution_id: &str) {
        if let Ok(mut guard) = self.cgroups.lock() {
            guard.remove(execution_id);
        }
    }

    pub fn cancel(&self, execution_id: &str) -> Result<bool, SandboxError> {
        let cgroup_dir = match self.cgroups.lock() {
            Ok(guard) => guard.get(execution_id).cloned(),
            Err(_) => None,
        };

        let Some(dir) = cgroup_dir else {
            return Ok(false);
        };

        let kill_path = dir.join("cgroup.kill");
        if kill_path.exists() {
            std::fs::write(&kill_path, b"1").map_err(|e| SandboxError::CgroupWriteFailed {
                file: kill_path.display().to_string(),
                value: "1".to_string(),
                source: e,
            })?;
            return Ok(true);
        }
        Ok(false)
    }

    /// Terminates every active execution cgroup during daemon shutdown.
    pub fn cancel_all(&self) {
        let dirs: Vec<PathBuf> = match self.cgroups.lock() {
            Ok(guard) => guard.values().cloned().collect(),
            Err(poisoned) => poisoned.into_inner().values().cloned().collect(),
        };
        for dir in dirs {
            let kill_path = dir.join("cgroup.kill");
            if kill_path.exists() {
                let _ = std::fs::write(&kill_path, b"1");
            }
        }
    }
}

static EXEC_SEMAPHORE: std::sync::OnceLock<tokio::sync::Semaphore> = std::sync::OnceLock::new();

fn execution_semaphore() -> &'static tokio::sync::Semaphore {
    EXEC_SEMAPHORE.get_or_init(|| {
        let permits = std::env::var("AEGIS_EXEC_CONCURRENCY")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|&n| n > 0)
            .unwrap_or(8);
        tokio::sync::Semaphore::new(permits)
    })
}

/// Closes the sandbox execution concurrency gate so queued tasks abort cleanly during shutdown.
pub fn close_execution_gate() {
    execution_semaphore().close();
}

/// Deterministic RAII guard ensuring cgroup kill, unmount, and cgroup removal even on cancellation.
pub struct SandboxGuard {
    execution_id: String,
    cgroup: Option<CgroupManager>,
    mounts: Option<EphemeralMounts>,
    child_pid: Option<libc::pid_t>,
    registry: Arc<ActiveExecutionRegistry>,
    torn_down: bool,
}

impl SandboxGuard {
    fn new(execution_id: String, registry: Arc<ActiveExecutionRegistry>) -> Self {
        Self {
            execution_id,
            cgroup: None,
            mounts: None,
            child_pid: None,
            registry,
            torn_down: false,
        }
    }

    /// Executes the invariant 5-step teardown order and returns cgroup stats + teardown duration.
    pub fn teardown(&mut self) -> Result<(CgroupStats, u64), SandboxError> {
        if self.torn_down {
            return Ok((CgroupStats::default(), 0));
        }

        let start = Instant::now();
        let mut first_err: Option<SandboxError> = None;

        // 1. Kill all processes inside the cgroup.
        if let Some(ref cgroup) = self.cgroup {
            let _ = cgroup.kill_all();
        }

        // Reap child process if still unwaited.
        if let Some(pid) = self.child_pid.take() {
            let mut status: libc::c_int = 0;
            unsafe {
                libc::waitpid(pid, &mut status, 0);
            }
        }

        // 2. Read final cgroup accounting metrics before deletion.
        let stats = if let Some(ref cgroup) = self.cgroup {
            match cgroup.read_stats() {
                Ok(s) => s,
                Err(e) => {
                    first_err = Some(e);
                    CgroupStats::default()
                }
            }
        } else {
            CgroupStats::default()
        };

        // 3 & 4. Unmount OverlayFS merged dir and tmpfs workspace dir, then rmdir workspace.
        if let Some(ref mounts) = self.mounts {
            if let Err(e) = fs_mount::teardown_mounts(mounts) {
                if first_err.is_none() {
                    first_err = Some(e);
                }
            }
        }

        // 5. Remove execution cgroup directory.
        if let Some(ref cgroup) = self.cgroup {
            if let Err(e) = cgroup.remove_dir() {
                if first_err.is_none() {
                    first_err = Some(e);
                }
            }
        }

        self.registry.unregister(&self.execution_id);
        self.torn_down = true;
        let teardown_us = start.elapsed().as_micros() as u64;

        match first_err {
            Some(err) => Err(err),
            None => Ok((stats, teardown_us)),
        }
    }
}

impl Drop for SandboxGuard {
    fn drop(&mut self) {
        if !self.torn_down {
            let _ = self.teardown();
        }
    }
}

struct NormalizedLimits {
    wall_timeout_ms: u32,
    memory_max_bytes: u64,
    cpu_quota_us: u32,
    pids_max: u32,
    tmpfs_size_bytes: u64,
}

fn normalize_limits(req: &ExecutionRequest) -> NormalizedLimits {
    let lim = req.limits.as_ref();
    let wall_timeout_ms = lim
        .map(|l| l.wall_timeout_ms)
        .filter(|&v| v > 0)
        .unwrap_or(5000)
        .min(60_000);
    let memory_max_bytes = lim
        .map(|l| l.memory_max_bytes)
        .filter(|&v| v > 0)
        .unwrap_or(128 * 1024 * 1024)
        .min(1024 * 1024 * 1024);
    let cpu_quota_us = lim
        .map(|l| l.cpu_quota_us)
        .filter(|&v| v > 0)
        .unwrap_or(50_000);
    let pids_max = lim
        .map(|l| l.pids_max)
        .filter(|&v| v > 0)
        .unwrap_or(16)
        .min(64);
    let tmpfs_size_bytes = lim
        .map(|l| l.tmpfs_size_bytes)
        .filter(|&v| v > 0)
        .unwrap_or(32 * 1024 * 1024);

    NormalizedLimits {
        wall_timeout_ms,
        memory_max_bytes,
        cpu_quota_us,
        pids_max,
        tmpfs_size_bytes,
    }
}

/// Uses `block_in_place` on multi-threaded Tokio runtimes and runs inline on `current_thread`
/// runtimes (such as default `#[tokio::test]`) where `block_in_place` would panic.
fn block_in_place_if_multithread<F, R>(f: F) -> R
where
    F: FnOnce() -> R,
{
    match tokio::runtime::Handle::try_current() {
        Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(f)
        }
        _ => f(),
    }
}

/// Runs all 6 sandbox stages and streams lifecycle + stdio events to `event_tx`.
pub async fn execute_in_sandbox(
    config: Arc<DaemonConfig>,
    metrics: Arc<DaemonMetrics>,
    registry: Arc<ActiveExecutionRegistry>,
    req: ExecutionRequest,
    event_tx: mpsc::Sender<Result<ExecutionEvent, tonic::Status>>,
) {
    let execution_id = if req.execution_id.trim().is_empty() {
        uuid::Uuid::now_v7().to_string()
    } else {
        req.execution_id.clone()
    };

    let _active_guard = ActiveCounterGuard::new(Arc::clone(&metrics));
    let Ok(exec_permit) = execution_semaphore().acquire().await else {
        metrics.executions_error.fetch_add(1, Ordering::Relaxed);
        let terminal = ExecutionEvent {
            execution_id,
            sequence_num: 1,
            timestamp_ns: 0,
            event_type: EventType::InternalError as i32,
            chunk_data: Vec::new(),
            metrics: None,
            error_message: "daemon shutting down".to_string(),
        };
        let _ = event_tx.send(Ok(terminal)).await;
        return;
    };
    let mut guard = SandboxGuard::new(execution_id.clone(), Arc::clone(&registry));
    let overall_start = Instant::now();
    let seq_counter = Arc::new(AtomicU64::new(1));

    let outcome = run_stages(
        &config,
        &execution_id,
        &req,
        &mut guard,
        &registry,
        &event_tx,
        &seq_counter,
        overall_start,
    )
    .await;

    let teardown_res = block_in_place_if_multithread(|| guard.teardown());
    drop(exec_permit);

    match (outcome, teardown_res) {
        (
            Ok((io_res, setup_duration_us, exec_duration_us)),
            Ok((cg_stats, teardown_duration_us)),
        ) => {
            metrics.setup_duration_us.observe(setup_duration_us);
            metrics.exec_duration_us.observe(exec_duration_us);
            metrics.teardown_duration_us.observe(teardown_duration_us);

            let (exit_code, signaled) = decode_wait_status(io_res.raw_wait_status);
            let (event_type, error_message) = if io_res.flood_killed {
                metrics.executions_error.fetch_add(1, Ordering::Relaxed);
                (
                    EventType::InternalError,
                    format!(
                        "output flood guard triggered: cumulative stdout+stderr exceeded {} bytes",
                        config.max_output_bytes
                    ),
                )
            } else if io_res.timed_out {
                metrics.executions_timeout.fetch_add(1, Ordering::Relaxed);
                (
                    EventType::TimeoutKilled,
                    "execution exceeded wall_timeout_ms".to_string(),
                )
            } else if cg_stats.oom_kill_count > 0 {
                metrics.executions_oom.fetch_add(1, Ordering::Relaxed);
                (
                    EventType::OomKilled,
                    "execution killed by cgroup v2 OOM killer".to_string(),
                )
            } else if signaled == libc::SIGSYS {
                metrics
                    .executions_syscall_violation
                    .fetch_add(1, Ordering::Relaxed);
                (
                    EventType::SyscallViolation,
                    "blocked syscall intercepted by seccomp-bpf (SIGSYS)".to_string(),
                )
            } else {
                metrics.executions_completed.fetch_add(1, Ordering::Relaxed);
                (EventType::Completed, String::new())
            };

            let terminal = ExecutionEvent {
                execution_id,
                sequence_num: seq_counter.fetch_add(1, Ordering::SeqCst),
                timestamp_ns: overall_start.elapsed().as_nanos() as u64,
                event_type: event_type as i32,
                chunk_data: Vec::new(),
                metrics: Some(ExecutionMetrics {
                    exit_code,
                    signaled,
                    setup_duration_us,
                    exec_duration_us,
                    teardown_duration_us,
                    peak_memory_bytes: cg_stats.peak_memory_bytes,
                    cpu_usage_us: cg_stats.cpu_usage_us,
                }),
                error_message,
            };

            let _ = event_tx.send(Ok(terminal)).await;
        }
        (Err(err), teardown_res) => {
            metrics.executions_error.fetch_add(1, Ordering::Relaxed);
            let teardown_duration_us = teardown_res.map(|(_, td)| td).unwrap_or(0);
            let terminal = ExecutionEvent {
                execution_id,
                sequence_num: seq_counter.fetch_add(1, Ordering::SeqCst),
                timestamp_ns: overall_start.elapsed().as_nanos() as u64,
                event_type: EventType::InternalError as i32,
                chunk_data: Vec::new(),
                metrics: Some(ExecutionMetrics {
                    exit_code: -1,
                    signaled: 0,
                    setup_duration_us: 0,
                    exec_duration_us: 0,
                    teardown_duration_us,
                    peak_memory_bytes: 0,
                    cpu_usage_us: 0,
                }),
                error_message: err.to_string(),
            };
            let _ = event_tx.send(Ok(terminal)).await;
        }
        (Ok((_, setup_duration_us, exec_duration_us)), Err(teardown_err)) => {
            metrics.executions_error.fetch_add(1, Ordering::Relaxed);
            let terminal = ExecutionEvent {
                execution_id,
                sequence_num: seq_counter.fetch_add(1, Ordering::SeqCst),
                timestamp_ns: overall_start.elapsed().as_nanos() as u64,
                event_type: EventType::InternalError as i32,
                chunk_data: Vec::new(),
                metrics: Some(ExecutionMetrics {
                    exit_code: -1,
                    signaled: 0,
                    setup_duration_us,
                    exec_duration_us,
                    teardown_duration_us: 0,
                    peak_memory_bytes: 0,
                    cpu_usage_us: 0,
                }),
                error_message: teardown_err.to_string(),
            };
            let _ = event_tx.send(Ok(terminal)).await;
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_stages(
    config: &DaemonConfig,
    execution_id: &str,
    req: &ExecutionRequest,
    guard: &mut SandboxGuard,
    registry: &Arc<ActiveExecutionRegistry>,
    event_tx: &mpsc::Sender<Result<ExecutionEvent, tonic::Status>>,
    seq_counter: &Arc<AtomicU64>,
    overall_start: Instant,
) -> Result<(io_streamer::IoExecutionResult, u64, u64), SandboxError> {
    let runtime = RuntimeType::try_from(req.runtime).unwrap_or(RuntimeType::Unspecified);
    let limits = normalize_limits(req);

    // Stage 1 & 2 & 3/4: Setup mounts, cgroup, and spawn isolated process off the async poller.
    let (setup_duration_us, exec_start, spawned) = block_in_place_if_multithread(
        || -> Result<(u64, Instant, namespace::SpawnedProcess), SandboxError> {
            let setup_start = Instant::now();
            let mounts = fs_mount::setup_mounts(
                config,
                execution_id,
                runtime,
                &req.entrypoint_code,
                limits.tmpfs_size_bytes,
            )?;
            guard.mounts = Some(mounts);

            let cgroup = CgroupManager::create(
                &config.cgroup_slice,
                execution_id,
                limits.memory_max_bytes,
                limits.cpu_quota_us,
                limits.pids_max,
            )?;
            registry.register(execution_id, cgroup.cgroup_dir().to_path_buf());
            guard.cgroup = Some(cgroup);

            let mounts_ref = guard
                .mounts
                .as_ref()
                .ok_or_else(|| SandboxError::TeardownFailed("mounts missing".to_string()))?;
            let cgroup_ref = guard
                .cgroup
                .as_ref()
                .ok_or_else(|| SandboxError::TeardownFailed("cgroup missing".to_string()))?;

            let setup_duration_us = setup_start.elapsed().as_micros() as u64;

            let exec_start = Instant::now();
            let spawned = namespace::spawn_isolated_process(
                mounts_ref,
                cgroup_ref,
                &req.env_vars,
                req.enable_loopback_only,
            )?;
            Ok((setup_duration_us, exec_start, spawned))
        },
    )?;
    guard.child_pid = Some(spawned.pid);
    let cgroup_ref = guard
        .cgroup
        .as_ref()
        .ok_or_else(|| SandboxError::TeardownFailed("cgroup missing".to_string()))?;

    let started_ev = ExecutionEvent {
        execution_id: execution_id.to_string(),
        sequence_num: seq_counter.fetch_add(1, Ordering::SeqCst),
        timestamp_ns: overall_start.elapsed().as_nanos() as u64,
        event_type: EventType::Started as i32,
        chunk_data: Vec::new(),
        metrics: None,
        error_message: String::new(),
    };
    let _ = event_tx.send(Ok(started_ev)).await;

    // Stage 5: Stream stdio and wait for exit or timeout.
    let io_res = io_streamer::stream_and_wait(io_streamer::StreamConfig {
        execution_id,
        child_pid: spawned.pid,
        stdin_write_fd: spawned.stdin_write_fd,
        stdout_read_fd: spawned.stdout_read_fd,
        stderr_read_fd: spawned.stderr_read_fd,
        stdin_payload: req.stdin_payload.clone(),
        wall_timeout_ms: limits.wall_timeout_ms,
        max_output_bytes: config.max_output_bytes,
        cgroup: cgroup_ref,
        event_tx,
        seq_counter,
        start_instant: overall_start,
    })
    .await?;
    guard.child_pid = None;
    let exec_duration_us = exec_start.elapsed().as_micros() as u64;

    Ok((io_res, setup_duration_us, exec_duration_us))
}

fn decode_wait_status(status: i32) -> (i32, i32) {
    if libc::WIFEXITED(status) {
        (libc::WEXITSTATUS(status), 0)
    } else if libc::WIFSIGNALED(status) {
        let sig = libc::WTERMSIG(status);
        (128 + sig, sig)
    } else {
        (-1, 0)
    }
}

struct ActiveCounterGuard {
    metrics: Arc<DaemonMetrics>,
}

impl ActiveCounterGuard {
    fn new(metrics: Arc<DaemonMetrics>) -> Self {
        metrics.inc_active();
        Self { metrics }
    }
}

impl Drop for ActiveCounterGuard {
    fn drop(&mut self) {
        self.metrics.dec_active();
    }
}
