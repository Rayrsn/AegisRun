use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::io::unix::AsyncFd;
use tokio::sync::mpsc;

use crate::error::SandboxError;
use crate::pb::{EventType, ExecutionEvent};
use crate::sandbox::cgroup_v2::CgroupManager;

const CHUNK_READ_SIZE: usize = 8192;

/// Summary returned after the sandbox process exits and stdio streams drain.
#[derive(Debug, Clone, Copy)]
pub struct IoExecutionResult {
    pub raw_wait_status: i32,
    pub timed_out: bool,
    pub flood_killed: bool,
    pub total_output_bytes: usize,
}

/// Parameters for streaming child stdio and enforcing execution limits.
pub struct StreamConfig<'a> {
    pub execution_id: &'a str,
    pub child_pid: libc::pid_t,
    pub stdin_write_fd: RawFd,
    pub stdout_read_fd: RawFd,
    pub stderr_read_fd: RawFd,
    pub stdin_payload: Vec<u8>,
    pub wall_timeout_ms: u32,
    pub max_output_bytes: usize,
    pub cgroup: &'a CgroupManager,
    pub event_tx: &'a mpsc::Sender<Result<ExecutionEvent, tonic::Status>>,
    pub seq_counter: &'a Arc<AtomicU64>,
    pub start_instant: std::time::Instant,
}

/// Streams stdin into the child, forwards stdout/stderr chunks, and enforces flood & wall-time caps.
pub async fn stream_and_wait(cfg: StreamConfig<'_>) -> Result<IoExecutionResult, SandboxError> {
    let stdin_fd = cfg.stdin_write_fd;
    let stdin_payload = cfg.stdin_payload;
    let stdin_task = tokio::task::spawn_blocking(move || {
        use std::io::Write as _;
        let mut file = unsafe { std::fs::File::from_raw_fd(stdin_fd) };
        if !stdin_payload.is_empty() {
            let _ = file.write_all(&stdin_payload);
            let _ = file.flush();
        }
        drop(file);
    });

    let total_bytes = Arc::new(AtomicUsize::new(0));
    let flood_killed = Arc::new(AtomicBool::new(false));

    let stdout_fd = wrap_nonblocking_fd(cfg.stdout_read_fd)?;
    let stderr_fd = wrap_nonblocking_fd(cfg.stderr_read_fd)?;

    let cgroup_dir_for_out = cfg.cgroup.cgroup_dir().to_path_buf();
    let cgroup_dir_for_err = cfg.cgroup.cgroup_dir().to_path_buf();

    let out_handle = tokio::spawn(pump_stream(
        cfg.execution_id.to_string(),
        stdout_fd,
        EventType::StdoutChunk,
        cfg.max_output_bytes,
        Arc::clone(&total_bytes),
        Arc::clone(&flood_killed),
        cgroup_dir_for_out,
        cfg.event_tx.clone(),
        Arc::clone(cfg.seq_counter),
        cfg.start_instant,
    ));

    let err_handle = tokio::spawn(pump_stream(
        cfg.execution_id.to_string(),
        stderr_fd,
        EventType::StderrChunk,
        cfg.max_output_bytes,
        Arc::clone(&total_bytes),
        Arc::clone(&flood_killed),
        cgroup_dir_for_err,
        cfg.event_tx.clone(),
        Arc::clone(cfg.seq_counter),
        cfg.start_instant,
    ));

    let child_pid = cfg.child_pid;
    let mut wait_task = tokio::task::spawn_blocking(move || wait_for_pid(child_pid));
    let timeout_duration = Duration::from_millis(u64::from(cfg.wall_timeout_ms));

    let (raw_wait_status, timed_out) =
        match tokio::time::timeout(timeout_duration, &mut wait_task).await {
            Ok(join_res) => {
                let status = join_res
                    .map_err(|e| SandboxError::IoStreamFailed(io::Error::other(e.to_string())))??;
                (status, false)
            }
            Err(_) => {
                let _ = cfg.cgroup.kill_all();
                let status = wait_task
                    .await
                    .map_err(|e| SandboxError::IoStreamFailed(io::Error::other(e.to_string())))??;
                (status, true)
            }
        };

    let _ = stdin_task.await;
    let _ = out_handle.await;
    let _ = err_handle.await;

    Ok(IoExecutionResult {
        raw_wait_status,
        timed_out,
        flood_killed: flood_killed.load(Ordering::SeqCst),
        total_output_bytes: total_bytes.load(Ordering::SeqCst),
    })
}

fn wrap_nonblocking_fd(fd: RawFd) -> Result<AsyncFd<OwnedFd>, SandboxError> {
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    if flags < 0 {
        return Err(SandboxError::IoStreamFailed(io::Error::last_os_error()));
    }
    if unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } != 0 {
        return Err(SandboxError::IoStreamFailed(io::Error::last_os_error()));
    }
    let owned = unsafe { OwnedFd::from_raw_fd(fd) };
    unsafe { AsyncFd::register(owned) }
        .map_err(|e| SandboxError::IoStreamFailed(io::Error::other(e.to_string())))
}

#[allow(clippy::too_many_arguments)]
async fn pump_stream(
    execution_id: String,
    async_fd: AsyncFd<OwnedFd>,
    event_type: EventType,
    max_output_bytes: usize,
    total_bytes: Arc<AtomicUsize>,
    flood_killed: Arc<AtomicBool>,
    cgroup_dir: std::path::PathBuf,
    event_tx: mpsc::Sender<Result<ExecutionEvent, tonic::Status>>,
    seq_counter: Arc<AtomicU64>,
    start_instant: std::time::Instant,
) {
    let mut buf = [0u8; CHUNK_READ_SIZE];

    loop {
        let mut guard = match async_fd.readable().await {
            Ok(g) => g,
            Err(_) => break,
        };

        loop {
            let raw_fd = guard.get_inner().as_raw_fd();
            let n = unsafe { libc::read(raw_fd, buf.as_mut_ptr() as *mut libc::c_void, buf.len()) };

            if n == 0 {
                return;
            }
            if n < 0 {
                let err = io::Error::last_os_error();
                if err.kind() == io::ErrorKind::WouldBlock {
                    guard.clear_ready();
                    break;
                }
                if err.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                return;
            }

            let read_len = n as usize;
            let new_total = total_bytes.fetch_add(read_len, Ordering::SeqCst) + read_len;
            if new_total > max_output_bytes {
                flood_killed.store(true, Ordering::SeqCst);
                let _ = std::fs::write(cgroup_dir.join("cgroup.kill"), b"1");
                return;
            }

            let seq = seq_counter.fetch_add(1, Ordering::SeqCst);
            let ts = start_instant.elapsed().as_nanos() as u64;
            let ev = ExecutionEvent {
                execution_id: execution_id.clone(),
                sequence_num: seq,
                timestamp_ns: ts,
                event_type: event_type as i32,
                chunk_data: buf[..read_len].to_vec(),
                metrics: None,
                error_message: String::new(),
            };

            if event_tx.send(Ok(ev)).await.is_err() {
                let _ = std::fs::write(cgroup_dir.join("cgroup.kill"), b"1");
                return;
            }
        }
    }
}

fn wait_for_pid(pid: libc::pid_t) -> Result<i32, SandboxError> {
    let mut status: libc::c_int = 0;
    loop {
        let res = unsafe { libc::waitpid(pid, &mut status, 0) };
        if res < 0 {
            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(SandboxError::IoStreamFailed(err));
        }
        return Ok(status);
    }
}
