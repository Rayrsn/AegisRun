use std::collections::HashMap;
use std::sync::{Arc, Once};

use tokio::sync::mpsc;

use daemon_rs::config::DaemonConfig;
use daemon_rs::metrics::DaemonMetrics;
use daemon_rs::pb::{EventType, ExecutionEvent, ExecutionRequest, ResourceLimits, RuntimeType};
use daemon_rs::preflight::run_preflight_checks;
use daemon_rs::sandbox::{execute_in_sandbox, ActiveExecutionRegistry};

static PREFLIGHT_ONCE: Once = Once::new();

fn init_config() -> DaemonConfig {
    let config = DaemonConfig::from_env();
    PREFLIGHT_ONCE.call_once(|| {
        run_preflight_checks(&config).expect("preflight checks must pass for kernel tests");
    });
    config
}

async fn run_python(
    code: &str,
    limits: ResourceLimits,
) -> (Vec<ExecutionEvent>, String, String, ExecutionEvent) {
    let config = Arc::new(init_config());
    let metrics = Arc::new(DaemonMetrics::new());
    let registry = Arc::new(ActiveExecutionRegistry::default());

    let req = ExecutionRequest {
        execution_id: uuid::Uuid::now_v7().to_string(),
        runtime: RuntimeType::Python312 as i32,
        entrypoint_code: code.to_string(),
        stdin_payload: Vec::new(),
        env_vars: HashMap::new(),
        limits: Some(limits),
        enable_loopback_only: true,
    };

    let (tx, mut rx) = mpsc::channel(256);
    let exec_handle = tokio::spawn(execute_in_sandbox(config, metrics, registry, req, tx));

    let mut events = Vec::new();
    let mut stdout_buf = Vec::new();
    let mut stderr_buf = Vec::new();

    while let Some(item) = rx.recv().await {
        let ev = item.expect("unexpected gRPC status error in stream");
        match EventType::try_from(ev.event_type) {
            Ok(EventType::StdoutChunk) => stdout_buf.extend_from_slice(&ev.chunk_data),
            Ok(EventType::StderrChunk) => stderr_buf.extend_from_slice(&ev.chunk_data),
            _ => {}
        }
        events.push(ev);
    }

    exec_handle.await.expect("sandbox execution task panicked");

    let terminal = events
        .last()
        .cloned()
        .expect("execution must emit a terminal event");
    let stdout = String::from_utf8_lossy(&stdout_buf).into_owned();
    let stderr = String::from_utf8_lossy(&stderr_buf).into_owned();

    (events, stdout, stderr, terminal)
}

fn default_limits() -> ResourceLimits {
    ResourceLimits {
        wall_timeout_ms: 5000,
        memory_max_bytes: 128 * 1024 * 1024,
        cpu_quota_us: 50_000,
        pids_max: 16,
        tmpfs_size_bytes: 32 * 1024 * 1024,
    }
}

#[tokio::test]
async fn test_cold_start_latency() {
    let (_events, stdout, stderr, terminal) =
        run_python("print(\"hello\")", default_limits()).await;

    assert_eq!(
        EventType::try_from(terminal.event_type),
        Ok(EventType::Completed),
        "stderr={stderr}, err={}",
        terminal.error_message
    );
    assert_eq!(stdout, "hello\n");
    let metrics = terminal
        .metrics
        .expect("terminal event must include metrics");
    assert_eq!(metrics.exit_code, 0);
    let overhead_us = metrics.setup_duration_us + metrics.teardown_duration_us;
    println!(
        "[cold_start] setup={}us exec={}us teardown={}us overhead={}us",
        metrics.setup_duration_us,
        metrics.exec_duration_us,
        metrics.teardown_duration_us,
        overhead_us
    );
    assert!(
        overhead_us < 35_000,
        "expected setup+teardown < 35000us, got {overhead_us}us"
    );
}

#[tokio::test]
async fn test_fork_bomb_containment() {
    let code = r#"
import os
blocked = False
for _ in range(100):
    try:
        pid = os.fork()
        if pid == 0:
            os._exit(0)
    except OSError:
        blocked = True
        break
print("FORK_BLOCKED" if blocked else "FORK_UNBLOCKED")
"#;
    let (_events, stdout, stderr, terminal) = run_python(code, default_limits()).await;

    assert_eq!(
        EventType::try_from(terminal.event_type),
        Ok(EventType::Completed),
        "stderr={stderr}, err={}",
        terminal.error_message
    );
    assert!(
        stdout.contains("FORK_BLOCKED"),
        "expected fork bomb to hit pids.max=16, got stdout={stdout:?} stderr={stderr:?}"
    );
}

#[tokio::test]
async fn test_oom_kill_detection() {
    let mut limits = default_limits();
    limits.memory_max_bytes = 64 * 1024 * 1024;

    let code = r#"
x = bytearray(b"A" * (256 * 1024 * 1024))
print(len(x))
"#;
    let (_events, _stdout, _stderr, terminal) = run_python(code, limits).await;

    assert_eq!(
        EventType::try_from(terminal.event_type),
        Ok(EventType::OomKilled),
        "expected OOM_KILLED, got {:?} ({})",
        EventType::try_from(terminal.event_type),
        terminal.error_message
    );
    let metrics = terminal
        .metrics
        .expect("terminal event must include metrics");
    assert!(
        metrics.peak_memory_bytes >= 60 * 1024 * 1024,
        "expected peak memory near 64 MiB limit, got {}",
        metrics.peak_memory_bytes
    );
}

#[tokio::test]
async fn test_filesystem_immutability_and_escape_prevention() {
    let code_exec_1 = r#"
import os

try:
    with open("/usr/bin/test_escape", "w") as f:
        f.write("pwned")
    raise AssertionError("writing to /usr/bin succeeded unexpectedly")
except OSError as e:
    print(f"RO_BLOCKED:{e.errno}")

with open("/tmp/secret.txt", "w") as f:
    f.write("ephemeral")
print("TMP_WRITTEN")

try:
    content = open("/etc/shadow").read()
    raise AssertionError(f"read /etc/shadow unexpectedly: {len(content)}")
except PermissionError:
    print("SHADOW_BLOCKED")
"#;
    let (_events1, stdout1, stderr1, term1) = run_python(code_exec_1, default_limits()).await;
    assert_eq!(
        EventType::try_from(term1.event_type),
        Ok(EventType::Completed),
        "exec1 failed: stderr={stderr1} err={}",
        term1.error_message
    );
    assert!(stdout1.contains("RO_BLOCKED:"), "stdout1={stdout1:?}");
    assert!(stdout1.contains("TMP_WRITTEN"), "stdout1={stdout1:?}");
    assert!(stdout1.contains("SHADOW_BLOCKED"), "stdout1={stdout1:?}");

    let code_exec_2 = r#"
import os
print("EXISTS" if os.path.exists("/tmp/secret.txt") else "VANISHED")
"#;
    let (_events2, stdout2, stderr2, term2) = run_python(code_exec_2, default_limits()).await;
    assert_eq!(
        EventType::try_from(term2.event_type),
        Ok(EventType::Completed),
        "exec2 failed: stderr={stderr2} err={}",
        term2.error_message
    );
    assert_eq!(stdout2.trim(), "VANISHED");
}

#[tokio::test]
async fn test_syscall_violation_detection() {
    let code = r#"
import ctypes
libc = ctypes.CDLL(None)
libc.syscall(165, 0, 0, 0, 0, 0)
print("MOUNT_RETURNED")
"#;
    let (_events, stdout, _stderr, terminal) = run_python(code, default_limits()).await;

    assert!(
        !stdout.contains("MOUNT_RETURNED"),
        "syscall should have killed process before returning"
    );
    assert_eq!(
        EventType::try_from(terminal.event_type),
        Ok(EventType::SyscallViolation),
        "expected SYSCALL_VIOLATION, got {:?} ({})",
        EventType::try_from(terminal.event_type),
        terminal.error_message
    );
    let metrics = terminal
        .metrics
        .expect("terminal event must include metrics");
    assert_eq!(metrics.signaled, libc::SIGSYS);
}

#[tokio::test]
async fn test_output_flood_guard() {
    let code = r#"
while True:
    print("A" * 65536)
"#;
    let (events, _stdout, _stderr, terminal) = run_python(code, default_limits()).await;
    assert_eq!(
        EventType::try_from(terminal.event_type),
        Ok(EventType::InternalError),
        "expected flood guard termination"
    );
    assert!(
        terminal.error_message.contains("output flood guard"),
        "unexpected error_message={}",
        terminal.error_message
    );
    let total_chunk_bytes: usize = events.iter().map(|e| e.chunk_data.len()).sum();
    assert!(
        total_chunk_bytes <= 4 * 1024 * 1024 + 8192,
        "flood guard allowed too many bytes: {total_chunk_bytes}"
    );
}
