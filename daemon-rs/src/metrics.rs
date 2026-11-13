use std::fmt::Write as _;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

const SETUP_BUCKETS_US: [u64; 8] = [1000, 2500, 5000, 10000, 20000, 35000, 50000, 100000];
const EXEC_BUCKETS_US: [u64; 8] = [1000, 5000, 10000, 50000, 100000, 500000, 1000000, 5000000];
const TEARDOWN_BUCKETS_US: [u64; 6] = [500, 1000, 2500, 5000, 10000, 25000];

/// Lock-free atomic histogram tracking microsecond durations.
#[derive(Debug)]
pub struct LatencyHistogram<const N: usize> {
    bounds: [u64; N],
    buckets: [AtomicU64; N],
    inf_count: AtomicU64,
    sum: AtomicU64,
}

impl<const N: usize> LatencyHistogram<N> {
    pub fn new(bounds: [u64; N]) -> Self {
        Self {
            bounds,
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            inf_count: AtomicU64::new(0),
            sum: AtomicU64::new(0),
        }
    }

    pub fn observe(&self, value: u64) {
        self.sum.fetch_add(value, Ordering::Relaxed);
        self.inf_count.fetch_add(1, Ordering::Relaxed);
        for (idx, bound) in self.bounds.iter().enumerate() {
            if value <= *bound {
                self.buckets[idx].fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    fn write_prometheus(&self, out: &mut String, name: &str, help: &str) {
        let _ = writeln!(out, "# HELP {name} {help}");
        let _ = writeln!(out, "# TYPE {name} histogram");
        for (idx, bound) in self.bounds.iter().enumerate() {
            let count = self.buckets[idx].load(Ordering::Relaxed);
            let _ = writeln!(out, "{name}_bucket{{le=\"{bound}\"}} {count}");
        }
        let total = self.inf_count.load(Ordering::Relaxed);
        let sum = self.sum.load(Ordering::Relaxed);
        let _ = writeln!(out, "{name}_bucket{{le=\"+Inf\"}} {total}");
        let _ = writeln!(out, "{name}_sum {sum}");
        let _ = writeln!(out, "{name}_count {total}");
    }
}

/// Thread-safe Prometheus metrics registry for `daemon-rs`.
#[derive(Debug)]
pub struct DaemonMetrics {
    pub active_sandboxes: AtomicU32,
    pub executions_completed: AtomicU64,
    pub executions_oom: AtomicU64,
    pub executions_timeout: AtomicU64,
    pub executions_syscall_violation: AtomicU64,
    pub executions_error: AtomicU64,
    pub setup_duration_us: LatencyHistogram<8>,
    pub exec_duration_us: LatencyHistogram<8>,
    pub teardown_duration_us: LatencyHistogram<6>,
}

impl DaemonMetrics {
    pub fn new() -> Self {
        Self {
            active_sandboxes: AtomicU32::new(0),
            executions_completed: AtomicU64::new(0),
            executions_oom: AtomicU64::new(0),
            executions_timeout: AtomicU64::new(0),
            executions_syscall_violation: AtomicU64::new(0),
            executions_error: AtomicU64::new(0),
            setup_duration_us: LatencyHistogram::new(SETUP_BUCKETS_US),
            exec_duration_us: LatencyHistogram::new(EXEC_BUCKETS_US),
            teardown_duration_us: LatencyHistogram::new(TEARDOWN_BUCKETS_US),
        }
    }

    pub fn inc_active(&self) -> u32 {
        self.active_sandboxes.fetch_add(1, Ordering::SeqCst) + 1
    }

    pub fn dec_active(&self) {
        let _ = self
            .active_sandboxes
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |v| {
                Some(v.saturating_sub(1))
            });
    }

    pub fn render_prometheus(&self, worker_node_id: &str) -> String {
        let mut out = String::with_capacity(2048);

        let _ = writeln!(
            out,
            "# HELP aegis_worker_active_sandboxes Real-time active sandboxes on worker"
        );
        let _ = writeln!(out, "# TYPE aegis_worker_active_sandboxes gauge");
        let _ = writeln!(
            out,
            "aegis_worker_active_sandboxes{{worker_node_id=\"{worker_node_id}\"}} {}",
            self.active_sandboxes.load(Ordering::Relaxed)
        );

        let _ = writeln!(
            out,
            "# HELP aegis_worker_executions_total Total executions by terminal status"
        );
        let _ = writeln!(out, "# TYPE aegis_worker_executions_total counter");
        let _ = writeln!(
            out,
            "aegis_worker_executions_total{{worker_node_id=\"{worker_node_id}\",status=\"COMPLETED\"}} {}",
            self.executions_completed.load(Ordering::Relaxed)
        );
        let _ = writeln!(
            out,
            "aegis_worker_executions_total{{worker_node_id=\"{worker_node_id}\",status=\"OOM_KILLED\"}} {}",
            self.executions_oom.load(Ordering::Relaxed)
        );
        let _ = writeln!(
            out,
            "aegis_worker_executions_total{{worker_node_id=\"{worker_node_id}\",status=\"TIMEOUT_KILLED\"}} {}",
            self.executions_timeout.load(Ordering::Relaxed)
        );
        let _ = writeln!(
            out,
            "aegis_worker_executions_total{{worker_node_id=\"{worker_node_id}\",status=\"SYSCALL_VIOLATION\"}} {}",
            self.executions_syscall_violation.load(Ordering::Relaxed)
        );
        let _ = writeln!(
            out,
            "aegis_worker_executions_total{{worker_node_id=\"{worker_node_id}\",status=\"INTERNAL_ERROR\"}} {}",
            self.executions_error.load(Ordering::Relaxed)
        );

        self.setup_duration_us.write_prometheus(
            &mut out,
            "aegis_sandbox_setup_duration_microseconds",
            "OverlayFS + cgroup v2 + namespace initialization overhead in microseconds",
        );
        self.exec_duration_us.write_prometheus(
            &mut out,
            "aegis_sandbox_exec_duration_microseconds",
            "Sandbox payload execution duration in microseconds",
        );
        self.teardown_duration_us.write_prometheus(
            &mut out,
            "aegis_sandbox_teardown_duration_microseconds",
            "Cgroup kill + unmount teardown duration in microseconds",
        );

        out
    }
}

impl Default for DaemonMetrics {
    fn default() -> Self {
        Self::new()
    }
}
