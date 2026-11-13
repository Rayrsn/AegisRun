use std::fs;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::Stream;
use tonic::{Request, Response, Status};

use crate::config::DaemonConfig;
use crate::metrics::DaemonMetrics;
use crate::pb::sandbox_worker_server::SandboxWorker;
use crate::pb::{
    CancelRequest, CancelResponse, Empty, ExecutionEvent, ExecutionRequest, WorkerHeartbeat,
    WorkerStatus,
};
use crate::sandbox::{self, ActiveExecutionRegistry};

/// gRPC implementation of the `aegis.v1.SandboxWorker` service.
#[derive(Debug, Clone)]
pub struct SandboxWorkerService {
    config: Arc<DaemonConfig>,
    metrics: Arc<DaemonMetrics>,
    registry: Arc<ActiveExecutionRegistry>,
}

impl SandboxWorkerService {
    pub fn new(
        config: Arc<DaemonConfig>,
        metrics: Arc<DaemonMetrics>,
        registry: Arc<ActiveExecutionRegistry>,
    ) -> Self {
        Self {
            config,
            metrics,
            registry,
        }
    }
}

type EventStream = Pin<Box<dyn Stream<Item = Result<ExecutionEvent, Status>> + Send>>;
type HeartbeatStream = Pin<Box<dyn Stream<Item = Result<WorkerHeartbeat, Status>> + Send>>;

#[tonic::async_trait]
impl SandboxWorker for SandboxWorkerService {
    type ExecuteToolStream = EventStream;
    type StreamWorkerHeartbeatStream = HeartbeatStream;

    async fn execute_tool(
        &self,
        request: Request<ExecutionRequest>,
    ) -> Result<Response<Self::ExecuteToolStream>, Status> {
        let req = request.into_inner();
        let active = self.metrics.active_sandboxes.load(Ordering::SeqCst);
        if active >= self.config.max_sandboxes {
            return Err(Status::resource_exhausted(format!(
                "worker capacity reached ({active}/{})",
                self.config.max_sandboxes
            )));
        }

        let (tx, rx) = mpsc::channel(64);
        let config = Arc::clone(&self.config);
        let metrics = Arc::clone(&self.metrics);
        let registry = Arc::clone(&self.registry);

        tokio::spawn(async move {
            sandbox::execute_in_sandbox(config, metrics, registry, req, tx).await;
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn cancel_execution(
        &self,
        request: Request<CancelRequest>,
    ) -> Result<Response<CancelResponse>, Status> {
        let req = request.into_inner();
        match self.registry.cancel(&req.execution_id) {
            Ok(true) => Ok(Response::new(CancelResponse {
                cancelled: true,
                message: format!("killed cgroup for execution {}", req.execution_id),
            })),
            Ok(false) => Ok(Response::new(CancelResponse {
                cancelled: false,
                message: format!("execution {} is not active", req.execution_id),
            })),
            Err(err) => Err(Status::internal(err.to_string())),
        }
    }

    async fn stream_worker_heartbeat(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::StreamWorkerHeartbeatStream>, Status> {
        let (tx, rx) = mpsc::channel(16);
        let config = Arc::clone(&self.config);
        let metrics = Arc::clone(&self.metrics);

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            loop {
                interval.tick().await;
                let (avail_bytes, total_bytes, mem_pct) = read_mem_info();
                let psi = read_cpu_pressure_avg10();
                let active = metrics.active_sandboxes.load(Ordering::Relaxed);
                let status = if active >= config.max_sandboxes || mem_pct > 90.0 {
                    WorkerStatus::Degraded
                } else {
                    WorkerStatus::Healthy
                };

                let ts = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_nanos() as u64)
                    .unwrap_or(0);

                let hb = WorkerHeartbeat {
                    worker_node_id: config.worker_node_id.clone(),
                    active_sandboxes: active,
                    max_sandboxes: config.max_sandboxes,
                    available_memory_bytes: avail_bytes,
                    total_memory_bytes: total_bytes,
                    cpu_pressure_psi: psi,
                    memory_used_pct: mem_pct,
                    status: status as i32,
                    timestamp_ns: ts,
                };

                if tx.send(Ok(hb)).await.is_err() {
                    break;
                }
            }
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}

fn read_mem_info() -> (u64, u64, f64) {
    let Ok(raw) = fs::read_to_string("/proc/meminfo") else {
        return (0, 0, 0.0);
    };
    let mut total_kb: u64 = 0;
    let mut avail_kb: u64 = 0;
    for line in raw.lines() {
        if let Some(rest) = line.strip_prefix("MemTotal:") {
            total_kb = rest
                .split_whitespace()
                .next()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
        } else if let Some(rest) = line.strip_prefix("MemAvailable:") {
            avail_kb = rest
                .split_whitespace()
                .next()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
        }
    }
    let total_bytes = total_kb * 1024;
    let avail_bytes = avail_kb * 1024;
    let used_pct = if total_bytes > 0 {
        ((total_bytes.saturating_sub(avail_bytes)) as f64 / total_bytes as f64) * 100.0
    } else {
        0.0
    };
    (avail_bytes, total_bytes, used_pct)
}

fn read_cpu_pressure_avg10() -> f64 {
    let Ok(raw) = fs::read_to_string("/proc/pressure/cpu") else {
        return 0.0;
    };
    for line in raw.lines() {
        if line.starts_with("some ") {
            for token in line.split_whitespace() {
                if let Some(val) = token.strip_prefix("avg10=") {
                    if let Ok(parsed) = val.parse::<f64>() {
                        return parsed;
                    }
                }
            }
        }
    }
    0.0
}
