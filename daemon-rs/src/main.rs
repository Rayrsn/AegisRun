use std::net::SocketAddr;
use std::sync::Arc;

use tokio::io::AsyncWriteExt as _;
use tokio::net::TcpListener;
use tonic::transport::Server;
use tracing::{error, info};

use daemon_rs::config::DaemonConfig;
use daemon_rs::grpc_service::SandboxWorkerService;
use daemon_rs::metrics::DaemonMetrics;
use daemon_rs::pb::sandbox_worker_server::SandboxWorkerServer;
use daemon_rs::preflight::run_preflight_checks;
use daemon_rs::sandbox::ActiveExecutionRegistry;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let config = Arc::new(DaemonConfig::from_env());
    if let Err(err) = run_preflight_checks(&config) {
        error!("preflight check failed: {err}");
        return Err(err.into());
    }
    info!(
        worker_node_id = %config.worker_node_id,
        "preflight checks passed; starting gRPC and metrics servers"
    );

    let metrics = Arc::new(DaemonMetrics::new());
    let registry = Arc::new(ActiveExecutionRegistry::default());

    spawn_metrics_server(Arc::clone(&config), Arc::clone(&metrics));

    let grpc_addr: SocketAddr = config.grpc_listen_addr.parse()?;
    let service = SandboxWorkerService::new(config, metrics, registry);

    info!(%grpc_addr, "SandboxWorker gRPC server listening");
    Server::builder()
        .add_service(SandboxWorkerServer::new(service))
        .serve(grpc_addr)
        .await?;

    Ok(())
}

fn spawn_metrics_server(config: Arc<DaemonConfig>, metrics: Arc<DaemonMetrics>) {
    tokio::spawn(async move {
        let listener = match TcpListener::bind(&config.metrics_listen_addr).await {
            Ok(l) => l,
            Err(err) => {
                error!(addr = %config.metrics_listen_addr, "metrics listener bind failed: {err}");
                return;
            }
        };
        info!(addr = %config.metrics_listen_addr, "Prometheus /metrics listener active");

        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                continue;
            };
            let body = metrics.render_prometheus(&config.worker_node_id);
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = stream.write_all(response.as_bytes()).await;
            let _ = stream.shutdown().await;
        }
    });
}
