use daemon_rs::pb::WorkerStatus;

#[tokio::main]
async fn main() {
    let status = WorkerStatus::Healthy;
    println!("daemon-rs initialized (status={status:?})");
}
