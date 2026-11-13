pub mod config;
pub mod error;
pub mod grpc_service;
pub mod metrics;
pub mod preflight;
pub mod sandbox;

#[allow(clippy::result_large_err)]
pub mod pb {
    tonic::include_proto!("aegis.v1");
}
