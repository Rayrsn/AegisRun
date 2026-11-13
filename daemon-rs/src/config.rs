use std::path::PathBuf;

use crate::error::SandboxError;
use crate::pb::RuntimeType;

/// Runtime configuration for the AegisRun Linux sandbox worker daemon.
#[derive(Debug, Clone)]
pub struct DaemonConfig {
    pub worker_node_id: String,
    pub grpc_listen_addr: String,
    pub metrics_listen_addr: String,
    pub cgroup_root: PathBuf,
    pub cgroup_slice: PathBuf,
    pub sandbox_base_dir: PathBuf,
    pub rootfs_base_dir: PathBuf,
    pub max_sandboxes: u32,
    pub max_output_bytes: usize,
}

impl DaemonConfig {
    /// Loads daemon configuration from environment variables with sensible defaults.
    pub fn from_env() -> Self {
        let worker_node_id = std::env::var("WORKER_NODE_ID")
            .ok()
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| "aegis-worker-1".to_string());

        let grpc_listen_addr = std::env::var("GRPC_LISTEN_ADDR")
            .ok()
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| "0.0.0.0:50051".to_string());

        let metrics_listen_addr = std::env::var("METRICS_LISTEN_ADDR")
            .ok()
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| "0.0.0.0:9091".to_string());

        let cgroup_root = std::env::var("AEGIS_CGROUP_ROOT")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("/sys/fs/cgroup"));

        let cgroup_slice = std::env::var("AEGIS_CGROUP_SLICE")
            .map(PathBuf::from)
            .unwrap_or_else(|_| cgroup_root.join("aegis.slice"));

        let sandbox_base_dir = std::env::var("AEGIS_SANDBOX_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("/run/aegis/sandboxes"));

        let rootfs_base_dir = std::env::var("AEGIS_ROOTFS_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("/var/lib/aegis/rootfs"));

        let max_sandboxes = std::env::var("AEGIS_MAX_SANDBOXES")
            .ok()
            .and_then(|s| s.parse::<u32>().ok())
            .filter(|&v| v > 0)
            .unwrap_or(64);

        Self {
            worker_node_id,
            grpc_listen_addr,
            metrics_listen_addr,
            cgroup_root,
            cgroup_slice,
            sandbox_base_dir,
            rootfs_base_dir,
            max_sandboxes,
            max_output_bytes: 4 * 1024 * 1024,
        }
    }

    /// Resolves the read-only rootfs path, interpreter binary, and script extension for a runtime.
    pub fn rootfs_for_runtime(
        &self,
        runtime: RuntimeType,
    ) -> Result<(PathBuf, &'static str, &'static str), SandboxError> {
        match runtime {
            RuntimeType::Python312 => Ok((
                self.rootfs_base_dir.join("python3.12"),
                "/usr/bin/python3",
                "py",
            )),
            RuntimeType::Node20 => Ok((self.rootfs_base_dir.join("node20"), "/usr/bin/node", "js")),
            RuntimeType::Bash5 => Ok((self.rootfs_base_dir.join("bash5"), "/bin/bash", "sh")),
            RuntimeType::Unspecified => Err(SandboxError::InvalidRequest(
                "runtime must not be RUNTIME_TYPE_UNSPECIFIED".to_string(),
            )),
        }
    }
}

impl Default for DaemonConfig {
    fn default() -> Self {
        Self::from_env()
    }
}
