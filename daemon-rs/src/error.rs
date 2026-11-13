use thiserror::Error;

/// Strongly-typed errors returned by the AegisRun Linux sandbox daemon.
#[derive(Debug, Error)]
pub enum SandboxError {
    #[error("startup preflight check failed: {0}")]
    PreflightFailed(String),

    #[error("invalid execution request: {0}")]
    InvalidRequest(String),

    #[error("filesystem mount failed at '{path}': {source}")]
    MountFailed {
        path: String,
        #[source]
        source: std::io::Error,
    },

    #[error("filesystem unmount failed at '{path}': {source}")]
    UnmountFailed {
        path: String,
        #[source]
        source: std::io::Error,
    },

    #[error("failed to create cgroup directory '{path}': {source}")]
    CgroupCreateFailed {
        path: String,
        #[source]
        source: std::io::Error,
    },

    #[error("failed to write '{value}' to cgroup file '{file}': {source}")]
    CgroupWriteFailed {
        file: String,
        value: String,
        #[source]
        source: std::io::Error,
    },

    #[error("failed to read cgroup file '{file}': {source}")]
    CgroupReadFailed {
        file: String,
        #[source]
        source: std::io::Error,
    },

    #[error("failed to remove cgroup directory '{path}': {source}")]
    CgroupDeleteFailed {
        path: String,
        #[source]
        source: std::io::Error,
    },

    #[error("namespace isolation setup failed: {0}")]
    NamespaceFailed(String),

    #[error("seccomp-bpf filter setup failed: {0}")]
    SeccompFailed(String),

    #[error("failed to spawn isolated sandbox process: {0}")]
    SpawnFailed(#[source] std::io::Error),

    #[error("sandbox I/O streaming failed: {0}")]
    IoStreamFailed(#[source] std::io::Error),

    #[error("deterministic sandbox teardown failed: {0}")]
    TeardownFailed(String),
}
