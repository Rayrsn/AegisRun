use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::error::SandboxError;

/// Resource accounting metrics read from cgroup v2 after execution exits.
#[derive(Debug, Clone, Copy, Default)]
pub struct CgroupStats {
    pub peak_memory_bytes: u64,
    pub cpu_usage_us: u64,
    pub oom_kill_count: u64,
}

/// Manages a per-execution cgroup v2 directory under `/sys/fs/cgroup/aegis.slice`.
#[derive(Debug)]
pub struct CgroupManager {
    cgroup_dir: PathBuf,
}

impl CgroupManager {
    /// Creates the execution cgroup and enforces memory, swap=0, CPU, and PIDs ceilings.
    pub fn create(
        slice_dir: &Path,
        execution_id: &str,
        memory_max_bytes: u64,
        cpu_quota_us: u32,
        pids_max: u32,
    ) -> Result<Self, SandboxError> {
        let cgroup_dir = slice_dir.join(format!("exec-{execution_id}"));
        fs::create_dir_all(&cgroup_dir).map_err(|e| SandboxError::CgroupCreateFailed {
            path: cgroup_dir.display().to_string(),
            source: e,
        })?;

        let mgr = Self { cgroup_dir };
        let setup_res = (|| {
            mgr.write_file("memory.max", &memory_max_bytes.to_string())?;
            mgr.write_file("memory.swap.max", "0")?;
            mgr.write_file("cpu.max", &format!("{cpu_quota_us} 100000"))?;
            mgr.write_file("pids.max", &pids_max.to_string())?;
            Ok(())
        })();

        if let Err(err) = setup_res {
            let _ = mgr.remove_dir();
            return Err(err);
        }

        Ok(mgr)
    }

    /// Returns the cgroup directory path on disk.
    pub fn cgroup_dir(&self) -> &Path {
        &self.cgroup_dir
    }

    /// Enrolls a host PID into this execution's cgroup.
    pub fn add_process(&self, pid: i32) -> Result<(), SandboxError> {
        self.write_file("cgroup.procs", &pid.to_string())
    }

    /// Sends SIGKILL to every process in the cgroup via cgroup v2 `cgroup.kill`.
    pub fn kill_all(&self) -> Result<(), SandboxError> {
        self.write_file("cgroup.kill", "1")
    }

    /// Reads memory.peak, memory.events, and cpu.stat prior to cgroup deletion.
    pub fn read_stats(&self) -> Result<CgroupStats, SandboxError> {
        let peak_memory_bytes = match fs::read_to_string(self.cgroup_dir.join("memory.peak")) {
            Ok(raw) => raw.trim().parse::<u64>().unwrap_or(0),
            Err(_) => 0,
        };

        let events_path = self.cgroup_dir.join("memory.events");
        let events_raw =
            fs::read_to_string(&events_path).map_err(|e| SandboxError::CgroupReadFailed {
                file: events_path.display().to_string(),
                source: e,
            })?;
        let oom_kill_count = parse_kv_u64(&events_raw, "oom_kill")
            .filter(|&v| v > 0)
            .or_else(|| parse_kv_u64(&events_raw, "oom"))
            .unwrap_or(0);

        let cpu_path = self.cgroup_dir.join("cpu.stat");
        let cpu_raw =
            fs::read_to_string(&cpu_path).map_err(|e| SandboxError::CgroupReadFailed {
                file: cpu_path.display().to_string(),
                source: e,
            })?;
        let cpu_usage_us = parse_kv_u64(&cpu_raw, "usage_usec").unwrap_or(0);

        Ok(CgroupStats {
            peak_memory_bytes,
            cpu_usage_us,
            oom_kill_count,
        })
    }

    /// Removes the cgroup directory, retrying briefly if the kernel is finishing task teardown.
    pub fn remove_dir(&self) -> Result<(), SandboxError> {
        if !self.cgroup_dir.exists() {
            return Ok(());
        }

        let mut last_err: Option<std::io::Error> = None;
        for _ in 0..25 {
            match fs::remove_dir(&self.cgroup_dir) {
                Ok(()) => return Ok(()),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
                Err(e) if e.raw_os_error() == Some(libc::EBUSY) => {
                    last_err = Some(e);
                    std::thread::sleep(Duration::from_millis(2));
                }
                Err(e) => {
                    return Err(SandboxError::CgroupDeleteFailed {
                        path: self.cgroup_dir.display().to_string(),
                        source: e,
                    });
                }
            }
        }

        Err(SandboxError::CgroupDeleteFailed {
            path: self.cgroup_dir.display().to_string(),
            source: last_err.unwrap_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::ResourceBusy, "cgroup remained busy")
            }),
        })
    }

    fn write_file(&self, name: &str, value: &str) -> Result<(), SandboxError> {
        let path = self.cgroup_dir.join(name);
        fs::write(&path, value.as_bytes()).map_err(|e| SandboxError::CgroupWriteFailed {
            file: path.display().to_string(),
            value: value.to_string(),
            source: e,
        })
    }
}

fn parse_kv_u64(contents: &str, key: &str) -> Option<u64> {
    for line in contents.lines() {
        let mut parts = line.split_whitespace();
        if parts.next() == Some(key) {
            return parts.next().and_then(|v| v.parse::<u64>().ok());
        }
    }
    None
}
