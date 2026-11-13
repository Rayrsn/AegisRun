use std::fs;
use std::os::unix::fs::PermissionsExt;

use crate::config::DaemonConfig;
use crate::error::SandboxError;

const CAP_SETGID: u32 = 6;
const CAP_SETUID: u32 = 7;
const CAP_SYS_CHROOT: u32 = 18;
const CAP_SYS_ADMIN: u32 = 21;

/// Validates Linux kernel version, cgroup v2 controllers, capabilities, and immutable rootfs.
pub fn run_preflight_checks(config: &DaemonConfig) -> Result<(), SandboxError> {
    verify_kernel_version()?;
    verify_cgroups_v2(config)?;
    verify_capabilities()?;
    verify_base_rootfs(config)?;
    Ok(())
}

fn verify_kernel_version() -> Result<(), SandboxError> {
    let release = fs::read_to_string("/proc/sys/kernel/osrelease")
        .map_err(|e| SandboxError::PreflightFailed(format!("failed to read osrelease: {e}")))?;
    let base = release.trim().split('-').next().unwrap_or("");
    let mut parts = base.split('.');
    let major: u32 = parts.next().and_then(|s| s.parse().ok()).ok_or_else(|| {
        SandboxError::PreflightFailed(format!("unparseable kernel release '{release}'"))
    })?;
    let minor: u32 = parts.next().and_then(|s| s.parse().ok()).ok_or_else(|| {
        SandboxError::PreflightFailed(format!("unparseable kernel release '{release}'"))
    })?;

    if major < 5 || (major == 5 && minor < 15) {
        return Err(SandboxError::PreflightFailed(format!(
            "Linux kernel >= 5.15 required for cgroup.kill and cgroups v2, found {major}.{minor}"
        )));
    }
    Ok(())
}

fn verify_cgroups_v2(config: &DaemonConfig) -> Result<(), SandboxError> {
    let controllers_path = config.cgroup_root.join("cgroup.controllers");
    let controllers = fs::read_to_string(&controllers_path).map_err(|e| {
        SandboxError::PreflightFailed(format!(
            "cgroups v2 unified hierarchy missing at {}: {e}",
            controllers_path.display()
        ))
    })?;

    let available: Vec<&str> = controllers.split_whitespace().collect();
    for required in ["cpu", "memory", "pids"] {
        if !available.contains(&required) {
            return Err(SandboxError::PreflightFailed(format!(
                "cgroup v2 controller '{required}' not enabled in {}",
                controllers_path.display()
            )));
        }
    }

    let root_subtree = config.cgroup_root.join("cgroup.subtree_control");
    let _ = fs::write(&root_subtree, "+cpu +memory +pids");

    fs::create_dir_all(&config.cgroup_slice).map_err(|e| SandboxError::CgroupCreateFailed {
        path: config.cgroup_slice.display().to_string(),
        source: e,
    })?;

    let slice_subtree = config.cgroup_slice.join("cgroup.subtree_control");
    fs::write(&slice_subtree, "+cpu +memory +pids").map_err(|e| {
        SandboxError::CgroupWriteFailed {
            file: slice_subtree.display().to_string(),
            value: "+cpu +memory +pids".to_string(),
            source: e,
        }
    })?;

    fs::create_dir_all(&config.sandbox_base_dir).map_err(|e| {
        SandboxError::PreflightFailed(format!(
            "failed to create sandbox base dir {}: {e}",
            config.sandbox_base_dir.display()
        ))
    })?;

    Ok(())
}

fn verify_capabilities() -> Result<(), SandboxError> {
    if nix::unistd::geteuid().is_root() {
        return Ok(());
    }

    let status = fs::read_to_string("/proc/self/status").map_err(|e| {
        SandboxError::PreflightFailed(format!("failed to read /proc/self/status: {e}"))
    })?;
    let cap_eff_hex = status
        .lines()
        .find_map(|line| line.strip_prefix("CapEff:"))
        .map(str::trim)
        .ok_or_else(|| {
            SandboxError::PreflightFailed("CapEff not found in /proc/self/status".to_string())
        })?;
    let cap_eff = u64::from_str_radix(cap_eff_hex, 16).map_err(|e| {
        SandboxError::PreflightFailed(format!("failed to parse CapEff '{cap_eff_hex}': {e}"))
    })?;

    for (cap, name) in [
        (CAP_SYS_ADMIN, "CAP_SYS_ADMIN"),
        (CAP_SYS_CHROOT, "CAP_SYS_CHROOT"),
        (CAP_SETUID, "CAP_SETUID"),
        (CAP_SETGID, "CAP_SETGID"),
    ] {
        if (cap_eff & (1u64 << cap)) == 0 {
            return Err(SandboxError::PreflightFailed(format!(
                "missing required Linux capability {name} (run as root or grant capabilities)"
            )));
        }
    }

    Ok(())
}

fn verify_base_rootfs(config: &DaemonConfig) -> Result<(), SandboxError> {
    let py_rootfs = config.rootfs_base_dir.join("python3.12");
    let py_meta = fs::metadata(&py_rootfs).map_err(|e| {
        SandboxError::PreflightFailed(format!(
            "python3.12 rootfs missing at {}: {e}",
            py_rootfs.display()
        ))
    })?;

    if !py_meta.is_dir() {
        return Err(SandboxError::PreflightFailed(format!(
            "{} is not a directory",
            py_rootfs.display()
        )));
    }

    let mode = py_meta.permissions().mode();
    if mode & 0o222 != 0 {
        return Err(SandboxError::PreflightFailed(format!(
            "base rootfs {} must be read-only (mode={mode:#o}); run build-rootfs.sh",
            py_rootfs.display()
        )));
    }

    let py_bin = py_rootfs.join("usr/bin/python3");
    let bin_meta = fs::metadata(&py_bin).map_err(|e| {
        SandboxError::PreflightFailed(format!(
            "python3 binary missing at {}: {e}",
            py_bin.display()
        ))
    })?;
    if bin_meta.permissions().mode() & 0o111 == 0 {
        return Err(SandboxError::PreflightFailed(format!(
            "{} is not executable",
            py_bin.display()
        )));
    }

    Ok(())
}
