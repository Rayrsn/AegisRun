use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use nix::errno::Errno;
use nix::mount::{mount, umount2, MntFlags, MsFlags};
use nix::unistd::{chown, Gid, Uid};

use crate::config::DaemonConfig;
use crate::error::SandboxError;
use crate::pb::RuntimeType;

pub const SANDBOX_UID: u32 = 10000;
pub const SANDBOX_GID: u32 = 10000;

/// Holds the ephemeral tmpfs and OverlayFS mount points for a single execution.
#[derive(Debug)]
pub struct EphemeralMounts {
    pub workspace_dir: PathBuf,
    pub upper_dir: PathBuf,
    pub work_dir: PathBuf,
    pub merged_dir: PathBuf,
    pub interpreter_bin: &'static str,
    pub script_container_path: String,
}

/// Provisions a RAM-backed tmpfs workspace and mounts an OverlayFS rootfs over it.
pub fn setup_mounts(
    config: &DaemonConfig,
    execution_id: &str,
    runtime: RuntimeType,
    entrypoint_code: &str,
    tmpfs_size_bytes: u64,
) -> Result<EphemeralMounts, SandboxError> {
    let (lower_dir, interpreter_bin, script_ext) = config.rootfs_for_runtime(runtime)?;
    if !lower_dir.exists() {
        return Err(SandboxError::InvalidRequest(format!(
            "rootfs directory does not exist: {}",
            lower_dir.display()
        )));
    }

    let workspace_dir = config.sandbox_base_dir.join(execution_id);
    fs::create_dir_all(&workspace_dir).map_err(|e| SandboxError::MountFailed {
        path: workspace_dir.display().to_string(),
        source: e,
    })?;

    let tmpfs_opts = format!("size={tmpfs_size_bytes},mode=0755");
    if let Err(err) = mount(
        Some("tmpfs"),
        &workspace_dir,
        Some("tmpfs"),
        MsFlags::MS_NOSUID | MsFlags::MS_NODEV,
        Some(tmpfs_opts.as_str()),
    ) {
        let _ = fs::remove_dir(&workspace_dir);
        return Err(SandboxError::MountFailed {
            path: workspace_dir.display().to_string(),
            source: std::io::Error::from_raw_os_error(err as i32),
        });
    }

    let upper_dir = workspace_dir.join("upper");
    let work_dir = workspace_dir.join("work");
    let merged_dir = workspace_dir.join("merged");

    let prepare_overlay = || -> Result<String, SandboxError> {
        for dir in [&upper_dir, &work_dir, &merged_dir] {
            fs::create_dir_all(dir).map_err(|e| SandboxError::MountFailed {
                path: dir.display().to_string(),
                source: e,
            })?;
        }

        prepare_upper_dirs(&upper_dir)?;

        let overlay_data = format!(
            "lowerdir={},upperdir={},workdir={}",
            lower_dir.display(),
            upper_dir.display(),
            work_dir.display()
        );
        mount(
            Some("overlay"),
            &merged_dir,
            Some("overlay"),
            MsFlags::MS_NOSUID | MsFlags::MS_NODEV,
            Some(overlay_data.as_str()),
        )
        .map_err(|err| SandboxError::MountFailed {
            path: merged_dir.display().to_string(),
            source: std::io::Error::from_raw_os_error(err as i32),
        })?;

        let script_filename = format!("main.{script_ext}");
        let script_host_path = merged_dir.join("tmp").join(&script_filename);
        fs::write(&script_host_path, entrypoint_code.as_bytes()).map_err(|e| {
            SandboxError::MountFailed {
                path: script_host_path.display().to_string(),
                source: e,
            }
        })?;

        fs::set_permissions(&script_host_path, fs::Permissions::from_mode(0o500)).map_err(|e| {
            SandboxError::MountFailed {
                path: script_host_path.display().to_string(),
                source: e,
            }
        })?;

        chown(
            &script_host_path,
            Some(Uid::from_raw(SANDBOX_UID)),
            Some(Gid::from_raw(SANDBOX_GID)),
        )
        .map_err(|err| SandboxError::MountFailed {
            path: script_host_path.display().to_string(),
            source: std::io::Error::from_raw_os_error(err as i32),
        })?;

        Ok(format!("/tmp/{script_filename}"))
    };

    match prepare_overlay() {
        Ok(script_container_path) => Ok(EphemeralMounts {
            workspace_dir,
            upper_dir,
            work_dir,
            merged_dir,
            interpreter_bin,
            script_container_path,
        }),
        Err(err) => {
            let _ = umount_if_mounted(&merged_dir);
            let _ = umount_if_mounted(&workspace_dir);
            let _ = fs::remove_dir(&workspace_dir);
            Err(err)
        }
    }
}

fn prepare_upper_dirs(upper_dir: &Path) -> Result<(), SandboxError> {
    let specs = [
        ("tmp", 0o1777),
        ("dev", 0o0755),
        ("proc", 0o0555),
        (".old_root", 0o0700),
    ];

    for (rel, mode) in specs {
        let dir = upper_dir.join(rel);
        fs::create_dir_all(&dir).map_err(|e| SandboxError::MountFailed {
            path: dir.display().to_string(),
            source: e,
        })?;
        fs::set_permissions(&dir, fs::Permissions::from_mode(mode)).map_err(|e| {
            SandboxError::MountFailed {
                path: dir.display().to_string(),
                source: e,
            }
        })?;
    }

    Ok(())
}

/// Detaches OverlayFS and tmpfs mounts and deletes the execution workspace directory.
pub fn teardown_mounts(mounts: &EphemeralMounts) -> Result<(), SandboxError> {
    let mut first_err: Option<SandboxError> = None;

    if let Err(e) = umount_if_mounted(&mounts.merged_dir) {
        first_err = Some(e);
    }

    if let Err(e) = umount_if_mounted(&mounts.workspace_dir) {
        if first_err.is_none() {
            first_err = Some(e);
        }
    }

    if mounts.workspace_dir.exists() {
        if let Err(e) = fs::remove_dir(&mounts.workspace_dir) {
            if first_err.is_none() {
                first_err = Some(SandboxError::UnmountFailed {
                    path: mounts.workspace_dir.display().to_string(),
                    source: e,
                });
            }
        }
    }

    match first_err {
        Some(err) => Err(err),
        None => Ok(()),
    }
}

fn umount_if_mounted(path: &Path) -> Result<(), SandboxError> {
    match umount2(path, MntFlags::MNT_DETACH) {
        Ok(()) => Ok(()),
        Err(Errno::EINVAL | Errno::ENOENT) => Ok(()),
        Err(err) => Err(SandboxError::UnmountFailed {
            path: path.display().to_string(),
            source: std::io::Error::from_raw_os_error(err as i32),
        }),
    }
}
