use std::collections::HashMap;
use std::ffi::CString;
use std::os::fd::RawFd;
use std::ptr;

use crate::error::SandboxError;
use crate::sandbox::cgroup_v2::CgroupManager;
use crate::sandbox::fs_mount::{EphemeralMounts, SANDBOX_GID, SANDBOX_UID};
use crate::sandbox::seccomp::{apply_seccomp_filter, build_seccomp_filter};

const IFF_UP: libc::c_short = 0x1;
const IFF_RUNNING: libc::c_short = 0x40;
const SIOCGIFFLAGS: libc::c_ulong = 0x8913;
const SIOCSIFFLAGS: libc::c_ulong = 0x8914;

/// Child process handles returned to the parent after namespace creation and `execve`.
pub struct SpawnedProcess {
    pub pid: libc::pid_t,
    pub stdin_write_fd: RawFd,
    pub stdout_read_fd: RawFd,
    pub stderr_read_fd: RawFd,
}

struct ChildDeviceBind {
    host_dev: CString,
    target_dev: CString,
}

/// Spawns an isolated process inside PID, Mount, Net, IPC, UTS, and Cgroup namespaces.
pub fn spawn_isolated_process(
    mounts: &EphemeralMounts,
    cgroup: &CgroupManager,
    env_vars: &HashMap<String, String>,
    enable_loopback_only: bool,
) -> Result<SpawnedProcess, SandboxError> {
    let (stdin_r, stdin_w) = create_cloexec_pipe()?;
    let (stdout_r, stdout_w) = create_cloexec_pipe()?;
    let (stderr_r, stderr_w) = create_cloexec_pipe()?;
    let (sync_r, sync_w) = create_cloexec_pipe()?;
    let (err_r, err_w) = create_cloexec_pipe()?;

    // Pre-allocate all CStrings and seccomp filter before clone() so the child does zero heap allocations.
    let merged_cstr = to_cstring(mounts.merged_dir.to_string_lossy().as_ref())?;
    let upper_tmp_cstr = to_cstring(mounts.upper_dir.join("tmp").to_string_lossy().as_ref())?;
    let merged_tmp_cstr = to_cstring(mounts.merged_dir.join("tmp").to_string_lossy().as_ref())?;
    let merged_proc_cstr = to_cstring(mounts.merged_dir.join("proc").to_string_lossy().as_ref())?;

    let mut device_binds = Vec::with_capacity(4);
    for dev in ["null", "zero", "random", "urandom"] {
        device_binds.push(ChildDeviceBind {
            host_dev: to_cstring(&format!("/dev/{dev}"))?,
            target_dev: to_cstring(
                mounts
                    .merged_dir
                    .join("dev")
                    .join(dev)
                    .to_string_lossy()
                    .as_ref(),
            )?,
        });
    }

    let interp_cstr = to_cstring(mounts.interpreter_bin)?;
    let script_cstr = to_cstring(&mounts.script_container_path)?;
    let argv_ptrs: [*const libc::c_char; 3] =
        [interp_cstr.as_ptr(), script_cstr.as_ptr(), ptr::null()];

    let mut env_cstrings = Vec::with_capacity(env_vars.len() + 3);
    env_cstrings.push(to_cstring("PATH=/usr/local/bin:/usr/bin:/bin")?);
    env_cstrings.push(to_cstring("HOME=/tmp")?);
    env_cstrings.push(to_cstring("PYTHONUNBUFFERED=1")?);
    for (k, v) in env_vars {
        if k.contains('=') || k.contains('\0') || v.contains('\0') {
            continue;
        }
        env_cstrings.push(to_cstring(&format!("{k}={v}"))?);
    }
    let mut envp_ptrs: Vec<*const libc::c_char> = env_cstrings.iter().map(|s| s.as_ptr()).collect();
    envp_ptrs.push(ptr::null());

    let seccomp_filter = build_seccomp_filter();

    let mut clone_flags = (libc::CLONE_NEWPID
        | libc::CLONE_NEWNS
        | libc::CLONE_NEWIPC
        | libc::CLONE_NEWUTS
        | libc::CLONE_NEWCGROUP
        | libc::SIGCHLD) as libc::c_ulong;
    if enable_loopback_only {
        clone_flags |= libc::CLONE_NEWNET as libc::c_ulong;
    }

    let pid_raw =
        unsafe { libc::syscall(libc::SYS_clone, clone_flags, 0usize, 0usize, 0usize, 0usize) };
    if pid_raw < 0 {
        let err = std::io::Error::last_os_error();
        close_fds(&[
            stdin_r, stdin_w, stdout_r, stdout_w, stderr_r, stderr_w, sync_r, sync_w, err_r, err_w,
        ]);
        return Err(SandboxError::NamespaceFailed(format!(
            "clone failed: {err}"
        )));
    }

    let child_pid = pid_raw as libc::pid_t;
    if child_pid == 0 {
        // Child process (PID 1 inside CLONE_NEWPID). Zero heap allocations allowed here.
        unsafe {
            child_setup_and_exec(
                stdin_r,
                stdin_w,
                stdout_r,
                stdout_w,
                stderr_r,
                stderr_w,
                sync_r,
                sync_w,
                err_r,
                err_w,
                &merged_cstr,
                &upper_tmp_cstr,
                &merged_tmp_cstr,
                &merged_proc_cstr,
                &device_binds,
                enable_loopback_only,
                &seccomp_filter,
                &interp_cstr,
                &argv_ptrs,
                &envp_ptrs,
            );
        }
    }

    // Parent process: close child pipe ends immediately.
    close_fds(&[stdin_r, stdout_w, stderr_w, sync_r, err_w]);

    // Enroll child into cgroup BEFORE signaling it to mount/exec.
    if let Err(err) = cgroup.add_process(child_pid) {
        close_fds(&[sync_w, err_r, stdin_w, stdout_r, stderr_r]);
        unsafe {
            libc::kill(child_pid, libc::SIGKILL);
            let mut status = 0;
            libc::waitpid(child_pid, &mut status, 0);
        }
        return Err(err);
    }

    // Wake child by closing sync_w.
    close_fds(&[sync_w]);

    // Wait for child execve (EOF on err_r) or failure payload ([step, errno]).
    let mut err_buf = [0i32; 2];
    let err_bytes = unsafe {
        libc::read(
            err_r,
            err_buf.as_mut_ptr() as *mut libc::c_void,
            std::mem::size_of::<[i32; 2]>(),
        )
    };
    close_fds(&[err_r]);

    if err_bytes > 0 {
        close_fds(&[stdin_w, stdout_r, stderr_r]);
        unsafe {
            let mut status = 0;
            libc::waitpid(child_pid, &mut status, 0);
        }
        let step = err_buf[0];
        let errno_val = err_buf[1];
        return Err(SandboxError::NamespaceFailed(format!(
            "child isolation failed at step {step}: {}",
            std::io::Error::from_raw_os_error(errno_val)
        )));
    }

    Ok(SpawnedProcess {
        pid: child_pid,
        stdin_write_fd: stdin_w,
        stdout_read_fd: stdout_r,
        stderr_read_fd: stderr_r,
    })
}

#[allow(clippy::too_many_arguments)]
unsafe fn child_setup_and_exec(
    stdin_r: RawFd,
    stdin_w: RawFd,
    stdout_r: RawFd,
    stdout_w: RawFd,
    stderr_r: RawFd,
    stderr_w: RawFd,
    sync_r: RawFd,
    sync_w: RawFd,
    err_r: RawFd,
    err_w: RawFd,
    merged_cstr: &CString,
    upper_tmp_cstr: &CString,
    merged_tmp_cstr: &CString,
    merged_proc_cstr: &CString,
    device_binds: &[ChildDeviceBind],
    enable_loopback_only: bool,
    seccomp_filter: &[libc::sock_filter],
    interp_cstr: &CString,
    argv_ptrs: &[*const libc::c_char; 3],
    envp_ptrs: &[*const libc::c_char],
) -> ! {
    close_fds(&[stdin_w, stdout_r, stderr_r, sync_w, err_r]);

    // Wait for parent cgroup enrollment signal (EOF on sync_r).
    let mut sync_byte = [0u8; 1];
    while libc::read(sync_r, sync_byte.as_mut_ptr() as *mut libc::c_void, 1) < 0 {
        if last_errno() != libc::EINTR {
            break;
        }
    }
    libc::close(sync_r);

    // Wire stdio pipes.
    if libc::dup2(stdin_r, libc::STDIN_FILENO) < 0
        || libc::dup2(stdout_w, libc::STDOUT_FILENO) < 0
        || libc::dup2(stderr_w, libc::STDERR_FILENO) < 0
    {
        fail_child(err_w, 1, last_errno());
    }
    close_fds(&[stdin_r, stdout_w, stderr_w]);

    // Close any inherited descriptors >= 3 except err_w (which has FD_CLOEXEC).
    for fd in 3..1024 {
        if fd != err_w {
            libc::close(fd);
        }
    }

    if enable_loopback_only {
        if let Err(e) = bring_up_loopback() {
            fail_child(err_w, 2, e);
        }
    }

    let hostname = b"aegis-sandbox";
    if libc::sethostname(hostname.as_ptr() as *const libc::c_char, hostname.len()) != 0 {
        fail_child(err_w, 3, last_errno());
    }

    // Make mount propagation private before mounting /dev, /proc, and pivot_root.
    if libc::mount(
        ptr::null(),
        c"/".as_ptr(),
        ptr::null(),
        (libc::MS_REC | libc::MS_PRIVATE) as libc::c_ulong,
        ptr::null(),
    ) != 0
    {
        fail_child(err_w, 4, last_errno());
    }

    // Bind-mount upper/tmp onto merged/tmp so /tmp stays writable after / is remounted MS_RDONLY.
    if libc::mount(
        upper_tmp_cstr.as_ptr(),
        merged_tmp_cstr.as_ptr(),
        ptr::null(),
        (libc::MS_BIND | libc::MS_NOSUID | libc::MS_NODEV) as libc::c_ulong,
        ptr::null(),
    ) != 0
    {
        fail_child(err_w, 5, last_errno());
    }

    // Bind-mount safe character devices.
    for dev in device_binds {
        let fd = libc::open(
            dev.target_dev.as_ptr(),
            libc::O_CREAT | libc::O_WRONLY | libc::O_CLOEXEC,
            0o666,
        );
        if fd < 0 {
            fail_child(err_w, 6, last_errno());
        }
        libc::close(fd);

        if libc::mount(
            dev.host_dev.as_ptr(),
            dev.target_dev.as_ptr(),
            ptr::null(),
            libc::MS_BIND as libc::c_ulong,
            ptr::null(),
        ) != 0
        {
            fail_child(err_w, 7, last_errno());
        }
        if libc::mount(
            ptr::null(),
            dev.target_dev.as_ptr(),
            ptr::null(),
            (libc::MS_BIND | libc::MS_REMOUNT | libc::MS_NOSUID | libc::MS_NOEXEC) as libc::c_ulong,
            ptr::null(),
        ) != 0
        {
            fail_child(err_w, 8, last_errno());
        }
    }

    // Mount isolated procfs for the child PID namespace.
    if libc::mount(
        c"proc".as_ptr(),
        merged_proc_cstr.as_ptr(),
        c"proc".as_ptr(),
        (libc::MS_NOSUID | libc::MS_NOEXEC | libc::MS_NODEV) as libc::c_ulong,
        ptr::null(),
    ) != 0
    {
        fail_child(err_w, 9, last_errno());
    }

    // pivot_root into merged rootfs and detach host root.
    if libc::chdir(merged_cstr.as_ptr()) != 0 {
        fail_child(err_w, 10, last_errno());
    }
    if libc::syscall(libc::SYS_pivot_root, c".".as_ptr(), c".old_root".as_ptr()) != 0 {
        fail_child(err_w, 11, last_errno());
    }
    if libc::chdir(c"/".as_ptr()) != 0 {
        fail_child(err_w, 12, last_errno());
    }
    if libc::umount2(c"/.old_root".as_ptr(), libc::MNT_DETACH) != 0 {
        fail_child(err_w, 13, last_errno());
    }
    let _ = libc::rmdir(c"/.old_root".as_ptr());

    // Enforce read-only root filesystem (writable /tmp remains mounted separately).
    if libc::mount(
        c"/".as_ptr(),
        c"/".as_ptr(),
        ptr::null(),
        (libc::MS_BIND | libc::MS_REMOUNT | libc::MS_RDONLY | libc::MS_NOSUID | libc::MS_NODEV)
            as libc::c_ulong,
        ptr::null(),
    ) != 0
    {
        fail_child(err_w, 14, last_errno());
    }

    if libc::chdir(c"/tmp".as_ptr()) != 0 {
        fail_child(err_w, 15, last_errno());
    }

    // Enforce POSIX resource limits.
    let nofile = libc::rlimit {
        rlim_cur: 64,
        rlim_max: 64,
    };
    if libc::setrlimit(libc::RLIMIT_NOFILE, &nofile) != 0 {
        fail_child(err_w, 16, last_errno());
    }
    let core = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    if libc::setrlimit(libc::RLIMIT_CORE, &core) != 0 {
        fail_child(err_w, 17, last_errno());
    }

    // Drop supplementary groups and all Linux bounding capabilities.
    if libc::setgroups(0, ptr::null()) != 0 {
        fail_child(err_w, 18, last_errno());
    }
    for cap in 0..=63 {
        if libc::prctl(libc::PR_CAPBSET_DROP, cap, 0, 0, 0) != 0 {
            let e = last_errno();
            if e != libc::EINVAL {
                fail_child(err_w, 19, e);
            }
        }
    }

    // Drop to unprivileged UID/GID 10000.
    if libc::setresgid(SANDBOX_GID, SANDBOX_GID, SANDBOX_GID) != 0 {
        fail_child(err_w, 20, last_errno());
    }
    if libc::setresuid(SANDBOX_UID, SANDBOX_UID, SANDBOX_UID) != 0 {
        fail_child(err_w, 21, last_errno());
    }

    // Install seccomp-bpf filter (also sets PR_SET_NO_NEW_PRIVS = 1).
    let seccomp_err = apply_seccomp_filter(seccomp_filter);
    if seccomp_err != 0 {
        fail_child(err_w, 22, seccomp_err);
    }

    libc::execve(interp_cstr.as_ptr(), argv_ptrs.as_ptr(), envp_ptrs.as_ptr());
    fail_child(err_w, 23, last_errno());
}

unsafe fn bring_up_loopback() -> Result<(), i32> {
    let sock = libc::socket(libc::AF_INET, libc::SOCK_DGRAM | libc::SOCK_CLOEXEC, 0);
    if sock < 0 {
        return Err(last_errno());
    }

    let mut ifr: libc::ifreq = std::mem::zeroed();
    ifr.ifr_name[0] = b'l' as libc::c_char;
    ifr.ifr_name[1] = b'o' as libc::c_char;

    if libc::ioctl(sock, SIOCGIFFLAGS, &mut ifr) < 0 {
        let e = last_errno();
        libc::close(sock);
        return Err(e);
    }

    ifr.ifr_ifru.ifru_flags |= IFF_UP | IFF_RUNNING;
    if libc::ioctl(sock, SIOCSIFFLAGS, &ifr) < 0 {
        let e = last_errno();
        libc::close(sock);
        return Err(e);
    }

    libc::close(sock);
    Ok(())
}

unsafe fn fail_child(err_w: RawFd, step: i32, errno_val: i32) -> ! {
    let payload = [step, errno_val];
    let _ = libc::write(
        err_w,
        payload.as_ptr() as *const libc::c_void,
        std::mem::size_of::<[i32; 2]>(),
    );
    libc::close(err_w);
    libc::_exit(127);
}

fn create_cloexec_pipe() -> Result<(RawFd, RawFd), SandboxError> {
    let mut fds = [0 as RawFd; 2];
    if unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC) } != 0 {
        return Err(SandboxError::SpawnFailed(std::io::Error::last_os_error()));
    }
    Ok((fds[0], fds[1]))
}

fn close_fds(fds: &[RawFd]) {
    for &fd in fds {
        if fd >= 0 {
            unsafe {
                libc::close(fd);
            }
        }
    }
}

fn to_cstring(s: &str) -> Result<CString, SandboxError> {
    CString::new(s)
        .map_err(|e| SandboxError::InvalidRequest(format!("interior NUL byte in '{s}': {e}")))
}

#[inline]
fn last_errno() -> i32 {
    std::io::Error::last_os_error()
        .raw_os_error()
        .unwrap_or(libc::EPERM)
}
