use std::mem::size_of;

const SECCOMP_MODE_FILTER: libc::c_int = 2;
const SECCOMP_RET_KILL_PROCESS: u32 = 0x8000_0000;
const SECCOMP_RET_ALLOW: u32 = 0x7fff_0000;

#[cfg(target_arch = "x86_64")]
const AUDIT_ARCH_CURRENT: u32 = 0xc000_003e;
#[cfg(target_arch = "aarch64")]
const AUDIT_ARCH_CURRENT: u32 = 0xc000_00b7;

const BPF_LD: u16 = 0x00;
const BPF_W: u16 = 0x00;
const BPF_ABS: u16 = 0x20;
const BPF_JMP: u16 = 0x05;
const BPF_JEQ: u16 = 0x10;
const BPF_JGE: u16 = 0x30;
const BPF_K: u16 = 0x00;
const BPF_RET: u16 = 0x06;

const fn bpf_stmt(code: u16, k: u32) -> libc::sock_filter {
    libc::sock_filter {
        code,
        jt: 0,
        jf: 0,
        k,
    }
}

const fn bpf_jump(code: u16, k: u32, jt: u8, jf: u8) -> libc::sock_filter {
    libc::sock_filter { code, jt, jf, k }
}

/// Builds the seccomp-bpf deny-list filter blocking privileged/escape syscalls.
pub fn build_seccomp_filter() -> Vec<libc::sock_filter> {
    let blocked_syscalls: &[libc::c_long] = &[
        libc::SYS_mount,
        libc::SYS_umount2,
        libc::SYS_pivot_root,
        libc::SYS_chroot,
        libc::SYS_ptrace,
        libc::SYS_process_vm_readv,
        libc::SYS_process_vm_writev,
        libc::SYS_kexec_load,
        libc::SYS_kexec_file_load,
        libc::SYS_reboot,
        libc::SYS_init_module,
        libc::SYS_finit_module,
        libc::SYS_delete_module,
        libc::SYS_bpf,
        libc::SYS_perf_event_open,
        libc::SYS_userfaultfd,
        libc::SYS_keyctl,
        libc::SYS_add_key,
        libc::SYS_request_key,
        libc::SYS_swapon,
        libc::SYS_swapoff,
        libc::SYS_acct,
        libc::SYS_settimeofday,
        libc::SYS_clock_settime,
    ];

    let mut filter = Vec::with_capacity(16 + blocked_syscalls.len() * 2);

    // Verify architecture matches host ABI (seccomp_data.arch at offset 4).
    filter.push(bpf_stmt(BPF_LD | BPF_W | BPF_ABS, 4));
    filter.push(bpf_jump(
        BPF_JMP | BPF_JEQ | BPF_K,
        AUDIT_ARCH_CURRENT,
        1,
        0,
    ));
    filter.push(bpf_stmt(BPF_RET | BPF_K, SECCOMP_RET_KILL_PROCESS));

    // Load syscall number (seccomp_data.nr at offset 0).
    filter.push(bpf_stmt(BPF_LD | BPF_W | BPF_ABS, 0));

    // Block x32 ABI syscalls (>= 0x40000000).
    #[cfg(target_arch = "x86_64")]
    {
        filter.push(bpf_jump(BPF_JMP | BPF_JGE | BPF_K, 0x4000_0000, 0, 1));
        filter.push(bpf_stmt(BPF_RET | BPF_K, SECCOMP_RET_KILL_PROCESS));
    }

    for &sys_nr in blocked_syscalls {
        filter.push(bpf_jump(BPF_JMP | BPF_JEQ | BPF_K, sys_nr as u32, 0, 1));
        filter.push(bpf_stmt(BPF_RET | BPF_K, SECCOMP_RET_KILL_PROCESS));
    }

    // Restrict socket() domain to AF_UNIX, AF_INET, or AF_INET6.
    filter.push(bpf_jump(
        BPF_JMP | BPF_JEQ | BPF_K,
        libc::SYS_socket as u32,
        0,
        5,
    ));
    filter.push(bpf_stmt(BPF_LD | BPF_W | BPF_ABS, 16));
    filter.push(bpf_jump(
        BPF_JMP | BPF_JEQ | BPF_K,
        libc::AF_UNIX as u32,
        3,
        0,
    ));
    filter.push(bpf_jump(
        BPF_JMP | BPF_JEQ | BPF_K,
        libc::AF_INET as u32,
        2,
        0,
    ));
    filter.push(bpf_jump(
        BPF_JMP | BPF_JEQ | BPF_K,
        libc::AF_INET6 as u32,
        1,
        0,
    ));
    filter.push(bpf_stmt(BPF_RET | BPF_K, SECCOMP_RET_KILL_PROCESS));

    filter.push(bpf_stmt(BPF_RET | BPF_K, SECCOMP_RET_ALLOW));
    filter
}

/// Applies `PR_SET_NO_NEW_PRIVS` and installs the compiled seccomp-bpf filter on the thread.
///
/// # Safety
/// Must be called with a valid, initialized BPF instruction slice right before `execve`.
pub unsafe fn apply_seccomp_filter(filter: &[libc::sock_filter]) -> i32 {
    if libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) != 0 {
        return last_errno();
    }

    let prog = libc::sock_fprog {
        len: filter.len() as libc::c_ushort,
        filter: filter.as_ptr() as *mut libc::sock_filter,
    };

    if libc::prctl(
        libc::PR_SET_SECCOMP,
        SECCOMP_MODE_FILTER,
        &prog as *const libc::sock_fprog,
    ) != 0
    {
        return last_errno();
    }

    let _ = size_of::<libc::sock_fprog>();
    0
}

#[inline]
fn last_errno() -> i32 {
    std::io::Error::last_os_error()
        .raw_os_error()
        .unwrap_or(libc::EPERM)
}
