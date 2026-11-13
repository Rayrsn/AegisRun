"""Adversarial 7-vector red-team containment suite against live AegisRun stack."""

from __future__ import annotations

import argparse
import dataclasses
import os
import sys
import time
from collections.abc import Callable
from pathlib import Path

# Allow running directly without installing aegis-sdk globally.
_REPO_ROOT = Path(__file__).resolve().parents[1]
_SDK_DIR = _REPO_ROOT / "sdk-py"
if str(_SDK_DIR) not in sys.path:
    sys.path.insert(0, str(_SDK_DIR))

from aegis_sdk import (
    AegisClient,
    AegisSandboxError,
    SandboxOOMError,
    SandboxSecurityViolationError,
)
from aegis_sdk.models import ExecutionResult, ResourceLimits


@dataclasses.dataclass(slots=True)
class AttackOutcome:
    """Result record for a single adversarial containment check."""

    vector: str
    passed: bool
    duration_ms: float
    detail: str


def _exec_py(
    client: AegisClient,
    name: str,
    code: str,
    *,
    limits: ResourceLimits | None = None,
) -> ExecutionResult:
    return client.execute(
        tool_name=name,
        entrypoint_code=code,
        runtime="PYTHON_3_12",
        limits=limits or ResourceLimits(wall_timeout_ms=5000, memory_max_bytes=128 * 1024 * 1024, pids_max=16),
        session_id=f"redteam-{name}-{time.time_ns()}",
    )


def vector_1_fork_bomb(client: AegisClient) -> str:
    """Prove cgroup v2 pids.max=16 blocks fork bombs and cgroup.kill reaps all children."""
    code = """
import os
import time

children = []
blocked = False
for _ in range(64):
    try:
        pid = os.fork()
        if pid == 0:
            time.sleep(0.25)
            os._exit(0)
        children.append(pid)
    except OSError as exc:
        print(f"FORK_BLOCKED:{exc.errno}:{len(children)}")
        blocked = True
        break

if not blocked:
    raise AssertionError("Fork bomb was NOT blocked by pids.max!")
"""
    res = _exec_py(
        client,
        "redteam_fork_bomb",
        code,
        limits=ResourceLimits(wall_timeout_ms=5000, memory_max_bytes=128 * 1024 * 1024, pids_max=16),
    )
    if "FORK_BLOCKED:" not in res.stdout:
        raise AssertionError(f"Expected FORK_BLOCKED marker, got stdout={res.stdout!r} stderr={res.stderr!r}")
    return res.stdout.strip()


def vector_2_memory_exhaustion(client: AegisClient) -> str:
    """Prove 128 MiB cgroup memory limit triggers immediate kernel OOM_KILLED."""
    code = """
x = bytearray(b"A" * (512 * 1024 * 1024))
print(len(x))
"""
    try:
        res = _exec_py(
            client,
            "redteam_oom",
            code,
            limits=ResourceLimits(wall_timeout_ms=5000, memory_max_bytes=128 * 1024 * 1024, pids_max=16),
        )
    except SandboxOOMError as exc:
        return f"OOM_KILLED peak={exc.peak_memory_bytes}B ({exc.to_llm_observation()})"
    raise AssertionError(f"Expected SandboxOOMError, got status={res.status} stdout={res.stdout!r}")


def vector_3_credential_exfiltration(client: AegisClient) -> str:
    """Prove UID 10000 execution, blocked /etc/shadow access, and isolated PID namespace."""
    code = """
import os

uid = os.getuid()
assert uid == 10000, f"expected UID 10000, got {uid}"

try:
    open("/etc/shadow", "r").read()
    raise AssertionError("/etc/shadow readable unexpectedly!")
except (PermissionError, FileNotFoundError) as exc:
    shadow_msg = type(exc).__name__

pid1_cmd = open("/proc/1/cmdline", "rb").read().replace(b"\\x00", b" ").decode("utf-8", errors="replace").strip()
pid1_env = open("/proc/1/environ", "rb").read()
assert b"POSTGRES_PASSWORD" not in pid1_env and b"AWS_SECRET" not in pid1_env
print(f"UID={uid} SHADOW={shadow_msg} PID1={pid1_cmd}")
"""
    res = _exec_py(client, "redteam_cred_exfil", code)
    if "UID=10000" not in res.stdout or "SHADOW=" not in res.stdout:
        raise AssertionError(f"Credential isolation assertion failed: stdout={res.stdout!r} stderr={res.stderr!r}")
    return res.stdout.strip()


def vector_4_seccomp_mount_escape(client: AegisClient) -> str:
    """Prove seccomp-bpf intercepts raw SYS_mount with SIGSYS / SYSCALL_VIOLATION."""
    code = """
import ctypes
libc = ctypes.CDLL(None)
libc.syscall(165, 0, 0, 0, 0, 0)
print("MOUNT_ESCAPE_SUCCEEDED")
"""
    try:
        res = _exec_py(client, "redteam_seccomp_mount", code)
    except SandboxSecurityViolationError as exc:
        return f"SYSCALL_VIOLATION ({exc.to_llm_observation()})"
    raise AssertionError(f"Expected SandboxSecurityViolationError, got status={res.status} stdout={res.stdout!r}")


def vector_5_filesystem_fill(client: AegisClient) -> str:
    """Prove rootfs is MS_RDONLY, tmpfs enforces 32 MiB size/inode bounds, and teardown leaves zero state."""
    code = """
import errno
import os

try:
    with open("/usr/bin/host_pwn.txt", "w") as f:
        f.write("escape")
    raise AssertionError("Write to /usr/bin unexpectedly succeeded!")
except OSError as exc:
    ro_errno = exc.errno

written = 0
fill_errno = None
try:
    with open("/tmp/junk.bin", "wb") as f:
        chunk = b"X" * (1024 * 1024)
        for _ in range(500):
            f.write(chunk)
            f.flush()
            written += 1
    raise AssertionError("500 MiB write succeeded despite 32 MiB tmpfs limit!")
except OSError as exc:
    fill_errno = exc.errno

assert ro_errno == errno.EROFS, f"expected EROFS ({errno.EROFS}), got {ro_errno}"
assert fill_errno == errno.ENOSPC, f"expected ENOSPC ({errno.ENOSPC}), got {fill_errno}"
print(f"RO_ERRNO={ro_errno} TMPFS_FILL_ERRNO={fill_errno} WRITTEN_MIB={written}")
"""
    res1 = _exec_py(client, "redteam_fs_fill", code)
    if "TMPFS_FILL_ERRNO=" not in res1.stdout:
        raise AssertionError(f"FS bounds assertion failed: stdout={res1.stdout!r} stderr={res1.stderr!r}")

    res2 = _exec_py(
        client,
        "redteam_fs_ephemeral_check",
        'import os\nprint("CLEAN" if not os.path.exists("/tmp/junk.bin") else "LEAKED")',
    )
    if res2.stdout.strip() != "CLEAN":
        raise AssertionError(f"Expected /tmp/junk.bin to vanish between executions, got {res2.stdout!r}")
    return f"{res1.stdout.strip()} (ephemeral=CLEAN)"


def vector_6_network_exfiltration(client: AegisClient) -> str:
    """Prove CLONE_NEWNET disables external TCP/UDP egress (ENETUNREACH)."""
    code = """
import errno
import socket

try:
    socket.create_connection(("1.1.1.1", 80), timeout=1.0)
    raise AssertionError("Outbound TCP connection to 1.1.1.1:80 succeeded unexpectedly!")
except OSError as exc:
    print(f"NET_BLOCKED:errno={exc.errno}:{exc}")
"""
    res = _exec_py(client, "redteam_net_exfil", code)
    if "NET_BLOCKED:" not in res.stdout:
        raise AssertionError(f"Expected network block, got stdout={res.stdout!r} stderr={res.stderr!r}")
    return res.stdout.strip()


def vector_7_output_flood_dos(client: AegisClient) -> str:
    """Prove io_streamer 4 MiB flood guard terminates infinite stdout floods immediately."""
    code = """
while True:
    print("A" * 65536, flush=True)
"""
    try:
        res = _exec_py(client, "redteam_output_flood", code)
    except AegisSandboxError as exc:
        if "output flood guard" not in str(exc) and len(exc.stdout) > (4 * 1024 * 1024 + 131_072):
            raise AssertionError(f"Unexpected output flood error: {exc}") from exc
        return f"FLOOD_KILLED ({exc})"
    raise AssertionError(f"Expected output flood kill, got status={res.status} stdout_len={len(res.stdout)}")


def verify_zero_host_leaks() -> tuple[int, int]:
    """Audit /proc/mounts and /sys/fs/cgroup/aegis.slice on Linux for leaked sandbox artifacts."""
    if sys.platform != "linux":
        return (0, 0)

    mounts_text = Path("/proc/mounts").read_text(encoding="utf-8", errors="replace")
    leaked_mounts = sum(1 for line in mounts_text.splitlines() if "/run/aegis/sandboxes" in line)

    cgroup_root = Path("/sys/fs/cgroup/aegis.slice")
    leaked_cgroups = 0
    if cgroup_root.is_dir():
        leaked_cgroups = sum(1 for entry in cgroup_root.iterdir() if entry.name.startswith("exec-"))

    return (leaked_mounts, leaked_cgroups)


def run_suite(control_url: str, api_key: str) -> int:
    """Run all 7 adversarial attack vectors sequentially and print a structured audit report."""
    vectors: list[tuple[str, Callable[[AegisClient], str]]] = [
        ("1. Fork Bomb DoS", vector_1_fork_bomb),
        ("2. Memory Exhaustion (512M > 128M)", vector_2_memory_exhaustion),
        ("3. Host Credential Exfiltration", vector_3_credential_exfiltration),
        ("4. Seccomp Container Escape (mount)", vector_4_seccomp_mount_escape),
        ("5. Filesystem Fill & Read-Only Rootfs", vector_5_filesystem_fill),
        ("6. Outbound Socket Exfiltration", vector_6_network_exfiltration),
        ("7. Stdout Flood DoS (>4 MiB)", vector_7_output_flood_dos),
    ]

    outcomes: list[AttackOutcome] = []
    with AegisClient(base_url=control_url, api_key=api_key, timeout=20.0) as client:
        for label, fn in vectors:
            t0 = time.perf_counter()
            try:
                detail = fn(client)
                outcomes.append(
                    AttackOutcome(
                        vector=label,
                        passed=True,
                        duration_ms=(time.perf_counter() - t0) * 1000.0,
                        detail=detail,
                    )
                )
            except Exception as exc:  # noqa: BLE001
                outcomes.append(
                    AttackOutcome(
                        vector=label,
                        passed=False,
                        duration_ms=(time.perf_counter() - t0) * 1000.0,
                        detail=repr(exc),
                    )
                )

    print("\n=== AegisRun 7-Vector Adversarial Red-Team Report ===")
    failures = 0
    for o in outcomes:
        mark = "PASS" if o.passed else "FAIL"
        if not o.passed:
            failures += 1
        print(f"[{mark}] {o.vector:<38} ({o.duration_ms:6.2f} ms) -> {o.detail}")

    leaked_mounts, leaked_cgroups = verify_zero_host_leaks()
    print(f"Post-Suite Leak Audit: mounts={leaked_mounts}, cgroups={leaked_cgroups}")
    if leaked_mounts != 0 or leaked_cgroups != 0:
        failures += 1

    return 1 if failures > 0 else 0


def main() -> int:
    """Parse CLI args and execute the adversarial containment suite."""
    parser = argparse.ArgumentParser(description="Run AegisRun 7-vector red-team security suite.")
    parser.add_argument("--url", default=os.getenv("AEGIS_CONTROL_URL", "http://localhost:8080"))
    parser.add_argument("--api-key", default=os.getenv("AEGIS_API_KEY", "aegis-dev-token"))
    args = parser.parse_args()
    return run_suite(args.url, args.api_key)


if __name__ == "__main__":
    raise SystemExit(main())
