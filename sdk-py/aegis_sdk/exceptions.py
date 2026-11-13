"""Typed sandbox exceptions with structured LLM self-correction observations."""

from __future__ import annotations


def _format_mb(num_bytes: int) -> str:
    mb = num_bytes / (1024 * 1024)
    if mb.is_integer():
        return f"{int(mb)}MB"
    return f"{mb:.1f}MB"


def _tail_snippet(text: str, max_chars: int = 300) -> str:
    cleaned = text.strip()
    if len(cleaned) <= max_chars:
        return cleaned
    return "..." + cleaned[-max_chars:]


class AegisSandboxError(Exception):
    """Base exception for all sandbox failures returned by AegisRun."""

    def __init__(
        self,
        execution_id: str,
        status: str,
        message: str,
        *,
        stdout: str = "",
        stderr: str = "",
    ) -> None:
        super().__init__(message)
        self.execution_id = execution_id
        self.status = status
        self.message = message
        self.stdout = stdout
        self.stderr = stderr

    def to_llm_observation(self) -> str:
        """Return a concise observation formatted for an autonomous agent to self-correct."""
        return (
            f"Tool execution failed (status={self.status}, execution_id={self.execution_id}): "
            f"{self.message}"
        )


class SandboxOOMError(AegisSandboxError):
    """Raised when the sandboxed payload triggers a cgroup v2 OOM kill."""

    def __init__(
        self,
        execution_id: str,
        peak_memory_bytes: int,
        limit_bytes: int,
        *,
        stdout: str = "",
        stderr: str = "",
    ) -> None:
        self.peak_memory_bytes = peak_memory_bytes
        self.limit_bytes = limit_bytes
        limit_str = _format_mb(limit_bytes) if limit_bytes > 0 else "configured"
        peak_str = _format_mb(peak_memory_bytes) if peak_memory_bytes > 0 else "unknown"
        msg = f"Memory limit ({limit_str}) exceeded (peak={peak_str})."
        super().__init__(execution_id, "OOM_KILLED", msg, stdout=stdout, stderr=stderr)

    def to_llm_observation(self) -> str:
        """Explain memory exhaustion and advise dataset reduction or chunking."""
        limit_str = _format_mb(self.limit_bytes) if self.limit_bytes > 0 else "configured limit"
        peak_str = _format_mb(self.peak_memory_bytes) if self.peak_memory_bytes > 0 else "unknown"
        return (
            f"Tool execution failed (execution_id={self.execution_id}): "
            f"Memory limit ({limit_str}) exceeded (peak={peak_str}). "
            "Reduce dataset size or chunk your computation."
        )


class SandboxTimeoutError(AegisSandboxError):
    """Raised when the sandbox exceeds its wall-clock deadline and is killed."""

    def __init__(
        self,
        execution_id: str,
        wall_timeout_ms: int,
        *,
        stdout: str = "",
        stderr: str = "",
    ) -> None:
        self.wall_timeout_ms = wall_timeout_ms
        msg = f"Wall-clock timeout ({wall_timeout_ms}ms) exceeded."
        super().__init__(execution_id, "TIMEOUT_KILLED", msg, stdout=stdout, stderr=stderr)

    def to_llm_observation(self) -> str:
        """Explain wall-clock timeout and advise algorithmic simplification."""
        return (
            f"Tool execution failed (execution_id={self.execution_id}): "
            f"Wall-clock timeout ({self.wall_timeout_ms}ms) exceeded. "
            "Optimize algorithmic complexity, avoid infinite loops, or reduce input size."
        )


class SandboxSecurityViolationError(AegisSandboxError):
    """Raised when seccomp-bpf or namespace containment terminates a restricted syscall."""

    def __init__(
        self,
        execution_id: str,
        syscall_info: str = "",
        *,
        stdout: str = "",
        stderr: str = "",
    ) -> None:
        self.syscall_info = syscall_info.strip()
        detail = f": {self.syscall_info}" if self.syscall_info else ""
        msg = f"Restricted system call or sandbox security policy violated{detail}."
        super().__init__(
            execution_id,
            "SYSCALL_VIOLATION",
            msg,
            stdout=stdout,
            stderr=stderr,
        )

    def to_llm_observation(self) -> str:
        """Explain sandbox security violation clearly to the calling agent."""
        detail = f" Details: {self.syscall_info}." if self.syscall_info else ""
        return (
            f"Tool execution blocked (execution_id={self.execution_id}): "
            f"Restricted system call or security policy violation.{detail} "
            "External network access, privileged syscalls, and host filesystem mutations are prohibited."
        )


class SandboxRuntimeError(AegisSandboxError):
    """Raised when user code executes inside the sandbox and raises an unhandled exception."""

    def __init__(
        self,
        execution_id: str,
        error_type: str,
        error_message: str,
        traceback_str: str = "",
        *,
        exit_code: int = 1,
        stdout: str = "",
        stderr: str = "",
    ) -> None:
        self.error_type = error_type
        self.error_message = error_message
        self.traceback_str = traceback_str
        self.exit_code = exit_code
        msg = f"{error_type}: {error_message}" if error_message else error_type
        super().__init__(execution_id, "RUNTIME_ERROR", msg, stdout=stdout, stderr=stderr)

    def to_llm_observation(self) -> str:
        """Format Python/runtime exception details so an LLM can repair its arguments or code."""
        trace_hint = _tail_snippet(self.traceback_str or self.stderr)
        suffix = f" Trace: {trace_hint}" if trace_hint else ""
        return (
            f"Tool execution failed (execution_id={self.execution_id}) with "
            f"{self.error_type}: {self.error_message}.{suffix} "
            "Fix the logic or input arguments and retry."
        )


class SandboxRejectedError(AegisSandboxError):
    """Raised when a Human-in-the-Loop approval gate rejects or expires an execution."""

    def __init__(
        self,
        execution_id: str,
        reason: str = "",
        *,
        stdout: str = "",
        stderr: str = "",
    ) -> None:
        self.reason = reason.strip()
        msg = self.reason or "Human reviewer rejected or timed out execution approval."
        super().__init__(execution_id, "REJECTED", msg, stdout=stdout, stderr=stderr)

    def to_llm_observation(self) -> str:
        """Inform the agent that the sensitive operation was rejected."""
        return (
            f"Tool execution rejected (execution_id={self.execution_id}): {self.message} "
            "Choose a safer alternative or ask the user for clarification."
        )

