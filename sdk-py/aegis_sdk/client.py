"""Synchronous and asynchronous HTTP/SSE clients for the AegisRun Control Plane."""

from __future__ import annotations

import asyncio
import json
import os
import random
import time
import uuid
from collections.abc import Callable
from typing import Any, Self

import httpx

from .exceptions import (
    AegisSandboxError,
    SandboxOOMError,
    SandboxRejectedError,
    SandboxRuntimeError,
    SandboxSecurityViolationError,
    SandboxTimeoutError,
)
from .harness import RETURN_SENTINEL, parse_harness_output
from .models import ExecutionResult, ResourceLimits, SandboxMetrics

_RETRYABLE_STATUS_CODES = frozenset({502, 503, 504})

_RUNTIME_MAP: dict[str, str] = {
    "PYTHON_3_12": "python3.12",
    "python3.12": "python3.12",
    "python": "python3.12",
    "NODE_20": "node20",
    "node20": "node20",
    "node": "node20",
    "BASH_5": "bash5",
    "bash5": "bash5",
    "bash": "bash5",
    "sh": "bash5",
}


def normalize_runtime(runtime: str) -> str:
    """Normalize enum-style or REST-style runtime identifiers for `/v1/execute`."""
    key = runtime.strip()
    mapped = _RUNTIME_MAP.get(key) or _RUNTIME_MAP.get(key.upper()) or _RUNTIME_MAP.get(key.lower())
    if mapped is None:
        raise ValueError(f"Unsupported sandbox runtime: {runtime!r}")
    return mapped


def _build_headers(
    api_key: str,
    agent_session_id: str,
    *,
    stream: bool,
) -> dict[str, str]:
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "X-Agent-Session-ID": agent_session_id,
    }
    if stream:
        headers["X-Execution-Mode"] = "stream"
        headers["Accept"] = "text/event-stream"
    else:
        headers["X-Execution-Mode"] = "sync"
        headers["Accept"] = "application/json"
    return headers


def _retry_delay_seconds(attempt: int) -> float:
    base = 0.05 * (2**attempt)
    jitter = random.uniform(0.0, 0.025)
    return float(base + jitter)


def _error_from_response_payload(
    *,
    execution_id: str,
    status: str,
    error_message: str,
    stdout: str,
    stderr: str,
    metrics: SandboxMetrics,
    limits: ResourceLimits,
) -> AegisSandboxError | None:
    if status == "OOM_KILLED":
        return SandboxOOMError(
            execution_id,
            metrics.peak_memory_bytes,
            limits.memory_max_bytes,
            stdout=stdout,
            stderr=stderr,
        )
    if status == "TIMEOUT_KILLED":
        return SandboxTimeoutError(
            execution_id,
            limits.wall_timeout_ms,
            stdout=stdout,
            stderr=stderr,
        )
    if status == "SYSCALL_VIOLATION":
        return SandboxSecurityViolationError(
            execution_id,
            error_message,
            stdout=stdout,
            stderr=stderr,
        )
    if status == "REJECTED":
        return SandboxRejectedError(
            execution_id,
            error_message,
            stdout=stdout,
            stderr=stderr,
        )
    return None


def _finalize_execution_result(
    *,
    execution_id: str,
    status: str,
    runtime: str,
    tool_name: str | None,
    worker_node_id: str | None,
    raw_stdout: str,
    raw_stderr: str,
    error_message: str | None,
    approval_required: bool,
    idempotent_replay: bool,
    metrics: SandboxMetrics,
    limits: ResourceLimits,
) -> ExecutionResult:
    user_stdout, harness_env = parse_harness_output(raw_stdout)

    mapped_err = _error_from_response_payload(
        execution_id=execution_id,
        status=status,
        error_message=error_message or "",
        stdout=user_stdout,
        stderr=raw_stderr,
        metrics=metrics,
        limits=limits,
    )
    if mapped_err is not None:
        raise mapped_err

    if harness_env is not None and harness_env.get("ok") is False:
        raise SandboxRuntimeError(
            execution_id,
            str(harness_env.get("error_type", "RuntimeError")),
            str(harness_env.get("error_message", "Execution raised an exception")),
            str(harness_env.get("traceback", "")),
            exit_code=metrics.exit_code,
            stdout=user_stdout,
            stderr=raw_stderr,
        )

    if status == "FAILED":
        raise AegisSandboxError(
            execution_id,
            status,
            error_message or f"Execution failed with exit code {metrics.exit_code}",
            stdout=user_stdout,
            stderr=raw_stderr,
        )

    return_val = harness_env.get("result") if harness_env is not None else None
    return ExecutionResult(
        execution_id=execution_id,
        status=status,
        runtime=runtime,
        tool_name=tool_name,
        worker_node_id=worker_node_id,
        exit_code=metrics.exit_code,
        stdout=user_stdout,
        stderr=raw_stderr,
        return_value=return_val,
        error_message=error_message,
        approval_required=approval_required,
        idempotent_replay=idempotent_replay,
        metrics=metrics,
    )


def _decode_json_execution_response(
    body: dict[str, Any],
    *,
    default_runtime: str,
    tool_name: str,
    idempotent_replay: bool,
    limits: ResourceLimits,
) -> ExecutionResult:
    raw_metrics = body.get("metrics") or {
        "exit_code": body.get("exit_code", 0),
        "signaled": body.get("signaled", 0),
        "setup_duration_us": body.get("setup_duration_us", 0),
        "exec_duration_us": body.get("exec_duration_us", 0),
        "teardown_duration_us": body.get("teardown_duration_us", 0),
        "peak_memory_bytes": body.get("peak_memory_bytes", 0),
        "cpu_usage_us": body.get("cpu_usage_us", 0),
    }
    metrics = SandboxMetrics.model_validate(raw_metrics)
    return _finalize_execution_result(
        execution_id=str(body.get("execution_id", "")),
        status=str(body.get("status", "COMPLETED")),
        runtime=str(body.get("runtime", default_runtime)),
        tool_name=str(body.get("tool_name") or tool_name),
        worker_node_id=body.get("worker_node_id"),
        raw_stdout=str(body.get("stdout", "")),
        raw_stderr=str(body.get("stderr", "")),
        error_message=body.get("error_message"),
        approval_required=bool(body.get("approval_required", False)),
        idempotent_replay=idempotent_replay,
        metrics=metrics,
        limits=limits,
    )


class _SSEStreamCollector:
    """Incrementally parses SSE frames and streams sanitized user chunks to callbacks."""

    def __init__(
        self,
        *,
        runtime: str,
        tool_name: str,
        limits: ResourceLimits,
        idempotent_replay: bool,
        on_stdout: Callable[[str], None] | None,
        on_stderr: Callable[[str], None] | None,
    ) -> None:
        self.runtime = runtime
        self.tool_name = tool_name
        self.limits = limits
        self.idempotent_replay = idempotent_replay
        self.on_stdout = on_stdout
        self.on_stderr = on_stderr

        self._cur_event = ""
        self._cur_data: list[str] = []
        self._raw_stdout: list[str] = []
        self._raw_stderr: list[str] = []
        self._stdout_pending = ""
        self._sentinel_seen = False

        self.execution_id = ""
        self.worker_node_id: str | None = None
        self.terminal_status = "COMPLETED"
        self.error_message: str | None = None
        self.approval_required = False
        self.metrics = SandboxMetrics()

    def feed_line(self, line: str) -> None:
        """Consume a single line from an SSE response stream."""
        if line == "":
            self._flush_frame()
            return
        if line.startswith("event:"):
            self._cur_event = line[len("event:") :].strip()
        elif line.startswith("data:"):
            self._cur_data.append(line[len("data:") :].strip())

    def _emit_clean_stdout_chunk(self, chunk: str) -> None:
        self._raw_stdout.append(chunk)
        if self.on_stdout is None or self._sentinel_seen:
            return

        buf = self._stdout_pending + chunk
        sentinel_idx = buf.find(RETURN_SENTINEL)
        if sentinel_idx != -1:
            emit_part = buf[:sentinel_idx].removesuffix("\n")
            if emit_part:
                self.on_stdout(emit_part)
            self._stdout_pending = ""
            self._sentinel_seen = True
            return

        # Hold back trailing characters matching a prefix of `\n<RETURN_SENTINEL>` or `<RETURN_SENTINEL>`.
        hold = 0
        for marker in (f"\n{RETURN_SENTINEL}", RETURN_SENTINEL):
            max_check = min(len(buf), len(marker) - 1)
            for length in range(max_check, 0, -1):
                if marker.startswith(buf[-length:]) and length > hold:
                    hold = length
                    break
        if hold > 0:
            emit_part = buf[:-hold]
            self._stdout_pending = buf[-hold:]
        else:
            emit_part = buf
            self._stdout_pending = ""
        if emit_part:
            self.on_stdout(emit_part)

    def _flush_frame(self) -> None:
        if not self._cur_event and not self._cur_data:
            return
        raw_json = "\n".join(self._cur_data)
        evt_name = self._cur_event
        self._cur_event = ""
        self._cur_data = []

        if not raw_json:
            return
        try:
            payload = json.loads(raw_json)
        except json.JSONDecodeError:
            return
        if not isinstance(payload, dict):
            return

        if payload.get("execution_id"):
            self.execution_id = str(payload["execution_id"])
        if payload.get("worker_node_id"):
            self.worker_node_id = str(payload["worker_node_id"])

        kind = (evt_name or str(payload.get("event_type", ""))).lower()
        if kind == "approval_required":
            self.approval_required = True
        elif kind == "stdout_chunk":
            self._emit_clean_stdout_chunk(str(payload.get("data", "")))
        elif kind == "stderr_chunk":
            err_chunk = str(payload.get("data", ""))
            self._raw_stderr.append(err_chunk)
            if self.on_stderr is not None and err_chunk:
                self.on_stderr(err_chunk)
        elif kind in {
            "completed",
            "oom_killed",
            "timeout_killed",
            "syscall_violation",
            "rejected",
            "failed",
        }:
            self.terminal_status = kind.upper()
            if payload.get("error_message"):
                self.error_message = str(payload["error_message"])
            if isinstance(payload.get("metrics"), dict):
                self.metrics = SandboxMetrics.model_validate(payload["metrics"])

    def build_result(self) -> ExecutionResult:
        """Flush buffered partial SSE state and construct or raise the final ExecutionResult."""
        self._flush_frame()
        if self.on_stdout is not None and self._stdout_pending and not self._sentinel_seen:
            self.on_stdout(self._stdout_pending)
            self._stdout_pending = ""

        return _finalize_execution_result(
            execution_id=self.execution_id,
            status=self.terminal_status,
            runtime=self.runtime,
            tool_name=self.tool_name,
            worker_node_id=self.worker_node_id,
            raw_stdout="".join(self._raw_stdout),
            raw_stderr="".join(self._raw_stderr),
            error_message=self.error_message,
            approval_required=self.approval_required,
            idempotent_replay=self.idempotent_replay,
            metrics=self.metrics,
            limits=self.limits,
        )


def _build_execute_body(
    *,
    agent_session_id: str,
    tool_name: str,
    runtime: str,
    entrypoint_code: str,
    args: dict[str, Any],
    limits: ResourceLimits,
    enable_loopback_only: bool,
    stream: bool,
    requires_approval: bool,
    webhook_url: str | None,
) -> tuple[str, dict[str, Any]]:
    norm_runtime = normalize_runtime(runtime)
    body: dict[str, Any] = {
        "agent_session_id": agent_session_id,
        "tool_name": tool_name,
        "runtime": norm_runtime,
        "entrypoint_code": entrypoint_code,
        "args": args,
        "limits": limits.model_dump(),
        "enable_loopback_only": enable_loopback_only,
        "mode": "stream" if stream else "sync",
        "requires_approval": requires_approval,
    }
    if webhook_url:
        body["webhook_url"] = webhook_url
    return norm_runtime, body


def _parse_http_error(resp: httpx.Response, raw_body: bytes) -> AegisSandboxError:
    detail = ""
    try:
        parsed = json.loads(raw_body.decode("utf-8", errors="replace"))
        if isinstance(parsed, dict) and parsed.get("error"):
            detail = str(parsed["error"])
    except json.JSONDecodeError:
        detail = raw_body.decode("utf-8", errors="replace").strip()
    if not detail:
        detail = f"Control plane returned HTTP {resp.status_code}"
    return AegisSandboxError("", f"HTTP_{resp.status_code}", detail)


def _resolve_setting(explicit: str | None, env_var: str, default: str) -> str:
    if explicit:
        return explicit
    return os.getenv(env_var) or default


class AegisClient:
    """Synchronous HTTP/SSE client for executing sandbox tools via control-go."""

    def __init__(
        self,
        base_url: str | None = None,
        api_key: str | None = None,
        *,
        timeout: float = 30.0,
        max_retries: int = 2,
        session_id: str | None = None,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self.base_url: str = _resolve_setting(base_url, "AEGIS_CONTROL_URL", "http://localhost:8080").rstrip("/")
        self.api_key: str = _resolve_setting(api_key, "AEGIS_API_KEY", "aegis-dev-token")
        self.max_retries = max(0, max_retries)
        self.default_session_id = session_id or f"py-sync-{uuid.uuid4().hex[:12]}"
        self._client = httpx.Client(base_url=self.base_url, timeout=timeout, transport=transport)

    def close(self) -> None:
        """Close underlying HTTP connection pool."""
        self._client.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    def execute(
        self,
        *,
        tool_name: str,
        entrypoint_code: str,
        args: dict[str, Any] | None = None,
        runtime: str = "PYTHON_3_12",
        limits: ResourceLimits | None = None,
        session_id: str | None = None,
        enable_loopback_only: bool = True,
        requires_approval: bool = False,
        webhook_url: str | None = None,
        on_stdout: Callable[[str], None] | None = None,
        on_stderr: Callable[[str], None] | None = None,
        stream: bool = False,
    ) -> ExecutionResult:
        """Execute a sandboxed payload synchronously with automatic 502/503/504 idempotent retries."""
        resolved_limits = limits or ResourceLimits()
        resolved_session = session_id or self.default_session_id
        use_stream = stream or on_stdout is not None or on_stderr is not None
        norm_runtime, body = _build_execute_body(
            agent_session_id=resolved_session,
            tool_name=tool_name,
            runtime=runtime,
            entrypoint_code=entrypoint_code,
            args=args or {},
            limits=resolved_limits,
            enable_loopback_only=enable_loopback_only,
            stream=use_stream,
            requires_approval=requires_approval,
            webhook_url=webhook_url,
        )
        headers = _build_headers(self.api_key, resolved_session, stream=use_stream)

        last_exc: Exception | None = None
        for attempt in range(self.max_retries + 1):
            try:
                if use_stream:
                    with self._client.stream("POST", "/v1/execute", json=body, headers=headers) as resp:
                        if resp.status_code in _RETRYABLE_STATUS_CODES and attempt < self.max_retries:
                            resp.read()
                            time.sleep(_retry_delay_seconds(attempt))
                            continue
                        if resp.status_code >= 400:
                            raise _parse_http_error(resp, resp.read())

                        replay = resp.headers.get("X-Aegis-Idempotent-Replay", "").lower() == "true"
                        collector = _SSEStreamCollector(
                            runtime=norm_runtime,
                            tool_name=tool_name,
                            limits=resolved_limits,
                            idempotent_replay=replay,
                            on_stdout=on_stdout,
                            on_stderr=on_stderr,
                        )
                        for line in resp.iter_lines():
                            collector.feed_line(line)
                        return collector.build_result()

                resp = self._client.post("/v1/execute", json=body, headers=headers)
                if resp.status_code in _RETRYABLE_STATUS_CODES and attempt < self.max_retries:
                    time.sleep(_retry_delay_seconds(attempt))
                    continue
                if resp.status_code >= 400:
                    raise _parse_http_error(resp, resp.content)
                replay = resp.headers.get("X-Aegis-Idempotent-Replay", "").lower() == "true"
                return _decode_json_execution_response(
                    resp.json(),
                    default_runtime=norm_runtime,
                    tool_name=tool_name,
                    idempotent_replay=replay,
                    limits=resolved_limits,
                )
            except httpx.TransportError as exc:
                last_exc = exc
                if attempt >= self.max_retries:
                    raise AegisSandboxError("", "TRANSPORT_ERROR", str(exc)) from exc
                time.sleep(_retry_delay_seconds(attempt))

        raise AegisSandboxError("", "RETRY_EXHAUSTED", str(last_exc or "retries exhausted"))


class AsyncAegisClient:
    """Asynchronous HTTP/SSE client for executing sandbox tools via control-go."""

    def __init__(
        self,
        base_url: str | None = None,
        api_key: str | None = None,
        *,
        timeout: float = 30.0,
        max_retries: int = 2,
        session_id: str | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self.base_url: str = _resolve_setting(base_url, "AEGIS_CONTROL_URL", "http://localhost:8080").rstrip("/")
        self.api_key: str = _resolve_setting(api_key, "AEGIS_API_KEY", "aegis-dev-token")
        self.max_retries = max(0, max_retries)
        self.default_session_id = session_id or f"py-async-{uuid.uuid4().hex[:12]}"
        self._client = httpx.AsyncClient(base_url=self.base_url, timeout=timeout, transport=transport)

    async def aclose(self) -> None:
        """Close underlying async HTTP connection pool."""
        await self._client.aclose()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_args: object) -> None:
        await self.aclose()

    async def execute(
        self,
        *,
        tool_name: str,
        entrypoint_code: str,
        args: dict[str, Any] | None = None,
        runtime: str = "PYTHON_3_12",
        limits: ResourceLimits | None = None,
        session_id: str | None = None,
        enable_loopback_only: bool = True,
        requires_approval: bool = False,
        webhook_url: str | None = None,
        on_stdout: Callable[[str], None] | None = None,
        on_stderr: Callable[[str], None] | None = None,
        stream: bool = False,
    ) -> ExecutionResult:
        """Execute a sandboxed payload asynchronously with automatic 502/503/504 idempotent retries."""
        resolved_limits = limits or ResourceLimits()
        resolved_session = session_id or self.default_session_id
        use_stream = stream or on_stdout is not None or on_stderr is not None
        norm_runtime, body = _build_execute_body(
            agent_session_id=resolved_session,
            tool_name=tool_name,
            runtime=runtime,
            entrypoint_code=entrypoint_code,
            args=args or {},
            limits=resolved_limits,
            enable_loopback_only=enable_loopback_only,
            stream=use_stream,
            requires_approval=requires_approval,
            webhook_url=webhook_url,
        )
        headers = _build_headers(self.api_key, resolved_session, stream=use_stream)

        last_exc: Exception | None = None
        for attempt in range(self.max_retries + 1):
            try:
                if use_stream:
                    async with self._client.stream("POST", "/v1/execute", json=body, headers=headers) as resp:
                        if resp.status_code in _RETRYABLE_STATUS_CODES and attempt < self.max_retries:
                            await resp.aread()
                            await asyncio.sleep(_retry_delay_seconds(attempt))
                            continue
                        if resp.status_code >= 400:
                            raise _parse_http_error(resp, await resp.aread())

                        replay = resp.headers.get("X-Aegis-Idempotent-Replay", "").lower() == "true"
                        collector = _SSEStreamCollector(
                            runtime=norm_runtime,
                            tool_name=tool_name,
                            limits=resolved_limits,
                            idempotent_replay=replay,
                            on_stdout=on_stdout,
                            on_stderr=on_stderr,
                        )
                        async for line in resp.aiter_lines():
                            collector.feed_line(line)
                        return collector.build_result()

                resp = await self._client.post("/v1/execute", json=body, headers=headers)
                if resp.status_code in _RETRYABLE_STATUS_CODES and attempt < self.max_retries:
                    await asyncio.sleep(_retry_delay_seconds(attempt))
                    continue
                if resp.status_code >= 400:
                    raise _parse_http_error(resp, resp.content)
                replay = resp.headers.get("X-Aegis-Idempotent-Replay", "").lower() == "true"
                return _decode_json_execution_response(
                    resp.json(),
                    default_runtime=norm_runtime,
                    tool_name=tool_name,
                    idempotent_replay=replay,
                    limits=resolved_limits,
                )
            except httpx.TransportError as exc:
                last_exc = exc
                if attempt >= self.max_retries:
                    raise AegisSandboxError("", "TRANSPORT_ERROR", str(exc)) from exc
                await asyncio.sleep(_retry_delay_seconds(attempt))

        raise AegisSandboxError("", "RETRY_EXHAUSTED", str(last_exc or "retries exhausted"))
