"""Unit and integration test suite for aegis-sdk."""

from __future__ import annotations

import json
import subprocess
import sys
from typing import Any

import httpx
import pytest

from aegis_sdk import (
    AegisClient,
    AsyncAegisClient,
    SandboxOOMError,
    SandboxRejectedError,
    SandboxRuntimeError,
    SandboxSecurityViolationError,
    SandboxTimeoutError,
    sandbox_tool,
)
from aegis_sdk.harness import RETURN_SENTINEL, synthesize_entrypoint


@sandbox_tool(runtime="PYTHON_3_12", memory_mb=128, timeout_ms=5000, requires_approval=False)
def analyze_records(
    records: list[dict[str, Any]],
    multiplier: float = 2.0,
    tags: list[str] | None = None,
) -> dict[str, Any]:
    """Compute scaled aggregate statistics over nested JSON records.

    Preserves multi-line docstrings and inner helper closures cleanly.
    """

    def _score(entry: dict[str, Any]) -> float:
        base = float(entry.get("value", 0.0))
        return base * multiplier

    scores = [_score(item) for item in records]
    print(f"processed={len(scores)}")
    return {
        "count": len(scores),
        "total": sum(scores),
        "tags": tags or ["default"],
    }


def test_ast_extraction_hygiene_and_closures() -> None:
    """Extracted source strips @sandbox_tool cleanly and executes isolated without aegis_sdk."""
    clean = analyze_records.clean_source
    assert "sandbox_tool" not in clean
    assert "aegis_sdk" not in clean
    assert "def analyze_records(" in clean
    assert "def _score(" in clean

    # Verify extracted function compiles and executes cleanly in a fresh namespace.
    isolated_ns: dict[str, Any] = {"Any": Any}
    exec(compile(clean, "<isolated-ast-test>", "exec"), isolated_ns)  # noqa: S102
    fn = isolated_ns["analyze_records"]
    out = fn([{"value": 1.5}, {"value": 2.5}], multiplier=3.0)
    assert out == {"count": 2, "total": 12.0, "tags": ["default"]}

    # Verify full synthesized harness script compiles and runs in a separate clean interpreter without aegis_sdk.
    harness_src = synthesize_entrypoint(clean, "analyze_records")
    compile(harness_src, "<isolated-harness>", "exec")
    proc = subprocess.run(
        [sys.executable, "-c", harness_src],
        input=json.dumps({"records": [{"value": 4.0}]}),
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr

    raw_out = proc.stdout
    assert raw_out.startswith("processed=1\n")
    assert RETURN_SENTINEL in raw_out
    sentinel_line = raw_out.split(RETURN_SENTINEL, maxsplit=1)[1].strip()
    envelope = json.loads(sentinel_line)
    assert envelope["ok"] is True
    assert envelope["result"] == {"count": 1, "total": 8.0, "tags": ["default"]}


def test_openai_and_anthropic_tool_schemas() -> None:
    """Generated schemas conform to OpenAI function calling and Anthropic tool specs."""
    openai_spec = analyze_records.to_openai_tool()
    assert openai_spec["type"] == "function"
    fn_block = openai_spec["function"]
    assert fn_block["name"] == "analyze_records"
    assert "Compute scaled aggregate statistics" in fn_block["description"]
    assert "Preserves multi-line docstrings" in fn_block["description"]

    params = fn_block["parameters"]
    assert params["type"] == "object"
    assert set(params["properties"].keys()) == {"records", "multiplier", "tags"}
    assert params["required"] == ["records"]
    assert params["properties"]["multiplier"]["default"] == 2.0

    anthropic_spec = analyze_records.to_anthropic_tool()
    assert anthropic_spec["name"] == "analyze_records"
    assert anthropic_spec["description"] == fn_block["description"]
    assert anthropic_spec["input_schema"] == params


def test_sync_client_retry_preserves_session_id_and_parses_result() -> None:
    """HTTP 502/503/504 triggers idempotent retry carrying identical X-Agent-Session-ID."""
    seen_sessions: list[str] = []
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        seen_sessions.append(request.headers["X-Agent-Session-ID"])
        payload = json.loads(request.content.decode("utf-8"))
        assert payload["agent_session_id"] == request.headers["X-Agent-Session-ID"]

        if attempts < 3:
            return httpx.Response(503, json={"error": "worker temporarily draining"})

        return httpx.Response(
            200,
            json={
                "execution_id": "exec-sync-1",
                "status": "COMPLETED",
                "runtime": "python3.12",
                "tool_name": "analyze_records",
                "exit_code": 0,
                "stdout": f"processed=2\n{RETURN_SENTINEL}{json.dumps({'ok': True, 'result': {'count': 2, 'total': 10.0, 'tags': ['default']}})}\n",
                "stderr": "",
                "setup_duration_us": 5100,
                "exec_duration_us": 1200,
                "teardown_duration_us": 900,
                "peak_memory_bytes": 12_582_912,
                "cpu_usage_us": 1100,
            },
        )

    transport = httpx.MockTransport(handler)
    with AegisClient(
        base_url="http://mock-control:8080",
        api_key="test-key",
        session_id="sess-idemp-fixed",
        transport=transport,
    ) as client:
        res = analyze_records.run([{"value": 2.0}, {"value": 3.0}], client=client)

    assert attempts == 3
    assert seen_sessions == ["sess-idemp-fixed", "sess-idemp-fixed", "sess-idemp-fixed"]
    assert res.execution_id == "exec-sync-1"
    assert res.stdout == "processed=2"
    assert res.return_value == {"count": 2, "total": 10.0, "tags": ["default"]}
    assert res.metrics.setup_duration_us == 5100


def test_sse_streaming_callbacks_strip_sentinel() -> None:
    """Real-time on_stdout/on_stderr callbacks receive clean output with split sentinel stripped."""
    sse_events = [
        ("started", {"execution_id": "exec-sse-1", "sequence_num": 1, "event_type": "started", "worker_node_id": "worker-1"}),
        ("stdout_chunk", {"execution_id": "exec-sse-1", "sequence_num": 2, "event_type": "stdout_chunk", "data": "step 1\n__AEGIS_RE"}),
        (
            "stdout_chunk",
            {
                "execution_id": "exec-sse-1",
                "sequence_num": 3,
                "event_type": "stdout_chunk",
                "data": "TURN_PAYLOAD__:" + json.dumps({"ok": True, "result": {"ok": 1}}) + "\n",
            },
        ),
        ("stderr_chunk", {"execution_id": "exec-sse-1", "sequence_num": 4, "event_type": "stderr_chunk", "data": "minor warning\n"}),
        (
            "completed",
            {
                "execution_id": "exec-sse-1",
                "sequence_num": 5,
                "event_type": "completed",
                "terminal": True,
                "metrics": {"exit_code": 0, "setup_duration_us": 4200, "exec_duration_us": 800},
            },
        ),
    ]
    sse_wire = "".join(
        f"event: {name}\ndata: {json.dumps(data)}\n\n" for name, data in sse_events
    ).encode("utf-8")

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["X-Execution-Mode"] == "stream"
        assert request.headers["Accept"] == "text/event-stream"
        return httpx.Response(200, headers={"Content-Type": "text/event-stream"}, content=sse_wire)

    out_chunks: list[str] = []
    err_chunks: list[str] = []
    with AegisClient(transport=httpx.MockTransport(handler)) as client:
        res = analyze_records.run(
            [{"value": 1.0}],
            client=client,
            on_stdout=out_chunks.append,
            on_stderr=err_chunks.append,
        )

    assert "".join(out_chunks) == "step 1"
    assert RETURN_SENTINEL not in "".join(out_chunks)
    assert "".join(err_chunks) == "minor warning\n"
    assert res.worker_node_id == "worker-1"
    assert res.return_value == {"ok": 1}


@sandbox_tool(runtime="PYTHON_3_12", memory_mb=128, timeout_ms=5000)
def divide_numbers(numerator: float, denominator: float) -> float:
    """Divide numerator by denominator."""
    return numerator / denominator


@pytest.mark.asyncio
async def test_async_client_and_llm_self_correction_exceptions() -> None:
    """Async execution maps runtime ZeroDivisionError and OOM/Timeout/Security errors to LLM observations."""
    mode = "runtime_zero_div"

    async def handler(request: httpx.Request) -> httpx.Response:
        if mode == "runtime_zero_div":
            env = {
                "ok": False,
                "error_type": "ZeroDivisionError",
                "error_message": "float division by zero",
                "traceback": "Traceback (most recent call last):\n  File \"<sandbox>\", line 3, in divide_numbers\nZeroDivisionError: float division by zero",
            }
            return httpx.Response(
                200,
                json={
                    "execution_id": "exec-zero",
                    "status": "COMPLETED",
                    "runtime": "python3.12",
                    "exit_code": 1,
                    "stdout": f"\n{RETURN_SENTINEL}{json.dumps(env)}\n",
                    "stderr": "",
                },
            )
        if mode == "oom":
            return httpx.Response(
                200,
                json={
                    "execution_id": "exec-oom",
                    "status": "OOM_KILLED",
                    "runtime": "python3.12",
                    "exit_code": 137,
                    "peak_memory_bytes": 136_000_000,
                },
            )
        if mode == "timeout":
            return httpx.Response(
                200,
                json={
                    "execution_id": "exec-timeout",
                    "status": "TIMEOUT_KILLED",
                    "runtime": "python3.12",
                    "exit_code": 137,
                },
            )
        if mode == "seccomp":
            return httpx.Response(
                200,
                json={
                    "execution_id": "exec-sec",
                    "status": "SYSCALL_VIOLATION",
                    "runtime": "python3.12",
                    "exit_code": 159,
                    "error_message": "blocked syscall ptrace (SYS_ptrace)",
                },
            )
        if mode == "rejected":
            return httpx.Response(
                200,
                json={
                    "execution_id": "exec-rej",
                    "status": "REJECTED",
                    "runtime": "python3.12",
                    "exit_code": -1,
                    "error_message": "human approver denied tool execution",
                },
            )
        return httpx.Response(
            200,
            json={
                "execution_id": "exec-ok",
                "status": "COMPLETED",
                "runtime": "python3.12",
                "exit_code": 0,
                "stdout": f"\n{RETURN_SENTINEL}{json.dumps({'ok': True, 'result': 5.0})}\n",
            },
        )

    async with AsyncAegisClient(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(SandboxRuntimeError) as zero_exc:
            await divide_numbers.run_async(10.0, 0.0, client=client)
        assert "ZeroDivisionError" in zero_exc.value.to_llm_observation()
        assert "division by zero" in zero_exc.value.to_llm_observation()

        mode = "oom"
        with pytest.raises(SandboxOOMError) as oom_exc:
            await divide_numbers.run_async(10.0, 2.0, client=client)
        obs_oom = oom_exc.value.to_llm_observation()
        assert "Memory limit (128MB) exceeded" in obs_oom
        assert "Reduce dataset size" in obs_oom

        mode = "timeout"
        with pytest.raises(SandboxTimeoutError) as to_exc:
            await divide_numbers.run_async(10.0, 2.0, client=client)
        assert "5000ms" in to_exc.value.to_llm_observation()

        mode = "seccomp"
        with pytest.raises(SandboxSecurityViolationError) as sec_exc:
            await divide_numbers.run_async(10.0, 2.0, client=client)
        assert "ptrace" in sec_exc.value.to_llm_observation()

        mode = "rejected"
        with pytest.raises(SandboxRejectedError) as rej_exc:
            await divide_numbers.run_async(10.0, 2.0, client=client)
        assert "rejected" in rej_exc.value.to_llm_observation().lower()

        mode = "ok"
        res = await divide_numbers.run_async(10.0, 2.0, client=client)
        assert res.return_value == 5.0

