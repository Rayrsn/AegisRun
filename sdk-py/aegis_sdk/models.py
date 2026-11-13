"""Pydantic v2 models describing AegisRun sandbox metrics and execution results."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class SandboxMetrics(BaseModel):
    """Low-level Linux sandbox telemetry returned by daemon-rs."""

    model_config = ConfigDict(extra="ignore")

    exit_code: int = 0
    signaled: int = 0
    setup_duration_us: int = 0
    exec_duration_us: int = 0
    teardown_duration_us: int = 0
    peak_memory_bytes: int = 0
    cpu_usage_us: int = 0


class ResourceLimits(BaseModel):
    """Resource quotas enforced by Linux cgroups v2 and tmpfs mounts."""

    model_config = ConfigDict(extra="ignore")

    wall_timeout_ms: int = Field(default=5000, ge=1)
    memory_max_bytes: int = Field(default=134_217_728, ge=1)
    cpu_quota_us: int = Field(default=50_000, ge=1)
    pids_max: int = Field(default=64, ge=1)
    tmpfs_size_bytes: int = Field(default=33_554_432, ge=1)


class ExecutionResult(BaseModel):
    """Canonical result of a sandboxed tool execution."""

    model_config = ConfigDict(extra="ignore")

    execution_id: str
    status: str
    runtime: str
    tool_name: str | None = None
    worker_node_id: str | None = None
    exit_code: int = 0
    stdout: str = ""
    stderr: str = ""
    return_value: Any = None
    error_message: str | None = None
    approval_required: bool = False
    idempotent_replay: bool = False
    metrics: SandboxMetrics = Field(default_factory=SandboxMetrics)

