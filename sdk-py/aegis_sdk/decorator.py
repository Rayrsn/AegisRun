"""AST-extracting @sandbox_tool decorator turning Python functions into isolated agent tools."""

from __future__ import annotations

import ast
import functools
import inspect
import textwrap
from collections.abc import Callable
from typing import Any, Generic, ParamSpec, TypeVar, overload

from .client import AegisClient, AsyncAegisClient, normalize_runtime
from .harness import synthesize_entrypoint
from .models import ExecutionResult, ResourceLimits
from .schema_gen import to_anthropic_tool, to_openai_tool

P = ParamSpec("P")
R = TypeVar("R")


def _is_sandbox_decorator_node(node: ast.expr) -> bool:
    target = node.func if isinstance(node, ast.Call) else node
    if isinstance(target, ast.Name):
        return target.id == "sandbox_tool"
    if isinstance(target, ast.Attribute):
        return target.attr == "sandbox_tool"
    return False


def extract_clean_function_source(func: Callable[..., Any]) -> str:
    """Extract, dedent, AST-strip @sandbox_tool, and verify compilability of ``func``."""
    raw_src = inspect.getsource(func)
    dedented = textwrap.dedent(raw_src)
    tree = ast.parse(dedented)

    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == func.__name__:
            node.decorator_list = [
                dec for dec in node.decorator_list if not _is_sandbox_decorator_node(dec)
            ]

    ast.fix_missing_locations(tree)
    clean_source = ast.unparse(tree)
    compile(clean_source, f"<sandbox_tool:{func.__name__}>", "exec")
    return clean_source


class SandboxTool(Generic[P, R]):
    """Callable sandbox tool wrapper carrying AST-extracted source and agent schemas."""

    def __init__(
        self,
        func: Callable[P, R],
        *,
        runtime: str = "PYTHON_3_12",
        memory_mb: int = 128,
        timeout_ms: int = 5000,
        cpu_quota_us: int = 50_000,
        pids_max: int = 64,
        tmpfs_mb: int = 32,
        enable_loopback_only: bool = True,
        requires_approval: bool = False,
        webhook_url: str | None = None,
        client: AegisClient | None = None,
        async_client: AsyncAegisClient | None = None,
    ) -> None:
        self._func = func
        self._sig = inspect.signature(func)
        self.runtime = normalize_runtime(runtime)
        self.limits = ResourceLimits(
            wall_timeout_ms=timeout_ms,
            memory_max_bytes=memory_mb * 1024 * 1024,
            cpu_quota_us=cpu_quota_us,
            pids_max=pids_max,
            tmpfs_size_bytes=tmpfs_mb * 1024 * 1024,
        )
        self.enable_loopback_only = enable_loopback_only
        self.requires_approval = requires_approval
        self.webhook_url = webhook_url
        self._client = client
        self._async_client = async_client

        self.clean_source = extract_clean_function_source(func)
        self.entrypoint_code = synthesize_entrypoint(self.clean_source, func.__name__)
        functools.update_wrapper(self, func)

    def bind_args(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        """Bind positional and keyword invocation arguments into a JSON-ready kwargs dict."""
        bound = self._sig.bind(*args, **kwargs)
        bound.apply_defaults()
        return dict(bound.arguments)

    def to_openai_tool(self, *, name: str | None = None, description: str | None = None) -> dict[str, Any]:
        """Generate an OpenAI function-calling schema for this tool."""
        return to_openai_tool(self._func, name=name, description=description)

    def to_anthropic_tool(self, *, name: str | None = None, description: str | None = None) -> dict[str, Any]:
        """Generate an Anthropic Messages API tool schema for this tool."""
        return to_anthropic_tool(self._func, name=name, description=description)

    def _build_exec_kwargs(
        self,
        payload_args: dict[str, Any],
        *,
        session_id: str | None,
        on_stdout: Callable[[str], None] | None,
        on_stderr: Callable[[str], None] | None,
        stream: bool,
    ) -> dict[str, Any]:
        return {
            "tool_name": self._func.__name__,
            "entrypoint_code": self.entrypoint_code,
            "args": payload_args,
            "runtime": self.runtime,
            "limits": self.limits,
            "session_id": session_id,
            "enable_loopback_only": self.enable_loopback_only,
            "requires_approval": self.requires_approval,
            "webhook_url": self.webhook_url,
            "on_stdout": on_stdout,
            "on_stderr": on_stderr,
            "stream": stream,
        }

    def _execute_sync(
        self,
        payload_args: dict[str, Any],
        *,
        client: AegisClient | None = None,
        session_id: str | None = None,
        on_stdout: Callable[[str], None] | None = None,
        on_stderr: Callable[[str], None] | None = None,
        stream: bool = False,
    ) -> ExecutionResult:
        exec_kwargs = self._build_exec_kwargs(
            payload_args,
            session_id=session_id,
            on_stdout=on_stdout,
            on_stderr=on_stderr,
            stream=stream,
        )
        active_client = client or self._client
        if active_client is not None:
            return active_client.execute(**exec_kwargs)

        with AegisClient() as ephemeral_client:
            return ephemeral_client.execute(**exec_kwargs)

    def run(
        self,
        *args: Any,
        client: AegisClient | None = None,
        session_id: str | None = None,
        on_stdout: Callable[[str], None] | None = None,
        on_stderr: Callable[[str], None] | None = None,
        stream: bool = False,
        **kwargs: Any,
    ) -> ExecutionResult:
        """Execute this tool remotely in an isolated Linux sandbox synchronously."""
        payload_args = self.bind_args(*args, **kwargs)
        return self._execute_sync(
            payload_args,
            client=client,
            session_id=session_id,
            on_stdout=on_stdout,
            on_stderr=on_stderr,
            stream=stream,
        )

    async def _execute_async(
        self,
        payload_args: dict[str, Any],
        *,
        client: AsyncAegisClient | None = None,
        session_id: str | None = None,
        on_stdout: Callable[[str], None] | None = None,
        on_stderr: Callable[[str], None] | None = None,
        stream: bool = False,
    ) -> ExecutionResult:
        exec_kwargs = self._build_exec_kwargs(
            payload_args,
            session_id=session_id,
            on_stdout=on_stdout,
            on_stderr=on_stderr,
            stream=stream,
        )
        active_client = client or self._async_client
        if active_client is not None:
            return await active_client.execute(**exec_kwargs)

        async with AsyncAegisClient() as ephemeral_client:
            return await ephemeral_client.execute(**exec_kwargs)

    async def run_async(
        self,
        *args: Any,
        client: AsyncAegisClient | None = None,
        session_id: str | None = None,
        on_stdout: Callable[[str], None] | None = None,
        on_stderr: Callable[[str], None] | None = None,
        stream: bool = False,
        **kwargs: Any,
    ) -> ExecutionResult:
        """Execute this tool remotely in an isolated Linux sandbox asynchronously."""
        payload_args = self.bind_args(*args, **kwargs)
        return await self._execute_async(
            payload_args,
            client=client,
            session_id=session_id,
            on_stdout=on_stdout,
            on_stderr=on_stderr,
            stream=stream,
        )

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R:
        payload_args = self.bind_args(*args, **kwargs)
        result = self._execute_sync(payload_args)
        return result.return_value  # type: ignore[no-any-return]


@overload
def sandbox_tool(func: Callable[P, R], /) -> SandboxTool[P, R]: ...


@overload
def sandbox_tool(
    func: None = None,
    /,
    *,
    runtime: str = "PYTHON_3_12",
    memory_mb: int = 128,
    timeout_ms: int = 5000,
    cpu_quota_us: int = 50_000,
    pids_max: int = 64,
    tmpfs_mb: int = 32,
    enable_loopback_only: bool = True,
    requires_approval: bool = False,
    webhook_url: str | None = None,
    client: AegisClient | None = None,
    async_client: AsyncAegisClient | None = None,
) -> Callable[[Callable[P, R]], SandboxTool[P, R]]: ...


def sandbox_tool(
    func: Callable[P, R] | None = None,
    /,
    *,
    runtime: str = "PYTHON_3_12",
    memory_mb: int = 128,
    timeout_ms: int = 5000,
    cpu_quota_us: int = 50_000,
    pids_max: int = 64,
    tmpfs_mb: int = 32,
    enable_loopback_only: bool = True,
    requires_approval: bool = False,
    webhook_url: str | None = None,
    client: AegisClient | None = None,
    async_client: AsyncAegisClient | None = None,
) -> SandboxTool[P, R] | Callable[[Callable[P, R]], SandboxTool[P, R]]:
    """Decorate a standard Python function into an AST-extracted sandboxed agent tool."""

    def wrap(target: Callable[P, R]) -> SandboxTool[P, R]:
        return SandboxTool(
            target,
            runtime=runtime,
            memory_mb=memory_mb,
            timeout_ms=timeout_ms,
            cpu_quota_us=cpu_quota_us,
            pids_max=pids_max,
            tmpfs_mb=tmpfs_mb,
            enable_loopback_only=enable_loopback_only,
            requires_approval=requires_approval,
            webhook_url=webhook_url,
            client=client,
            async_client=async_client,
        )

    if func is not None:
        return wrap(func)
    return wrap

