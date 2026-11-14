# `sdk-py/` — AegisRun Python Developer SDK

`aegis-sdk` (`sdk-py/`) provides a typed Python 3.11+ interface for turning regular Python functions into isolated AegisRun sandbox tools with OpenAI and Anthropic JSON tool schemas, real-time SSE output streaming, and LLM self-correction error helpers.

---

## Key Features

- **Zero-Dependency `@sandbox_tool` AST Extractor (`decorator.py`, `harness.py`)**: Uses `ast` to extract user function source, strip `@sandbox_tool`, serialize closure bindings as literal AST assignments, and synthesize a self-contained stdin/stdout JSON harness (`__AEGIS_RETURN_PAYLOAD__:`). No `aegis_sdk` package is required inside the rootfs.
- **OpenAI & Anthropic Schema Generators (`schema_gen.py`)**: Dynamically builds Pydantic v2 models from function signatures and exports `to_openai_tool()` and `to_anthropic_tool()` definitions.
- **Sync & Async HTTP/SSE Clients (`client.py`)**: `AegisClient` and `AsyncAegisClient` support both `sync` JSON mode and `stream` SSE callbacks (`on_stdout`, `on_stderr`), automatically preserving `X-Agent-Session-ID` across `502`/`503`/`504` retries so `control-go` idempotency prevents double execution.
- **LLM Self-Correction Exceptions (`exceptions.py`)**: Typed hierarchy (`AegisSandboxError`, `SandboxOOMError`, `SandboxTimeoutError`, `SandboxSecurityViolationError`, `SandboxRejectedError`, `SandboxRuntimeError`) with `.to_llm_observation() -> str` formatted for agent feedback loops.

---

## Installation

```bash
# Editable install with dev dependencies (ruff, mypy, pytest)
uv pip install -e "./sdk-py[dev]"
# or with pip:
pip install -e "./sdk-py[dev]"
```

---

## Quick Usage

```python
from aegis_sdk import AegisClient, AegisSandboxError, sandbox_tool


@sandbox_tool(runtime="PYTHON_3_12", memory_mb=128, timeout_ms=5000)
def analyze(values: list[float]) -> dict[str, float]:
    """Compute summary statistics inside an isolated micro-sandbox."""
    return {"count": float(len(values)), "sum": float(sum(values))}


openai_tool = analyze.to_openai_tool()
anthropic_tool = analyze.to_anthropic_tool()

with AegisClient(base_url="http://localhost:8080", api_key="aegis-dev-token") as client:
    try:
        res = analyze.run([1.5, 2.5, 4.0], client=client)
        print(res.return_value, res.metrics)
    except AegisSandboxError as exc:
        print(exc.to_llm_observation())
```

Client settings fall back automatically to `AEGIS_CONTROL_URL` (`http://localhost:8080`) and `AEGIS_API_KEY` (`aegis-dev-token`).

See runnable examples in [`examples/openai_code_interpreter.py`](examples/openai_code_interpreter.py) and [`examples/security_pentest_demo.py`](examples/security_pentest_demo.py).

---

## Lint, Typecheck & Tests

```bash
# From repo root:
make sdk-lint
make sdk-test

# Or directly inside sdk-py/:
cd sdk-py
ruff check .
mypy aegis_sdk
pytest -v
```
