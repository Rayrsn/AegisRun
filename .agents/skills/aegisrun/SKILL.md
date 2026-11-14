---
name: aegisrun
description: >-
  Install, start, and execute untrusted Python, Node.js, or Bash code inside
  AegisRun Linux micro-sandboxes (<35ms cold start). Use when asked to install
  AegisRun, run untrusted code safely, wrap functions with @sandbox_tool, or
  generate OpenAI and Anthropic tool schemas backed by AegisRun.
---

# AegisRun Agent Skill

Use **AegisRun** whenever an AI agent or workflow needs to install the SDK, bring up an isolated code-execution backend, or execute untrusted `PYTHON_3_12`, `NODE_20`, or `BASH_5` payloads safely without Docker runtime overhead.

> **Platform note:** `aegis-sdk` (`sdk-py`) and `control-go` run on Linux, macOS, and Windows. The worker daemon (`daemon-rs`) requires a **Linux kernel >= 5.15** with cgroups v2 (`root` / `CAP_SYS_ADMIN`), such as native Linux or **WSL2** on Windows.

---

## 1. Install the Python SDK (One-Liner)

```bash
# With uv:
uv pip install "aegis-sdk @ git+https://github.com/Rayrsn/AegisRun.git#subdirectory=sdk-py"

# Or with pip:
pip install "git+https://github.com/Rayrsn/AegisRun.git#subdirectory=sdk-py"
```

Verify the import:

```bash
python -c "import aegis_sdk; print(aegis_sdk.__all__)"
```

Default connection environment variables:
- `AEGIS_CONTROL_URL` (default `http://localhost:8080`)
- `AEGIS_API_KEY` (default `aegis-dev-token`)

---

## 2. Clone & Launch the Backend Stack on Linux / WSL2

```bash
git clone https://github.com/Rayrsn/AegisRun.git
cd AegisRun
sudo bash deploy/scripts/quickstart.sh
```

`quickstart.sh` starts Redis/Valkey (`:6379`) and PostgreSQL (`:5432`), applies `migrations/0001_init_executions.up.sql`, builds the read-only Alpine rootfs (`/var/lib/aegis/rootfs/*`) if missing, compiles/launches `daemon-rs` (`:50051`) and `control-go` (`:8080`), and waits for `/readyz`.

Verify readiness:

```bash
curl -fsS http://127.0.0.1:8080/readyz
# Expected: {"status":"ready","workers_healthy":1}
```

> **Windows + WSL2 NAT note:** If running Python on Windows against a WSL2-hosted stack and `localhost:8080` refuses connections, point `AEGIS_CONTROL_URL` at the WSL2 `eth0` IP (`wsl -- ip -4 -br addr show eth0`).

Stop the background services when done:

```bash
sudo bash deploy/scripts/quickstart.sh --stop
```

---

## 3. Wrap & Execute Python Tools (`@sandbox_tool`)

Write `@sandbox_tool` functions in a `.py` file on disk (avoid running them from `<stdin>` / `python -`, because `@sandbox_tool` uses `inspect.getsource()` to strip the decorator via AST so the sandbox rootfs needs zero `aegis_sdk` dependency).

```python
from aegis_sdk import AegisClient, AegisSandboxError, sandbox_tool


@sandbox_tool(
    runtime="PYTHON_3_12",
    memory_mb=128,
    timeout_ms=5000,
    requires_approval=False,
    webhook_url=None,
)
def analyze_numbers(values: list[float]) -> dict[str, float]:
    """Compute count, total, and mean inside an isolated micro-sandbox."""
    return {
        "count": float(len(values)),
        "total": float(sum(values)),
        "mean": float(sum(values) / len(values)),
    }


# Generate LLM provider schemas:
openai_schema = analyze_numbers.to_openai_tool()
anthropic_schema = analyze_numbers.to_anthropic_tool()

# Execute safely and pass LLM observations on failure:
with AegisClient(base_url="http://127.0.0.1:8080", api_key="aegis-dev-token") as client:
    try:
        res = analyze_numbers.run(
            values=[10.0, 20.0, 30.0],
            client=client,
            stream=True,
            on_stdout=lambda chunk: print(chunk, end=""),
        )
        print(res.return_value, res.metrics)
    except AegisSandboxError as exc:
        # Structured self-correction feedback for OOM, timeout, seccomp SIGSYS, or runtime errors:
        print(exc.to_llm_observation())
```

Typed exceptions implementing `.to_llm_observation() -> str`:
- `SandboxOOMError`
- `SandboxTimeoutError`
- `SandboxSecurityViolationError`
- `SandboxRuntimeError`
- `SandboxRejectedError`

---

## 4. Direct HTTP Execution (`POST /v1/execute`)

From any language or shell tool (supports `PYTHON_3_12`, `NODE_20`, and `BASH_5`):

```bash
curl -fsS -X POST http://127.0.0.1:8080/v1/execute \
  -H "Authorization: Bearer aegis-dev-token" \
  -H "Content-Type: application/json" \
  -H "X-Agent-Session-ID: agent-session-1" \
  -d '{
    "agent_session_id": "agent-session-1",
    "tool_name": "quick_check",
    "runtime": "PYTHON_3_12",
    "entrypoint_code": "import os; print(\"uid=\", os.getuid())",
    "limits": {"wall_timeout_ms": 5000, "memory_max_bytes": 134217728}
  }'
```

---

## 5. Verify Security & Zero Kernel Leaks

```bash
make test
mount | grep "/run/aegis/sandboxes" | wc -l
ls -1 /sys/fs/cgroup/aegis.slice/ 2>/dev/null | grep "^exec-" | wc -l
```

Both leak counters must return `0`.
