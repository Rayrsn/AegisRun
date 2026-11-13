"""Self-contained Python entrypoint synthesis and return payload extraction."""

from __future__ import annotations

import json
from typing import Any

RETURN_SENTINEL = "__AEGIS_RETURN_PAYLOAD__:"


def synthesize_entrypoint(clean_func_source: str, func_name: str) -> str:
    """Wrap ``clean_func_source`` in a zero-dependency stdin/stdout JSON harness."""
    runner = f"""
import json as _aegis_json
import sys as _aegis_sys
import traceback as _aegis_tb


def _aegis_emit(payload: dict) -> None:
    _aegis_sys.stdout.flush()
    _aegis_sys.stdout.write("\\n{RETURN_SENTINEL}" + _aegis_json.dumps(payload, default=str) + "\\n")
    _aegis_sys.stdout.flush()


def _aegis_main() -> int:
    _raw_stdin = _aegis_sys.stdin.read().strip()
    if _raw_stdin:
        try:
            _kwargs = _aegis_json.loads(_raw_stdin)
        except Exception as _exc:
            _aegis_emit({{
                "ok": False,
                "error_type": type(_exc).__name__,
                "error_message": f"Invalid JSON stdin payload: {{_exc}}",
                "traceback": _aegis_tb.format_exc(),
            }})
            return 1
    else:
        _kwargs = {{}}

    if not isinstance(_kwargs, dict):
        _aegis_emit({{
            "ok": False,
            "error_type": "TypeError",
            "error_message": "Tool stdin payload must decode to a JSON object",
            "traceback": "",
        }})
        return 1

    try:
        _result = {func_name}(**_kwargs)
    except Exception as _exc:
        _aegis_emit({{
            "ok": False,
            "error_type": type(_exc).__name__,
            "error_message": str(_exc),
            "traceback": _aegis_tb.format_exc(),
        }})
        return 1

    _aegis_emit({{"ok": True, "result": _result}})
    return 0


if __name__ == "__main__":
    raise SystemExit(_aegis_main())
"""
    header = "from __future__ import annotations\n\nfrom typing import Any\n"
    return f"{header}\n{clean_func_source.rstrip()}\n\n{runner.lstrip()}"


def parse_harness_output(raw_stdout: str) -> tuple[str, dict[str, Any] | None]:
    """Separate user stdout from the final sentinel JSON return envelope."""
    idx = raw_stdout.rfind(RETURN_SENTINEL)
    if idx == -1:
        return raw_stdout, None

    user_stdout = raw_stdout[:idx].removesuffix("\n")

    payload_part = raw_stdout[idx + len(RETURN_SENTINEL) :].splitlines()[0].strip()
    try:
        decoded = json.loads(payload_part)
    except json.JSONDecodeError:
        return raw_stdout, None

    if not isinstance(decoded, dict):
        return raw_stdout, None
    return user_stdout, decoded

