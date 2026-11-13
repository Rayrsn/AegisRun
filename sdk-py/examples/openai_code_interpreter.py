"""Autonomous data-analysis agent example using OpenAI tool schemas and AegisRun."""

from __future__ import annotations

import json
import os
from typing import Any

from aegis_sdk import AegisClient, AegisSandboxError, sandbox_tool


@sandbox_tool(runtime="PYTHON_3_12", memory_mb=128, timeout_ms=5000, requires_approval=False)
def compute_portfolio_sharpe(prices: list[float], risk_free_rate: float = 0.04) -> dict[str, Any]:
    """Compute annualized return statistics and Sharpe ratio for a price series."""
    import numpy as np

    arr = np.asarray(prices, dtype=float)
    returns = np.diff(arr) / arr[:-1]
    volatility = float(np.std(returns))
    if volatility == 0.0:
        raise ZeroDivisionError("Price series has zero volatility; cannot compute Sharpe ratio.")
    sharpe = (float(np.mean(returns)) - risk_free_rate) / volatility
    return {"sharpe_ratio": sharpe, "samples": len(prices)}


def handle_tool_call(arguments_json: str, client: AegisClient) -> dict[str, str]:
    """Execute an OpenAI-style tool call and format a role=tool observation message."""
    kwargs = json.loads(arguments_json)
    try:
        res = compute_portfolio_sharpe.run(
            client=client,
            on_stdout=lambda chunk: print(f"[sandbox stdout] {chunk}", end=""),
            **kwargs,
        )
        content = json.dumps({"status": res.status, "result": res.return_value})
    except AegisSandboxError as exc:
        content = exc.to_llm_observation()
    return {"role": "tool", "content": content}


if __name__ == "__main__":
    print("OpenAI tool schema:")
    print(json.dumps(compute_portfolio_sharpe.to_openai_tool(), indent=2))
    print("\nAnthropic tool schema:")
    print(json.dumps(compute_portfolio_sharpe.to_anthropic_tool(), indent=2))

    base_url = os.getenv("AEGIS_CONTROL_URL", "http://localhost:8080")
    api_key = os.getenv("AEGIS_API_KEY", "aegis-dev-token")
    with AegisClient(base_url=base_url, api_key=api_key) as client:
        msg = handle_tool_call('{"prices": [100.0, 102.5, 101.0, 106.2]}', client)
        print("\nAgent tool message:", msg)

