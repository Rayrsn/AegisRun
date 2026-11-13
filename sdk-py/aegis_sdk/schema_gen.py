"""OpenAI and Anthropic JSON Schema generators built dynamically via Pydantic v2."""

from __future__ import annotations

import copy
import inspect
from collections.abc import Callable
from typing import Any, get_type_hints

from pydantic import BaseModel, Field, create_model


def _safe_type_hints(func: Callable[..., Any]) -> dict[str, Any]:
    try:
        return get_type_hints(func)
    except (NameError, TypeError, AttributeError):
        return {}


def _is_mutable_default(value: Any) -> bool:
    return isinstance(value, (list, dict, set))


def build_parameters_model(func: Callable[..., Any]) -> type[BaseModel]:
    """Build a dynamic Pydantic BaseModel representing the signature of ``func``."""
    sig = inspect.signature(func)
    hints = _safe_type_hints(func)
    fields: dict[str, tuple[Any, Any]] = {}

    for param_name, param in sig.parameters.items():
        if param.kind in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD):
            continue

        annotation = hints.get(
            param_name,
            Any if param.annotation is inspect.Parameter.empty else param.annotation,
        )

        if param.default is inspect.Parameter.empty:
            fields[param_name] = (annotation, ...)
        elif _is_mutable_default(param.default):
            default_snapshot = copy.deepcopy(param.default)
            fields[param_name] = (
                annotation,
                Field(default_factory=lambda v=default_snapshot: copy.deepcopy(v)),
            )
        else:
            fields[param_name] = (annotation, param.default)

    model_name = f"{func.__name__.title().replace('_', '')}Input"
    return create_model(model_name, **fields)  # type: ignore[call-overload, no-any-return]


def build_parameters_schema(func: Callable[..., Any]) -> dict[str, Any]:
    """Generate a JSON Schema Draft 2020-12 object schema for ``func`` parameters."""
    model = build_parameters_model(func)
    schema = model.model_json_schema()
    schema.setdefault("type", "object")
    schema.setdefault("properties", {})
    return schema


def resolve_tool_metadata(
    func: Callable[..., Any],
    *,
    name: str | None = None,
    description: str | None = None,
) -> tuple[str, str]:
    """Resolve canonical tool name and cleaned docstring description."""
    tool_name = name or func.__name__
    doc = description if description is not None else (inspect.getdoc(func) or f"Execute {tool_name}.")
    return tool_name, doc


def to_openai_tool(
    func: Callable[..., Any],
    *,
    name: str | None = None,
    description: str | None = None,
) -> dict[str, Any]:
    """Export an OpenAI Chat Completions / Responses compatible tool specification."""
    tool_name, doc = resolve_tool_metadata(func, name=name, description=description)
    return {
        "type": "function",
        "function": {
            "name": tool_name,
            "description": doc,
            "parameters": build_parameters_schema(func),
        },
    }


def to_anthropic_tool(
    func: Callable[..., Any],
    *,
    name: str | None = None,
    description: str | None = None,
) -> dict[str, Any]:
    """Export an Anthropic Messages API compatible tool specification."""
    tool_name, doc = resolve_tool_metadata(func, name=name, description=description)
    return {
        "name": tool_name,
        "description": doc,
        "input_schema": build_parameters_schema(func),
    }

