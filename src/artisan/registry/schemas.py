"""JSON Schema generation for an operation's ``Params`` class.

Pydantic's ``model_json_schema()`` is the structural source of truth;
``_extract_arg_descriptions`` from ``operations.base._param_docs`` provides
the docstring-derived description merge. ``Field(description=...)``
descriptions are written by Pydantic and take precedence — the docstring
extractor only fills gaps.

The "what are this op's params?" lookup is centralized in
``_params_class``. Parameter-less ops receive the empty ``Params`` shape;
there is no flat-form branch in any consumer.
"""

from __future__ import annotations

from typing import Any

from artisan.operations.base._param_docs import (
    _extract_arg_descriptions,
    _params_class,
)
from artisan.operations.base.operation_definition import OperationDefinition


def params_schema_for(op_cls: type[OperationDefinition]) -> dict[str, Any]:
    """JSON Schema for an op's ``Params``, with docstring descriptions merged.

    Args:
        op_cls: The ``OperationDefinition`` subclass to inspect.

    Returns:
        JSON Schema dict. For parameter-less ops (no ``params`` field
        declared, or annotation is not a ``BaseModel``), returns the empty
        ``Params`` shape: ``{"type": "object", "title": "Params",
        "properties": {}}``.
    """
    params_cls = _params_class(op_cls)
    if params_cls is None:
        return {"type": "object", "title": "Params", "properties": {}}
    base = params_cls.model_json_schema()
    descriptions = _extract_arg_descriptions(params_cls)
    for prop_name, schema in base.get("properties", {}).items():
        if "description" not in schema and prop_name in descriptions:
            schema["description"] = descriptions[prop_name]
    return base
