"""JSON Schema generation for an operation's ``Params`` class.

Pydantic's ``model_json_schema()`` is the structural source of truth;
``_extract_arg_descriptions`` from ``operations.base._param_docs`` provides
the docstring-derived description merge. ``Field(description=...)``
descriptions are written by Pydantic and take precedence — the docstring
extractor only fills gaps.

Resolution rule for "what are this op's params?" is centralized in
``_params_class``: ``cls.model_fields.get("params")``. Absent or
non-``BaseModel`` annotation -> no params; ``params_schema_for`` returns
the empty ``Params`` schema. No flat-form branch.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel

from artisan.operations.base._param_docs import _extract_arg_descriptions
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


def _params_class(op_cls: type[OperationDefinition]) -> type[BaseModel] | None:
    """Resolve the op's ``Params`` class via the single lookup rule.

    Args:
        op_cls: The ``OperationDefinition`` subclass to inspect.

    Returns:
        The ``Params`` ``BaseModel`` subclass, or ``None`` for
        parameter-less ops.
    """
    field = op_cls.model_fields.get("params")
    if field is None:
        return None
    annotation = field.annotation
    if annotation is None:
        return None
    try:
        if not issubclass(annotation, BaseModel):
            return None
    except TypeError:
        # Non-class annotations (e.g. Optional[X], generic aliases).
        return None
    return annotation
