"""Introspection helpers for ``OperationDefinition.Params``.

Two related helpers:

- ``_params_class(op_cls)`` — the single resolution rule for "what is
  this op's ``Params`` class?". Returns the Pydantic ``BaseModel``
  subclass, or ``None`` for parameter-less ops. Used by registry schema
  generation, the fail-fast undocumented-params check, and
  pipeline-side validation.
- ``_extract_arg_descriptions(model_cls)`` — pulls
  ``{field_name: description}`` from a Pydantic class's Google-style
  ``Attributes:`` / ``Args:`` docstring sections.

Placed in ``operations/base/`` rather than ``registry/`` so the
fail-fast subclass check on ``OperationDefinition.__pydantic_init_subclass__``
can use these without ``operations`` having to import ``registry``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import griffe
from pydantic import BaseModel

if TYPE_CHECKING:
    from artisan.operations.base.operation_definition import OperationDefinition


def _params_class(op_cls: type[OperationDefinition]) -> type[BaseModel] | None:
    """Resolve the op's ``Params`` class via the single lookup rule.

    Args:
        op_cls: The ``OperationDefinition`` subclass to inspect.

    Returns:
        The ``Params`` ``BaseModel`` subclass, or ``None`` for
        parameter-less ops (no ``params`` field, ``None`` annotation, or
        annotation that isn't a ``BaseModel`` subclass).
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


def _extract_arg_descriptions(model_cls: type[BaseModel]) -> dict[str, str]:
    """Return ``{field_name: description}`` from a model's class docstring.

    Args:
        model_cls: A Pydantic ``BaseModel`` subclass whose ``__doc__`` may
            contain Google-style ``Attributes:`` and/or ``Args:`` sections.

    Returns:
        Mapping from field name to description text. Empty when the class
        has no docstring or no recognized sections. On collision for the
        same name, the ``Attributes:`` entry wins.
    """
    docstring = model_cls.__doc__
    if not docstring:
        return {}

    parsed = griffe.Docstring(docstring, parser="google").parsed
    args: dict[str, str] = {}
    attrs: dict[str, str] = {}
    for section in parsed:
        if section.kind is griffe.DocstringSectionKind.parameters:
            args.update({p.name: p.description for p in section.value})
        elif section.kind is griffe.DocstringSectionKind.attributes:
            attrs.update({a.name: a.description for a in section.value})
    return {**args, **attrs}
