"""Docstring-derived parameter descriptions for ``Params`` classes.

Pydantic's ``Params.model_json_schema()`` only carries
``Field(description=...)`` text. For agent-facing schemas we want the
class docstring to be the source of descriptions too. This helper parses
the Google-style docstring of a Pydantic ``BaseModel`` and returns the
``{field_name: description}`` map the schema generator merges in.

Reads both ``Attributes:`` and ``Args:`` sections — ``Attributes:`` is the
semantically correct Google section for class fields (and the convention
in existing Artisan ``Params`` classes), but ``Args:`` is also accepted
for contributors arriving from function-style ecosystems (PydanticAI tool
wrappers, Sphinx tutorials). When both sections define the same name,
``Attributes:`` wins.

Placed in ``operations/base/`` rather than ``registry/`` so the
fail-fast subclass check on ``OperationDefinition.__pydantic_init_subclass__``
can use it without ``operations`` having to import ``registry``.
"""

from __future__ import annotations

import griffe
from pydantic import BaseModel


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
