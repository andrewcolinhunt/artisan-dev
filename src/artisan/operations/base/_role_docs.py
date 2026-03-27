"""Shared role-doc generation, enum validation, and registry lookup.

Used by both OperationDefinition and CompositeDefinition to avoid
duplicating the same regex, docstring builder, and validation logic.
"""

from __future__ import annotations

import re
from typing import Any

_ROLE_SECTION_RE = re.compile(
    r"\n?\s*(Input Roles|Output Roles):.*?(?=\n\s*\S|\n\s*\n|\Z)",
    re.DOTALL,
)


def build_role_docs(cls: type) -> str:
    """Generate Input/Output Roles docstring sections from class specs."""
    sections: list[str] = []

    if cls.inputs:
        lines = ["    Input Roles:"]
        for role, spec in cls.inputs.items():
            type_label = spec.artifact_type if spec.artifact_type else "any"
            desc = f" -- {spec.description}" if spec.description else ""
            lines.append(f"        {role} ({type_label}){desc}")
        sections.append("\n".join(lines))

    if cls.outputs:
        lines = ["    Output Roles:"]
        for role, spec in cls.outputs.items():
            type_label = spec.artifact_type if spec.artifact_type else "any"
            desc = f" -- {spec.description}" if spec.description else ""
            lines.append(f"        {role} ({type_label}){desc}")
        sections.append("\n".join(lines))

    return "\n\n".join(sections)


def append_role_docs(cls: type) -> None:
    """Replace role sections in the class docstring with freshly generated ones."""
    doc = cls.__doc__ or ""
    doc = _ROLE_SECTION_RE.sub("", doc).rstrip()

    role_docs = build_role_docs(cls)
    if role_docs:
        cls.__doc__ = f"{doc}\n\n{role_docs}\n"
    else:
        cls.__doc__ = doc


def validate_role_enums(cls: type, _class_label: str) -> None:
    """Validate that OutputRole/InputRole enums match outputs/inputs dicts.

    Args:
        cls: The definition subclass being validated.
        class_label: Human-readable label for error messages (e.g. "operation").
    """
    if cls.outputs:
        out_enum = getattr(cls, "OutputRole", None)
        if out_enum is None:
            msg = (
                f"{cls.__name__} must define OutputRole(StrEnum) matching outputs keys"
            )
            raise TypeError(msg)
        if set(out_enum) != set(cls.outputs):
            msg = (
                f"{cls.__name__}.OutputRole values {set(out_enum)} "
                f"don't match outputs keys {set(cls.outputs)}"
            )
            raise TypeError(msg)

    if cls.inputs:
        in_enum = getattr(cls, "InputRole", None)
        if in_enum is None:
            msg = f"{cls.__name__} must define InputRole(StrEnum) matching inputs keys"
            raise TypeError(msg)
        if set(in_enum) != set(cls.inputs):
            msg = (
                f"{cls.__name__}.InputRole values {set(in_enum)} "
                f"don't match inputs keys {set(cls.inputs)}"
            )
            raise TypeError(msg)


def get_registered(
    name: str,
    registry: dict[str, Any],
    label: str,
) -> Any:
    """Look up a definition class by name from a registry dict.

    Args:
        name: Definition name to look up.
        registry: The registry dict mapping names to classes.
        label: Human-readable label for error messages (e.g. "operation").

    Returns:
        The registered class.

    Raises:
        KeyError: If name is not registered.
    """
    if name not in registry:
        msg = f"Unknown {label}: {name!r}. Registered: {list(registry.keys())}"
        raise KeyError(msg)
    return registry[name]
