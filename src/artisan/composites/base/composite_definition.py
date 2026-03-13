"""CompositeDefinition base class and subclass validation.

Composites are reusable compositions of operations with declared I/O.
Subclass validation, role-doc generation, and the composite registry live here.
"""

from __future__ import annotations

import re
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
)

from pydantic import BaseModel, ConfigDict

from artisan.schemas.execution.execution_config import ExecutionConfig
from artisan.schemas.operation_config.resource_config import ResourceConfig
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec

if TYPE_CHECKING:
    from artisan.composites.base.composite_context import CompositeContext

_ROLE_SECTION_RE = re.compile(
    r"\n?\s*(Input Roles|Output Roles):.*?(?=\n\s*\S|\n\s*\n|\Z)",
    re.DOTALL,
)


def _build_role_docs(cls: type) -> str:
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


def _append_role_docs(cls: type) -> None:
    """Replace role sections in the class docstring with freshly generated ones."""
    doc = cls.__doc__ or ""
    doc = _ROLE_SECTION_RE.sub("", doc).rstrip()

    role_docs = _build_role_docs(cls)
    if role_docs:
        cls.__doc__ = f"{doc}\n\n{role_docs}\n"
    else:
        cls.__doc__ = doc


class CompositeDefinition(BaseModel):
    """Base class for composite operations.

    Subclasses declare input/output specs, implement compose(), and are
    automatically validated and registered on definition.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    _registry: ClassVar[dict[str, type[CompositeDefinition]]] = {}

    # ---------- Metadata ----------
    name: ClassVar[str] = ""
    description: ClassVar[str] = ""

    # ---------- Inputs ----------
    inputs: ClassVar[dict[str, InputSpec]] = {}

    # ---------- Outputs ----------
    outputs: ClassVar[dict[str, OutputSpec]] = {}

    # ---------- Resources ----------
    resources: ResourceConfig = ResourceConfig()

    # ---------- Execution ----------
    execution: ExecutionConfig = ExecutionConfig()

    # ---------- Compose ----------
    def compose(self, ctx: CompositeContext) -> None:
        """Wire internal operations together.

        Args:
            ctx: Composite context providing input(), run(), and output().

        Raises:
            NotImplementedError: If not overridden by subclass.
        """
        raise NotImplementedError(f"{self.__class__.__name__} must implement compose()")

    # ---------- Validation ----------
    @classmethod
    def __pydantic_init_subclass__(cls, **kwargs: Any) -> None:
        """Validate subclass declarations, generate role docs, and register."""
        super().__pydantic_init_subclass__(**kwargs)

        # Skip abstract classes (no name set)
        if not cls.name:
            return

        # compose() must be overridden
        if cls.compose is CompositeDefinition.compose:
            raise TypeError(f"{cls.__name__} must implement compose() method")

        # Validate OutputRole enum matches outputs dict
        if cls.outputs:
            out_enum = getattr(cls, "OutputRole", None)
            if out_enum is None:
                raise TypeError(
                    f"{cls.__name__} must define OutputRole(StrEnum) "
                    "matching outputs keys"
                )
            if set(out_enum) != set(cls.outputs):
                raise TypeError(
                    f"{cls.__name__}.OutputRole values {set(out_enum)} "
                    f"don't match outputs keys {set(cls.outputs)}"
                )

        # Validate InputRole enum matches inputs dict
        if cls.inputs:
            in_enum = getattr(cls, "InputRole", None)
            if in_enum is None:
                raise TypeError(
                    f"{cls.__name__} must define InputRole(StrEnum) "
                    "matching inputs keys"
                )
            if set(in_enum) != set(cls.inputs):
                raise TypeError(
                    f"{cls.__name__}.InputRole values {set(in_enum)} "
                    f"don't match inputs keys {set(cls.inputs)}"
                )

        # Generate runtime docstring with role documentation
        _append_role_docs(cls)

        # Register in composite registry
        CompositeDefinition._registry[cls.name] = cls

    # ---------- Registry ----------
    @classmethod
    def get(cls, name: str) -> type[CompositeDefinition]:
        """Look up a composite class by name.

        Args:
            name: Composite name.

        Returns:
            The CompositeDefinition subclass.

        Raises:
            KeyError: If name is not registered.
        """
        if name not in cls._registry:
            raise KeyError(
                f"Unknown composite: {name!r}. "
                f"Registered: {list(cls._registry.keys())}"
            )
        return cls._registry[name]

    @classmethod
    def get_all(cls) -> dict[str, type[CompositeDefinition]]:
        """Return a copy of the composite registry."""
        return dict(cls._registry)
