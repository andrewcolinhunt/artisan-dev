"""CompositeDefinition base class and subclass validation.

Composites are reusable compositions of operations with declared I/O.
Subclass validation, role-doc generation, and the composite registry live here.
"""

from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
)

from pydantic import BaseModel, ConfigDict

from artisan.operations.base._role_docs import (
    append_role_docs,
    get_registered,
    validate_role_enums,
)
from artisan.schemas.execution.execution_config import ExecutionConfig
from artisan.schemas.operation_config.resource_config import ResourceConfig
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec

if TYPE_CHECKING:
    from artisan.composites.base.composite_context import CompositeContext


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
        msg = f"{self.__class__.__name__} must implement compose()"
        raise NotImplementedError(msg)

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
            msg = f"{cls.__name__} must implement compose() method"
            raise TypeError(msg)

        validate_role_enums(cls, "composite")
        append_role_docs(cls)

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
        return get_registered(name, cls._registry, "composite")

    @classmethod
    def get_all(cls) -> dict[str, type[CompositeDefinition]]:
        """Return a copy of the composite registry."""
        return dict(cls._registry)
