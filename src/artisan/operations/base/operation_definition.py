"""OperationDefinition base class and subclass validation.

Operations are Pydantic models declaring inputs, outputs, and a three-phase
lifecycle (preprocess, execute, postprocess). Subclass validation, role-doc
generation, and the operation registry live here.
"""

from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
)

if TYPE_CHECKING:
    import polars as pl

    from artisan.storage.core.artifact_store import ArtifactStore

from pydantic import BaseModel, ConfigDict

from artisan.operations.base._role_docs import (
    append_role_docs,
    get_registered,
    validate_role_enums,
)
from artisan.schemas.enums import GroupByStrategy
from artisan.schemas.execution.curator_result import ArtifactResult, CuratorResult
from artisan.schemas.execution.execution_config import ExecutionConfig
from artisan.schemas.operation_config.environments import Environments
from artisan.schemas.operation_config.resource_config import ResourceConfig
from artisan.schemas.operation_config.tool_spec import ToolSpec
from artisan.schemas.specs.input_models import (
    ExecuteInput,
    PostprocessInput,
    PreprocessInput,
)
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec


class OperationDefinition(BaseModel):
    """Base class for all pipeline operations.

    Subclasses declare input/output specs, implement the lifecycle methods
    (preprocess, execute/execute_curator, postprocess), and are automatically
    validated and registered on definition.

    Attributes:
        name (str): Unique operation identifier used for registry lookup.
        description (str): Human-readable summary shown in docs and logs.
        inputs (dict[str, InputSpec]): Named input specifications.
        outputs (dict[str, OutputSpec]): Named output specifications.
        resources (ResourceConfig): Hardware resource allocation for SLURM jobs.
        execution (ExecutionConfig): Batching and scheduling configuration.
    """

    model_config = ConfigDict(
        extra="forbid",
        str_strip_whitespace=True,
    )

    _registry: ClassVar[dict[str, type[OperationDefinition]]] = {}

    # ---------- Metadata ----------
    name: ClassVar[str] = ""
    description: ClassVar[str] = ""

    # ---------- Inputs ----------
    inputs: ClassVar[dict[str, InputSpec]] = {}
    """Input specification for this operation.

    Defines what inputs the operation accepts.
    Required - operations without inputs will fail validation.
    Empty dict {} is valid for generative operations (no inputs).

    Example:
        inputs: ClassVar[dict[str, InputSpec]] = {
            "data": InputSpec(required=True, description="Input data"),
            "reference": InputSpec(required=False, description="Optional reference"),
        }
    """

    # ---------- Outputs ----------
    outputs: ClassVar[dict[str, OutputSpec]] = {}
    """Output specification for this operation.

    Defines what outputs the operation produces and their types.
    Required - operations without outputs will fail validation.
    Empty dict {} is valid for operations that only have side effects.

    Example:
        outputs: ClassVar[dict[str, OutputSpec]] = {
            "processed_data": OutputSpec(
                artifact_type=ArtifactTypes.DATA,
                infer_lineage_from={"inputs": ["data"]},
            ),
            "scores": OutputSpec(
                artifact_type=ArtifactTypes.METRIC,
                infer_lineage_from={"outputs": ["processed_data"]},
            ),
        }
    """

    # ---------- Behavior ----------
    runtime_defined_inputs: ClassVar[bool] = False
    """If True, input roles are provided by the user at pipeline construction time,
    not declared in inputs. Accepts both list and dict input formats.

    - List format: All artifacts flattened into a single _merged_streams role,
      useful when names don't matter (e.g., MergeOp)
    - Dict format: Role names from user-provided keys, useful when names are
      meaningful (e.g., roles that map to specific artifact types)

    If False (default), input roles must match inputs keys exactly.

    Example:
        class MergeOp(OperationDefinition):
            runtime_defined_inputs = True
            inputs = {}  # No declared inputs - provided at runtime
    """

    hydrate_inputs: ClassVar[bool] = True
    """Operation-level default hydration behavior.

    Used when runtime_defined_inputs=True and no InputSpec is available
    for a role. If False, all inputs are loaded in ID-only mode.

    Example: Merge operation sets hydrate_inputs=False because it only
    passes through artifact IDs without reading content.
    """

    independent_input_streams: ClassVar[bool] = False
    """If True, input roles can have different numbers of artifacts.

    Most operations require all input roles to have equal lengths for 1:1 pairing
    (zip semantics). Set to True for operations that union/concatenate inputs
    rather than pair them (e.g., MergeOp).

    If False (default), ExecutionUnit validation enforces equal lengths across
    all input roles.

    Example:
        class MergeOp(OperationDefinition):
            independent_input_streams = True  # Unions streams of any size
    """

    group_by: ClassVar[GroupByStrategy | None] = None
    """Strategy for pairing multiple input streams before delivery to the operation.

    None for single-input operations. ZIP, LINEAGE, or CROSS_PRODUCT for
    multi-input operations.
    """

    # ---------- Tool ----------
    tool: ToolSpec | None = None
    """External binary/script this operation invokes. None for pure-Python ops."""

    # ---------- Environments ----------
    environments: Environments = Environments()
    """Multi-environment configuration. Selects which runtime wraps commands."""

    # ---------- Resources ----------
    resources: ResourceConfig = ResourceConfig()
    """Hardware resource allocation for SLURM jobs."""

    # ---------- Execution ----------
    execution: ExecutionConfig = ExecutionConfig()
    """Batching and scheduling configuration."""

    # ---------- Lifecycle ----------
    def preprocess(self, _inputs: PreprocessInput) -> dict[str, Any]:
        """Transform framework artifacts into the format expected by execute.

        Required for creator operations with inputs. Override to extract
        paths, decode content, or reshape artifacts before execution.
        Generative creators (no inputs) can use the default (returns ``{}``).

        Args:
            inputs: Artifacts keyed by role and a working directory for
                intermediate files.

        Returns:
            Dict of prepared inputs forwarded to ``execute()``.

        Example:
            >>> def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
            ...     return {
            ...         role: [a.materialized_path for a in artifacts]
            ...         for role, artifacts in inputs.input_artifacts.items()
            ...     }
        """
        return {}

    def execute(self, inputs: ExecuteInput) -> Any:
        """Run the core computation for a creator operation.

        Override to implement the operation's logic. Receives prepared inputs
        from preprocess and writes output files to ``inputs.execute_dir``.
        Config parameters are accessed via ``self``.

        The framework calls this method; direct calls bypass orchestration
        (sandboxing, lineage, caching) and should only be used for testing.

        Args:
            inputs: Prepared inputs from preprocess and the execute directory.

        Returns:
            Raw result of any type, passed to postprocess as memory_outputs.

        Raises:
            NotImplementedError: If the subclass does not override this method.
        """
        msg = f"{self.__class__.__name__} must implement execute() method"
        raise NotImplementedError(msg)

    def execute_curator(
        self,
        inputs: dict[str, pl.DataFrame],
        step_number: int,
        artifact_store: ArtifactStore,
    ) -> CuratorResult:
        """Run the core computation for a curator operation.

        Override instead of ``execute()`` for operations that manipulate
        artifact metadata without worker dispatch. Curator operations execute
        locally, skip sandboxing, and receive DataFrames with at least an
        ``artifact_id`` column per role.

        Args:
            inputs: Role names mapped to DataFrames, each with an
                ``artifact_id`` column.
            step_number: Current pipeline step number.
            artifact_store: Store for hydration and lineage lookups.

        Returns:
            CuratorResult (ArtifactResult or PassthroughResult).

        Raises:
            NotImplementedError: If not overridden (operation is a creator).
        """
        msg = (
            f"{self.__class__.__name__} does not implement execute_curator() - "
            "this is a creator operation, not a curator operation"
        )
        raise NotImplementedError(msg)

    def postprocess(self, _inputs: PostprocessInput) -> ArtifactResult:
        """Construct draft artifacts from execution outputs.

        Override to select files from ``file_outputs``, unpack
        ``memory_outputs``, and build drafts via ``Artifact.draft()``.
        The default returns a successful result with no artifacts.

        Args:
            inputs: File outputs, memory outputs, input context, and
                step metadata from the completed execution.

        Returns:
            ArtifactResult containing draft artifacts keyed by output role.
        """
        # Default: success with no memory outputs
        return ArtifactResult(success=True)

    # ---------- Validation ----------
    @classmethod
    def __pydantic_init_subclass__(cls, **kwargs: Any) -> None:
        """Validate subclass declarations, generate role docs, and register.

        Runs after Pydantic finishes processing the class so that ClassVar
        attributes (name, inputs, outputs) are available for validation.
        """
        super().__pydantic_init_subclass__(**kwargs)

        # Skip abstract classes (no name set)
        if not cls.name:
            return

        # Check if either execute or execute_curator is implemented
        has_execute = cls.execute is not OperationDefinition.execute
        has_execute_curator = (
            cls.execute_curator is not OperationDefinition.execute_curator
        )
        if not has_execute and not has_execute_curator:
            msg = (
                f"{cls.__name__} must implement either execute() (creator ops) "
                "or execute_curator() (curator ops)"
            )
            raise TypeError(msg)

        # Creator ops must declare explicit lineage for all outputs
        if has_execute:
            for role_name, spec in cls.outputs.items():
                if spec.infer_lineage_from is None:
                    msg = (
                        f"{cls.__name__}.outputs['{role_name}'] must set "
                        "infer_lineage_from (explicit lineage required for creators)"
                    )
                    raise TypeError(msg)

        # Creator ops with inputs must implement preprocess
        if has_execute and cls.inputs:
            has_preprocess = cls.preprocess is not OperationDefinition.preprocess
            if not has_preprocess:
                msg = (
                    f"{cls.__name__} is a creator operation with inputs — "
                    "must implement preprocess()"
                )
                raise TypeError(msg)

        validate_role_enums(cls, "operation")
        append_role_docs(cls)

        # Register in operation registry (concrete ops only)
        if cls.name:
            OperationDefinition._registry[cls.name] = cls

    # ---------- Registry ----------
    @classmethod
    def get(cls, name: str) -> type[OperationDefinition]:
        """Look up an operation class by name.

        Args:
            name: Operation name (e.g. "tool_a").

        Returns:
            The OperationDefinition subclass.

        Raises:
            KeyError: If name is not registered.
        """
        return get_registered(name, cls._registry, "operation")

    @classmethod
    def get_all(cls) -> dict[str, type[OperationDefinition]]:
        """Return a copy of the operation registry."""
        return dict(cls._registry)
