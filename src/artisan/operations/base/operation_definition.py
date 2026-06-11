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
    Literal,
)

if TYPE_CHECKING:
    import polars as pl

    from artisan.registry.models import OperationMetadata, OperationSummary
    from artisan.storage.core.artifact_store import ArtifactStore

from pydantic import BaseModel, ConfigDict

from artisan.errors import ArtisanError, ErrorCode
from artisan.operations.base._param_docs import (
    _extract_arg_descriptions,
    _params_class,
)
from artisan.operations.base._role_docs import (
    append_role_docs,
    get_registered,
    validate_role_enums,
)
from artisan.operations.base.operation_example import OperationExample
from artisan.schemas.enums import GroupByStrategy
from artisan.schemas.execution.batch_strategy import BatchStrategy
from artisan.schemas.execution.curator_result import ArtifactResult, CuratorResult
from artisan.schemas.operation_config.compute import ComputeProvider
from artisan.schemas.operation_config.compute_resources import ComputeResources
from artisan.schemas.operation_config.environments import Environments
from artisan.schemas.operation_config.runner_resources import RunnerResources
from artisan.schemas.operation_config.tool_spec import ToolSpec
from artisan.schemas.specs.input_models import (
    ExecuteInput,
    PostprocessInput,
    PreprocessInput,
)
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec
from artisan.utils.external_tools import run_command


def tool_command_inputs(prepared: dict[str, Any]) -> dict[str, Any]:
    """Normalize prepared inputs for ``execute_command``.

    Per-artifact dispatch delivers each sliced role as a one-element list
    (the list interface ``execute_function()`` implementations expect); a tool
    command addresses one artifact's files, so the framework unwraps
    single-element lists before ``execute_command`` — identically under the
    local subprocess and the endpoint client.

    Args:
        prepared: ``ExecuteInput.inputs`` for one artifact.

    Returns:
        The dict with one-element list values unwrapped to their item.
    """
    return {
        key: value[0] if isinstance(value, list) and len(value) == 1 else value
        for key, value in prepared.items()
    }


class OperationDefinition(BaseModel):
    """Base class for all pipeline operations.

    Subclasses declare input/output specs, implement the lifecycle methods
    (preprocess, one of the execute slots, postprocess), and are
    automatically validated and registered on definition. The execute
    slots are ``execute_function()`` (a Python body),
    ``execute_command()`` (a tool argv), and ``execute_curator()``
    (metadata-only curators).

    The framework synthesizes a unit-level log at
    ``<sandbox_root>/tool_output.log`` and exposes it as
    ``ExecuteInput.log_path``. The filename ``tool_output.log`` is
    reserved by the framework — do not write a file by that name to
    ``<sandbox_root>``.

    Attributes:
        name (str): Unique operation identifier used for registry lookup.
        description (str): Human-readable summary shown in docs and logs.
        inputs (dict[str, InputSpec]): Named input specifications.
        outputs (dict[str, OutputSpec]): Named output specifications.
        runner_resources (RunnerResources): Hardware resource allocation for SLURM jobs.
        batch_strategy (BatchStrategy): Batching and scheduling configuration.
    """

    model_config = ConfigDict(
        extra="forbid",
        str_strip_whitespace=True,
    )

    _registry: ClassVar[dict[str, type[OperationDefinition]]] = {}
    _name_collisions: ClassVar[list[tuple[str, str, str]]] = []
    """Dropped registration attempts: (name, first_module, second_module).

    Populated by ``__pydantic_init_subclass__`` whenever a second class tries
    to register under an existing name. First registration wins; the second
    is dropped. ``artisan.registry.discover()`` reads this list to populate
    ``DiscoveryReport.name_collisions``.
    """

    # ---------- Metadata ----------
    name: ClassVar[str] = ""
    description: ClassVar[str] = ""
    examples: ClassVar[list[OperationExample]] = []
    """Author-declared usage examples. Surfaced by ``artisan.registry.examples(name)``."""

    tags: ClassVar[list[str]] = []
    """Free-form tags for agent-side filtering (e.g. ``"source"``, ``"transform"``)."""

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

    group_by: GroupByStrategy | None = None
    """Strategy for pairing multiple input streams before delivery to the operation.

    Default ``None`` for single-input operations; subclasses set ``LINEAGE``,
    ``ZIP``, or ``CROSS_PRODUCT`` for multi-input operations. Per-step callers
    override via ``pipeline.run(..., group_by=...)``; the override wins over
    any class-level default.

    Notes:
        - **CROSS_PRODUCT output collisions.** Outputs are content-addressed by
          ``xxh3_128`` of their bytes. CROSS_PRODUCT operation authors MUST
          ensure each ``(input_pair → output)`` produces output bytes that
          depend on **all** inputs in the pair; otherwise outputs from distinct
          pairs collide to a single ``artifact_id`` and only one row survives
          commit.
        - **CROSS_PRODUCT lineage automatic recovery.** Lineage capture
          recovers pair indices from the per-slot execute directory layout
          when ``per_artifact_dispatch=True`` (the default) **and** every
          output draft is backed by a file under its slot's ``execute_dir``.
          When ``per_artifact_dispatch=False`` + CROSS_PRODUCT +
          ``artifacts_per_unit > 1``, or for memory-only outputs (no file
          on disk), the framework cannot recover pair-index automatically;
          the operation author must set ``ArtifactResult.lineage``
          explicitly when ``group_by`` is active.
    """

    per_artifact_dispatch: ClassVar[bool] = True
    """Whether execute fans out per artifact in batch dispatch.

    When True (default), the framework splits preprocess output into
    per-artifact ExecuteInputs and dispatches each separately. Modal
    sends them to parallel containers; local/SLURM loops sequentially.

    Set to False for operations that run external tools via
    ``run_command()`` where a single subprocess should process all
    artifacts to amortize model loading.
    """

    # ---------- Tool ----------
    tool: ToolSpec | None = None
    """External binary/script this operation invokes. None for pure-Python ops."""

    # ---------- Environments ----------
    environments: Environments = Environments()
    """Multi-environment configuration. Selects which runtime wraps commands."""

    # ---------- Compute provider ----------
    compute_provider: ComputeProvider = ComputeProvider()
    """Compute provider routing. Selects where the execute phase runs (local/Modal)."""

    # ---------- Compute resources ----------
    compute_resources: ComputeResources = ComputeResources()
    """Hardware resources requested from the compute provider (Modal)."""

    # ---------- Runner resources ----------
    runner_resources: RunnerResources = RunnerResources()  # type: ignore[call-arg]
    """Hardware resources for the step runner (local / SLURM)."""

    # ---------- Batch strategy ----------
    batch_strategy: BatchStrategy = BatchStrategy()  # type: ignore[call-arg]
    """Batching and scheduling — how artifacts are sliced into units/workers."""

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
            Dict of prepared inputs forwarded to the execute phase.

        Example:
            >>> def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
            ...     return {
            ...         role: [a.materialized_path for a in artifacts]
            ...         for role, artifacts in inputs.input_artifacts.items()
            ...     }
        """
        return {}

    def execute_command(self, inputs: dict[str, Any]) -> list[str]:
        """Assemble the tool command from params + prepared inputs.

        Command ops override this instead of ``execute_function()``; the
        framework runs the returned argv — locally as a subprocess, or on
        the deployed tool endpoint's worker under
        ``compute_provider='modal'``.

        Args:
            inputs: Prepared inputs from ``preprocess()``
                (``ExecuteInput.inputs``).

        Returns:
            The argv list, typically ``[*self.tool.parts(), ...]``.

        Raises:
            NotImplementedError: If the subclass does not override this method.
        """
        msg = f"{self.__class__.__name__} does not implement execute_command()"
        raise NotImplementedError(msg)

    def is_command_op(self) -> bool:
        """True when this op runs via the framework tool path.

        Command ops declare a ``ToolSpec`` (``tool``) and override
        ``execute_command()`` instead of ``execute_function()``.
        """
        return (
            type(self).execute_command is not OperationDefinition.execute_command
            and self.tool is not None
        )

    def execute_function(self, inputs: ExecuteInput) -> Any:
        """Run the core computation for a creator operation.

        Pure-Python creator ops override this method: receive prepared inputs
        from preprocess, write output files to ``inputs.execute_dir``, access
        config parameters via ``self``.

        Command ops — subclasses declaring a ``tool`` plus
        ``execute_command()`` — inherit this framework implementation
        instead. It dispatches on
        ``compute_provider`` and runs the tool: locally as a subprocess via
        ``run_command`` (wrapped by the active environment), or remotely via
        the deployed tool endpoint. Under both providers it returns ``None``;
        a tool op's products are the files written to ``inputs.execute_dir``
        plus the tool log. Memory results and post-run glue belong in
        ``postprocess()``.

        The framework calls this method; direct calls bypass orchestration
        (sandboxing, lineage, caching) and should only be used for testing.

        Args:
            inputs: Prepared inputs from preprocess and the execute directory.

        Returns:
            Raw result of any type, passed to postprocess as memory_outputs.
            The framework tool-op implementation returns ``None``.

        Raises:
            NotImplementedError: If the subclass neither overrides this
                method nor declares a ToolSpec + ``execute_command()``.
        """
        if not self.is_command_op():
            msg = f"{self.__class__.__name__} must implement execute_function() method"
            raise NotImplementedError(msg)
        if self.compute_provider.active == "modal":
            # Deferred: operations/ may not import execution/ at module
            # level (dependency direction); the client is the one exception,
            # reached only on the modal path.
            from artisan.execution.tool_endpoint.client import call_endpoint

            call_endpoint(self, inputs)
        else:
            run_command(
                self.environments.current(),
                self.execute_command(tool_command_inputs(inputs.inputs)),
                cwd=inputs.execute_dir,
                log_path=inputs.log_path,
            )

    def execute_curator(
        self,
        inputs: dict[str, pl.DataFrame],
        step_number: int,
        artifact_store: ArtifactStore,
    ) -> CuratorResult:
        """Run the core computation for a curator operation.

        Override instead of ``execute_function()`` for operations that
        manipulate artifact metadata without worker dispatch. Curator
        operations execute locally, skip sandboxing, and receive DataFrames
        with at least an ``artifact_id`` column per role.

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

        # Check that one of the execute slots is implemented
        has_execute_function = (
            cls.execute_function is not OperationDefinition.execute_function
        )
        has_execute_curator = (
            cls.execute_curator is not OperationDefinition.execute_curator
        )
        has_execute_command = (
            cls.execute_command is not OperationDefinition.execute_command
        )
        if has_execute_command and cls.model_fields["tool"].default is None:
            msg = (
                f"{cls.__name__} implements execute_command() but declares no "
                "ToolSpec — set the `tool` field"
            )
            raise TypeError(msg)
        if (
            not has_execute_function
            and not has_execute_curator
            and not has_execute_command
        ):
            msg = (
                f"{cls.__name__} must implement execute_function() (creator "
                "ops), execute_curator() (curator ops), or declare a ToolSpec "
                "+ execute_command() (command ops)"
            )
            raise TypeError(msg)

        # Modal compute runs execute_command on a deployed tool endpoint —
        # a class whose default provider is modal must be a command op.
        provider_default = cls.model_fields["compute_provider"].default
        if (
            isinstance(provider_default, ComputeProvider)
            and provider_default.active == "modal"
            and not has_execute_command
        ):
            msg = (
                f"{cls.__name__} defaults compute_provider.active='modal' but "
                "modal requires a ToolSpec + execute_command() (command op)"
            )
            raise TypeError(msg)

        # Creator ops (custom execute or tool command) must declare explicit
        # lineage for all outputs
        is_creator = has_execute_function or has_execute_command
        if is_creator:
            for role_name, spec in cls.outputs.items():
                if spec.infer_lineage_from is None:
                    msg = (
                        f"{cls.__name__}.outputs['{role_name}'] must set "
                        "infer_lineage_from (explicit lineage required for creators)"
                    )
                    raise TypeError(msg)

        # Creator ops with inputs must implement preprocess
        if is_creator and cls.inputs:
            has_preprocess = cls.preprocess is not OperationDefinition.preprocess
            if not has_preprocess:
                msg = (
                    f"{cls.__name__} is a creator operation with inputs — "
                    "must implement preprocess()"
                )
                raise TypeError(msg)

        validate_role_enums(cls, "operation")
        append_role_docs(cls)
        cls._validate_params_documented()

        # Register in operation registry (concrete ops only). First
        # registration wins; collisions are recorded for discovery.
        if cls.name:
            existing = OperationDefinition._registry.get(cls.name)
            if existing is None:
                OperationDefinition._registry[cls.name] = cls
            elif existing is not cls:
                OperationDefinition._name_collisions.append(
                    (cls.name, existing.__module__, cls.__module__)
                )

    @classmethod
    def _validate_params_documented(cls) -> None:
        """Fail-fast if any ``Params`` field lacks a description source.

        A description must come from either ``Field(description=...)`` or
        the class docstring (``Attributes:`` or ``Args:`` section). Parameter-less
        ops and ops with empty ``Params`` short-circuit; otherwise import
        fails with ``ArtisanError(code=OP_PARAMS_UNDOCUMENTED)`` before the
        op reaches the registry — so the gap surfaces at the contributor's
        editor, not on the agent wire.
        """
        params_cls = _params_class(cls)
        if params_cls is None or not params_cls.model_fields:
            return
        described = _extract_arg_descriptions(params_cls)
        missing = [
            name
            for name, f in params_cls.model_fields.items()
            if not f.description and name not in described
        ]
        if not missing:
            return
        raise ArtisanError(
            code=ErrorCode.OP_PARAMS_UNDOCUMENTED,
            error_type="config",
            operation_name=cls.name,
            message=(
                f"Operation {cls.name!r}: Params fields {missing} have no "
                f"description. Add a Google-style 'Attributes:' section to "
                f"the Params docstring (or 'Args:'), or use "
                f"Field(description=...) per field."
            ),
            hint="Document each Params field.",
            recovery_hint="CHECK_INPUT",
        )

    # ---------- Introspection (agent-facing) ----------
    @classmethod
    def _kind(cls) -> Literal["creator", "curator"]:
        """Return ``"curator"`` if ``execute_curator`` is overridden, else ``"creator"``."""
        if cls.execute_curator is not OperationDefinition.execute_curator:
            return "curator"
        return "creator"

    @classmethod
    def to_summary(cls) -> OperationSummary:
        """Return the lightweight ``OperationSummary`` for ``list_operations()``."""
        from artisan.registry.models import OperationSummary

        return OperationSummary(
            name=cls.name,
            kind=cls._kind(),
            description=cls.description,
            input_roles=list(cls.inputs.keys()),
            output_roles=list(cls.outputs.keys()),
            tags=list(cls.tags),
        )

    @classmethod
    def to_metadata(cls) -> OperationMetadata:
        """Return the full ``OperationMetadata`` for ``describe(name)``."""
        from artisan.registry.models import (
            InputSpecMetadata,
            OperationMetadata,
            OutputSpecMetadata,
        )
        from artisan.registry.schemas import params_schema_for

        return OperationMetadata(
            **cls.to_summary().model_dump(),
            inputs={
                role: InputSpecMetadata(
                    artifact_type=spec.artifact_type,
                    required=spec.required,
                    description=spec.description,
                    materialize=spec.materialize,
                )
                for role, spec in cls.inputs.items()
            },
            outputs={
                role: OutputSpecMetadata(
                    artifact_type=spec.artifact_type,
                    required=spec.required,
                    description=spec.description,
                )
                for role, spec in cls.outputs.items()
            },
            params_schema=params_schema_for(cls),
            examples=list(cls.examples),
            source_module=cls.__module__,
        )

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
        result: type[OperationDefinition] = get_registered(
            name, cls._registry, "operation"
        )
        return result

    @classmethod
    def get_all(cls) -> dict[str, type[OperationDefinition]]:
        """Return a copy of the operation registry."""
        return dict(cls._registry)
