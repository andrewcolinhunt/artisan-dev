"""Main interface for defining and executing artisan pipelines.

Key exports: ``PipelineManager`` (create, run, submit, finalize).
"""

from __future__ import annotations

import contextvars
import json
import logging
import signal
import threading
import time
from collections.abc import Iterator
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, overload
from uuid import uuid4

import polars as pl
import xxhash

from artisan.execution.executors.curator import is_curator_operation
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.orchestration.backends import Backend, BackendBase, resolve_backend
from artisan.orchestration.engine.step_executor import (
    execute_step,
    instantiate_operation,
)
from artisan.orchestration.engine.step_tracker import StepTracker
from artisan.orchestration.step_future import StepFuture
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.schemas.enums import CachePolicy, FailurePolicy
from artisan.schemas.orchestration.output_reference import OutputReference
from artisan.schemas.orchestration.pipeline_config import PipelineConfig
from artisan.schemas.orchestration.step_result import StepResult
from artisan.schemas.orchestration.step_start_record import StepStartRecord
from artisan.utils.hashing import compute_step_spec_id

logger = logging.getLogger(__name__)


# =============================================================================
# Module-level helper functions
# =============================================================================


def _generate_run_id(name: str) -> str:
    """Generate a human-readable pipeline run ID."""
    return f"{name}_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}_{uuid4().hex[:8]}"


def _generate_step_run_id(step_spec_id: str) -> str:
    """Generate a unique 32-char hex step run identifier."""
    data = f"{step_spec_id}:{datetime.now(UTC).isoformat()}"
    return xxhash.xxh3_128(data.encode()).hexdigest()


def _qualified_name(operation: type[OperationDefinition]) -> str:
    """Return the fully-qualified class name for audit logging."""
    return f"{operation.__module__}.{operation.__qualname__}"


def _extract_source_steps(inputs: Any) -> set[int]:
    """Extract upstream step numbers from OutputReferences in inputs."""
    steps: set[int] = set()
    if inputs is None:
        return steps
    if isinstance(inputs, dict):
        for v in inputs.values():
            if isinstance(v, OutputReference):
                steps.add(v.source_step)
    elif isinstance(inputs, list):
        for item in inputs:
            if isinstance(item, OutputReference):
                steps.add(item.source_step)
    return steps


def _serialize_input_refs(inputs: Any) -> str:
    """Serialize input references to a JSON string for delta persistence."""
    if inputs is None:
        return "null"
    if isinstance(inputs, dict):
        serialized = {}
        for key, value in inputs.items():
            if isinstance(value, OutputReference):
                serialized[key] = {
                    "type": "output_ref",
                    "source_step": value.source_step,
                    "role": value.role,
                    "artifact_type": value.artifact_type,
                }
            else:
                serialized[key] = {"type": "literal", "value": value}
        return json.dumps(serialized)
    if isinstance(inputs, list):
        serialized_list = []
        for item in inputs:
            if isinstance(item, OutputReference):
                serialized_list.append(
                    {
                        "type": "output_ref",
                        "source_step": item.source_step,
                        "role": item.role,
                        "artifact_type": item.artifact_type,
                    }
                )
            else:
                serialized_list.append({"type": "literal", "value": item})
        return json.dumps(serialized_list)
    return json.dumps(str(inputs))


def _extract_name_from_run_id(run_id: str) -> str:
    """Extract the pipeline name prefix from a run ID."""
    parts = run_id.rsplit("_", 3)
    return parts[0]


def _set_default(o: Any) -> Any:
    """JSON default handler for sets and Paths."""
    if isinstance(o, set):
        return sorted(o)
    if isinstance(o, Path):
        return str(o)
    raise TypeError(f"Object of type {type(o).__name__} is not JSON serializable")


def _is_file_path_input(inputs: Any) -> bool:
    """Return True if inputs is a non-empty list of raw file path strings."""
    return (
        isinstance(inputs, list)
        and bool(inputs)
        and isinstance(inputs[0], str)
        and not isinstance(inputs[0], OutputReference)
    )


def _promote_file_paths_to_store(
    file_paths: list[str],
    config: PipelineConfig,
    step_number: int,
    operation_name: str,
    failure_policy: FailurePolicy,
) -> tuple[dict[str, list[str]] | None, int]:
    """Validate file paths, create FileRefArtifacts, and commit to delta.

    Args:
        file_paths: Raw file path strings from the user.
        config: Pipeline configuration.
        step_number: Pipeline step number.
        operation_name: Operation name (for logging).
        failure_policy: Failure policy for the step.

    Returns:
        Tuple of (resolved inputs dict or None if all invalid,
        count of valid files).
    """
    from artisan.schemas.artifact.file_ref import FileRefArtifact
    from artisan.schemas.enums import TablePath
    from artisan.storage.io.commit import DeltaCommitter
    from artisan.utils.filename import strip_extensions

    valid_paths: list[str] = []
    invalid_paths: list[str] = []
    for path_str in file_paths:
        p = Path(path_str)
        if not p.exists():
            invalid_paths.append(f"Not found: {p}")
        elif not p.is_file():
            invalid_paths.append(f"Not a file: {p}")
        else:
            valid_paths.append(path_str)

    if invalid_paths:
        logger.warning(
            "Skipping %d invalid input files for step %d: %s",
            len(invalid_paths),
            step_number,
            "; ".join(invalid_paths),
        )

    if not valid_paths:
        return None, 0

    # Create FileRefArtifacts and finalize
    file_ref_artifacts: list[FileRefArtifact] = []
    for path_str in valid_paths:
        path = Path(path_str)
        content = path.read_bytes()
        content_hash = xxhash.xxh3_128(content).hexdigest()
        size_bytes = len(content)
        artifact = FileRefArtifact.draft(
            path=str(path.absolute()),
            content_hash=content_hash,
            size_bytes=size_bytes,
            step_number=step_number,
            original_name=strip_extensions(path.name),
            extension=path.suffix,
        ).finalize()
        file_ref_artifacts.append(artifact)

    # Build DataFrames for file_refs table and artifact_index
    file_ref_rows = [a.to_row() for a in file_ref_artifacts]
    file_ref_df = pl.DataFrame(file_ref_rows, schema=FileRefArtifact.POLARS_SCHEMA)

    index_rows = [
        {
            "artifact_id": a.artifact_id,
            "artifact_type": a.artifact_type,
            "origin_step_number": a.origin_step_number,
            "metadata": json.dumps({}),
        }
        for a in file_ref_artifacts
    ]
    index_df = pl.DataFrame(
        index_rows,
        schema={
            "artifact_id": pl.String,
            "artifact_type": pl.String,
            "origin_step_number": pl.Int32,
            "metadata": pl.String,
        },
    )

    # Commit directly to Delta Lake (pre-dispatch)
    committer = DeltaCommitter(config.delta_root, config.staging_root)
    committer.commit_dataframe(file_ref_df, "artifacts/file_refs")
    committer.commit_dataframe(index_df, TablePath.ARTIFACT_INDEX)

    artifact_ids = [a.artifact_id for a in file_ref_artifacts]
    resolved_inputs = {"file": sorted(artifact_ids)}

    logger.debug(
        "Step %d (%s): promoted %d file paths to Delta Lake",
        step_number,
        operation_name,
        len(valid_paths),
    )
    return resolved_inputs, len(valid_paths)


# =============================================================================
# Validation helpers
# =============================================================================


def _validate_params(
    operation: type,
    params: dict[str, Any],
) -> None:
    """Raise ValueError if any param keys are unrecognized by the operation."""
    if "params" in operation.model_fields:
        params_cls = operation.model_fields["params"].annotation
        valid_keys = set(params_cls.model_fields)
    else:
        # Flat fields — exclude ClassVar and base fields
        base_fields = set(OperationDefinition.model_fields)
        valid_keys = set(operation.model_fields) - base_fields
    unknown = set(params) - valid_keys
    if unknown:
        msg = (
            f"Unknown params for {operation.name}: {sorted(unknown)}. "
            f"Valid keys: {sorted(valid_keys)}"
        )
        raise ValueError(msg)


def _validate_resources(resources: dict[str, Any]) -> None:
    """Raise ValueError if any resource keys are unrecognized."""
    from artisan.schemas.operation_config.resource_config import ResourceConfig

    valid_keys = set(ResourceConfig.model_fields)
    unknown = set(resources) - valid_keys
    if unknown:
        msg = (
            f"Unknown resource keys: {sorted(unknown)}. "
            f"Valid keys: {sorted(valid_keys)}"
        )
        raise ValueError(msg)


def _validate_execution(execution: dict[str, Any]) -> None:
    """Raise ValueError if any execution keys are unrecognized."""
    from artisan.schemas.execution.execution_config import ExecutionConfig

    valid_keys = set(ExecutionConfig.model_fields)
    unknown = set(execution) - valid_keys
    if unknown:
        msg = (
            f"Unknown execution keys: {sorted(unknown)}. "
            f"Valid keys: {sorted(valid_keys)}"
        )
        raise ValueError(msg)


def _validate_environment(
    operation: type[OperationDefinition],
    environment: str | dict[str, Any],
) -> None:
    """Raise ValueError if environment override is invalid for this operation."""
    if isinstance(environment, str):
        temp = operation()
        if environment not in temp.environments.available():
            msg = (
                f"Environment '{environment}' not configured on {operation.name}. "
                f"Available: {temp.environments.available()}"
            )
            raise ValueError(msg)
    else:
        from artisan.schemas.operation_config.environment_spec import (
            ApptainerEnvironmentSpec,
            DockerEnvironmentSpec,
            LocalEnvironmentSpec,
            PixiEnvironmentSpec,
        )
        from artisan.schemas.operation_config.environments import Environments

        valid_keys = set(Environments.model_fields)
        unknown = set(environment) - valid_keys
        if unknown:
            msg = (
                f"Unknown environment keys: {sorted(unknown)}. "
                f"Valid keys: {sorted(valid_keys)}"
            )
            raise ValueError(msg)
        # Validate nested dicts against their EnvironmentSpec subclass
        env_field_types = {
            "local": LocalEnvironmentSpec,
            "docker": DockerEnvironmentSpec,
            "apptainer": ApptainerEnvironmentSpec,
            "pixi": PixiEnvironmentSpec,
        }
        for key, value in environment.items():
            if key == "active" or not isinstance(value, dict):
                continue
            spec_cls = env_field_types.get(key)
            if spec_cls:
                valid_spec_keys = set(spec_cls.model_fields)
                bad = set(value) - valid_spec_keys
                if bad:
                    msg = (
                        f"Unknown keys for {key} environment: {sorted(bad)}. "
                        f"Valid: {sorted(valid_spec_keys)}"
                    )
                    raise ValueError(msg)


def _validate_tool(
    operation: type[OperationDefinition],
    tool: dict[str, Any],
) -> None:
    """Raise ValueError if tool overrides are invalid for this operation."""
    from artisan.schemas.operation_config.tool_spec import ToolSpec

    temp = operation()
    if temp.tool is None:
        msg = f"Operation '{operation.name}' has no tool to override"
        raise ValueError(msg)
    valid_keys = set(ToolSpec.model_fields)
    unknown = set(tool) - valid_keys
    if unknown:
        msg = (
            f"Unknown tool keys: {sorted(unknown)}. "
            f"Valid keys: {sorted(valid_keys)}"
        )
        raise ValueError(msg)


def _validate_input_roles(
    operation: type,
    inputs: Any,
) -> None:
    """Raise ValueError if dict input roles are not declared by the operation.

    No-op for non-dict inputs or runtime_defined_inputs operations.
    """
    if not isinstance(inputs, dict):
        return
    if getattr(operation, "runtime_defined_inputs", False):
        return
    valid_roles = set(operation.inputs)
    unknown = set(inputs) - valid_roles
    if unknown:
        msg = (
            f"Unknown input roles for {operation.name}: {sorted(unknown)}. "
            f"Valid roles: {sorted(valid_roles)}"
        )
        raise ValueError(msg)


def _validate_required_inputs(
    operation: type,
    inputs: Any,
) -> None:
    """Raise ValueError if any required input roles are missing."""
    if getattr(operation, "runtime_defined_inputs", False):
        return
    if not operation.inputs:
        return
    if not isinstance(inputs, dict):
        return

    provided_roles = set(inputs.keys())

    missing = [
        role
        for role, spec in operation.inputs.items()
        if spec.required and role not in provided_roles
    ]
    if missing:
        msg = (
            f"Missing required input(s) for {operation.name}: {sorted(missing)}. "
            f"Declared inputs: {sorted(operation.inputs.keys())}"
        )
        raise ValueError(msg)


def _validate_input_types(
    operation: type,
    inputs: Any,
) -> None:
    """Raise ValueError if upstream output types don't match input specs."""
    if not isinstance(inputs, dict):
        return
    for role, ref in inputs.items():
        if not isinstance(ref, OutputReference):
            continue
        input_spec = operation.inputs.get(role)
        if input_spec is None:
            continue
        if ref.artifact_type == ArtifactTypes.ANY:
            continue
        if not input_spec.accepts_type(ref.artifact_type):
            msg = (
                f"Type mismatch on input '{role}' for {operation.name}: "
                f"upstream step {ref.source_step} produces '{ref.artifact_type}', "
                f"but '{role}' expects '{input_spec.artifact_type}'"
            )
            raise ValueError(msg)


# =============================================================================
# Step registry entry (populated at declaration time)
# =============================================================================


@dataclass(frozen=True)
class _StepEntry:
    """Metadata about a declared step, available before execution completes."""

    step_number: int
    output_roles: frozenset[str]
    output_types: dict[str, str | None]


# =============================================================================
# PipelineManager
# =============================================================================


class PipelineManager:
    """Main interface for defining and executing pipelines.

    PipelineManager orchestrates the execution of pipeline steps, managing:
    - Step sequencing and numbering
    - Step-level caching via steps delta table
    - OutputReference resolution
    - Worker dispatch (local or SLURM)
    - Delta Lake commits
    - Error handling and failure policies
    - Async step execution via submit()

    Usage::

        pipeline = PipelineManager.create(
            name="my_pipeline",
            delta_root=Path("/data/delta"),
            staging_root=Path("/data/staging"),
        )

        step0 = pipeline.run(IngestData, inputs=files)
        step1 = pipeline.run(ScoreOp, inputs={"data": step0.output("data")})

        result = pipeline.finalize()
    """

    def __init__(
        self,
        config: PipelineConfig,
        configure_logging: bool = True,
    ):
        """Initialize from a PipelineConfig.

        Prefer ``PipelineManager.create()`` over direct instantiation.

        Args:
            config: Full pipeline configuration.
            configure_logging: If True (default), call
                :func:`~artisan.utils.logging.configure_logging` so
                users don't need to set up logging manually.
        """
        if configure_logging:
            from artisan.utils.logging import configure_logging as _configure

            _configure(logs_root=config.delta_root.parent / "logs")

        self._config = config

        if config.recover_staging:
            from artisan.storage.io.commit import DeltaCommitter

            committer = DeltaCommitter(config.delta_root, config.staging_root)
            committer.recover_staged(preserve_staging=config.preserve_staging)

        self._start_time: float = time.time()
        self._current_step: int = 0
        self._step_results: list[StepResult] = []
        self._named_steps: dict[str, list[StepResult]] = {}
        self._step_registry: dict[str, list[_StepEntry]] = {}
        self._step_spec_ids: dict[int, str] = {}
        self._step_tracker = StepTracker(config.delta_root, config.pipeline_run_id)
        self._stopped: bool = False
        self._cancel_event = threading.Event()
        self._prev_sigint: signal.Handlers | None = None
        self._prev_sigterm: signal.Handlers | None = None
        self._active_futures: dict[int, StepFuture] = {}
        self._executor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="pipeline-step"
        )

    @property
    def config(self) -> PipelineConfig:
        """Pipeline configuration (read-only)."""
        return self._config

    @property
    def current_step(self) -> int:
        """Current step counter (read-only)."""
        return self._current_step

    def __repr__(self) -> str:
        """Return an unambiguous representation for debugging."""
        return (
            f"PipelineManager("
            f"name={self._config.name!r}, "
            f"steps={len(self._step_results)}, "
            f"delta_root={self._config.delta_root!r})"
        )

    def __str__(self) -> str:
        """Return a human-readable summary of pipeline progress."""
        if not self._step_results:
            return f"Pipeline '{self._config.name}': no steps executed"

        succeeded = sum(1 for r in self._step_results if r.success)
        total = len(self._step_results)
        status = (
            "all succeeded" if succeeded == total else f"{succeeded}/{total} succeeded"
        )
        return f"Pipeline '{self._config.name}': {total} steps, {status}"

    def __len__(self) -> int:
        """Return the number of completed steps."""
        return len(self._step_results)

    def __iter__(self) -> Iterator[StepResult]:
        """Iterate over step results."""
        return iter(self._step_results)

    @overload
    def __getitem__(self, index: int) -> StepResult: ...

    @overload
    def __getitem__(self, index: slice) -> list[StepResult]: ...

    def __getitem__(self, index: int | slice) -> StepResult | list[StepResult]:
        """Retrieve step results by index or slice."""
        return self._step_results[index]

    def __bool__(self) -> bool:
        """Return True if at least one step ran and all succeeded."""
        return bool(self._step_results) and all(r.success for r in self._step_results)

    def __contains__(self, step_name: str) -> bool:
        """Return True if a step with the given name has been recorded."""
        return any(r.step_name == step_name for r in self._step_results)

    def _register_step(
        self,
        name: str,
        step_number: int,
        operation_outputs: dict,
    ) -> None:
        """Record step metadata at declaration time for ``output()`` lookups."""
        entry = _StepEntry(
            step_number=step_number,
            output_roles=frozenset(operation_outputs.keys()),
            output_types={
                r: s.artifact_type if s.artifact_type else None
                for r, s in operation_outputs.items()
            },
        )
        self._step_registry.setdefault(name, []).append(entry)

    # =========================================================================
    # Cancellation
    # =========================================================================

    def cancel(self) -> None:
        """Request cancellation of the running pipeline.

        Idempotent and thread-safe. Sets an event that step executors
        check between phases, causing them to return early with
        ``metadata={"cancelled": True}``.
        """
        if not self._cancel_event.is_set():
            logger.warning("Pipeline '%s': cancellation requested.", self._config.name)
        self._cancel_event.set()

    def _install_signal_handlers(self) -> None:
        """Install SIGINT/SIGTERM handlers that call cancel().

        No-op when called from a non-main thread (e.g. Jupyter workers).
        """
        try:
            self._prev_sigint = signal.getsignal(signal.SIGINT)
            self._prev_sigterm = signal.getsignal(signal.SIGTERM)
            signal.signal(signal.SIGINT, self._handle_signal)
            signal.signal(signal.SIGTERM, self._handle_signal)
        except ValueError:
            pass  # Not on main thread (e.g. Jupyter)

    def _handle_signal(self, signum: int, frame: Any) -> None:
        """Signal handler: escalating cancel → restore → force-kill."""
        sig_name = signal.Signals(signum).name
        if self._cancel_event.is_set():
            logger.warning(
                "Pipeline '%s': received second %s — restoring default handlers. "
                "Press Ctrl+C again to force exit.",
                self._config.name,
                sig_name,
            )
            self._restore_signal_handlers()
            return
        logger.warning(
            "Pipeline '%s': received %s — cancelling.",
            self._config.name,
            sig_name,
        )
        self.cancel()

    def _restore_signal_handlers(self) -> None:
        """Restore previous signal handlers."""
        try:
            if self._prev_sigint is not None:
                signal.signal(signal.SIGINT, self._prev_sigint)
                self._prev_sigint = None
            if self._prev_sigterm is not None:
                signal.signal(signal.SIGTERM, self._prev_sigterm)
                self._prev_sigterm = None
        except ValueError:
            pass  # Not on main thread

    def output(
        self,
        name: str,
        role: str,
        *,
        step_number: int | None = None,
    ) -> OutputReference:
        """Get a reference to outputs from a named step.

        Args:
            name: Step name to look up (custom or operation default).
            role: Output role name to reference.
            step_number: If given, select the step with this number (validated
                against *name*). Defaults to the most recent step with *name*.

        Returns:
            OutputReference for wiring to downstream steps.

        Raises:
            ValueError: If no step with that name exists, role is invalid,
                or step_number doesn't match any step with the given name.
        """
        entries = self._step_registry.get(name)
        if not entries:
            available = sorted(self._step_registry.keys()) or ["(none)"]
            msg = f"No step named '{name}'. Available: {', '.join(available)}"
            raise ValueError(msg)

        if step_number is None:
            entry = entries[-1]
        else:
            for e in entries:
                if e.step_number == step_number:
                    entry = e
                    break
            else:
                step_numbers = [e.step_number for e in entries]
                msg = (
                    f"Step '{name}' has no entry with step_number={step_number}. "
                    f"Available step numbers: {step_numbers}"
                )
                raise ValueError(msg)

        if role not in entry.output_roles:
            available_roles = ", ".join(sorted(entry.output_roles)) or "(none)"
            msg = (
                f"Output role '{role}' not available for step '{name}'. "
                f"Available roles: {available_roles}"
            )
            raise ValueError(msg)

        return OutputReference(
            source_step=entry.step_number,
            role=role,
            artifact_type=entry.output_types.get(role),
        )

    # =========================================================================
    # Factory / classmethods
    # =========================================================================

    @classmethod
    def create(
        cls,
        name: str,
        delta_root: Path | str,
        staging_root: Path | str,
        working_root: Path | str | None = None,
        failure_policy: FailurePolicy = FailurePolicy.CONTINUE,
        cache_policy: CachePolicy = CachePolicy.ALL_SUCCEEDED,
        backend: str | BackendBase = "local",
        preserve_staging: bool = False,
        preserve_working: bool = False,
        recover_staging: bool = True,
        prefect_server: str | None = None,
    ) -> PipelineManager:
        """Factory method to create a PipelineManager.

        Automatically discovers and connects to a running Prefect server.
        Resolution order: explicit argument > PREFECT_SUBMITIT_SERVER env var
        > PREFECT_API_URL env var > discovery file > error with instructions.

        Args:
            name: Pipeline identifier (used for logging and Prefect).
            delta_root: Root path for Delta Lake tables.
            staging_root: Root path for worker staging files.
            working_root: Root path for worker sandboxes. If None, uses
                tempfile.gettempdir() (respects $TMPDIR).
            failure_policy: Default failure handling for steps.
            cache_policy: Controls when completed steps qualify as cache hits.
            backend: Default backend for step execution. Accepts a BackendBase
                instance or string name (e.g. "local", "slurm").
            preserve_staging: Debug flag to preserve staging files after commit.
            preserve_working: Debug flag to preserve sandbox after execution.
            recover_staging: Commit leftover staging files from prior crashed
                runs at pipeline init. Defaults to True.
            prefect_server: Prefect server URL. If None, auto-discovered.

        Returns:
            Configured PipelineManager instance.

        Raises:
            PrefectServerNotFound: If no server can be discovered.
            PrefectServerUnreachable: If the server is not responding.
        """
        from artisan.orchestration.prefect_server import (
            activate_server,
            discover_server,
        )

        server_info = discover_server(prefect_server)
        activate_server(server_info)

        resolved = resolve_backend(backend)
        pipeline_run_id = _generate_run_id(name)
        config = PipelineConfig(
            name=name,
            pipeline_run_id=pipeline_run_id,
            delta_root=Path(delta_root),
            staging_root=Path(staging_root),
            **(
                {"working_root": Path(working_root)} if working_root is not None else {}
            ),
            failure_policy=failure_policy,
            cache_policy=cache_policy,
            default_backend=resolved.name,
            preserve_staging=preserve_staging,
            preserve_working=preserve_working,
            recover_staging=recover_staging,
        )
        instance = cls(config)
        logger.info("Pipeline '%s' initialized (run_id=%s)", name, pipeline_run_id)
        logger.info("  delta_root: %s", config.delta_root)
        logger.info("  staging_root: %s", config.staging_root)
        return instance

    @classmethod
    def resume(
        cls,
        delta_root: Path | str,
        staging_root: Path | str,
        pipeline_run_id: str | None = None,
        name: str | None = None,
        working_root: Path | str | None = None,
        prefect_server: str | None = None,
        **kwargs: Any,
    ) -> PipelineManager:
        """Resume a pipeline from persisted step state.

        Args:
            delta_root: Root path for Delta Lake tables.
            staging_root: Root path for worker staging files.
            pipeline_run_id: Run to resume. If None, resumes the most recent.
            name: Pipeline name override.
            working_root: Root path for worker sandboxes. If None, uses
                tempfile.gettempdir() (respects $TMPDIR).
            prefect_server: Prefect server URL. If None, auto-discovered.
            **kwargs: Additional PipelineConfig options.

        Returns:
            PipelineManager with state restored from delta.

        Raises:
            ValueError: If no pipeline run found to resume.
            PrefectServerNotFound: If no server can be discovered.
            PrefectServerUnreachable: If the server is not responding.
        """
        from artisan.orchestration.prefect_server import (
            activate_server,
            discover_server,
        )

        server_info = discover_server(prefect_server)
        activate_server(server_info)

        delta_root = Path(delta_root)
        tracker = StepTracker(delta_root)
        completed_steps = tracker.load_completed_steps(pipeline_run_id)

        if not completed_steps:
            msg = "No completed steps found"
            if pipeline_run_id:
                msg += f" for run '{pipeline_run_id}'"
            raise ValueError(msg)

        run_id = pipeline_run_id or completed_steps[0].pipeline_run_id
        config = PipelineConfig(
            name=name or _extract_name_from_run_id(run_id),
            pipeline_run_id=run_id,
            delta_root=delta_root,
            staging_root=Path(staging_root),
            **(
                {"working_root": Path(working_root)} if working_root is not None else {}
            ),
            **kwargs,
        )

        instance = cls(config)
        for step_state in completed_steps:
            result = step_state.to_step_result()
            instance._step_results.append(result)
            instance._named_steps.setdefault(result.step_name, []).append(result)
            instance._step_registry.setdefault(result.step_name, []).append(
                _StepEntry(
                    step_number=result.step_number,
                    output_roles=result.output_roles,
                    output_types=result.output_types,
                )
            )
            instance._step_spec_ids[step_state.step_number] = step_state.step_spec_id
        instance._current_step = max(s.step_number for s in completed_steps) + 1

        return instance

    @classmethod
    def list_runs(cls, delta_root: Path | str) -> pl.DataFrame:
        """List all pipeline runs in the delta root.

        Returns:
            DataFrame with pipeline_run_id, step_count, last_status,
            started_at, ended_at — one row per run.
        """
        tracker = StepTracker(Path(delta_root))
        return tracker.list_runs()

    # =========================================================================
    # Step execution: run() and submit()
    # =========================================================================

    def run(
        self,
        operation: type[OperationDefinition] | type,
        inputs: (
            dict[str, OutputReference | list[str]]
            | list[OutputReference]
            | list[str]
            | None
        ) = None,
        params: dict[str, Any] | None = None,
        backend: str | BackendBase | None = None,
        resources: dict[str, Any] | None = None,
        execution: dict[str, Any] | None = None,
        environment: str | dict[str, Any] | None = None,
        tool: dict[str, Any] | None = None,
        failure_policy: FailurePolicy | None = None,
        compact: bool = True,
        name: str | None = None,
        intermediates: str = "discard",
    ) -> StepResult:
        """Execute a pipeline step (blocking).

        Accepts both OperationDefinition and CompositeDefinition subclasses.
        Equivalent to submit(...).result().

        Args:
            operation: OperationDefinition or CompositeDefinition subclass.
            inputs: Input specification (dict, list, or None).
            params: Parameter overrides.
            backend: Backend for execution. None uses pipeline default.
            resources: Resource overrides (cpus, memory_gb, etc.).
            execution: Batching/scheduling overrides (artifacts_per_unit, etc.).
            environment: Environment override (operations only).
            tool: Tool overrides (operations only).
            failure_policy: Override pipeline-level failure policy.
            compact: Run Delta Lake compaction after commit.
            name: Custom step name. Defaults to operation.name.
            intermediates: How to handle intermediate artifacts in composites:
                "discard" (default), "persist", or "expose".

        Returns:
            StepResult with output references and execution metadata.
        """
        return self.submit(
            operation,
            inputs=inputs,
            params=params,
            backend=backend,
            resources=resources,
            execution=execution,
            environment=environment,
            tool=tool,
            failure_policy=failure_policy,
            compact=compact,
            name=name,
            intermediates=intermediates,
        ).result()

    def submit(
        self,
        operation: type[OperationDefinition] | type,
        inputs: (
            dict[str, OutputReference | list[str]]
            | list[OutputReference]
            | list[str]
            | None
        ) = None,
        params: dict[str, Any] | None = None,
        backend: str | BackendBase | None = None,
        resources: dict[str, Any] | None = None,
        execution: dict[str, Any] | None = None,
        environment: str | dict[str, Any] | None = None,
        tool: dict[str, Any] | None = None,
        failure_policy: FailurePolicy | None = None,
        compact: bool = True,
        name: str | None = None,
        intermediates: str = "discard",
    ) -> StepFuture:
        """Submit a pipeline step (non-blocking).

        Accepts both OperationDefinition and CompositeDefinition subclasses.

        Args:
            operation: OperationDefinition or CompositeDefinition subclass.
            inputs: Input specification (dict, list, or None).
            params: Parameter overrides.
            backend: Backend for execution. None uses pipeline default.
            resources: Resource overrides (cpus, memory_gb, etc.).
            execution: Batching/scheduling overrides (artifacts_per_unit, etc.).
            environment: Environment override (operations only).
            tool: Tool overrides (operations only).
            failure_policy: Override pipeline-level failure policy.
            compact: Run Delta Lake compaction after commit.
            name: Custom step name. Defaults to operation.name.
            intermediates: How to handle intermediate artifacts in composites.

        Returns:
            StepFuture with output() for wiring to downstream steps.

        Raises:
            ValueError: If any override keys are unrecognized.
        """
        from artisan.composites.base.composite_definition import CompositeDefinition

        # Detect composite and route accordingly
        if isinstance(operation, type) and issubclass(operation, CompositeDefinition):
            return self._submit_composite(
                composite_class=operation,
                inputs=inputs,
                params=params,
                backend=backend,
                resources=resources,
                execution=execution,
                intermediates=intermediates,
                failure_policy=failure_policy,
                compact=compact,
                name=name or operation.name,
            )

        # Validate overrides immediately (fail-fast, before waiting for predecessors)
        if params:
            _validate_params(operation, params)
        if resources:
            _validate_resources(resources)
        if execution:
            _validate_execution(execution)
        if environment is not None:
            _validate_environment(operation, environment)
        if tool:
            _validate_tool(operation, tool)
        _validate_input_roles(operation, inputs)
        _validate_required_inputs(operation, inputs)
        _validate_input_types(operation, inputs)

        step_name = name or operation.name

        # If an earlier step was skipped (empty inputs), skip all remaining steps
        if self._stopped:
            step_number = self._current_step
            logger.info(
                "Step %d (%s): pipeline stopped (earlier step had empty inputs)"
                " — skipping.",
                step_number,
                step_name,
            )
            output_types_map: dict[str, str | None] = {
                role: spec.artifact_type if spec.artifact_type else None
                for role, spec in operation.outputs.items()
            }
            skipped_result = StepResult(
                step_name=step_name,
                step_number=step_number,
                success=True,
                total_count=0,
                succeeded_count=0,
                failed_count=0,
                output_roles=frozenset(operation.outputs.keys()),
                output_types=output_types_map,
                metadata={
                    "skipped": True,
                    "skip_reason": "pipeline_stopped",
                },
            )
            self._step_results.append(skipped_result)
            self._register_step(step_name, step_number, operation.outputs)
            self._named_steps.setdefault(skipped_result.step_name, []).append(
                skipped_result
            )
            self._current_step += 1

            resolved_stopped: Future[StepResult] = Future()
            resolved_stopped.set_result(skipped_result)
            return StepFuture(
                step_number=step_number,
                step_name=step_name,
                output_roles=frozenset(operation.outputs.keys()),
                output_types=output_types_map,
                future=resolved_stopped,
            )

        # If pipeline has been cancelled, skip remaining steps
        if self._cancel_event.is_set():
            step_number = self._current_step
            logger.info(
                "Step %d (%s): pipeline cancelled — skipping.",
                step_number,
                step_name,
            )
            output_types_map = {
                role: spec.artifact_type if spec.artifact_type else None
                for role, spec in operation.outputs.items()
            }
            skipped_result = StepResult(
                step_name=step_name,
                step_number=step_number,
                success=True,
                total_count=0,
                succeeded_count=0,
                failed_count=0,
                output_roles=frozenset(operation.outputs.keys()),
                output_types=output_types_map,
                metadata={
                    "skipped": True,
                    "skip_reason": "cancelled",
                },
            )
            self._step_results.append(skipped_result)
            self._register_step(step_name, step_number, operation.outputs)
            self._named_steps.setdefault(skipped_result.step_name, []).append(
                skipped_result
            )
            self._current_step += 1

            resolved_cancelled: Future[StepResult] = Future()
            resolved_cancelled.set_result(skipped_result)
            return StepFuture(
                step_number=step_number,
                step_name=step_name,
                output_roles=frozenset(operation.outputs.keys()),
                output_types=output_types_map,
                future=resolved_cancelled,
            )

        self._wait_for_predecessors(inputs)

        # Re-check cancel after waiting — may have been set while blocked
        if self._cancel_event.is_set():
            step_number = self._current_step
            output_types_map = {
                role: spec.artifact_type if spec.artifact_type else None
                for role, spec in operation.outputs.items()
            }
            skipped_result = StepResult(
                step_name=step_name,
                step_number=step_number,
                success=True,
                total_count=0,
                succeeded_count=0,
                failed_count=0,
                output_roles=frozenset(operation.outputs.keys()),
                output_types=output_types_map,
                metadata={"skipped": True, "skip_reason": "cancelled"},
            )
            self._step_results.append(skipped_result)
            self._register_step(step_name, step_number, operation.outputs)
            self._named_steps.setdefault(skipped_result.step_name, []).append(
                skipped_result
            )
            self._current_step += 1
            resolved_post_wait: Future[StepResult] = Future()
            resolved_post_wait.set_result(skipped_result)
            return StepFuture(
                step_number=step_number,
                step_name=step_name,
                output_roles=frozenset(operation.outputs.keys()),
                output_types=output_types_map,
                future=resolved_post_wait,
            )

        step_number = self._current_step

        # Instantiate operation to get full params (defaults + overrides)
        # for deterministic step_spec_id computation
        temp_instance = instantiate_operation(
            operation, params, resources, execution, environment, tool
        )
        if "params" in type(temp_instance).model_fields:
            full_params = temp_instance.params.model_dump(mode="json")
        else:
            # Flat-field operations: dump all user-defined instance fields
            # (exclude base OperationDefinition fields like resources, execution)
            base_fields = set(OperationDefinition.model_fields)
            full_params = {
                k: v
                for k, v in temp_instance.model_dump(mode="json").items()
                if k not in base_fields
            }

        # Merge environment + tool overrides for hashing
        from artisan.orchestration.engine.step_executor import _merge_config_overrides

        config_overrides = _merge_config_overrides(environment, tool)

        # Compute step_spec_id for caching
        input_spec = self._build_input_spec(inputs)
        step_spec_id = compute_step_spec_id(
            operation_name=operation.name,
            step_number=step_number,
            params=full_params if full_params else None,
            input_spec=input_spec,
            config_overrides=config_overrides,
        )

        # Check step cache
        cached = self._step_tracker.check_cache(step_spec_id, self._config.cache_policy)
        if cached is not None:
            logger.info(
                "Step %d (%s) CACHED — skipping execution",
                step_number,
                step_name,
            )
            self._step_spec_ids[step_number] = step_spec_id
            self._step_results.append(cached)
            self._register_step(step_name, step_number, operation.outputs)
            self._named_steps.setdefault(cached.step_name, []).append(cached)
            self._current_step += 1

            resolved: Future[StepResult] = Future()
            resolved.set_result(cached)
            return StepFuture(
                step_number=step_number,
                step_name=cached.step_name,
                output_roles=cached.output_roles,
                output_types=cached.output_types,
                future=resolved,
            )

        # Promote raw file paths to FileRefArtifacts before dispatch
        if _is_file_path_input(inputs):
            if is_curator_operation(temp_instance):
                promoted, _count = _promote_file_paths_to_store(
                    inputs,
                    self._config,
                    step_number,
                    operation.name,
                    failure_policy or self._config.failure_policy,
                )
                if promoted is None:
                    from artisan.orchestration.engine.step_executor import (
                        build_step_result,
                    )

                    failed_result = build_step_result(
                        operation=temp_instance,
                        step_number=step_number,
                        succeeded_count=0,
                        failed_count=len(inputs),
                        failure_policy=failure_policy or self._config.failure_policy,
                        metadata={"error": "All input files are invalid"},
                    )
                    self._step_spec_ids[step_number] = step_spec_id
                    self._step_results.append(failed_result)
                    self._register_step(step_name, step_number, operation.outputs)
                    self._named_steps.setdefault(failed_result.step_name, []).append(
                        failed_result
                    )
                    self._current_step += 1
                    resolved_fail: Future[StepResult] = Future()
                    resolved_fail.set_result(failed_result)
                    return StepFuture(
                        step_number=step_number,
                        step_name=failed_result.step_name,
                        output_roles=frozenset(operation.outputs.keys()),
                        output_types={
                            r: s.artifact_type for r, s in operation.outputs.items()
                        },
                        future=resolved_fail,
                    )
                inputs = promoted
            else:
                raise ValueError(
                    "Raw file paths are not allowed for creator operations. "
                    "Use a curator ingest operation to bring files into the "
                    "pipeline first."
                )

        # Cache miss — dispatch in background
        # Install signal handlers on the first dispatched step
        if self._current_step == 0:
            self._install_signal_handlers()
        self._register_step(step_name, step_number, operation.outputs)
        self._current_step += 1
        self._step_spec_ids[step_number] = step_spec_id
        step_run_id = _generate_step_run_id(step_spec_id)

        # Resolve output metadata from operation class
        output_types_map: dict[str, str | None] = {
            role: spec.artifact_type if spec.artifact_type else None
            for role, spec in operation.outputs.items()
        }

        # Resolve backend: per-step override > pipeline default
        # Curator operations always run locally
        if is_curator_operation(temp_instance):
            resolved_backend = Backend.LOCAL
        elif backend is not None:
            resolved_backend = resolve_backend(backend)
        else:
            resolved_backend = resolve_backend(self._config.default_backend)

        # Record step start in delta
        compute_options_data = {
            "resources": resources or {},
            "execution": execution or {},
            "environment": (environment if environment is not None else {}),
            "tool": tool or {},
        }
        start_record = StepStartRecord(
            step_run_id=step_run_id,
            step_spec_id=step_spec_id,
            step_number=step_number,
            step_name=step_name,
            operation_class=_qualified_name(operation),
            params_json=json.dumps(params or {}, default=_set_default),
            input_refs_json=_serialize_input_refs(inputs),
            compute_backend=resolved_backend.name,
            compute_options_json=json.dumps(compute_options_data, default=_set_default),
            output_roles_json=json.dumps(sorted(operation.outputs.keys())),
            output_types_json=json.dumps(output_types_map),
        )
        self._step_tracker.record_step_start(start_record)

        # Capture all parameters for the closure
        _failure_policy = failure_policy or self._config.failure_policy

        def _run() -> StepResult:
            # Bail out immediately if cancelled while queued in the executor
            if self._cancel_event.is_set():
                cancelled_result = StepResult(
                    step_name=step_name,
                    step_number=step_number,
                    success=True,
                    total_count=0,
                    succeeded_count=0,
                    failed_count=0,
                    output_roles=frozenset(output_types_map.keys()),
                    output_types=output_types_map,
                    metadata={"cancelled": True},
                )
                self._step_results.append(cancelled_result)
                self._named_steps.setdefault(cancelled_result.step_name, []).append(
                    cancelled_result
                )
                return cancelled_result

            logger.info(
                "Step %d (%s) starting... [backend=%s]",
                step_number,
                step_name,
                resolved_backend.name,
            )
            start = time.perf_counter()
            try:
                result = execute_step(
                    operation_class=operation,
                    inputs=inputs,
                    params=params,
                    backend=resolved_backend,
                    resources=resources,
                    execution=execution,
                    environment=environment,
                    tool=tool,
                    step_number=step_number,
                    config=self._config,
                    failure_policy=_failure_policy,
                    compact=compact,
                    step_spec_id=step_spec_id,
                    cancel_event=self._cancel_event,
                )
                elapsed = time.perf_counter() - start
                result = result.model_copy(
                    update={"step_name": step_name, "duration_seconds": elapsed}
                )

                if result.metadata.get("cancelled"):
                    self._step_tracker.record_step_cancelled(start_record)
                    logger.info(
                        "Step %d (%s): cancelled.",
                        step_number,
                        step_name,
                    )
                    self._step_results.append(result)
                    self._named_steps.setdefault(result.step_name, []).append(result)
                    return result

                if result.metadata.get("skipped"):
                    self._step_tracker.record_step_skipped(start_record, result)
                    self._stopped = True
                    logger.info(
                        "Step %d (%s): all input roles are empty"
                        " — skipping. Pipeline stopped.",
                        step_number,
                        step_name,
                    )
                else:
                    self._step_tracker.record_step_completed(start_record, result)
                    logger.info(
                        "Step %d (%s) completed in %.1fs [%d/%d succeeded]",
                        step_number,
                        step_name,
                        elapsed,
                        result.succeeded_count,
                        result.total_count,
                    )
                self._step_results.append(result)
                self._named_steps.setdefault(result.step_name, []).append(result)
                return result

            except Exception as e:
                elapsed = time.perf_counter() - start
                error_msg = f"{type(e).__name__}: {e}"
                self._step_tracker.record_step_failed(start_record, error_msg)

                logger.error(
                    "Step %d (%s) failed after %.1fs: %s",
                    step_number,
                    step_name,
                    elapsed,
                    error_msg,
                )
                failed_result = StepResult(
                    step_name=step_name,
                    step_number=step_number,
                    success=False,
                    total_count=0,
                    succeeded_count=0,
                    failed_count=0,
                    duration_seconds=elapsed,
                    metadata={"error": error_msg},
                )
                self._step_results.append(failed_result)
                self._named_steps.setdefault(failed_result.step_name, []).append(
                    failed_result
                )
                return failed_result

        # Submit to executor with context propagation
        ctx = contextvars.copy_context()
        cf_future = self._executor.submit(ctx.run, _run)

        future = StepFuture(
            step_number=step_number,
            step_name=step_name,
            output_roles=frozenset(output_types_map.keys()),
            output_types=output_types_map,
            future=cf_future,
        )
        self._active_futures[step_number] = future
        return future

    def expand(
        self,
        composite: type,
        inputs: (dict[str, OutputReference | list[str]] | None) = None,
        params: dict[str, Any] | None = None,
        resources: dict[str, Any] | None = None,
        execution: dict[str, Any] | None = None,
        backend: str | BackendBase | None = None,
        environment: str | dict[str, Any] | None = None,
        tool: dict[str, Any] | None = None,
        name: str | None = None,
    ) -> Any:
        """Expand a composite into individual pipeline steps.

        Each internal operation becomes its own pipeline step with
        independent worker dispatch, batching, and caching.

        Args:
            composite: CompositeDefinition subclass to expand.
            inputs: Input specification for the composite.
            params: Parameter overrides for the composite.
            resources: Per-operation overrides forwarded from compose().
            execution: Per-operation overrides forwarded from compose().
            backend: Backend override.
            environment: Environment override.
            tool: Tool overrides.
            name: Step name prefix. Defaults to composite.name.

        Returns:
            ExpandedCompositeResult with .output(role) for downstream wiring.
        """
        from artisan.composites.base.composite_context import ExpandedCompositeContext
        from artisan.composites.base.composite_definition import CompositeDefinition
        from artisan.schemas.composites.composite_ref import ExpandedCompositeResult

        if not (
            isinstance(composite, type) and issubclass(composite, CompositeDefinition)
        ):
            raise TypeError(
                f"expand() requires a CompositeDefinition subclass, got {composite}"
            )

        # Validate inputs
        _validate_input_roles(composite, inputs)
        _validate_required_inputs(composite, inputs)
        _validate_input_types(composite, inputs)

        if params:
            _validate_params(composite, params)

        # Wait for predecessors
        self._wait_for_predecessors(inputs)

        # Instantiate the composite
        init_kwargs: dict[str, Any] = {}
        if params:
            init_kwargs["params"] = params
        instance = composite(**init_kwargs)

        # Build input OutputReferences
        input_refs: dict[str, OutputReference] = {}
        if isinstance(inputs, dict):
            for role, ref in inputs.items():
                if isinstance(ref, OutputReference):
                    input_refs[role] = ref

        step_name_prefix = name or composite.name

        # Create expanded context
        ctx = ExpandedCompositeContext(
            pipeline=self,
            input_refs=input_refs,
            composite=instance,
            step_name_prefix=step_name_prefix,
        )

        # Execute compose() — each ctx.run() creates real pipeline steps
        instance.compose(ctx)

        # Build result from output mappings
        return ExpandedCompositeResult(
            output_map=ctx.get_output_map(),
            output_types=ctx.get_output_types(),
        )

    def _submit_composite(
        self,
        composite_class: type,
        inputs: Any,
        params: dict[str, Any] | None,
        backend: Any | None,
        resources: dict[str, Any] | None,
        execution: dict[str, Any] | None,
        intermediates: str,
        failure_policy: Any | None,
        compact: bool,
        name: str,
    ) -> StepFuture:
        """Internal: submit a composite step for collapsed execution.

        Args:
            composite_class: CompositeDefinition subclass.
            inputs: Initial inputs.
            backend: Backend override.
            resources: Composite-level resource overrides.
            execution: Composite-level execution overrides.
            intermediates: "discard", "persist", or "expose".
            params: Parameter overrides.
            failure_policy: Override pipeline failure policy.
            compact: Run Delta Lake compaction.
            name: Step name.

        Returns:
            StepFuture for downstream wiring.
        """
        from artisan.execution.models.execution_composite import CompositeIntermediates
        from artisan.orchestration.engine.step_executor import execute_composite_step
        from artisan.schemas.execution.execution_config import ExecutionConfig
        from artisan.schemas.operation_config.resource_config import ResourceConfig

        # Validate inputs against composite declarations
        _validate_input_roles(composite_class, inputs)
        _validate_required_inputs(composite_class, inputs)
        _validate_input_types(composite_class, inputs)

        if params:
            _validate_params(composite_class, params)
        if resources:
            _validate_resources(resources)
        if execution:
            _validate_execution(execution)

        if self._stopped:
            step_number = self._current_step
            output_types_map: dict[str, str | None] = {
                role: spec.artifact_type if spec.artifact_type else None
                for role, spec in composite_class.outputs.items()
            }
            skipped_result = StepResult(
                step_name=name,
                step_number=step_number,
                success=True,
                total_count=0,
                succeeded_count=0,
                failed_count=0,
                output_roles=frozenset(composite_class.outputs.keys()),
                output_types=output_types_map,
                metadata={"skipped": True, "skip_reason": "pipeline_stopped"},
            )
            self._step_results.append(skipped_result)
            self._register_step(name, step_number, composite_class.outputs)
            self._named_steps.setdefault(name, []).append(skipped_result)
            self._current_step += 1

            resolved_stopped: Future[StepResult] = Future()
            resolved_stopped.set_result(skipped_result)
            return StepFuture(
                step_number=step_number,
                step_name=name,
                output_roles=frozenset(composite_class.outputs.keys()),
                output_types=output_types_map,
                future=resolved_stopped,
            )

        # If pipeline has been cancelled, skip remaining steps
        if self._cancel_event.is_set():
            step_number = self._current_step
            output_types_map: dict[str, str | None] = {
                role: spec.artifact_type if spec.artifact_type else None
                for role, spec in composite_class.outputs.items()
            }
            skipped_result = StepResult(
                step_name=name,
                step_number=step_number,
                success=True,
                total_count=0,
                succeeded_count=0,
                failed_count=0,
                output_roles=frozenset(composite_class.outputs.keys()),
                output_types=output_types_map,
                metadata={"skipped": True, "skip_reason": "cancelled"},
            )
            self._step_results.append(skipped_result)
            self._register_step(name, step_number, composite_class.outputs)
            self._named_steps.setdefault(name, []).append(skipped_result)
            self._current_step += 1
            resolved_cancelled: Future[StepResult] = Future()
            resolved_cancelled.set_result(skipped_result)
            return StepFuture(
                step_number=step_number,
                step_name=name,
                output_roles=frozenset(composite_class.outputs.keys()),
                output_types=output_types_map,
                future=resolved_cancelled,
            )

        self._wait_for_predecessors(inputs)

        # Re-check cancel after waiting — may have been set while blocked
        if self._cancel_event.is_set():
            step_number = self._current_step
            output_types_map = {
                role: spec.artifact_type if spec.artifact_type else None
                for role, spec in composite_class.outputs.items()
            }
            skipped_result = StepResult(
                step_name=name,
                step_number=step_number,
                success=True,
                total_count=0,
                succeeded_count=0,
                failed_count=0,
                output_roles=frozenset(composite_class.outputs.keys()),
                output_types=output_types_map,
                metadata={"skipped": True, "skip_reason": "cancelled"},
            )
            self._step_results.append(skipped_result)
            self._register_step(name, step_number, composite_class.outputs)
            self._named_steps.setdefault(name, []).append(skipped_result)
            self._current_step += 1
            resolved_post_wait: Future[StepResult] = Future()
            resolved_post_wait.set_result(skipped_result)
            return StepFuture(
                step_number=step_number,
                step_name=name,
                output_roles=frozenset(composite_class.outputs.keys()),
                output_types=output_types_map,
                future=resolved_post_wait,
            )

        step_number = self._current_step

        # Compute composite step_spec_id
        temp = instantiate_operation(composite_class, params)
        if hasattr(temp, "params"):
            full_params = temp.params.model_dump(mode="json")
        else:
            full_params = {}

        input_spec = self._build_input_spec(inputs)
        from artisan.utils.hashing import compute_composite_spec_id

        step_spec_id = compute_composite_spec_id(
            composite_name=composite_class.name,
            params=full_params or None,
            input_spec=input_spec,
        )

        # Check step cache
        cached = self._step_tracker.check_cache(step_spec_id, self._config.cache_policy)
        if cached is not None:
            logger.info("Step %d (%s) CACHED — skipping execution", step_number, name)
            self._step_spec_ids[step_number] = step_spec_id
            self._step_results.append(cached)
            self._register_step(name, step_number, composite_class.outputs)
            self._named_steps.setdefault(cached.step_name, []).append(cached)
            self._current_step += 1

            resolved: Future[StepResult] = Future()
            resolved.set_result(cached)
            return StepFuture(
                step_number=step_number,
                step_name=cached.step_name,
                output_roles=cached.output_roles,
                output_types=cached.output_types,
                future=resolved,
            )

        # Cache miss — build and dispatch
        self._register_step(name, step_number, composite_class.outputs)
        self._current_step += 1
        self._step_spec_ids[step_number] = step_spec_id

        output_types_map = {
            role: spec.artifact_type if spec.artifact_type else None
            for role, spec in composite_class.outputs.items()
        }

        # Resolve backend
        if backend is not None:
            resolved_backend = resolve_backend(backend)
        else:
            resolved_backend = resolve_backend(self._config.default_backend)

        _failure_policy = failure_policy or self._config.failure_policy
        composite_intermediates = CompositeIntermediates(intermediates)
        composite_resources = ResourceConfig(**(resources or {}))
        composite_execution = ExecutionConfig(**(execution or {}))

        def _run() -> StepResult:
            # Bail out immediately if cancelled while queued in the executor
            if self._cancel_event.is_set():
                cancelled_result = StepResult(
                    step_name=name,
                    step_number=step_number,
                    success=True,
                    total_count=0,
                    succeeded_count=0,
                    failed_count=0,
                    output_roles=frozenset(output_types_map.keys()),
                    output_types=output_types_map,
                    metadata={"cancelled": True},
                )
                self._step_results.append(cancelled_result)
                self._named_steps.setdefault(name, []).append(cancelled_result)
                return cancelled_result

            logger.info(
                "Step %d (%s) starting composite... [backend=%s]",
                step_number,
                name,
                resolved_backend.name,
            )
            start = time.perf_counter()
            try:
                result = execute_composite_step(
                    composite_class=composite_class,
                    inputs=inputs,
                    params=params,
                    backend=resolved_backend,
                    composite_resources=composite_resources,
                    composite_execution=composite_execution,
                    intermediates=composite_intermediates,
                    step_number=step_number,
                    config=self._config,
                    failure_policy=_failure_policy,
                    compact=compact,
                )
                elapsed = time.perf_counter() - start
                result = result.model_copy(
                    update={"step_name": name, "duration_seconds": elapsed},
                )
                self._step_tracker.record_step_completed(
                    StepStartRecord(
                        step_run_id=_generate_step_run_id(step_spec_id),
                        step_spec_id=step_spec_id,
                        step_number=step_number,
                        step_name=name,
                        operation_class=f"{composite_class.__module__}.{composite_class.__qualname__}",
                        params_json=json.dumps(full_params or {}),
                        input_refs_json=_serialize_input_refs(inputs),
                        compute_backend=resolved_backend.name,
                        compute_options_json="{}",
                        output_roles_json=json.dumps(
                            sorted(composite_class.outputs.keys())
                        ),
                        output_types_json=json.dumps(output_types_map),
                    ),
                    result,
                )
                logger.info(
                    "Step %d (%s) completed in %.1fs [%d/%d succeeded]",
                    step_number,
                    name,
                    elapsed,
                    result.succeeded_count,
                    result.total_count,
                )
                self._step_results.append(result)
                self._named_steps.setdefault(result.step_name, []).append(result)
                return result

            except Exception as e:
                elapsed = time.perf_counter() - start
                error_msg = f"{type(e).__name__}: {e}"
                logger.error(
                    "Step %d (%s) failed after %.1fs: %s",
                    step_number,
                    name,
                    elapsed,
                    error_msg,
                )
                failed_result = StepResult(
                    step_name=name,
                    step_number=step_number,
                    success=False,
                    total_count=0,
                    succeeded_count=0,
                    failed_count=0,
                    duration_seconds=elapsed,
                    metadata={"error": error_msg},
                )
                self._step_results.append(failed_result)
                self._named_steps.setdefault(name, []).append(failed_result)
                return failed_result

        ctx = contextvars.copy_context()
        cf_future = self._executor.submit(ctx.run, _run)

        future = StepFuture(
            step_number=step_number,
            step_name=name,
            output_roles=frozenset(output_types_map.keys()),
            output_types=output_types_map,
            future=cf_future,
        )
        self._active_futures[step_number] = future
        return future

    # =========================================================================
    # Internal helpers
    # =========================================================================

    def _build_input_spec(self, inputs: Any) -> dict[str, tuple[str, str]]:
        """Convert inputs to (upstream_spec_id, role) tuples for hashing."""
        if inputs is None:
            return {}
        if isinstance(inputs, dict):
            spec: dict[str, tuple[str, str]] = {}
            for role, value in inputs.items():
                if isinstance(value, OutputReference):
                    upstream_spec_id = self._step_spec_ids[value.source_step]
                    spec[role] = (upstream_spec_id, value.role)
                elif isinstance(value, list):
                    ids_hash = xxhash.xxh3_128(
                        ",".join(sorted(value)).encode()
                    ).hexdigest()
                    spec[role] = (ids_hash, "")
            return spec
        if isinstance(inputs, list):
            if inputs and isinstance(inputs[0], OutputReference):
                parts = []
                for ref in inputs:
                    upstream_spec_id = self._step_spec_ids[ref.source_step]
                    parts.append(f"{upstream_spec_id}:{ref.role}")
                composite_hash = xxhash.xxh3_128(",".join(parts).encode()).hexdigest()
                return {"_merged_streams": (composite_hash, "")}
            paths_hash = xxhash.xxh3_128(
                ",".join(sorted(str(p) for p in inputs)).encode()
            ).hexdigest()
            return {"_file_paths": (paths_hash, "")}
        return {}

    def _wait_for_predecessors(self, inputs: Any) -> None:
        """Block until all upstream step futures have completed.

        Polls with a timeout so that cancellation is detected promptly
        instead of blocking indefinitely on ``future.result()``.
        """
        source_steps = _extract_source_steps(inputs)
        for step_num in source_steps:
            if step_num in self._active_futures:
                future = self._active_futures[step_num]
                while not self._cancel_event.is_set():
                    try:
                        future.result(timeout=0.5)
                        break
                    except TimeoutError:
                        continue
                    except Exception:
                        logger.warning(
                            "Predecessor step %d failed"
                            " — downstream will see empty inputs.",
                            step_num,
                        )
                        break

    # =========================================================================
    # Finalize
    # =========================================================================

    def finalize(self) -> dict[str, Any]:
        """Finalize pipeline execution and return summary.

        Waits for any active futures and shuts down the executor.
        When cancellation has been requested, uses a short timeout
        on futures to avoid blocking indefinitely.

        Returns:
            Summary dict with step results and statistics.

        Example:
            pipeline = PipelineManager.create(...)
            step0 = pipeline.run(IngestData, inputs=files)
            step1 = pipeline.run(ScoreOp, inputs={"data": step0.output("data")})
            result = pipeline.finalize()
        """
        for step_num, future in self._active_futures.items():
            try:
                while not self._cancel_event.is_set():
                    try:
                        future.result(timeout=0.5)
                        break
                    except TimeoutError:
                        continue
                else:
                    # Cancel detected — short wait for cleanup
                    try:
                        future.result(timeout=5.0)
                    except (TimeoutError, Exception):
                        pass
            except Exception as exc:
                logger.error(
                    "Step %d future failed during finalize: %s: %s",
                    step_num,
                    type(exc).__name__,
                    exc,
                )

        cancelled = self._cancel_event.is_set()
        self._executor.shutdown(wait=not cancelled, cancel_futures=cancelled)
        self._restore_signal_handlers()

        # Results may arrive out of order (sync skips before async completions)
        self._step_results.sort(key=lambda r: r.step_number)

        total_elapsed = time.time() - self._start_time
        all_ok = all(r.success for r in self._step_results)
        status = "all succeeded" if all_ok else "some steps failed"
        logger.info(
            "Pipeline '%s' complete: %d steps, %s",
            self._config.name,
            len(self._step_results),
            status,
        )
        for r in self._step_results:
            duration = f"{r.duration_seconds:.1f}s" if r.duration_seconds else "n/a"
            logger.info(
                "  Step %d: %-16s %s  [%d/%d]",
                r.step_number,
                r.step_name,
                duration,
                r.succeeded_count,
                r.total_count,
            )
        logger.info("  Total: %.1fs", total_elapsed)

        return {
            "pipeline_name": self._config.name,
            "total_steps": len(self._step_results),
            "steps": [
                {
                    "step_number": r.step_number,
                    "name": r.step_name,
                    "success": r.success,
                    "total": r.total_count,
                    "succeeded": r.succeeded_count,
                    "failed": r.failed_count,
                    "duration_seconds": r.duration_seconds,
                }
                for r in self._step_results
            ],
            "overall_success": all(r.success for r in self._step_results),
        }
