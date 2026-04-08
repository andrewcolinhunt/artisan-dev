"""Main interface for defining and executing artisan pipelines.

Key exports: ``PipelineManager`` (create, run, submit, finalize).
"""

from __future__ import annotations

import atexit
import contextlib
import contextvars
import json
import logging
import os
import signal
import threading
import time
import weakref
from collections.abc import Iterator
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, cast, overload
from uuid import uuid4

import polars as pl

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
from artisan.schemas.specs.output_spec import OutputSpec
from artisan.utils.hashing import compute_artifact_id, compute_step_spec_id, digest_utf8
from artisan.utils.json import artisan_json_default as _set_default
from artisan.utils.path import uri_join, uri_parent

if TYPE_CHECKING:
    from artisan.composites.base.composite_definition import CompositeDefinition

# Validation helpers accept both OperationDefinition and CompositeDefinition,
# which share ClassVars (name, inputs, outputs) but have no common base.
_OpLike = type[OperationDefinition] | type["CompositeDefinition"]

logger = logging.getLogger(__name__)


# =============================================================================
# Module-level helper functions
# =============================================================================


def _generate_run_id(name: str) -> str:
    """Generate a human-readable pipeline run ID."""
    return f"{name}_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}_{uuid4().hex[:8]}"


def _generate_step_run_id(step_spec_id: str) -> str:
    """Generate a unique 32-char hex step run identifier."""
    from artisan.utils.hashing import digest_utf8

    return digest_utf8(f"{step_spec_id}:{datetime.now(UTC).isoformat()}")


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
) -> tuple[dict[str, list[str]] | None, int]:
    """Validate file paths, create FileRefArtifacts, and commit to delta.

    Args:
        file_paths: Raw file path strings from the user.
        config: Pipeline configuration.
        step_number: Pipeline step number.
        operation_name: Operation name (for logging).

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
        if not os.path.exists(path_str):
            invalid_paths.append(f"Not found: {path_str}")
        elif not os.path.isfile(path_str):
            invalid_paths.append(f"Not a file: {path_str}")
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
        with open(path_str, "rb") as f:
            content = f.read()
        content_hash = compute_artifact_id(content)
        size_bytes = len(content)
        basename = os.path.basename(path_str)
        name_part, ext_part = os.path.splitext(basename)
        artifact = cast(
            FileRefArtifact,
            FileRefArtifact.draft(
                path=os.path.abspath(path_str),
                content_hash=content_hash,
                size_bytes=size_bytes,
                step_number=step_number,
                original_name=strip_extensions(basename),
                extension=ext_part,
            ).finalize(),
        )
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
    from artisan.storage.io.staging import StagingManager

    fs = config.storage.filesystem()
    storage_options = config.storage.delta_storage_options()
    staging_manager = StagingManager(config.staging_root, fs)
    committer = DeltaCommitter(
        config.delta_root,
        staging_manager,
        fs=fs,
        storage_options=storage_options,
    )
    committer.commit_dataframe(file_ref_df, "artifacts/file_refs")
    committer.commit_dataframe(index_df, TablePath.ARTIFACT_INDEX)

    artifact_ids: list[str] = [
        a.artifact_id for a in file_ref_artifacts if a.artifact_id is not None
    ]
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
    operation: _OpLike,
    params: dict[str, Any],
) -> None:
    """Raise ValueError if any param keys are unrecognized by the operation."""
    if "params" in operation.model_fields:
        params_cls = operation.model_fields["params"].annotation
        valid_keys = set(params_cls.model_fields) if params_cls is not None else set()
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
        from pydantic import BaseModel

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
        env_field_types: dict[str, type[BaseModel]] = {
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
        msg = f"Unknown tool keys: {sorted(unknown)}. Valid keys: {sorted(valid_keys)}"
        raise ValueError(msg)


def _validate_input_roles(
    operation: _OpLike,
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
    operation: _OpLike,
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
    operation: _OpLike,
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


def _atexit_shutdown_executor(ref: weakref.ref) -> None:
    """Last-resort cleanup: shut down a leaked ThreadPoolExecutor at exit."""
    executor = ref()
    if executor is not None:
        executor.shutdown(wait=False)


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
            delta_root="/data/delta",
            staging_root="/data/staging",
        )

        step0 = pipeline.run(IngestData, inputs=files)
        step1 = pipeline.run(ScoreOp, inputs={"data": step0.output("data")})

        result = pipeline.finalize()

    Or as a context manager (finalize called automatically)::

        with PipelineManager.create(...) as pipeline:
            pipeline.run(IngestData, inputs=files)
        # finalize() called on exit
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

            _configure(logs_root=uri_join(uri_parent(config.delta_root), "logs"))

        self._config = config

        if config.recover_staging:
            from artisan.storage.io.commit import DeltaCommitter
            from artisan.storage.io.staging import StagingManager

            fs = config.storage.filesystem()
            storage_options = config.storage.delta_storage_options()
            staging_manager = StagingManager(config.staging_root, fs)
            committer = DeltaCommitter(
                config.delta_root,
                staging_manager,
                fs=fs,
                storage_options=storage_options,
            )
            committer.recover_staged(preserve_staging=config.preserve_staging)

        self._start_time: float = time.time()
        self._current_step: int = 0
        self._step_results: list[StepResult] = []
        self._named_steps: dict[str, list[StepResult]] = {}
        self._step_registry: dict[str, list[_StepEntry]] = {}
        self._step_spec_ids: dict[int, str] = {}
        self._step_run_ids: dict[int, str] = {}
        self._step_tracker = StepTracker(
            config.delta_root,
            config.pipeline_run_id,
            storage_options=config.storage.delta_storage_options(),
            fs=config.storage.filesystem(),
        )
        self._stopped: bool = False
        self._cancel_event = threading.Event()
        self._prev_sigint: Any = None
        self._prev_sigterm: Any = None
        self._active_futures: dict[int, StepFuture] = {}
        self._executor: ThreadPoolExecutor | None = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="pipeline-step"
        )
        self._finalized: bool = False
        self._summary: dict[str, Any] | None = None
        atexit.register(_atexit_shutdown_executor, weakref.ref(self._executor))

    # -- Resource cleanup ------------------------------------------------------

    def __del__(self) -> None:
        """Release executor threads if finalize() was never called."""
        if not self._finalized:
            self._shutdown_executor(wait=False)

    def __enter__(self) -> PipelineManager:
        """Support ``with PipelineManager.create(...) as pipeline:``."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None:
        """Finalize on context exit if not already done."""
        if not self._finalized:
            self.finalize()

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
        operation_outputs: dict[str, OutputSpec],
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

    @staticmethod
    def _build_output_types(
        operation_outputs: dict[str, OutputSpec],
    ) -> dict[str, str | None]:
        """Extract output role to artifact type mapping from operation outputs."""
        return {
            role: spec.artifact_type if spec.artifact_type else None
            for role, spec in operation_outputs.items()
        }

    def _skip_step(
        self,
        step_name: str,
        operation_outputs: dict[str, OutputSpec],
        skip_reason: str,
    ) -> StepFuture:
        """Record a skipped step and return a resolved StepFuture.

        Handles all bookkeeping: result creation, step registration,
        step counter increment, and future resolution.

        Args:
            step_name: Human-readable step name.
            operation_outputs: The operation's outputs dict (role -> OutputSpec).
            skip_reason: Why this step was skipped
                (e.g. "pipeline_stopped", "cancelled").

        Returns:
            A resolved StepFuture with the skipped result.
        """
        step_number = self._current_step
        output_types = self._build_output_types(operation_outputs)
        output_roles = frozenset(operation_outputs.keys())

        result = StepResult(
            step_name=step_name,
            step_number=step_number,
            success=True,
            total_count=0,
            succeeded_count=0,
            failed_count=0,
            output_roles=output_roles,
            output_types=output_types,
            metadata={"skipped": True, "skip_reason": skip_reason},
        )
        self._step_results.append(result)
        self._register_step(step_name, step_number, operation_outputs)
        self._named_steps.setdefault(step_name, []).append(result)
        self._current_step += 1

        resolved: Future[StepResult] = Future()
        resolved.set_result(result)
        return StepFuture(
            step_number=step_number,
            step_name=step_name,
            output_roles=output_roles,
            output_types=output_types,
            future=resolved,
        )

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

    def _handle_signal(self, signum: int, _frame: Any) -> None:
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
            artifact_type=entry.output_types.get(role) or ArtifactTypes.ANY,
        )

    # =========================================================================
    # Factory / classmethods
    # =========================================================================

    @classmethod
    def create(
        cls,
        name: str,
        delta_root: str,
        staging_root: str,
        working_root: str | None = None,
        files_root: str | None = None,
        failure_policy: FailurePolicy = FailurePolicy.CONTINUE,
        cache_policy: CachePolicy = CachePolicy.ALL_SUCCEEDED,
        backend: str | BackendBase = "local",
        preserve_staging: bool = False,
        preserve_working: bool = False,
        recover_staging: bool = True,
        skip_cache: bool = False,
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
            files_root: Root path for Artisan-managed external files. If None,
                defaults to a sibling "files" directory next to delta_root.
            failure_policy: Default failure handling for steps.
            cache_policy: Controls when completed steps qualify as cache hits.
            backend: Default backend for step execution. Accepts a BackendBase
                instance or string name (e.g. "local", "slurm").
            preserve_staging: Debug flag to preserve staging files after commit.
            preserve_working: Debug flag to preserve sandbox after execution.
            recover_staging: Commit leftover staging files from prior crashed
                runs at pipeline init. Defaults to True.
            skip_cache: Bypass all cache lookups for every step.
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
            delta_root=delta_root,
            staging_root=staging_root,
            **({"working_root": working_root} if working_root is not None else {}),
            **({"files_root": files_root} if files_root is not None else {}),
            failure_policy=failure_policy,
            cache_policy=cache_policy,
            default_backend=resolved.name,
            preserve_staging=preserve_staging,
            preserve_working=preserve_working,
            recover_staging=recover_staging,
            skip_cache=skip_cache,
        )
        instance = cls(config)
        logger.info("Pipeline '%s' initialized (run_id=%s)", name, pipeline_run_id)
        logger.info("  delta_root: %s", config.delta_root)
        logger.info("  staging_root: %s", config.staging_root)
        return instance

    @classmethod
    def resume(
        cls,
        delta_root: str,
        staging_root: str,
        pipeline_run_id: str | None = None,
        name: str | None = None,
        working_root: str | None = None,
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

        from artisan.schemas.execution.storage_config import StorageConfig

        storage = kwargs.get("storage") or StorageConfig()
        tracker = StepTracker(
            delta_root,
            storage_options=storage.delta_storage_options(),
            fs=storage.filesystem(),
        )
        completed_steps = tracker.load_completed_steps(pipeline_run_id)

        if not completed_steps:
            msg = "No completed steps found"
            if pipeline_run_id:
                msg += f" for run '{pipeline_run_id}'"
            raise ValueError(msg)

        run_id = pipeline_run_id or completed_steps[0].pipeline_run_id
        config_kwargs: dict[str, Any] = dict(
            name=name or _extract_name_from_run_id(run_id),
            pipeline_run_id=run_id,
            delta_root=delta_root,
            staging_root=staging_root,
            **kwargs,
        )
        if working_root is not None:
            config_kwargs["working_root"] = working_root
        config = PipelineConfig(**config_kwargs)

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
            if step_state.step_run_id:
                instance._step_run_ids[step_state.step_number] = step_state.step_run_id
        instance._current_step = max(s.step_number for s in completed_steps) + 1

        return instance

    @classmethod
    def list_runs(
        cls,
        delta_root: str,
        storage_options: dict[str, str] | None = None,
    ) -> pl.DataFrame:
        """List all pipeline runs in the delta root.

        Args:
            delta_root: Root path for Delta Lake tables.
            storage_options: Delta-rs storage options for cloud backends.

        Returns:
            DataFrame with pipeline_run_id, step_count, last_status,
            started_at, ended_at — one row per run.
        """
        from artisan.schemas.execution.storage_config import StorageConfig

        storage = StorageConfig()
        tracker = StepTracker(
            delta_root,
            storage_options=storage_options,
            fs=storage.filesystem(),
        )
        return tracker.list_runs()

    # =========================================================================
    # Step execution: run() and submit()
    # =========================================================================

    def run(
        self,
        operation: type[OperationDefinition] | type[CompositeDefinition],
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
        skip_cache: bool = False,
        post_step: type[OperationDefinition] | None = None,
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
            skip_cache: Bypass cache lookups for this step.
            post_step: Operation to auto-insert after the main step. When
                provided, the main step's outputs are wired as inputs to the
                post_step, and the returned StepResult is from the post_step.

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
            skip_cache=skip_cache,
            post_step=post_step,
        ).result()

    def submit(
        self,
        operation: type[OperationDefinition] | type[CompositeDefinition],
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
        skip_cache: bool = False,
        post_step: type[OperationDefinition] | None = None,
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
            skip_cache: Bypass cache lookups for this step.
            post_step: Operation to auto-insert after the main step. When
                provided, the main step's outputs are wired as inputs to the
                post_step, and the returned StepFuture is from the post_step.
                Behavioral flags (backend, compact, skip_cache, failure_policy)
                are forwarded; overrides (params, resources, etc.) are not.

        Returns:
            StepFuture with output() for wiring to downstream steps.

        Raises:
            ValueError: If any override keys are unrecognized.
        """
        from artisan.composites.base.composite_definition import CompositeDefinition

        # 1. Route composites to a dedicated handler — composites expand into
        #    multiple internal operations and have their own caching/dispatch.
        #    post_step is not applied to composites (deferred; composites
        #    handle internal steps in compose()).
        if isinstance(operation, type) and issubclass(operation, CompositeDefinition):  # type: ignore[redundant-expr]
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
                skip_cache=skip_cache,
            )

        # 2. Fail-fast validation before any blocking work. Checks params,
        #    resources, execution, environment, and tool keys against the
        #    operation's declared fields, plus input role/type compatibility.
        self._validate_operation_overrides(
            operation,
            inputs,
            params,
            resources,
            execution,
            environment,
            tool,
        )

        step_name = name or operation.name

        # 3. Early exit: skip if pipeline is stopped (earlier step had empty
        #    inputs) or cancelled. Also blocks until predecessor steps finish,
        #    then re-checks cancellation.
        early = self._check_early_exit(step_name, operation.outputs, inputs)
        if early is not None:
            return early

        step_number = self._current_step

        # 4. Instantiate the operation with merged defaults + overrides to
        #    compute a deterministic step_spec_id (content hash of operation
        #    name, params, input provenance, and config overrides). This ID
        #    drives the step-level cache.
        step_spec_id, temp_instance = self._prepare_step_spec(
            operation,
            params,
            resources,
            execution,
            environment,
            tool,
            step_number,
            inputs,
        )

        # 5. Cache check: if a prior run produced identical spec_id, return
        #    the cached StepResult immediately without re-executing.
        #    Assigns main_future so post_step can chain from it.
        main_future: StepFuture | None = None
        if not (skip_cache or self._config.skip_cache):
            main_future = self._try_cached_step(
                step_spec_id,
                step_number,
                step_name,
                operation.outputs,
            )

        # 6. File path promotion: if the user passed raw file paths (list of
        #    strings), validate them and commit FileRefArtifacts to Delta Lake
        #    so downstream execution sees artifact IDs, not filesystem paths.
        #    Only curator operations accept raw paths; creators must receive
        #    artifact references from a prior ingest step.
        if main_future is None and _is_file_path_input(inputs):
            file_result = self._handle_file_path_inputs(
                cast(list[str], inputs),
                temp_instance,
                operation,
                step_number,
                step_spec_id,
                step_name,
                failure_policy,
            )
            if isinstance(file_result, StepFuture):
                return file_result
            inputs = file_result  # type: ignore[assignment]

        # 7. Dispatch: register the step, resolve the backend (local vs SLURM),
        #    record the step start in Delta, and submit the _run() closure to
        #    the thread pool executor for background execution.
        if main_future is None:
            main_future = self._dispatch_step(
                operation=operation,
                inputs=inputs,
                params=params,
                backend=backend,
                resources=resources,
                execution=execution,
                environment=environment,
                tool=tool,
                failure_policy=failure_policy,
                compact=compact,
                step_name=step_name,
                step_number=step_number,
                step_spec_id=step_spec_id,
                temp_instance=temp_instance,
                skip_cache=skip_cache,
            )

        # 8. Post-step: if provided, wire main step outputs as post_step
        #    inputs and recursively submit. Behavioral flags are forwarded;
        #    overrides (params, resources, etc.) are not.
        if post_step is not None:
            post_inputs: dict[str, OutputReference] = {
                role: main_future.output(role)
                for role in operation.outputs
                if role in post_step.inputs
            }
            return self.submit(
                post_step,
                inputs=post_inputs,
                backend=backend,
                compact=compact,
                skip_cache=skip_cache,
                failure_policy=failure_policy,
                name=f"{step_name}.post",
            )

        return main_future

    # =========================================================================
    # submit() helpers
    # =========================================================================

    @staticmethod
    def _validate_operation_overrides(
        operation: type[OperationDefinition],
        inputs: Any,
        params: dict[str, Any] | None,
        resources: dict[str, Any] | None,
        execution: dict[str, Any] | None,
        environment: str | dict[str, Any] | None,
        tool: dict[str, Any] | None,
    ) -> None:
        """Validate all overrides against the operation (fail-fast).

        Checks each override dict against the operation's declared fields
        and raises ValueError with the unrecognized keys before any
        blocking work (predecessor waits, execution) begins. Input
        validation covers role existence, required roles, and upstream
        type compatibility.
        """
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

    def _check_early_exit(
        self,
        step_name: str,
        operation_outputs: dict[str, OutputSpec],
        inputs: Any,
    ) -> StepFuture | None:
        """Check stop/cancel conditions and wait for predecessors.

        Three gates are checked in order:
        1. ``_stopped`` — set when a prior step had empty inputs, halting
           the pipeline to prevent meaningless downstream work.
        2. ``_cancel_event`` — set by SIGINT/SIGTERM or explicit cancel().
        3. Predecessor wait — blocks until all upstream StepFutures
           complete, then re-checks cancellation (which may have been
           signalled while waiting).

        Used by both ``submit()`` and ``_submit_composite()``.

        Returns:
            Resolved StepFuture if the step should be skipped,
            None if execution should proceed.
        """
        if self._stopped:
            logger.info(
                "Step %d (%s): pipeline stopped (earlier step had empty inputs)"
                " — skipping.",
                self._current_step,
                step_name,
            )
            return self._skip_step(step_name, operation_outputs, "pipeline_stopped")

        if self._cancel_event.is_set():
            logger.info(
                "Step %d (%s): pipeline cancelled — skipping.",
                self._current_step,
                step_name,
            )
            return self._skip_step(step_name, operation_outputs, "cancelled")

        self._wait_for_predecessors(inputs)

        # Re-check cancel — may have been set while blocked on predecessors
        if self._cancel_event.is_set():
            return self._skip_step(step_name, operation_outputs, "cancelled")

        return None

    def _prepare_step_spec(
        self,
        operation: type[OperationDefinition],
        params: dict[str, Any] | None,
        resources: dict[str, Any] | None,
        execution: dict[str, Any] | None,
        environment: str | dict[str, Any] | None,
        tool: dict[str, Any] | None,
        step_number: int,
        inputs: Any,
    ) -> tuple[str, OperationDefinition]:
        """Instantiate operation and compute deterministic step spec ID.

        The step_spec_id is a content hash of (operation name, step number,
        merged params, upstream spec IDs, and config overrides). Two runs
        with identical inputs and configuration produce the same spec ID,
        enabling the step-level cache to skip re-execution.

        The temp_instance is kept around because downstream code needs it
        for ``is_curator_operation()`` checks and ``build_step_result()``
        on the file-promotion failure path.

        Returns:
            Tuple of (step_spec_id, temp_instance).
        """
        # Instantiate with merged defaults + user overrides so we can
        # dump the *full* params (including defaults) for hashing.
        temp_instance = instantiate_operation(
            operation, params, resources, execution, environment, tool
        )
        if "params" in type(temp_instance).model_fields:
            full_params = temp_instance.params.model_dump(mode="json")  # type: ignore[attr-defined]
        else:
            # Flat-field operations: exclude base OperationDefinition
            # fields (resources, execution, etc.) — only user params.
            base_fields = set(OperationDefinition.model_fields)
            full_params = {
                k: v
                for k, v in temp_instance.model_dump(mode="json").items()
                if k not in base_fields
            }

        # TODO: _merge_config_overrides should not start with _
        from artisan.orchestration.engine.step_executor import _merge_config_overrides

        config_overrides = _merge_config_overrides(environment, tool)

        input_spec = self._build_input_spec(inputs)
        step_spec_id = compute_step_spec_id(
            operation_name=operation.name,
            step_number=step_number,
            params=full_params if full_params else None,
            input_spec=input_spec,
            config_overrides=config_overrides,
        )

        return step_spec_id, temp_instance

    def _try_cached_step(
        self,
        step_spec_id: str,
        step_number: int,
        step_name: str,
        operation_outputs: dict[str, OutputSpec],
    ) -> StepFuture | None:
        """Return a resolved StepFuture if step is cached, None otherwise.

        Looks up step_spec_id in the steps delta table. On a hit, records
        the cached result in all bookkeeping structures (step_results,
        step_registry, named_steps) and advances the step counter — so the
        caller can return immediately without any execution.
        """
        cached = self._step_tracker.check_cache(
            step_spec_id,
            self._config.cache_policy,
        )
        if cached is None:
            return None

        logger.info(
            "Step %d (%s) CACHED — skipping execution",
            step_number,
            step_name,
        )
        self._step_spec_ids[step_number] = step_spec_id
        if cached.step_run_id:
            self._step_run_ids[step_number] = cached.step_run_id
        self._step_results.append(cached)
        self._register_step(step_name, step_number, operation_outputs)
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

    def _handle_file_path_inputs(
        self,
        inputs: list[str],
        temp_instance: OperationDefinition,
        operation: type[OperationDefinition],
        step_number: int,
        step_spec_id: str,
        step_name: str,
        failure_policy: FailurePolicy | None,
    ) -> dict[str, list[str]] | StepFuture:
        """Promote raw file paths to FileRefArtifacts in the store.

        When the user passes ``["path/a.nc", "path/b.nc"]`` as inputs,
        this method validates each path, hashes file contents, creates
        FileRefArtifact records, and commits them to Delta Lake. The
        returned dict maps the ``"file"`` role to a sorted list of
        artifact IDs that the executor will resolve at dispatch time.

        Only curator operations (IngestData, IngestFiles, etc.) accept
        raw paths. Creator operations must receive artifact references
        from a prior ingest step.

        Returns:
            Promoted inputs dict ``{"file": [artifact_ids...]}`` on
            success, or a resolved StepFuture wrapping a failure result
            if all files are invalid.

        Raises:
            ValueError: If operation is not a curator operation.
        """
        if not is_curator_operation(temp_instance):
            msg = (
                "Raw file paths are not allowed for creator operations. "
                "Use a curator ingest operation to bring files into the "
                "pipeline first."
            )
            raise ValueError(msg)

        promoted, _count = _promote_file_paths_to_store(
            inputs,
            self._config,
            step_number,
            operation.name,
        )
        if promoted is not None:
            return promoted

        from artisan.orchestration.engine.step_executor import build_step_result

        _fp = failure_policy or self._config.failure_policy
        failed_result = build_step_result(
            operation=temp_instance,
            step_number=step_number,
            succeeded_count=0,
            failed_count=len(inputs),
            failure_policy=_fp,
            metadata={"error": "All input files are invalid"},
        )
        self._step_spec_ids[step_number] = step_spec_id
        self._step_results.append(failed_result)
        self._register_step(step_name, step_number, operation.outputs)
        self._named_steps.setdefault(failed_result.step_name, []).append(failed_result)
        self._current_step += 1
        resolved_fail: Future[StepResult] = Future()
        resolved_fail.set_result(failed_result)
        return StepFuture(
            step_number=step_number,
            step_name=failed_result.step_name,
            output_roles=frozenset(operation.outputs.keys()),
            output_types={r: s.artifact_type for r, s in operation.outputs.items()},
            future=resolved_fail,
        )

    def _dispatch_step(
        self,
        operation: type[OperationDefinition],
        inputs: Any,
        params: dict[str, Any] | None,
        backend: str | BackendBase | None,
        resources: dict[str, Any] | None,
        execution: dict[str, Any] | None,
        environment: str | dict[str, Any] | None,
        tool: dict[str, Any] | None,
        failure_policy: FailurePolicy | None,
        compact: bool,
        step_name: str,
        step_number: int,
        step_spec_id: str,
        temp_instance: OperationDefinition,
        skip_cache: bool = False,
    ) -> StepFuture:
        """Register step, resolve backend, and submit execution to thread pool.

        This is the final phase of submit(). It performs three things
        synchronously on the calling thread, then hands off to the
        single-threaded executor:

        1. **Bookkeeping** — registers the step in the step registry,
           advances the step counter, installs signal handlers (first
           dispatch only), and generates a unique step_run_id.
        2. **Backend resolution** — curator operations are forced to
           LOCAL; otherwise per-step override > pipeline default.
        3. **Delta recording** — writes a StepStartRecord to the steps
           delta table for audit/resume.

        The ``_run()`` closure is then submitted to the ThreadPoolExecutor
        (max_workers=1, so steps execute sequentially). The closure calls
        ``execute_step()`` which handles batching, worker dispatch, and
        Delta commits. Results are recorded back via ``_step_results``
        and ``_named_steps`` for finalize() to collect.

        Returns:
            StepFuture tracking the background execution.
        """
        if self._current_step == 0:
            self._install_signal_handlers()
        self._register_step(step_name, step_number, operation.outputs)
        self._current_step += 1
        self._step_spec_ids[step_number] = step_spec_id
        step_run_id = _generate_step_run_id(step_spec_id)
        self._step_run_ids[step_number] = step_run_id

        output_types_map = self._build_output_types(operation.outputs)

        # Curator operations always run locally (they read/write Delta
        # directly). Non-curator: per-step override > pipeline default.
        resolved_backend: BackendBase
        if is_curator_operation(temp_instance):
            resolved_backend = Backend.LOCAL
        elif backend is not None:
            resolved_backend = resolve_backend(backend)
        else:
            resolved_backend = resolve_backend(self._config.default_backend)

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

        _failure_policy = failure_policy or self._config.failure_policy

        def _run() -> StepResult:
            # Last-chance cancel check: the step may have been queued in
            # the executor while a cancel signal arrived.
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
                # Snapshot step_run_ids for scoped output resolution
                upstream_step_run_ids = dict(self._step_run_ids)
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
                    skip_cache=skip_cache or self._config.skip_cache,
                    step_run_id=step_run_id,
                    step_run_ids=upstream_step_run_ids,
                )
                elapsed = time.perf_counter() - start
                result = result.model_copy(
                    update={
                        "step_name": step_name,
                        "duration_seconds": elapsed,
                        "step_run_id": step_run_id,
                    }
                )

                # execute_step may detect cancellation mid-batch and
                # return a result with metadata={"cancelled": True}
                # rather than raising — record and bail.
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

                # Empty inputs at dispatch time: the step is "skipped"
                # and _stopped is set so all subsequent steps short-circuit
                # via _check_early_exit.
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
        composite: type[CompositeDefinition],
        inputs: (dict[str, OutputReference | list[str]] | None) = None,
        params: dict[str, Any] | None = None,
        resources: dict[str, Any] | None = None,  # noqa: ARG002 — reserved for forwarding to composite steps
        execution: dict[str, Any] | None = None,  # noqa: ARG002
        backend: str | BackendBase | None = None,  # noqa: ARG002
        environment: str | dict[str, Any] | None = None,  # noqa: ARG002
        tool: dict[str, Any] | None = None,  # noqa: ARG002
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
            isinstance(composite, type) and issubclass(composite, CompositeDefinition)  # type: ignore[redundant-expr]
        ):
            msg = f"expand() requires a CompositeDefinition subclass, got {composite}"  # type: ignore[unreachable]
            raise TypeError(msg)

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
        composite_class: type[CompositeDefinition],
        inputs: Any,
        params: dict[str, Any] | None,
        backend: str | BackendBase | None,
        resources: dict[str, Any] | None,
        execution: dict[str, Any] | None,
        intermediates: str,
        failure_policy: FailurePolicy | None,
        compact: bool,
        name: str,
        skip_cache: bool = False,
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
            skip_cache: Bypass cache lookups for this step.

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

        early = self._check_early_exit(name, composite_class.outputs, inputs)
        if early is not None:
            return early

        step_number = self._current_step

        # Compute composite step_spec_id
        temp = instantiate_operation(composite_class, params)  # type: ignore[arg-type]
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
        cached = (
            None
            if (skip_cache or self._config.skip_cache)
            else self._step_tracker.check_cache(step_spec_id, self._config.cache_policy)
        )
        if cached is not None:
            logger.info("Step %d (%s) CACHED — skipping execution", step_number, name)
            self._step_spec_ids[step_number] = step_spec_id
            if cached.step_run_id:
                self._step_run_ids[step_number] = cached.step_run_id
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

        output_types_map = self._build_output_types(composite_class.outputs)

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
            composite_step_run_id = _generate_step_run_id(step_spec_id)
            self._step_run_ids[step_number] = composite_step_run_id
            upstream_step_run_ids = dict(self._step_run_ids)
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
                    step_run_ids=upstream_step_run_ids,
                    step_run_id=composite_step_run_id,
                )
                elapsed = time.perf_counter() - start
                result = result.model_copy(
                    update={
                        "step_name": name,
                        "duration_seconds": elapsed,
                        "step_run_id": composite_step_run_id,
                    },
                )
                self._step_tracker.record_step_completed(
                    StepStartRecord(
                        step_run_id=composite_step_run_id,
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
                    ids_hash = digest_utf8(",".join(sorted(value)))
                    spec[role] = (ids_hash, "")
            return spec
        if isinstance(inputs, list):
            if inputs and isinstance(inputs[0], OutputReference):
                parts = []
                for ref in inputs:
                    upstream_spec_id = self._step_spec_ids[ref.source_step]
                    parts.append(f"{upstream_spec_id}:{ref.role}")
                composite_hash = digest_utf8(",".join(parts))
                return {"_merged_streams": (composite_hash, "")}
            paths_hash = digest_utf8(",".join(sorted(str(p) for p in inputs)))
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

    def _shutdown_executor(self, *, wait: bool = True) -> None:
        """Shut down the thread pool executor if still running.

        Args:
            wait: If True, wait for pending futures. If False, abandon them.
        """
        if self._executor is not None:
            cancelled = self._cancel_event.is_set()
            self._executor.shutdown(
                wait=wait and not cancelled, cancel_futures=cancelled
            )
            self._executor = None

    def finalize(self) -> dict[str, Any]:
        """Finalize pipeline execution and return summary.

        Waits for any active futures and shuts down the executor.
        When cancellation has been requested, uses a short timeout
        on futures to avoid blocking indefinitely.

        Safe to call multiple times — subsequent calls return the cached
        summary without re-running cleanup.

        Returns:
            Summary dict with step results and statistics.

        Example:
            pipeline = PipelineManager.create(...)
            step0 = pipeline.run(IngestData, inputs=files)
            step1 = pipeline.run(ScoreOp, inputs={"data": step0.output("data")})
            result = pipeline.finalize()
        """
        if self._finalized:
            return self._summary  # type: ignore[return-value]
        self._finalized = True

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
                    with contextlib.suppress(TimeoutError, Exception):
                        future.result(timeout=5.0)
            except Exception as exc:
                logger.error(
                    "Step %d future failed during finalize: %s: %s",
                    step_num,
                    type(exc).__name__,
                    exc,
                )

        self._shutdown_executor()
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

        self._summary = {
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
        return self._summary
