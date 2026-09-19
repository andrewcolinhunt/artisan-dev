"""Main interface for defining and executing artisan pipelines.

Key exports: ``PipelineManager`` (create, run, submit, finalize).
"""

from __future__ import annotations

import atexit
import contextvars
import json
import logging
import os
import signal
import threading
import time
import weakref
from collections.abc import Iterator
from concurrent.futures import CancelledError, Future, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, cast, overload
from uuid import uuid4

import polars as pl

from artisan.errors import PersistenceIntegrityError
from artisan.execution.executors.curator import is_curator_operation
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.orchestration.engine.inputs import PreparedInputs, prepare_inputs
from artisan.orchestration.engine.step_executor import (
    execute_step,
    instantiate_operation,
)
from artisan.orchestration.engine.step_tracker import StepTracker
from artisan.orchestration.runners import Runner, RunnerBase, resolve_runner
from artisan.orchestration.runners.local import LocalRunner
from artisan.orchestration.step_future import StepFuture
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.schemas.enums import CachePolicy, FailurePolicy, GroupByStrategy, TablePath
from artisan.schemas.execution.batch_strategy import BatchStrategy
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.schemas.operation_config.compute import ComputeProvider
from artisan.schemas.operation_config.compute_resources import ComputeResources
from artisan.schemas.operation_config.environments import Environments
from artisan.schemas.operation_config.runner_resources import RunnerResources
from artisan.schemas.operation_config.tool_spec import ToolSpec
from artisan.schemas.orchestration.output_reference import OutputReference
from artisan.schemas.orchestration.pipeline_config import PipelineConfig
from artisan.schemas.orchestration.step_lifecycle import (
    TERMINAL_STEP_STATUSES,
    CancellationAcknowledgement,
    CancellationStatus,
    StepDisposition,
    StepStatus,
)
from artisan.schemas.orchestration.step_overrides import StepOverrides
from artisan.schemas.orchestration.step_result import StepResult
from artisan.schemas.orchestration.step_start_record import StepStartRecord
from artisan.schemas.orchestration.step_state import StepState
from artisan.schemas.specs.output_spec import OutputSpec
from artisan.utils.hashing import (
    compute_step_spec_id,
    compute_stream_digest,
    effective_config_payload,
    serialize_params,
)
from artisan.utils.json import artisan_json_default as _set_default
from artisan.utils.path import uri_join, uri_parent

if TYPE_CHECKING:
    from artisan.composites.base.composite_definition import CompositeDefinition
    from artisan.composites.base.results import CompositeResult

# Validation helpers accept both OperationDefinition and CompositeDefinition,
# which share ClassVars (name, inputs, outputs) but have no common base.
_OpLike = type[OperationDefinition] | type["CompositeDefinition"]

logger = logging.getLogger(__name__)

_PIPELINE_DEFAULT_RUNNER_OPTION = "pipeline_default_step_runner"
_PIPELINE_DEFAULT_LOCAL_RUNNER_OPTION = "pipeline_default_local_runner"


@dataclass(frozen=True)
class _StoredDefaultRunner:
    """Serializable default-runner metadata recovered from step records."""

    name: str
    local_default_max_workers: int | None = None


# =============================================================================
# Module-level helper functions
# =============================================================================


def _generate_run_id(name: str) -> str:
    """Generate a human-readable pipeline run ID."""
    return f"{name}_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}_{uuid4().hex[:8]}"


def _generate_step_run_id() -> str:
    """Generate a unique occurrence ID for one logical step attempt."""
    return uuid4().hex


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


def _parse_stored_local_runner(options: dict[str, Any]) -> int | None:
    """Return a persisted built-in local pool size, when present."""
    config = options.get(_PIPELINE_DEFAULT_LOCAL_RUNNER_OPTION)
    if config is None:
        return None
    if options.get(_PIPELINE_DEFAULT_RUNNER_OPTION) != LocalRunner.name:
        msg = "Persisted local runner configuration requires default runner 'local'"
        raise ValueError(msg)
    if not isinstance(config, dict):
        msg = "Persisted local runner configuration must be a JSON object"
        raise ValueError(msg)
    max_workers = config.get("default_max_workers")
    if (
        isinstance(max_workers, bool)
        or not isinstance(max_workers, int)
        or max_workers < 1
    ):
        msg = "Persisted LocalRunner.default_max_workers must be a positive integer"
        raise ValueError(msg)
    return max_workers


def _load_stored_default_runner(
    steps: list[StepState],
) -> _StoredDefaultRunner:
    """Read a pipeline's default runner from persisted step options.

    Args:
        steps: Completed states for one pipeline run.
    Returns:
        Stored runner metadata.

    Raises:
        ValueError: If stored runner metadata is invalid.
    """
    stored_name: str | None = None
    local_max_workers: int | None = None
    for step in steps:
        options = json.loads(step.compute_options_json)
        if not isinstance(options, dict):
            continue
        if _PIPELINE_DEFAULT_RUNNER_OPTION in options:
            name = options[_PIPELINE_DEFAULT_RUNNER_OPTION]
            if not isinstance(name, str) or not name:
                msg = "Persisted default_step_runner must be a non-empty string"
                raise ValueError(msg)
            if stored_name is not None and name != stored_name:
                msg = (
                    "Persisted default_step_runner is inconsistent across step records"
                )
                raise ValueError(msg)
            stored_name = name

        current_local_max = _parse_stored_local_runner(options)
        if current_local_max is not None:
            if local_max_workers is not None and current_local_max != local_max_workers:
                msg = "Persisted LocalRunner configuration is inconsistent across steps"
                raise ValueError(msg)
            local_max_workers = current_local_max

    if stored_name is None:
        msg = "Persisted step records do not identify default_step_runner"
        raise ValueError(msg)
    return _StoredDefaultRunner(stored_name, local_max_workers)


def _is_file_path_input(inputs: Any) -> bool:
    """Return True if inputs is a non-empty list of raw file path strings."""
    return isinstance(inputs, list) and bool(inputs) and isinstance(inputs[0], str)


def _promote_file_paths_to_store(
    file_paths: list[str],
    config: PipelineConfig,
    step_number: int,
    operation_name: str,
    step_run_id: str,
) -> tuple[dict[str, list[str]] | None, int, set[str]]:
    """Validate file paths, create FileRefArtifacts, and commit to delta.

    Args:
        file_paths: Raw file path strings from the user.
        config: Pipeline configuration.
        step_number: Pipeline step number.
        operation_name: Operation name (for logging).

    Returns:
        Tuple of resolved inputs (or None), valid-file count, and IDs verified
        by this promotion read.
    """
    from fsspec import AbstractFileSystem
    from fsspec.implementations.local import LocalFileSystem

    from artisan.errors import ArtifactIntegrityError
    from artisan.schemas.artifact.external import (
        sanitized_uri,
        validate_persistable_uri,
    )
    from artisan.schemas.artifact.file_ref import FileRefArtifact
    from artisan.schemas.enums import TablePath
    from artisan.schemas.execution.fs import resolve_fs
    from artisan.storage.io.commit import DeltaCommitter
    from artisan.utils.filename import strip_extensions

    # Per-path fs resolution (two-step rule from schemas/execution/fs.py):
    # protocol-match → config.storage.filesystem(); else → url_to_fs
    # ambient discovery. This lets a local pipeline ingest from S3, and
    # lets test fixtures with explicit credentials in StorageConfig
    # reach MinIO without setting AWS_* env vars (would leak across
    # xdist workers).
    valid_paths: list[tuple[str, AbstractFileSystem, str]] = []
    invalid_paths: list[str] = []
    for path_str in file_paths:
        try:
            fs, stripped = resolve_fs(path_str, config.storage)
            if not fs.exists(stripped):
                invalid_paths.append(f"Not found: {sanitized_uri(path_str)}")
                continue
            if not fs.isfile(stripped):
                invalid_paths.append(f"Not a file: {sanitized_uri(path_str)}")
                continue
        except Exception as exc:
            invalid_paths.append(
                f"Inaccessible: {sanitized_uri(path_str)} ({type(exc).__name__})"
            )
            continue
        valid_paths.append((path_str, fs, stripped))

    if invalid_paths:
        msg = (
            f"Raw input verification failed for step {step_number} "
            f"({operation_name}): {'; '.join(invalid_paths)}"
        )
        raise ArtifactIntegrityError(msg)

    # Create FileRefArtifacts and finalize
    file_ref_artifacts: list[FileRefArtifact] = []
    for original, fs, stripped in valid_paths:
        try:
            with fs.open(stripped, "rb") as f:
                content_hash, size_bytes = compute_stream_digest(f)
        except Exception as exc:
            msg = (
                "Raw input verification failed while reading "
                f"{sanitized_uri(original)} ({type(exc).__name__})"
            )
            raise ArtifactIntegrityError(msg) from exc
        # basename/splitext are pure string ops — work on URIs.
        basename = os.path.basename(original)
        _name_part, ext_part = os.path.splitext(basename)
        # Local paths get an absolute path; cloud URIs get stored as-is.
        # `isinstance` (not `fs.protocol == "file"`) because fsspec
        # implementations may declare `protocol` as a tuple
        # (e.g. s3fs uses `("s3", "s3a")`).
        stored_path = (
            os.path.abspath(original) if isinstance(fs, LocalFileSystem) else original
        )
        try:
            validate_persistable_uri(stored_path)
        except ValueError as exc:
            msg = f"Raw input location is not persistable: {sanitized_uri(stored_path)}"
            raise ArtifactIntegrityError(msg) from exc
        artifact = cast(
            FileRefArtifact,
            FileRefArtifact.draft(
                path=stored_path,
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

    # Register verified raw inputs through their own pre-dispatch commit.
    from artisan.storage.io.commit_plan import build_commit_plan
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
    location_df = pl.DataFrame(
        [
            {"artifact_id": artifact.artifact_id, "uri": artifact.path}
            for artifact in file_ref_artifacts
        ],
        schema={"artifact_id": pl.String, "uri": pl.String},
    )
    staging_manager.stage_orchestrator_dataframe(
        file_ref_df,
        "artifacts/file_refs",
        commit_kind="input_registration",
        step_run_id=step_run_id,
        step_number=step_number,
        operation_name=operation_name,
    )
    staging_manager.stage_orchestrator_dataframe(
        index_df,
        TablePath.ARTIFACT_INDEX.value,
        commit_kind="input_registration",
        step_run_id=step_run_id,
        step_number=step_number,
        operation_name=operation_name,
    )
    staging_manager.stage_orchestrator_dataframe(
        location_df,
        TablePath.ARTIFACT_LOCATIONS.value,
        commit_kind="input_registration",
        step_run_id=step_run_id,
        step_number=step_number,
        operation_name=operation_name,
    )
    plan = build_commit_plan(
        delta_root=config.delta_root,
        staging_root=config.staging_root,
        fs=fs,
        commit_kind="input_registration",
        step_run_id=step_run_id,
        step_number=step_number,
        operation_name=operation_name,
    )
    committer.commit_logical(plan, preserve_staging=config.preserve_staging)

    artifact_ids: list[str] = [
        a.artifact_id for a in file_ref_artifacts if a.artifact_id is not None
    ]
    resolved_inputs = {"file": artifact_ids}

    logger.debug(
        "Step %d (%s): promoted %d file paths to Delta Lake",
        step_number,
        operation_name,
        len(valid_paths),
    )
    return resolved_inputs, len(valid_paths), set(artifact_ids)


# =============================================================================
# Validation helpers
# =============================================================================


def _validate_params(
    operation: _OpLike,
    params: dict[str, Any],
) -> None:
    """Raise ValueError if any param keys are unrecognized by the operation.

    Delegates the ``Params`` lookup to
    ``operations.base._param_docs._params_class`` so all three consumers
    (registry, fail-fast check, this validator) share one rule.
    """
    from artisan.operations.base._param_docs import _params_class

    params_cls = _params_class(operation)
    valid_keys = set(params_cls.model_fields) if params_cls is not None else set()
    unknown = set(params) - valid_keys
    if unknown:
        msg = (
            f"Unknown params for {operation.name}: {sorted(unknown)}. "
            f"Valid keys: {sorted(valid_keys)}"
        )
        raise ValueError(msg)


def _validate_resources(resources: dict[str, Any] | Any) -> None:
    """Raise ValueError if any resource keys are unrecognized.

    A ``RunnerResources`` instance is already valid by construction;
    this function only checks raw dicts.
    """
    from artisan.schemas.operation_config.runner_resources import RunnerResources

    if isinstance(resources, RunnerResources):
        return
    valid_keys = set(RunnerResources.model_fields)
    unknown = set(resources) - valid_keys
    if unknown:
        msg = (
            f"Unknown resource keys: {sorted(unknown)}. "
            f"Valid keys: {sorted(valid_keys)}"
        )
        raise ValueError(msg)


def _validate_execution(execution: dict[str, Any] | Any) -> None:
    """Raise ValueError if any execution keys are unrecognized.

    A ``BatchStrategy`` instance is already valid by construction;
    this function only checks raw dicts.
    """
    from artisan.schemas.execution.batch_strategy import BatchStrategy

    if isinstance(execution, BatchStrategy):
        return
    valid_keys = set(BatchStrategy.model_fields)
    unknown = set(execution) - valid_keys
    if unknown:
        msg = (
            f"Unknown execution keys: {sorted(unknown)}. "
            f"Valid keys: {sorted(valid_keys)}"
        )
        raise ValueError(msg)


def _validate_environment(
    operation: type[OperationDefinition],
    environment: str | dict[str, Any] | Any,
) -> None:
    """Raise ValueError if environment override is invalid for this operation.

    A typed ``Environments`` instance is already valid by construction;
    this function only checks raw dict / string forms.
    """
    if isinstance(environment, Environments):
        return
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
        _reject_inactive_provider_config(environment, kwarg="environment")


def _validate_compute_provider(value: dict[str, Any]) -> None:
    """Reject unknown keys and silent inactive-provider configuration.

    Args:
        value: User-supplied compute_provider override dict.

    Raises:
        ValueError: If keys are unrecognized or if the dict configures
            a provider that is not the active one.
    """
    from pydantic import ValidationError

    try:
        ComputeProvider.model_validate(value)
    except ValidationError as e:
        msg = f"Unknown compute_provider override key(s): {e}"
        raise ValueError(msg) from e
    _reject_inactive_provider_config(value, kwarg="compute_provider")


def _validate_compute_resources(value: dict[str, Any]) -> None:
    """Reject unknown keys on compute_resources override.

    Args:
        value: User-supplied compute_resources override dict.

    Raises:
        ValueError: If keys are unrecognized.
    """
    from pydantic import ValidationError

    try:
        ComputeResources.model_validate(value)
    except ValidationError as e:
        msg = f"Unknown compute_resources override key(s): {e}"
        raise ValueError(msg) from e


def _reject_inactive_provider_config(value: dict[str, Any], *, kwarg: str) -> None:
    """Raise if dict configures a provider without setting it active.

    Closes the silent case-3 misconfiguration: a user passes
    ``environment={"docker": {"image": "..."}}`` thinking they have
    configured docker, but the active selector still points at local.

    Args:
        value: The user-supplied dict.
        kwarg: The kwarg name (for the error message).

    Raises:
        ValueError: If a provider key carries a non-empty config dict
            and ``active`` is not set to that provider.
    """
    provider_keys_with_config = {
        k for k, v in value.items() if k != "active" and isinstance(v, dict) and v
    }
    active = value.get("active")
    inactive_configured = provider_keys_with_config - ({active} if active else set())
    if inactive_configured:
        msg = (
            f"Configured inactive provider(s) "
            f"{sorted(inactive_configured)!r} on {kwarg!r} with "
            f"active={active!r}. Set 'active' to the provider you want "
            f"to configure, or pass a string to select without "
            f"configuring."
        )
        raise ValueError(msg)


def _validate_tool(
    operation: type[OperationDefinition],
    tool: dict[str, Any] | Any,
) -> None:
    """Raise ValueError if tool overrides are invalid for this operation.

    A ``ToolSpec`` instance is already valid by construction; this
    function only checks raw dicts.
    """
    from artisan.schemas.operation_config.tool_spec import ToolSpec

    if isinstance(tool, ToolSpec):
        return
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


class _StepStatusReader:
    """Thread-safe view updated only after a lifecycle snapshot is durable."""

    def __init__(self, status: StepStatus) -> None:
        self._status = status
        self._lock = threading.Lock()

    def get(self) -> StepStatus:
        """Return the latest durable status."""
        with self._lock:
            return self._status

    def set(self, status: StepStatus) -> None:
        """Publish a newly durable status."""
        with self._lock:
            self._status = status


# =============================================================================
# PipelineManager
# =============================================================================


def _atexit_shutdown_executor(ref: weakref.ref[ThreadPoolExecutor]) -> None:
    """Last-resort cleanup: shut down a leaked ThreadPoolExecutor at exit."""
    executor = ref()
    if executor is not None:
        executor.shutdown(wait=False)


def _resolve_runtime_default_runner(
    stored_name: str,
    runtime_runner: RunnerBase | None,
) -> RunnerBase:
    """Resolve a persisted runner name or validate its runtime instance.

    Args:
        stored_name: Stable runner name stored in ``PipelineConfig``.
        runtime_runner: Explicit provider instance, when one is required.

    Returns:
        Runner instance used for default step dispatch.

    Raises:
        ValueError: If the instance does not match the stored name or core
            cannot reconstruct an external provider by name.
    """
    if runtime_runner is not None:
        if runtime_runner.name != stored_name:
            msg = (
                f"Runtime runner name {runtime_runner.name!r} does not match "
                f"stored default_step_runner {stored_name!r}"
            )
            raise ValueError(msg)
        return runtime_runner

    try:
        return resolve_runner(stored_name)
    except ValueError as exc:
        msg = (
            f"Step runner {stored_name!r} is not built into Artisan and cannot "
            "be reconstructed by name. Pass an initialized provider runner as "
            "default_step_runner."
        )
        raise ValueError(msg) from exc


def _restore_persisted_local_runner(
    stored_runner: _StoredDefaultRunner,
    runtime_runner: RunnerBase | None,
    run_id: str,
) -> RunnerBase | None:
    """Reconstruct or validate the narrowly persisted LocalRunner config."""
    if stored_runner.local_default_max_workers is None:
        return runtime_runner

    expected = stored_runner.local_default_max_workers
    if runtime_runner is None:
        return LocalRunner(default_max_workers=expected)
    if type(runtime_runner) is not LocalRunner:
        msg = (
            "Persisted built-in LocalRunner configuration cannot be restored with "
            f"{type(runtime_runner).__name__}; resume run {run_id!r} without a runner "
            "or pass a LocalRunner instance."
        )
        raise ValueError(msg)
    if runtime_runner.default_max_workers != expected:
        msg = (
            f"LocalRunner(default_max_workers={runtime_runner.default_max_workers}) "
            "does not match persisted "
            f"LocalRunner(default_max_workers={expected}) for pipeline run {run_id!r}."
        )
        raise ValueError(msg)
    return runtime_runner


class PipelineManager:
    """Main interface for defining and executing pipelines.

    PipelineManager orchestrates the execution of pipeline steps, managing:
    - Step sequencing and numbering
    - Step-level caching via steps delta table
    - OutputReference resolution
    - Worker dispatch through local or external runners
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
        default_step_runner: RunnerBase | None = None,
    ) -> None:
        """Initialize from a PipelineConfig.

        Prefer ``PipelineManager.create()`` over direct instantiation.

        Args:
            config: Full pipeline configuration.
            configure_logging: If True (default), call
                :func:`~artisan.utils.logging.configure_logging` so
                users don't need to set up logging manually.
            default_step_runner: Runtime runner instance corresponding to
                ``config.default_step_runner``. Required when the stored name
                belongs to an external provider that core cannot reconstruct.
        """
        self._default_step_runner = _resolve_runtime_default_runner(
            config.default_step_runner,
            default_step_runner,
        )

        if configure_logging:
            from artisan.utils.logging import configure_logging as _configure

            # logs_root must be local (configure_logging os.makedirs it
            # at DEBUG level). When delta_root is cloud, pass None so no
            # file handler is attached.
            logs_root = (
                uri_join(uri_parent(config.delta_root), "logs")
                if config.storage.is_local
                else None
            )
            _configure(logs_root=logs_root)

        self._config = config

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
        committer.initialize_tables()
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
        self._step_start_records: dict[int, StepStartRecord] = {}
        self._step_status_readers: dict[int, _StepStatusReader] = {}
        self._executor: ThreadPoolExecutor | None = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="pipeline-step"
        )
        self._finalized: bool = False
        self._summary: dict[str, Any] | None = None
        atexit.register(_atexit_shutdown_executor, weakref.ref(self._executor))

    # -- Resource cleanup ------------------------------------------------------

    def __del__(self) -> None:
        """Release executor threads if finalize() was never called."""
        if not getattr(self, "_finalized", True):
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

        succeeded = sum(
            1
            for result in self._step_results
            if result.status in {StepStatus.SUCCEEDED, StepStatus.SKIPPED}
        )
        total = len(self._step_results)
        status = (
            "all succeeded" if succeeded == total else f"{succeeded}/{total} succeeded"
        )
        return f"Pipeline '{self._config.name}': {total} steps, {status}"

    def __len__(self) -> int:
        """Return the number of terminal step results."""
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
        return bool(self._step_results) and all(
            result.status in {StepStatus.SUCCEEDED, StepStatus.SKIPPED}
            for result in self._step_results
        )

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
        step_run_id: str,
    ) -> StepFuture:
        """Record a skipped step and return a resolved StepFuture.

        Handles all bookkeeping: result creation, step registration,
        step counter increment, and future resolution.

        Args:
            step_name: Human-readable step name.
            operation_outputs: The operation's outputs dict (role -> OutputSpec).
            skip_reason: Why this step was skipped (for example,
                ``"pipeline_stopped"``).
            step_run_id: Fresh run-owned identity for this skipped attempt.

        Returns:
            A resolved StepFuture with the skipped result.
        """
        step_number = self._current_step
        output_types = self._build_output_types(operation_outputs)
        output_roles = frozenset(operation_outputs.keys())

        result = StepResult(
            step_name=step_name,
            step_number=step_number,
            status=StepStatus.SKIPPED,
            total_count=0,
            succeeded_count=0,
            failed_count=0,
            metadata={"skip_reason": skip_reason},
            step_run_id=step_run_id,
        )
        self._step_tracker.transition(
            step_run_id,
            StepStatus.PENDING,
            StepStatus.SKIPPED,
            result=result,
        )
        reader = self._step_status_readers[step_number]
        reader.set(StepStatus.SKIPPED)
        self._step_results.append(result)
        self._register_step(step_name, step_number, operation_outputs)
        self._named_steps.setdefault(step_name, []).append(result)
        self._step_run_ids[step_number] = step_run_id
        self._current_step += 1

        resolved: Future[StepResult] = Future()
        resolved.set_result(result)
        return StepFuture(
            step_number=step_number,
            step_name=step_name,
            output_roles=output_roles,
            output_types=output_types,
            future=resolved,
            status_reader=reader.get,
        )

    def _cancel_step(
        self,
        step_name: str,
        operation_outputs: dict[str, OutputSpec],
        step_run_id: str,
        expected: StepStatus,
        *,
        register: bool,
    ) -> StepResult:
        """Persist requested, confirmed, and cancelled for one attempt."""
        step_number = self._step_start_records_by_id(step_run_id).step_number
        current = self._step_tracker.current_state(step_run_id)
        existing = self._publish_existing_terminal(
            current,
            operation_outputs,
            register=register,
        )
        if existing is not None:
            return existing
        requested = CancellationAcknowledgement(CancellationStatus.REQUESTED)
        confirmed = CancellationAcknowledgement(
            CancellationStatus.CONFIRMED,
            "Work stopped before an output commit",
        )
        current = self._step_tracker.record_cancellation(
            step_run_id, expected, requested
        )
        existing = self._publish_existing_terminal(
            current,
            operation_outputs,
            register=register,
        )
        if existing is not None:
            return existing
        current = self._step_tracker.record_cancellation(
            step_run_id, expected, confirmed
        )
        existing = self._publish_existing_terminal(
            current,
            operation_outputs,
            register=register,
        )
        if existing is not None:
            return existing
        result = StepResult(
            step_name=step_name,
            step_number=step_number,
            status=StepStatus.CANCELLED,
            cancellation_status=CancellationStatus.CONFIRMED,
            error=confirmed.message,
            step_run_id=step_run_id,
        )
        self._step_tracker.transition(
            step_run_id,
            expected,
            StepStatus.CANCELLED,
            result=result,
        )
        self._step_status_readers[step_number].set(StepStatus.CANCELLED)
        if register:
            self._register_step(step_name, step_number, operation_outputs)
            self._current_step += 1
        self._step_results.append(result)
        self._named_steps.setdefault(step_name, []).append(result)
        return result

    def _publish_existing_terminal(
        self,
        state: StepState,
        operation_outputs: dict[str, OutputSpec],
        *,
        register: bool,
    ) -> StepResult | None:
        """Publish a terminal state that won a cancellation race."""
        if state.status not in TERMINAL_STEP_STATUSES:
            return None
        result = state.to_step_result()
        self._step_status_readers[state.step_number].set(state.status)
        if register:
            self._register_step(state.step_name, state.step_number, operation_outputs)
            self._current_step += 1
        if not any(
            saved.step_run_id == state.step_run_id for saved in self._step_results
        ):
            self._step_results.append(result)
            self._named_steps.setdefault(result.step_name, []).append(result)
        return result

    def _step_start_records_by_id(self, step_run_id: str) -> StepStartRecord:
        """Return the manager-owned start record for an attempt."""
        return next(
            record
            for record in self._step_start_records.values()
            if record.step_run_id == step_run_id
        )

    def _resolved_step_future(self, result: StepResult) -> StepFuture:
        """Wrap an already-terminal result with its durable status reader."""
        resolved: Future[StepResult] = Future()
        resolved.set_result(result)
        return StepFuture(
            step_number=result.step_number,
            step_name=result.step_name,
            output_roles=result.output_roles,
            output_types=result.output_types,
            future=resolved,
            status_reader=self._step_status_readers[result.step_number].get,
        )

    def _compact_after_terminal(self, enabled: bool) -> None:
        """Run best-effort maintenance after lifecycle publication."""
        if not enabled:
            return
        from artisan.orchestration.engine.step_executor import _compact_step_tables

        try:
            _compact_step_tables(
                self._config.delta_root,
                self._config.staging_root,
                fs=self._config.storage.filesystem(),
                storage_options=self._config.storage.delta_storage_options(),
            )
        except Exception as exc:
            logger.warning(
                "Compaction failed after terminalization: %s: %s",
                type(exc).__name__,
                exc,
            )

    def _failed_step(
        self,
        operation: type[OperationDefinition],
        step_name: str,
        step_run_id: str,
        error: str,
        *,
        register: bool,
        step_spec_id: str | None = None,
        cancellation_status: CancellationStatus | None = None,
        duration_seconds: float | None = None,
    ) -> StepResult:
        """Fail an attempt through the authoritative tracker transition API."""
        record = self._step_start_records_by_id(step_run_id)
        current = self._step_tracker.current_state(step_run_id)
        existing = self._publish_existing_terminal(
            current,
            operation.outputs,
            register=register,
        )
        if existing is not None:
            return existing
        if current.status == StepStatus.PENDING:
            self._step_tracker.transition(
                step_run_id,
                StepStatus.PENDING,
                StepStatus.RUNNING,
                step_spec_id=step_spec_id,
            )
            self._step_status_readers[record.step_number].set(StepStatus.RUNNING)
            current = self._step_tracker.current_state(step_run_id)
        if current.cancellation_status is None and cancellation_status is not None:
            current = self._step_tracker.record_cancellation(
                step_run_id,
                StepStatus.RUNNING,
                CancellationAcknowledgement(CancellationStatus.REQUESTED),
            )
        if current.cancellation_status == CancellationStatus.REQUESTED:
            final_cancellation = cancellation_status or CancellationStatus.UNKNOWN
            current = self._step_tracker.record_cancellation(
                step_run_id,
                StepStatus.RUNNING,
                CancellationAcknowledgement(
                    final_cancellation,
                    (
                        error
                        if cancellation_status is not None
                        else "Cancellation outcome became unknown during failure handling"
                    ),
                ),
            )
        cancellation_status = current.cancellation_status
        result = StepResult(
            step_name=step_name,
            step_number=record.step_number,
            status=StepStatus.FAILED,
            cancellation_status=cancellation_status,
            error=error,
            duration_seconds=duration_seconds,
            step_run_id=step_run_id,
        )
        self._step_tracker.transition(
            step_run_id,
            StepStatus.RUNNING,
            StepStatus.FAILED,
            step_spec_id=step_spec_id,
            result=result,
        )
        self._step_status_readers[record.step_number].set(StepStatus.FAILED)
        if register:
            self._register_step(step_name, record.step_number, operation.outputs)
            self._current_step += 1
        self._step_results.append(result)
        self._named_steps.setdefault(step_name, []).append(result)
        return result

    # =========================================================================
    # Cancellation
    # =========================================================================

    def cancel(self) -> None:
        """Request cancellation of the running pipeline.

        Idempotent and thread-safe. Sets an event that step executors check
        between phases. Each affected attempt persists a cancellation request,
        its acknowledgement, and the resulting terminal state. Executor
        shutdown belongs to :meth:`finalize`, outside signal-handler context.
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
        default_step_runner: str | RunnerBase = "local",
        preserve_staging: bool = False,
        preserve_working: bool = False,
        skip_cache: bool = False,
    ) -> PipelineManager:
        """Factory method to create a PipelineManager.

        Args:
            name: Pipeline identifier used for logging and run IDs.
            delta_root: Root path for Delta Lake tables.
            staging_root: Root path for worker staging files.
            working_root: Root path for worker sandboxes. If None, uses
                tempfile.gettempdir() (respects $TMPDIR).
            files_root: Root path for Artisan-managed external files. If None,
                defaults to a sibling "files" directory next to delta_root.
            failure_policy: Default failure handling for steps.
            cache_policy: Controls which usable terminal steps qualify as cache hits.
            default_step_runner: Default step runner for step execution. Accepts a
                ``RunnerBase`` instance or a built-in string name (currently
                ``"local"``). External providers are passed as instances.
            preserve_staging: Debug flag to preserve staging files after commit.
            preserve_working: Debug flag to preserve sandbox after execution.
            skip_cache: Bypass all cache lookups for every step.

        Returns:
            Configured PipelineManager instance.
        """
        resolved = resolve_runner(default_step_runner)
        pipeline_run_id = _generate_run_id(name)
        config = PipelineConfig(
            name=name,
            pipeline_run_id=pipeline_run_id,
            delta_root=delta_root,
            staging_root=staging_root,
            **({"working_root": working_root} if working_root is not None else {}),  # type: ignore[arg-type]  # conditional kwarg expansion
            **({"files_root": files_root} if files_root is not None else {}),  # type: ignore[arg-type]  # conditional kwarg expansion
            failure_policy=failure_policy,
            cache_policy=cache_policy,
            default_step_runner=resolved.name,
            preserve_staging=preserve_staging,
            preserve_working=preserve_working,
            skip_cache=skip_cache,
        )
        instance = cls(config, default_step_runner=resolved)
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
        default_step_runner: str | RunnerBase | None = None,
        files_root: str | None = None,
        failure_policy: FailurePolicy = FailurePolicy.CONTINUE,
        cache_policy: CachePolicy = CachePolicy.ALL_SUCCEEDED,
        preserve_staging: bool = False,
        preserve_working: bool = False,
        skip_cache: bool = False,
        storage: StorageConfig | None = None,
    ) -> PipelineManager:
        """Resume a pipeline from persisted step state.

        Args:
            delta_root: Root path for Delta Lake tables.
            staging_root: Root path for worker staging files.
            pipeline_run_id: Run to resume. If None, resumes the most recent.
            name: Pipeline name override.
            working_root: Root path for worker sandboxes. If None, uses
                tempfile.gettempdir() (respects $TMPDIR).
            default_step_runner: Runtime default runner. Built-in names can be
                reconstructed from persisted state; external providers must be
                supplied as matching instances.
            files_root: Root path for Artisan-managed external files. If None,
                derives a sibling path from a local delta_root.
            failure_policy: Default failure handling for subsequent steps.
            cache_policy: Controls which usable terminal steps qualify as cache hits.
            preserve_staging: Preserve staging files after commit.
            preserve_working: Preserve worker sandboxes after execution.
            skip_cache: Bypass cache lookups for subsequent steps.
            storage: Filesystem and Delta storage configuration.

        Returns:
            PipelineManager with state restored from delta.

        Raises:
            ValueError: If no pipeline run found to resume.
        """
        storage = storage or StorageConfig()
        tracker = StepTracker(
            delta_root,
            storage_options=storage.delta_storage_options(),
            fs=storage.filesystem(),
        )
        current_steps = tracker.load_current_states(pipeline_run_id)

        if not current_steps:
            msg = "No step attempts found"
            if pipeline_run_id:
                msg += f" for run '{pipeline_run_id}'"
            raise ValueError(msg)

        run_id = pipeline_run_id or current_steps[0].pipeline_run_id
        resumable_steps = tracker.load_resumable_steps(run_id)

        runtime_runner: RunnerBase | None
        requested_runner_name: str | None
        if isinstance(default_step_runner, RunnerBase):
            runtime_runner = default_step_runner
            requested_runner_name = default_step_runner.name
        else:
            runtime_runner = None
            requested_runner_name = default_step_runner
        stored_runner = _load_stored_default_runner(current_steps)
        stored_runner_name = stored_runner.name
        config_kwargs: dict[str, Any] = {
            "name": name or _extract_name_from_run_id(run_id),
            "pipeline_run_id": run_id,
            "delta_root": delta_root,
            "staging_root": staging_root,
            "files_root": files_root,
            "failure_policy": failure_policy,
            "cache_policy": cache_policy,
            "preserve_staging": preserve_staging,
            "preserve_working": preserve_working,
            "skip_cache": skip_cache,
            "storage": storage,
        }
        if (
            requested_runner_name is not None
            and requested_runner_name != stored_runner_name
        ):
            msg = (
                f"Requested default_step_runner {requested_runner_name!r} does not "
                f"match persisted default_step_runner {stored_runner_name!r} for "
                f"pipeline run {run_id!r}. Resume with the original provider runner."
            )
            raise ValueError(msg)
        runtime_runner = _restore_persisted_local_runner(
            stored_runner,
            runtime_runner,
            run_id,
        )
        config_kwargs["default_step_runner"] = stored_runner_name
        if working_root is not None:
            config_kwargs["working_root"] = working_root
        config = PipelineConfig(**config_kwargs)

        instance = cls(config, default_step_runner=runtime_runner)
        for step_state in resumable_steps:
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
            if step_state.step_spec_id is not None:
                instance._step_spec_ids[step_state.step_number] = (
                    step_state.step_spec_id
                )
            if step_state.step_run_id:
                instance._step_run_ids[step_state.step_number] = step_state.step_run_id
        instance._current_step = max(s.step_number for s in current_steps) + 1

        return instance

    # =========================================================================
    # Step execution: run() and submit()
    # =========================================================================

    def run(
        self,
        operation: type[OperationDefinition],
        *,
        inputs: (
            dict[str, OutputReference | list[str]]
            | list[OutputReference]
            | list[str]
            | None
        ) = None,
        params: dict[str, Any] | None = None,
        step_runner: str | RunnerBase | None = None,
        runner_resources: dict[str, Any] | RunnerResources | None = None,
        batch_strategy: dict[str, Any] | BatchStrategy | None = None,
        environment: str | dict[str, Any] | Environments | None = None,
        tool: dict[str, Any] | ToolSpec | None = None,
        compute_provider: str | dict[str, Any] | ComputeProvider | None = None,
        compute_resources: dict[str, Any] | ComputeResources | None = None,
        failure_policy: FailurePolicy | None = None,
        group_by: GroupByStrategy | None = None,
        compact: bool = True,
        name: str | None = None,
        skip_cache: bool = False,
    ) -> StepResult:
        """Execute an operation step (blocking).

        Composites must use ``run_composite``; passing one here raises.
        Equivalent to ``submit(...).result()``.

        Args:
            operation: OperationDefinition subclass.
            inputs: Input specification (dict, list, or None).
            params: Parameter overrides.
            step_runner: Step runner for execution. None uses pipeline default.
            runner_resources: Resource overrides (cpus, memory_gb, etc.).
            batch_strategy: Batching/scheduling overrides (artifacts_per_unit, etc.).
            environment: Environment override.
            tool: Tool overrides.
            compute_provider: Compute provider override (string or dict).
            compute_resources: Compute-provider hardware override
                (gpu, cpu, memory_gb, timeout).
            failure_policy: Override pipeline-level failure policy.
            group_by: Override the operation's class-level ``group_by`` for
                this step only. ``None`` (default) preserves the operation's
                declared default. A ``GroupByStrategy`` member switches
                pairing strategy and wins over any class-level default.
            compact: Run Delta Lake compaction after commit.
            name: Custom step name. Defaults to operation.name.
            skip_cache: Bypass cache lookups for this step.

        Returns:
            StepResult with output references and execution metadata.

        Raises:
            TypeError: If ``operation`` is a CompositeDefinition subclass.
        """
        return self.submit(
            operation,
            inputs=inputs,
            params=params,
            step_runner=step_runner,
            runner_resources=runner_resources,
            batch_strategy=batch_strategy,
            environment=environment,
            tool=tool,
            compute_provider=compute_provider,
            compute_resources=compute_resources,
            failure_policy=failure_policy,
            group_by=group_by,
            compact=compact,
            name=name,
            skip_cache=skip_cache,
        ).result()

    def submit(
        self,
        operation: type[OperationDefinition],
        *,
        inputs: (
            dict[str, OutputReference | list[str]]
            | list[OutputReference]
            | list[str]
            | None
        ) = None,
        params: dict[str, Any] | None = None,
        step_runner: str | RunnerBase | None = None,
        runner_resources: dict[str, Any] | RunnerResources | None = None,
        batch_strategy: dict[str, Any] | BatchStrategy | None = None,
        environment: str | dict[str, Any] | Environments | None = None,
        tool: dict[str, Any] | ToolSpec | None = None,
        compute_provider: str | dict[str, Any] | ComputeProvider | None = None,
        compute_resources: dict[str, Any] | ComputeResources | None = None,
        failure_policy: FailurePolicy | None = None,
        group_by: GroupByStrategy | None = None,
        compact: bool = True,
        name: str | None = None,
        skip_cache: bool = False,
    ) -> StepFuture:
        """Submit an operation step (non-blocking).

        Composites must use ``submit_composite``; passing one here raises.

        Args:
            operation: OperationDefinition subclass.
            inputs: Input specification (dict, list, or None).
            params: Parameter overrides.
            step_runner: Step runner for execution. None uses pipeline default.
            runner_resources: Resource overrides (cpus, memory_gb, etc.).
            batch_strategy: Batching/scheduling overrides (artifacts_per_unit, etc.).
            environment: Environment override.
            tool: Tool overrides.
            compute_provider: Compute provider override (string or dict).
            compute_resources: Compute-provider hardware override
                (gpu, cpu, memory_gb, timeout).
            failure_policy: Override pipeline-level failure policy.
            group_by: Override the operation's class-level ``group_by`` for
                this step only. ``None`` (default) preserves the operation's
                declared default. A ``GroupByStrategy`` member switches
                pairing strategy and wins over any class-level default.
            compact: Run Delta Lake compaction after commit.
            name: Custom step name. Defaults to operation.name.
            skip_cache: Bypass cache lookups for this step.

        Returns:
            StepFuture for wiring to downstream steps.

        Raises:
            TypeError: If ``operation`` is a CompositeDefinition subclass,
                or if ``group_by`` is not a ``GroupByStrategy`` member.
            ValueError: If any override keys are unrecognized.
        """
        from artisan.composites.base.composite_definition import CompositeDefinition

        # Bundle and coerce every per-step override once at the boundary so
        # every downstream consumer sees one frozen, single-shaped record.
        ov = StepOverrides.from_user(
            params=params,
            step_runner=step_runner,
            runner_resources=runner_resources,
            batch_strategy=batch_strategy,
            environment=environment,
            tool=tool,
            compute_provider=compute_provider,
            compute_resources=compute_resources,
            failure_policy=failure_policy,
            group_by=group_by,
            compact=compact,
            skip_cache=skip_cache,
            name=name,
        )

        # Reject composites at the boundary: their separate submission surface
        # keeps composite-only arguments out of operation signatures.
        # Keep the runtime guard for untyped Python callers while the public
        # annotation continues to describe valid calls only.
        operation_value: object = operation
        if isinstance(operation_value, type) and issubclass(
            operation_value, CompositeDefinition
        ):
            msg = (
                f"submit() rejects composites — got {operation.__name__}. "
                "Use submit_composite() / run_composite() for "
                "CompositeDefinition subclasses."
            )
            raise TypeError(msg)

        # Validate the public call shape before allocating an attempt.
        self._validate_operation_overrides(operation, inputs, ov)

        step_name = ov.name or operation.name

        # API-shape validation is the only phase allowed before allocation.
        step_number = self._current_step
        step_run_id = _generate_step_run_id()
        attempt_started_at = time.perf_counter()
        self._step_run_ids[step_number] = step_run_id

        start_record = self._build_step_start_record(
            operation,
            inputs,
            ov,
            step_name=step_name,
            step_number=step_number,
            step_spec_id=None,
            step_run_id=step_run_id,
            resolved_runner=self._step_runner_name(operation, ov),
        )
        self._step_start_records[step_number] = start_record
        self._step_tracker.create_attempt(start_record)
        self._step_status_readers[step_number] = _StepStatusReader(StepStatus.PENDING)

        # Resolve pre-work skip and cancellation conditions while still pending.
        early = self._check_early_exit(
            step_name,
            operation.outputs,
            inputs,
            step_run_id,
        )
        if early is not None:
            return early

        try:
            self._step_tracker.transition(
                step_run_id,
                StepStatus.PENDING,
                StepStatus.RUNNING,
            )
            self._step_status_readers[step_number].set(StepStatus.RUNNING)

            # Prepare once after running is durable: input resolution, path
            # promotion, cache lookup, and dispatch are operational work.
            prepared_operation = instantiate_operation(operation, ov)
            input_refs = inputs
            already_verified: set[str] = set()
            if _is_file_path_input(inputs):
                promoted_inputs, already_verified = self._handle_file_path_inputs(
                    cast(list[str], inputs),
                    prepared_operation,
                    operation,
                    step_number,
                    step_name,
                    ov.failure_policy,
                    step_run_id,
                )
                inputs = cast(
                    dict[str, OutputReference | list[str]],
                    promoted_inputs,
                )

            prepared_inputs = prepare_inputs(
                inputs,  # type: ignore[arg-type]
                self._config.delta_root,
                self._config.storage.filesystem(),
                group_by=prepared_operation.group_by,
                step_run_ids=self._step_run_ids,
                storage_options=self._config.storage.delta_storage_options(),
                files_root=self._config.files_root,
                already_verified=already_verified,
            )
            if prepared_inputs.inputs and all(
                not artifact_ids for artifact_ids in prepared_inputs.inputs.values()
            ):
                msg = "Input precondition changed after the attempt entered running"
                raise PersistenceIntegrityError(msg)

            step_spec_id = self._prepare_step_spec(
                prepared_operation,
                step_number,
                prepared_inputs,
            )

            if prepared_operation.cacheable and not (
                ov.skip_cache or self._config.skip_cache
            ):
                cached = self._try_cached_step(
                    operation,
                    input_refs,
                    ov,
                    step_spec_id=step_spec_id,
                    step_number=step_number,
                    step_name=step_name,
                    prepared_operation=prepared_operation,
                    step_run_id=step_run_id,
                    attempt_started_at=attempt_started_at,
                )
                if cached is not None:
                    return cached

            return self._dispatch_step(
                operation=operation,
                inputs=prepared_inputs,
                input_refs=input_refs,
                ov=ov,
                step_name=step_name,
                step_number=step_number,
                step_spec_id=step_spec_id,
                prepared_operation=prepared_operation,
                step_run_id=step_run_id,
            )
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"
            result = self._failed_step(
                operation,
                step_name,
                step_run_id,
                error,
                register=True,
                step_spec_id=locals().get("step_spec_id"),
            )
            return self._resolved_step_future(result)

    # =========================================================================
    # submit() helpers
    # =========================================================================

    @staticmethod
    def _validate_operation_overrides(
        operation: type[OperationDefinition],
        inputs: Any,
        ov: StepOverrides,
    ) -> None:
        """Validate all overrides against the operation (fail-fast).

        Checks each override dict against the operation's declared fields
        and raises ValueError with the unrecognized keys before any
        blocking work (predecessor waits, execution) begins. Input
        validation covers role existence, required roles, and upstream
        type compatibility.
        """
        if ov.params:
            _validate_params(operation, ov.params)
        if ov.runner_resources:
            _validate_resources(ov.runner_resources)
        if ov.batch_strategy:
            _validate_execution(ov.batch_strategy)
        if ov.environment is not None:
            _validate_environment(operation, ov.environment)
        if ov.tool:
            _validate_tool(operation, ov.tool)
        if isinstance(ov.compute_provider, dict):
            _validate_compute_provider(ov.compute_provider)
        if isinstance(ov.compute_resources, dict):
            _validate_compute_resources(ov.compute_resources)
        # StepOverrides annotations do not constrain untyped runtime callers.
        group_by: object = ov.group_by
        if group_by is not None and not isinstance(group_by, GroupByStrategy):
            msg = (
                f"group_by must be a GroupByStrategy member, got "
                f"{type(group_by).__name__}: {group_by!r}. Valid members: "
                f"{[s.name for s in GroupByStrategy]}."
            )
            raise TypeError(msg)
        _validate_input_roles(operation, inputs)
        _validate_required_inputs(operation, inputs)
        _validate_input_types(operation, inputs)

    @staticmethod
    def _validate_composite_overrides(
        composite: type[CompositeDefinition],
        inputs: Any,
        params: dict[str, Any] | None,
        runner_resources: dict[str, Any] | None,
        batch_strategy: dict[str, Any] | None,
        environment: str | dict[str, Any] | None,
        tool: dict[str, Any] | None,
        compute_provider: str | dict[str, Any] | None,
        compute_resources: dict[str, Any] | None,
    ) -> None:
        """Validate composite-level overrides (fail-fast).

        Composite-level overrides become defaults for every child step, so
        they are checked for shape here rather than against the composite,
        which is not an OperationDefinition and owns no environment or tool of
        its own. Operation-specific checks — a string environment being
        configured, a tool existing to override — run when each child step is
        submitted inside ``compose()``.

        Raises:
            ValueError: If an override dict carries unrecognized keys or a
                silently-inactive provider configuration.
        """
        from artisan.schemas.operation_config.tool_spec import ToolSpec

        if params:
            _validate_params(composite, params)
        if runner_resources:
            _validate_resources(runner_resources)
        if batch_strategy:
            _validate_execution(batch_strategy)
        if isinstance(environment, dict):
            # Dict form is validated by shape/key only; the operation arg is
            # unused for the dict branch of _validate_environment.
            _validate_environment(composite, environment)  # type: ignore[arg-type]
        if tool:
            unknown = set(tool) - set(ToolSpec.model_fields)
            if unknown:
                msg = (
                    f"Unknown tool keys: {sorted(unknown)}. "
                    f"Valid keys: {sorted(ToolSpec.model_fields)}"
                )
                raise ValueError(msg)
        if isinstance(compute_provider, dict):
            _validate_compute_provider(compute_provider)
        if isinstance(compute_resources, dict):
            _validate_compute_resources(compute_resources)
        _validate_input_roles(composite, inputs)
        _validate_required_inputs(composite, inputs)
        _validate_input_types(composite, inputs)

    def _check_early_exit(
        self,
        step_name: str,
        operation_outputs: dict[str, OutputSpec],
        inputs: Any,
        step_run_id: str,
    ) -> StepFuture | None:
        """Check stop/cancel conditions and wait for predecessors.

        The check covers a prior stop, a cancellation request, predecessor
        completion, and inputs that are already known to have no usable values.

        Used by ``submit()``.

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
            return self._skip_step(
                step_name,
                operation_outputs,
                "pipeline_stopped",
                step_run_id,
            )

        if self._cancel_event.is_set():
            logger.info(
                "Step %d (%s): pipeline cancellation confirmed before work.",
                self._current_step,
                step_name,
            )
            result = self._cancel_step(
                step_name,
                operation_outputs,
                step_run_id,
                StepStatus.PENDING,
                register=True,
            )
            return self._resolved_step_future(result)

        self._wait_for_predecessors(inputs)

        # Re-check cancel — may have been set while blocked on predecessors
        if self._cancel_event.is_set():
            result = self._cancel_step(
                step_name,
                operation_outputs,
                step_run_id,
                StepStatus.PENDING,
                register=True,
            )
            return self._resolved_step_future(result)

        if self._inputs_known_empty(inputs):
            self._stopped = True
            return self._skip_step(
                step_name,
                operation_outputs,
                "empty_inputs",
                step_run_id,
            )

        return None

    def _inputs_known_empty(self, inputs: Any) -> bool:
        """Return whether declared inputs have no usable values before work."""
        if inputs is None or inputs == {}:
            return False
        values = list(inputs.values()) if isinstance(inputs, dict) else inputs
        if not isinstance(values, list):
            values = [values]
        return all(self._input_value_known_empty(value) for value in values)

    def _input_value_known_empty(self, value: Any) -> bool:
        """Resolve one literal or upstream reference for pre-work emptiness."""
        if isinstance(value, list):
            return all(self._input_value_known_empty(item) for item in value)
        if isinstance(value, OutputReference):
            source = next(
                (
                    result
                    for result in self._step_results
                    if result.step_number == value.source_step
                ),
                None,
            )
            return source is not None and (
                source.status not in {StepStatus.SUCCEEDED, StepStatus.PARTIAL}
                or source.succeeded_count == 0
            )
        return value is None

    def _prepare_step_spec(
        self,
        operation: OperationDefinition,
        step_number: int,
        prepared_inputs: PreparedInputs,
    ) -> str:
        """Compute a deterministic step spec ID from a prepared operation.

        The step_spec_id is a content hash of (operation name, step number,
        merged params, upstream spec IDs, and config overrides). Two runs
        with identical inputs and configuration produce the same spec ID,
        enabling the step-level cache to skip re-execution.

        Returns:
            Deterministic step spec ID for the prepared operation.
        """
        full_params = serialize_params(operation)

        config_overrides = effective_config_payload(operation)

        return compute_step_spec_id(
            operation_name=operation.name,
            step_number=step_number,
            params=full_params if full_params else None,
            inputs=prepared_inputs.cache_inputs,
            config_overrides=config_overrides,
        )

    def _resolve_step_runner(
        self,
        prepared_operation: type[OperationDefinition] | OperationDefinition,
        ov: StepOverrides,
    ) -> RunnerBase:
        """Resolve the effective runner for one step."""
        if is_curator_operation(prepared_operation):
            return Runner.LOCAL
        if ov.step_runner is not None:
            return resolve_runner(ov.step_runner)
        return self._default_step_runner

    def _step_runner_name(
        self,
        operation: type[OperationDefinition],
        ov: StepOverrides,
    ) -> str:
        """Return the auditable runner name without doing provider work."""
        if is_curator_operation(operation):
            return Runner.LOCAL.name
        if isinstance(ov.step_runner, RunnerBase):
            return ov.step_runner.name
        if isinstance(ov.step_runner, str):
            return ov.step_runner
        return self._default_step_runner.name

    def _default_runner_metadata(self) -> dict[str, Any]:
        """Build the durable, core-owned default-runner metadata."""
        # ``compute_backend`` is the effective runner and may reflect a curator's
        # forced-local route or a step override, so it cannot recover the default.
        metadata: dict[str, Any] = {
            _PIPELINE_DEFAULT_RUNNER_OPTION: self._config.default_step_runner,
        }
        if type(self._default_step_runner) is LocalRunner:
            metadata[_PIPELINE_DEFAULT_LOCAL_RUNNER_OPTION] = {
                "default_max_workers": self._default_step_runner.default_max_workers,
            }
        return metadata

    def _build_step_start_record(
        self,
        operation: type[OperationDefinition],
        inputs: Any,
        ov: StepOverrides,
        *,
        step_name: str,
        step_number: int,
        step_spec_id: str | None,
        step_run_id: str,
        resolved_runner: RunnerBase | str,
    ) -> StepStartRecord:
        """Build common persisted metadata for executed and cached steps."""
        # Keep the internal resources/execution keys stable across their public
        # API renames; StepOverrides has already canonicalized the value shapes.
        compute_options_data = {
            "resources": ov.runner_resources or {},
            "execution": ov.batch_strategy or {},
            "environment": ov.environment if ov.environment is not None else {},
            "tool": ov.tool or {},
            "compute_provider": (
                ov.compute_provider if ov.compute_provider is not None else {}
            ),
            "compute_resources": ov.compute_resources or {},
            "group_by": (ov.group_by.value if ov.group_by is not None else None),
            **self._default_runner_metadata(),
        }
        return StepStartRecord(
            step_run_id=step_run_id,
            step_spec_id=step_spec_id,
            step_number=step_number,
            step_name=step_name,
            operation_class=_qualified_name(operation),
            params_json=json.dumps(ov.params or {}, default=_set_default),
            input_refs_json=_serialize_input_refs(inputs),
            compute_backend=(
                resolved_runner.name
                if isinstance(resolved_runner, RunnerBase)
                else resolved_runner
            ),
            compute_options_json=json.dumps(compute_options_data, default=_set_default),
            output_roles_json=json.dumps(sorted(operation.outputs.keys())),
            output_types_json=json.dumps(self._build_output_types(operation.outputs)),
        )

    def _try_cached_step(
        self,
        operation: type[OperationDefinition],
        inputs: Any,
        ov: StepOverrides,
        *,
        step_spec_id: str,
        step_number: int,
        step_name: str,
        prepared_operation: OperationDefinition,
        step_run_id: str,
        attempt_started_at: float,
    ) -> StepFuture | None:
        """Return a resolved StepFuture if step is cached, None otherwise.

        A committed hit terminalizes the current attempt as a cache hit.
        Cancellation at the final persistence boundary records the current
        attempt as cancelled without writing a reuse relation.
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
        current_metadata = {
            key: value
            for key, value in cached.result.metadata.items()
            if key != "timings"
        }
        result = cached.result.model_copy(
            update={
                "step_name": step_name,
                "step_number": step_number,
                "step_run_id": step_run_id,
                "disposition": StepDisposition.CACHE_HIT,
                "cancellation_status": None,
                "error": (
                    cached.result.error
                    if cached.result.status == StepStatus.PARTIAL
                    else None
                ),
                "metadata": current_metadata,
            }
        )
        committed_result = self._commit_whole_step_reuse(
            step_run_id,
            cached.execution_run_ids,
            step_number=step_number,
            operation_name=prepared_operation.name,
            result=result,
            step_spec_id=step_spec_id,
            attempt_started_at=attempt_started_at,
        )
        if committed_result is not None:
            result = committed_result
            self._step_status_readers[step_number].set(result.status)
            self._compact_after_terminal(ov.compact)
        else:
            result = self._cancel_step(
                step_name,
                operation.outputs,
                step_run_id,
                StepStatus.RUNNING,
                register=True,
            )
            return self._resolved_step_future(result)

        self._step_spec_ids[step_number] = step_spec_id
        self._step_run_ids[step_number] = step_run_id
        self._step_results.append(result)
        self._register_step(step_name, step_number, operation.outputs)
        self._named_steps.setdefault(result.step_name, []).append(result)
        self._current_step += 1

        return self._resolved_step_future(result)

    def _commit_whole_step_reuse(
        self,
        current_step_run_id: str,
        cached_execution_run_ids: tuple[str, ...],
        *,
        step_number: int,
        operation_name: str,
        result: StepResult,
        step_spec_id: str,
        attempt_started_at: float,
    ) -> StepResult | None:
        """Validate, stage, and commit a whole-step cache relation.

        Returns:
            Committed result, or None if cancellation wins before staging.
        """
        from artisan.storage.core.run_scope import validate_cached_executions
        from artisan.storage.io.staging import StagingManager

        fs = self._config.storage.filesystem()
        options = self._config.storage.delta_storage_options()
        execution_ids = validate_cached_executions(
            self._config.delta_root,
            current_step_run_id,
            set(cached_execution_run_ids),
            fs=fs,
            storage_options=options,
            files_root=self._config.files_root,
        )
        if self._cancel_event.is_set():
            return None
        staging = StagingManager(self._config.staging_root, fs)
        staging.stage_cache_reuse(
            current_step_run_id,
            execution_ids,
            step_number=step_number,
            operation_name=operation_name,
        )
        return self._commit_execution_result(
            result,
            (),
            step_name=result.step_name,
            step_spec_id=step_spec_id,
            operation_name=operation_name,
            attempt_started_at=attempt_started_at,
        )

    def _commit_execution_result(
        self,
        result: StepResult,
        execution_run_ids: tuple[str, ...],
        *,
        step_name: str,
        step_spec_id: str,
        operation_name: str,
        attempt_started_at: float,
    ) -> StepResult:
        """Persist worker evidence and its terminal candidate as one commit."""
        from artisan.storage.io.commit import DeltaCommitter
        from artisan.storage.io.commit_plan import build_commit_plan
        from artisan.storage.io.staging import StagingManager

        step_run_id = result.step_run_id
        if step_run_id is None:
            msg = "Persistence-bearing step result has no step_run_id"
            raise PersistenceIntegrityError(msg)
        finalized = result.model_copy(
            update={
                "step_name": step_name,
                "duration_seconds": time.perf_counter() - attempt_started_at,
                "step_run_id": step_run_id,
            }
        )
        if finalized.cancellation_status is not None:
            self._step_tracker.record_cancellation(
                step_run_id,
                StepStatus.RUNNING,
                CancellationAcknowledgement(CancellationStatus.REQUESTED),
            )
            self._step_tracker.record_cancellation(
                step_run_id,
                StepStatus.RUNNING,
                CancellationAcknowledgement(
                    finalized.cancellation_status,
                    finalized.error,
                ),
            )
        candidate = self._step_tracker.prepare_terminal_candidate(
            step_run_id,
            StepStatus.RUNNING,
            finalized.status,
            step_spec_id=step_spec_id,
            result=finalized,
        )
        fs = self._config.storage.filesystem()
        options = self._config.storage.delta_storage_options()
        staging = StagingManager(self._config.staging_root, fs)
        staging.stage_orchestrator_dataframe(
            candidate,
            TablePath.STEPS.value,
            commit_kind="step_result",
            step_run_id=step_run_id,
            step_number=finalized.step_number,
            operation_name=operation_name,
        )
        plan = build_commit_plan(
            delta_root=self._config.delta_root,
            staging_root=self._config.staging_root,
            fs=fs,
            commit_kind="step_result",
            step_run_id=step_run_id,
            step_number=finalized.step_number,
            operation_name=operation_name,
            execution_run_ids=execution_run_ids,
        )
        DeltaCommitter(
            self._config.delta_root,
            staging,
            fs=fs,
            storage_options=options,
        ).commit_logical(
            plan,
            preserve_staging=self._config.preserve_staging,
        )
        return self._step_tracker.current_state(step_run_id).to_step_result()

    def _handle_file_path_inputs(
        self,
        inputs: list[str],
        prepared_operation: OperationDefinition,
        operation: type[OperationDefinition],
        step_number: int,
        step_name: str,
        failure_policy: FailurePolicy | None,
        step_run_id: str,
    ) -> tuple[dict[str, list[str]], set[str]]:
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
        if not is_curator_operation(prepared_operation):
            msg = (
                "Raw file paths are not allowed for creator operations. "
                "Use a curator ingest operation to bring files into the "
                "pipeline first."
            )
            raise ValueError(msg)

        promoted, _count, verified = _promote_file_paths_to_store(
            inputs,
            self._config,
            step_number,
            operation.name,
            step_run_id,
        )
        if promoted is None:
            msg = "File promotion returned no resolved inputs"
            raise RuntimeError(msg)
        return promoted, verified

    def _dispatch_step(
        self,
        operation: type[OperationDefinition],
        inputs: PreparedInputs,
        input_refs: Any,
        ov: StepOverrides,
        *,
        step_name: str,
        step_number: int,
        step_spec_id: str,
        prepared_operation: OperationDefinition,
        step_run_id: str,
    ) -> StepFuture:
        """Register and submit an already-running lifecycle attempt."""
        resolved_runner = self._resolve_step_runner(prepared_operation, ov)
        if self._current_step == 0:
            self._install_signal_handlers()
        self._register_step(step_name, step_number, operation.outputs)
        self._current_step += 1
        self._step_spec_ids[step_number] = step_spec_id
        self._step_run_ids[step_number] = step_run_id

        output_types_map = self._build_output_types(operation.outputs)

        def _run() -> StepResult:
            if self._cancel_event.is_set():
                return self._cancel_step(
                    step_name,
                    operation.outputs,
                    step_run_id,
                    StepStatus.RUNNING,
                    register=False,
                )

            logger.info(
                "Step %d (%s) starting... [step_runner=%s]",
                step_number,
                step_name,
                resolved_runner.name,
            )
            start = time.perf_counter()
            try:
                # Snapshot step_run_ids for scoped output resolution
                upstream_step_run_ids = dict(self._step_run_ids)
                result = execute_step(
                    operation=prepared_operation,
                    inputs=inputs,
                    ov=ov,
                    step_runner=resolved_runner,
                    step_number=step_number,
                    config=self._config,
                    cancel_event=self._cancel_event,
                    step_run_id=step_run_id,
                    step_run_ids=upstream_step_run_ids,
                    persist_result=lambda result, execution_ids: (
                        self._commit_execution_result(
                            result,
                            execution_ids,
                            step_name=step_name,
                            step_spec_id=step_spec_id,
                            operation_name=prepared_operation.name,
                            attempt_started_at=start,
                        )
                    ),
                )
                elapsed = time.perf_counter() - start

                if result.status == StepStatus.CANCELLED:
                    logger.info(
                        "Step %d (%s): cancelled.",
                        step_number,
                        step_name,
                    )
                    return self._cancel_step(
                        step_name,
                        operation.outputs,
                        step_run_id,
                        StepStatus.RUNNING,
                        register=False,
                    )

                if result.status == StepStatus.SKIPPED:
                    result = result.model_copy(
                        update={
                            "step_name": step_name,
                            "duration_seconds": elapsed,
                            "step_run_id": step_run_id,
                        }
                    )
                    self._step_tracker.transition(
                        step_run_id,
                        StepStatus.RUNNING,
                        StepStatus.SKIPPED,
                        step_spec_id=step_spec_id,
                        result=result,
                    )
                self._step_status_readers[step_number].set(result.status)
                self._compact_after_terminal(ov.compact)
                logger.info(
                    "Step %d (%s) %s in %.1fs [%d/%d succeeded]",
                    step_number,
                    step_name,
                    result.status.value,
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
                    step_name,
                    elapsed,
                    error_msg,
                )
                return self._failed_step(
                    operation,
                    step_name,
                    step_run_id,
                    error_msg,
                    register=False,
                    step_spec_id=step_spec_id,
                    duration_seconds=elapsed,
                )

        ctx = contextvars.copy_context()
        assert self._executor is not None, "executor must be live during submit"
        cf_future = self._executor.submit(ctx.run, _run)

        future = StepFuture(
            step_number=step_number,
            step_name=step_name,
            output_roles=frozenset(output_types_map.keys()),
            output_types=output_types_map,
            future=cf_future,
            status_reader=self._step_status_readers[step_number].get,
        )
        self._active_futures[step_number] = future
        return future

    def _settle_cancelled_futures(self) -> None:
        """Record steps whose closure was cancelled before it could execute."""
        for step_number, future in self._active_futures.items():
            start_record = self._step_start_records.get(step_number)
            if start_record is None or not future.cancelled_before_start:
                continue
            current = self._step_tracker.current_state(start_record.step_run_id)
            self._cancel_step(
                future.step_name,
                {},
                start_record.step_run_id,
                current.status,
                register=False,
            )

    def submit_composite(
        self,
        composite: type[CompositeDefinition],
        *,
        inputs: (dict[str, OutputReference | list[str]] | None) = None,
        params: dict[str, Any] | None = None,
        name: str | None = None,
        step_runner: str | RunnerBase | None = None,
        runner_resources: dict[str, Any] | RunnerResources | None = None,
        batch_strategy: dict[str, Any] | BatchStrategy | None = None,
        environment: str | dict[str, Any] | Environments | None = None,
        tool: dict[str, Any] | ToolSpec | None = None,
        compute_provider: str | dict[str, Any] | ComputeProvider | None = None,
        compute_resources: dict[str, Any] | ComputeResources | None = None,
        failure_policy: FailurePolicy | None = None,
        compact: bool = True,
        skip_cache: bool = False,
    ) -> CompositeResult:
        """Submit a composite (non-blocking).

        Each internal ``ctx.run()`` becomes its own pipeline step with
        independent worker dispatch, batching, caching, and provenance. Every
        override kwarg is a **default for each child step**: the context
        applies it to any ``ctx.run()`` that does not set the same knob
        (per-knob, wholesale — no deep merge). ``params`` is the composite's
        own parameters, consumed inside ``compose()``, and is never forwarded
        to child steps.

        Args:
            composite: CompositeDefinition subclass.
            inputs: Input specification for the composite.
            params: Parameter overrides for the composite itself.
            name: Step-name prefix for child steps. Defaults to composite.name.
            step_runner: Default step runner for child steps.
            runner_resources: Default resource overrides for child steps.
            batch_strategy: Default batching overrides for child steps.
            environment: Default environment override for child steps.
            tool: Default tool overrides for child steps.
            compute_provider: Default compute provider for child steps.
            compute_resources: Default hardware resources for child steps.
            failure_policy: Default failure policy for child steps.
            compact: Default Delta compaction flag for child steps.
            skip_cache: Default cache-bypass flag for child steps.

        Returns:
            CompositeResult exposing ``.output(role) -> OutputReference`` for
            downstream wiring and ``.wait()`` to block on child completion.

        Raises:
            TypeError: If ``composite`` is not a CompositeDefinition subclass.
            ValueError: If any composite-level override key is unrecognized.
        """
        from artisan.composites.base.composite_context import CompositeContext
        from artisan.composites.base.composite_definition import CompositeDefinition
        from artisan.composites.base.results import CompositeResult

        # Bundle + coerce the child-step defaults once at the boundary.
        # ``group_by`` is operation-only, so it is never populated here.
        ov = StepOverrides.from_user(
            step_runner=step_runner,
            runner_resources=runner_resources,
            batch_strategy=batch_strategy,
            environment=environment,
            tool=tool,
            compute_provider=compute_provider,
            compute_resources=compute_resources,
            failure_policy=failure_policy,
            compact=compact,
            skip_cache=skip_cache,
        )

        # Keep the runtime guard for untyped Python callers while the public
        # annotation continues to describe valid calls only.
        composite_value: object = composite
        if not (
            isinstance(composite_value, type)
            and issubclass(composite_value, CompositeDefinition)
        ):
            msg = (
                "submit_composite() requires a CompositeDefinition subclass, "
                f"got {composite!r}"
            )
            raise TypeError(msg)

        # Fail-fast validation of composite-level overrides before any work.
        self._validate_composite_overrides(
            composite,
            inputs,
            params,
            ov.runner_resources,
            ov.batch_strategy,
            ov.environment,
            ov.tool,
            ov.compute_provider,
            ov.compute_resources,
        )

        self._wait_for_predecessors(inputs)

        init_kwargs: dict[str, Any] = {}
        if params:
            init_kwargs["params"] = params
        instance = composite(**init_kwargs)

        input_refs: dict[str, OutputReference] = {}
        if isinstance(inputs, dict):
            for role, ref in inputs.items():
                if isinstance(ref, OutputReference):
                    input_refs[role] = ref

        step_name_prefix = name or composite.name

        # Composite-level overrides become per-knob defaults for child steps.
        # params and name are deliberately excluded — params belongs to the
        # composite's own Params, and name is the child-step prefix.
        step_defaults: dict[str, Any] = {
            "step_runner": ov.step_runner,
            "runner_resources": ov.runner_resources,
            "batch_strategy": ov.batch_strategy,
            "environment": ov.environment,
            "tool": ov.tool,
            "compute_provider": ov.compute_provider,
            "compute_resources": ov.compute_resources,
            "failure_policy": ov.failure_policy,
            "compact": ov.compact,
            "skip_cache": ov.skip_cache,
        }

        ctx = CompositeContext(
            pipeline=self,
            input_refs=input_refs,
            composite=instance,
            step_name_prefix=step_name_prefix,
            step_defaults=step_defaults,
        )
        instance.compose(ctx)

        return CompositeResult(
            output_map=ctx.get_output_map(),
            output_types=ctx.get_output_types(),
            child_futures=ctx.get_child_futures(),
        )

    def run_composite(
        self,
        composite: type[CompositeDefinition],
        *,
        inputs: (dict[str, OutputReference | list[str]] | None) = None,
        params: dict[str, Any] | None = None,
        name: str | None = None,
        step_runner: str | RunnerBase | None = None,
        runner_resources: dict[str, Any] | RunnerResources | None = None,
        batch_strategy: dict[str, Any] | BatchStrategy | None = None,
        environment: str | dict[str, Any] | Environments | None = None,
        tool: dict[str, Any] | ToolSpec | None = None,
        compute_provider: str | dict[str, Any] | ComputeProvider | None = None,
        compute_resources: dict[str, Any] | ComputeResources | None = None,
        failure_policy: FailurePolicy | None = None,
        compact: bool = True,
        skip_cache: bool = False,
    ) -> CompositeResult:
        """Execute a composite (blocking).

        Submits the composite and blocks until every child step completes via
        ``CompositeResult.wait()``. See ``submit_composite`` for argument
        details.

        Returns:
            The CompositeResult after all child steps have resolved.

        Raises:
            TypeError: If ``composite`` is not a CompositeDefinition subclass.
            ValueError: If any composite-level override key is unrecognized.
        """
        result = self.submit_composite(
            composite,
            inputs=inputs,
            params=params,
            name=name,
            step_runner=step_runner,
            runner_resources=runner_resources,
            batch_strategy=batch_strategy,
            environment=environment,
            tool=tool,
            compute_provider=compute_provider,
            compute_resources=compute_resources,
            failure_policy=failure_policy,
            compact=compact,
            skip_cache=skip_cache,
        )
        return result.wait()

    # =========================================================================
    # Internal helpers
    # =========================================================================

    def _wait_for_predecessors(self, inputs: Any) -> None:
        """Block until all upstream step futures are done.

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
            self._executor.shutdown(wait=wait, cancel_futures=cancelled)
            self._executor = None

    def finalize(self) -> dict[str, Any]:
        """Finalize pipeline execution and return summary.

        Shuts down the executor and waits for all running step closures to
        finish their terminal writes before collecting their futures. Queued
        closures are cancelled when cancellation has been requested.

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

        self._shutdown_executor()
        for step_num, future in self._active_futures.items():
            try:
                future.result()
            except CancelledError:
                continue
            except Exception as exc:
                logger.error(
                    "Step %d future failed during finalize: %s: %s",
                    step_num,
                    type(exc).__name__,
                    exc,
                )

        if self._cancel_event.is_set():
            self._settle_cancelled_futures()
        self._restore_signal_handlers()

        # Results may arrive out of order (sync skips before async completions)
        self._step_results.sort(key=lambda r: r.step_number)

        total_elapsed = time.time() - self._start_time
        all_ok = bool(self._step_results) and all(
            result.status in {StepStatus.SUCCEEDED, StepStatus.SKIPPED}
            for result in self._step_results
        )
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
                    "status": r.status.value,
                    "total": r.total_count,
                    "succeeded": r.succeeded_count,
                    "failed": r.failed_count,
                    "duration_seconds": r.duration_seconds,
                }
                for r in self._step_results
            ],
            "overall_success": all_ok,
        }
        self._finalized = True
        return self._summary
