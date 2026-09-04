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
from artisan.orchestration.engine.step_executor import (
    execute_step,
    instantiate_operation,
)
from artisan.orchestration.engine.step_tracker import StepTracker
from artisan.orchestration.runners import Runner, RunnerBase, resolve_runner
from artisan.orchestration.runners.local import LocalRunner
from artisan.orchestration.step_future import StepFuture
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.schemas.enums import CachePolicy, FailurePolicy, GroupByStrategy
from artisan.schemas.execution.batch_strategy import BatchStrategy
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.schemas.operation_config.compute import ComputeProvider
from artisan.schemas.operation_config.compute_resources import ComputeResources
from artisan.schemas.operation_config.environments import Environments
from artisan.schemas.operation_config.runner_resources import RunnerResources
from artisan.schemas.operation_config.tool_spec import ToolSpec
from artisan.schemas.orchestration.output_reference import OutputReference
from artisan.schemas.orchestration.pipeline_config import PipelineConfig
from artisan.schemas.orchestration.step_overrides import StepOverrides
from artisan.schemas.orchestration.step_result import StepResult
from artisan.schemas.orchestration.step_start_record import StepStartRecord
from artisan.schemas.orchestration.step_state import StepState
from artisan.schemas.specs.output_spec import OutputSpec
from artisan.utils.hashing import (
    compute_artifact_id,
    compute_step_spec_id,
    digest_utf8,
    effective_config_payload,
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


def _generate_step_run_id(step_spec_id: str) -> str:
    """Generate a unique 32-char hex step run identifier."""
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
    requested_name: str | None = None,
) -> _StoredDefaultRunner | None:
    """Read a pipeline's default runner from persisted step options.

    Args:
        steps: Completed states for one pipeline run.
        requested_name: Explicit runner name supplied for resume, when any.

    Returns:
        Stored runner metadata. Legacy records require the caller to state the
        historical default because they persist only each step's effective runner.

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
        if requested_name is None:
            msg = (
                "Legacy step records do not persist the historical pipeline "
                "default; pass an explicit default_step_runner to resume safely"
            )
            raise ValueError(msg)
        stored_name = requested_name
    return _StoredDefaultRunner(stored_name, local_max_workers)


def _is_file_path_input(inputs: Any) -> bool:
    """Return True if inputs is a non-empty list of raw file path strings."""
    return isinstance(inputs, list) and bool(inputs) and isinstance(inputs[0], str)


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
    from fsspec import AbstractFileSystem
    from fsspec.implementations.local import LocalFileSystem

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
                invalid_paths.append(f"Not found: {path_str}")
                continue
            if not fs.isfile(stripped):
                invalid_paths.append(f"Not a file: {path_str}")
                continue
        except Exception as exc:
            # Invalid URI, missing fsspec driver (e.g. gcs:// without
            # gcsfs installed), or auth/network failure on fs.exists.
            # User-provided paths must never crash pipeline kickoff —
            # they surface alongside not-found paths in the warning below.
            invalid_paths.append(f"Inaccessible: {path_str} ({exc})")
            continue
        valid_paths.append((path_str, fs, stripped))

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
    for original, fs, stripped in valid_paths:
        with fs.open(stripped, "rb") as f:
            content = f.read()
        content_hash = compute_artifact_id(content)
        size_bytes = len(content)
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
    stored_runner: _StoredDefaultRunner | None,
    runtime_runner: RunnerBase | None,
    run_id: str,
) -> RunnerBase | None:
    """Reconstruct or validate the narrowly persisted LocalRunner config."""
    if stored_runner is None or stored_runner.local_default_max_workers is None:
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
        self._step_start_records: dict[int, StepStartRecord] = {}
        self._cancelled_result_lock = threading.Lock()
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
        default_step_runner: str | RunnerBase = "local",
        default_compute_provider: str = "local",
        preserve_staging: bool = False,
        preserve_working: bool = False,
        recover_staging: bool = True,
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
            cache_policy: Controls when completed steps qualify as cache hits.
            default_step_runner: Default step runner for step execution. Accepts a
                ``RunnerBase`` instance or a built-in string name (currently
                ``"local"``). External providers are passed as instances.
            default_compute_provider: Default compute provider for step execution.
            preserve_staging: Debug flag to preserve staging files after commit.
            preserve_working: Debug flag to preserve sandbox after execution.
            recover_staging: Commit leftover staging files from prior crashed
                runs at pipeline init. Defaults to True.
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
            default_compute_provider=default_compute_provider,
            preserve_staging=preserve_staging,
            preserve_working=preserve_working,
            recover_staging=recover_staging,
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
        default_compute_provider: str = "local",
        preserve_staging: bool = False,
        preserve_working: bool = False,
        recover_staging: bool = True,
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
                supplied as matching instances. For legacy records without a
                stored default, an omitted value falls back to local.
            files_root: Root path for Artisan-managed external files. If None,
                derives a sibling path from a local delta_root.
            failure_policy: Default failure handling for subsequent steps.
            cache_policy: Controls when completed steps qualify as cache hits.
            default_compute_provider: Default compute provider for subsequent
                step execution.
            preserve_staging: Preserve staging files after commit.
            preserve_working: Preserve worker sandboxes after execution.
            recover_staging: Commit leftover staging from interrupted runs.
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
        completed_steps = tracker.load_completed_steps(pipeline_run_id)

        if not completed_steps:
            msg = "No completed steps found"
            if pipeline_run_id:
                msg += f" for run '{pipeline_run_id}'"
            raise ValueError(msg)

        run_id = pipeline_run_id or completed_steps[0].pipeline_run_id
        runtime_runner: RunnerBase | None
        requested_runner_name: str | None
        if isinstance(default_step_runner, RunnerBase):
            runtime_runner = default_step_runner
            requested_runner_name = default_step_runner.name
        else:
            runtime_runner = None
            requested_runner_name = default_step_runner
        stored_runner = _load_stored_default_runner(
            completed_steps,
            requested_name=requested_runner_name,
        )
        stored_runner_name = stored_runner.name if stored_runner is not None else None
        config_kwargs: dict[str, Any] = {
            "name": name or _extract_name_from_run_id(run_id),
            "pipeline_run_id": run_id,
            "delta_root": delta_root,
            "staging_root": staging_root,
            "files_root": files_root,
            "failure_policy": failure_policy,
            "cache_policy": cache_policy,
            "default_compute_provider": default_compute_provider,
            "preserve_staging": preserve_staging,
            "preserve_working": preserve_working,
            "recover_staging": recover_staging,
            "skip_cache": skip_cache,
            "storage": storage,
        }
        if (
            stored_runner_name is not None
            and requested_runner_name is not None
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
        resumed_runner_name = stored_runner_name or requested_runner_name
        if resumed_runner_name is not None:
            config_kwargs["default_step_runner"] = resumed_runner_name
        if working_root is not None:
            config_kwargs["working_root"] = working_root
        config = PipelineConfig(**config_kwargs)

        instance = cls(config, default_step_runner=runtime_runner)
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

        # 0. Bundle + coerce every per-step override once at the boundary so
        #    every downstream consumer sees one frozen, single-shaped record.
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

        # 1. Reject composites at the boundary — they have a separate surface
        #    (``submit_composite``/``run_composite``) so composite-only kwargs
        #    don't pollute operation signatures.
        if isinstance(operation, type) and issubclass(operation, CompositeDefinition):
            msg = (
                f"submit() rejects composites — got {operation.__name__}. "
                "Use submit_composite() / run_composite() for "
                "CompositeDefinition subclasses."
            )
            raise TypeError(msg)

        # 2. Fail-fast validation before any blocking work. Checks params,
        #    resources, execution, environment, and tool keys against the
        #    operation's declared fields, plus input role/type compatibility.
        self._validate_operation_overrides(operation, inputs, ov)

        step_name = ov.name or operation.name

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
            ov,
            step_number,
            inputs,
        )

        # 5. Cache check: if a prior run produced identical spec_id, return
        #    the cached StepResult immediately without re-executing.
        if not (ov.skip_cache or self._config.skip_cache):
            cached = self._try_cached_step(
                operation,
                inputs,
                ov,
                step_spec_id=step_spec_id,
                step_number=step_number,
                step_name=step_name,
                temp_instance=temp_instance,
            )
            if cached is not None:
                return cached

        # 6. File path promotion: if the user passed raw file paths (list of
        #    strings), validate them and commit FileRefArtifacts to Delta Lake
        #    so downstream execution sees artifact IDs, not filesystem paths.
        #    Only curator operations accept raw paths; creators must receive
        #    artifact references from a prior ingest step.
        if _is_file_path_input(inputs):
            file_result = self._handle_file_path_inputs(
                cast(list[str], inputs),
                temp_instance,
                operation,
                step_number,
                step_spec_id,
                step_name,
                ov.failure_policy,
            )
            if isinstance(file_result, StepFuture):
                return file_result
            inputs = file_result  # type: ignore[assignment]

        # 7. Dispatch: register the step, resolve its runner,
        #    record the step start in Delta, and submit the _run() closure to
        #    the thread pool executor for background execution.
        return self._dispatch_step(
            operation=operation,
            inputs=inputs,
            ov=ov,
            step_name=step_name,
            step_number=step_number,
            step_spec_id=step_spec_id,
            temp_instance=temp_instance,
        )

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
        if ov.group_by is not None and not isinstance(ov.group_by, GroupByStrategy):
            msg = (
                f"group_by must be a GroupByStrategy member, got "
                f"{type(ov.group_by).__name__}: {ov.group_by!r}. Valid members: "
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
    ) -> StepFuture | None:
        """Check stop/cancel conditions and wait for predecessors.

        Three gates are checked in order:
        1. ``_stopped`` — set when a prior step had empty inputs, halting
           the pipeline to prevent meaningless downstream work.
        2. ``_cancel_event`` — set by SIGINT/SIGTERM or explicit cancel().
        3. Predecessor wait — blocks until all upstream StepFutures
           complete, then re-checks cancellation (which may have been
           signalled while waiting).

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
        ov: StepOverrides,
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
        temp_instance = instantiate_operation(operation, ov)
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

        config_overrides = effective_config_payload(temp_instance)

        input_spec = self._build_input_spec(inputs)
        step_spec_id = compute_step_spec_id(
            operation_name=operation.name,
            step_number=step_number,
            params=full_params if full_params else None,
            input_spec=input_spec,
            config_overrides=config_overrides,
        )

        return step_spec_id, temp_instance

    def _resolve_step_runner(
        self,
        temp_instance: OperationDefinition,
        ov: StepOverrides,
    ) -> RunnerBase:
        """Resolve the effective runner for one step."""
        if is_curator_operation(temp_instance):
            return Runner.LOCAL
        if ov.step_runner is not None:
            return resolve_runner(ov.step_runner)
        return self._default_step_runner

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
        step_spec_id: str,
        step_run_id: str,
        resolved_runner: RunnerBase,
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
            compute_backend=resolved_runner.name,
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
        temp_instance: OperationDefinition,
    ) -> StepFuture | None:
        """Return a resolved StepFuture if step is cached, None otherwise.

        A hit writes one current-run completed row, then records the result in
        memory and advances the step counter without executing the operation.
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
        result = cached.model_copy(
            update={"step_name": step_name, "step_number": step_number}
        )
        resolved_runner = self._resolve_step_runner(temp_instance, ov)
        # Cached outputs remain owned by their original execution attempt. Reusing
        # that identity keeps downstream resolution scoped to the actual rows.
        start_record = self._build_step_start_record(
            operation,
            inputs,
            ov,
            step_name=step_name,
            step_number=step_number,
            step_spec_id=step_spec_id,
            step_run_id=result.step_run_id or "",
            resolved_runner=resolved_runner,
        )
        self._step_tracker.record_step_completed(start_record, result)

        self._step_spec_ids[step_number] = step_spec_id
        if result.step_run_id:
            self._step_run_ids[step_number] = result.step_run_id
        self._step_results.append(result)
        self._register_step(step_name, step_number, operation.outputs)
        self._named_steps.setdefault(result.step_name, []).append(result)
        self._current_step += 1

        resolved: Future[StepResult] = Future()
        resolved.set_result(result)
        return StepFuture(
            step_number=step_number,
            step_name=result.step_name,
            output_roles=result.output_roles,
            output_types=result.output_types,
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
        ov: StepOverrides,
        *,
        step_name: str,
        step_number: int,
        step_spec_id: str,
        temp_instance: OperationDefinition,
    ) -> StepFuture:
        """Register step, resolve step_runner, and submit execution to thread pool.

        This is the final phase of submit(). It performs three things
        synchronously on the calling thread, then hands off to the
        single-threaded executor:

        1. **Bookkeeping** — registers the step in the step registry,
           advances the step counter, installs signal handlers (first
           dispatch only), and generates a unique step_run_id.
        2. **Step runner resolution** — curator operations are forced to
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

        resolved_runner = self._resolve_step_runner(temp_instance, ov)
        start_record = self._build_step_start_record(
            operation,
            inputs,
            ov,
            step_name=step_name,
            step_number=step_number,
            step_spec_id=step_spec_id,
            step_run_id=step_run_id,
            resolved_runner=resolved_runner,
        )
        self._step_start_records[step_number] = start_record
        self._step_tracker.record_step_start(start_record)

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
                self._record_cancelled_result(start_record, cancelled_result)
                return cancelled_result

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
                    operation_class=operation,
                    inputs=inputs,
                    ov=ov,
                    step_runner=resolved_runner,
                    step_number=step_number,
                    config=self._config,
                    step_spec_id=step_spec_id,
                    cancel_event=self._cancel_event,
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
                    logger.info(
                        "Step %d (%s): cancelled.",
                        step_number,
                        step_name,
                    )
                    self._record_cancelled_result(start_record, result)
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
        assert self._executor is not None, "executor must be live during submit"
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

    def _record_cancelled_result(
        self,
        start_record: StepStartRecord,
        result: StepResult,
    ) -> None:
        """Persist and append one cancellation result for a started step."""
        with self._cancelled_result_lock:
            if any(r.step_number == result.step_number for r in self._step_results):
                return
            self._step_tracker.record_step_cancelled(start_record)
            self._step_results.append(result)
            self._named_steps.setdefault(result.step_name, []).append(result)

    def _settle_unfinished_cancellations(self) -> None:
        """Give every unfinished started step a terminal cancellation result."""
        for step_number, future in self._active_futures.items():
            start_record = self._step_start_records.get(step_number)
            if start_record is None or future.done:
                continue
            result = StepResult(
                step_name=future.step_name,
                step_number=step_number,
                success=True,
                total_count=0,
                succeeded_count=0,
                failed_count=0,
                output_roles=future.output_roles,
                output_types=future.output_types,
                metadata={"cancelled": True, "cleanup_pending": True},
                step_run_id=self._step_run_ids.get(step_number),
            )
            self._record_cancelled_result(start_record, result)

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

        if not (
            isinstance(composite, type) and issubclass(composite, CompositeDefinition)
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

        cancellation_deadline: float | None = None
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
                    if cancellation_deadline is None:
                        cancellation_deadline = time.monotonic() + 5.0
                    remaining = max(0.0, cancellation_deadline - time.monotonic())
                    with contextlib.suppress(TimeoutError, Exception):
                        future.result(timeout=remaining)
            except Exception as exc:
                logger.error(
                    "Step %d future failed during finalize: %s: %s",
                    step_num,
                    type(exc).__name__,
                    exc,
                )

        if self._cancel_event.is_set():
            self._settle_unfinished_cancellations()
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
        self._finalized = True
        return self._summary
