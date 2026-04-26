"""Split creator lifecycle into prep and post phases for per-artifact dispatch.

``prep_unit()`` runs setup + preprocess (batched) and splits the preprocess
output into per-artifact ``ExecuteInput`` objects.  ``post_unit()`` runs
postprocess + lineage + name derivation (batched) after per-artifact execute
results are reassembled.

The monolithic ``run_creator_lifecycle()`` delegates to these functions
internally for round-trip equivalence.
"""

from __future__ import annotations

import logging
import os
import shutil
import tempfile
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from artisan.execution.executors.creator import LifecycleResult

from artisan.execution.context.builder import build_creator_execution_context
from artisan.execution.context.sandbox import create_sandbox, output_snapshot
from artisan.execution.inputs.instantiation import instantiate_inputs
from artisan.execution.inputs.materialization import materialize_inputs
from artisan.execution.lineage.builder import build_edges
from artisan.execution.lineage.capture import capture_lineage_metadata
from artisan.execution.lineage.enrich import build_artifact_edges_from_dict
from artisan.execution.lineage.filesystem_match import (
    augment_match_map_from_artifacts,
    build_filesystem_match_map,
)
from artisan.execution.lineage.name_derivation import derive_human_names
from artisan.execution.lineage.validation import (
    validate_artifacts_match_specs,
    validate_lineage_completeness,
    validate_lineage_integrity,
)
from artisan.execution.models.artifact_source import ArtifactSource
from artisan.execution.models.execution_unit import ExecutionUnit
from artisan.execution.utils import finalize_artifacts, generate_execution_run_id
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.execution.runtime_environment import RuntimeEnvironment
from artisan.schemas.specs.input_models import (
    ExecuteInput,
    PostprocessInput,
    PreprocessInput,
)
from artisan.schemas.specs.input_spec import InputSpec
from artisan.utils.path import shard_uri
from artisan.utils.timing import phase_timer

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# PreppedUnit — bridge state between prep and post phases
# ---------------------------------------------------------------------------


@dataclass
class PreppedUnit:
    """State captured between prep and post phases.

    Holds per-unit lifecycle state and per-artifact execute inputs.
    Created by ``prep_unit()``, consumed by ``post_unit()``.

    Attributes:
        unit: Original execution unit.
        execution_run_id: Generated run ID for this execution.
        timestamp_start: When execution began.
        sandbox_path: Root sandbox directory path.
        execute_dir: Base execute directory (contains artifact sub-dirs).
        postprocess_dir: Postprocess directory from sandbox.
        log_path: Tool output log file path.
        files_dir: File reference output directory (or None).
        operation: The original operation instance.
        input_artifacts: Hydrated input artifacts keyed by role.
        associated: Associated artifacts from multi-role inputs.
        materialized_artifact_ids: IDs of materialized input artifacts.
        execution_context: Pre-built context for recording.
        timings: Phase timings accumulated during prep.
        artifact_execute_inputs: Per-artifact ExecuteInputs, one per
            artifact index. Length equals batch size.
        artifact_execute_dirs: Per-artifact execute sub-directory
            paths, positionally aligned with artifact_execute_inputs.
    """

    unit: ExecutionUnit
    execution_run_id: str
    timestamp_start: datetime
    sandbox_path: str
    execute_dir: str
    postprocess_dir: str
    log_path: str
    files_dir: str | None
    operation: Any
    input_artifacts: dict[str, list[Artifact]]
    associated: dict[tuple[str, str], list[Artifact]]
    materialized_artifact_ids: set[str]
    execution_context: Any
    timings: dict[str, Any] = field(default_factory=dict)
    artifact_execute_inputs: list[ExecuteInput] = field(default_factory=list)
    artifact_execute_dirs: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# prep_unit — setup + preprocess + per-artifact splitting
# ---------------------------------------------------------------------------


def prep_unit(
    unit: ExecutionUnit,
    runtime_env: RuntimeEnvironment,
    worker_id: int = 0,
    execution_run_id: str | None = None,
    sources: dict[str, ArtifactSource] | None = None,
    *,
    split_per_artifact: bool = True,
) -> PreppedUnit:
    """Run setup, preprocess, and optionally split per-artifact.

    Args:
        unit: Execution unit specifying the operation and its inputs.
        runtime_env: Paths and step_runner configuration.
        worker_id: Numeric worker identifier.
        execution_run_id: Pre-generated run ID. Generated if None.
        sources: Optional pre-resolved artifact sources.
        split_per_artifact: When True (default), split preprocess
            output into per-artifact ExecuteInputs based on the
            operation's ``per_artifact_dispatch`` setting. When False,
            produce a single ExecuteInput with the full prepared_inputs
            (monolithic behavior for ``run_creator_lifecycle``).

    Returns:
        PreppedUnit with ExecuteInputs ready for dispatch.

    Raises:
        ValueError: If working_root is not set.
    """
    timings: dict[str, Any] = {}
    operation = unit.operation
    operation_class = type(operation)
    original_inputs = _extract_inputs(unit)
    _validate_operation_outputs(operation_class)

    if execution_run_id is None:
        timestamp_start = datetime.now(UTC)
        execution_run_id = generate_execution_run_id(
            unit.execution_spec_id, timestamp_start, worker_id
        )
    else:
        timestamp_start = datetime.now(UTC)

    # --- setup phase ---
    with phase_timer("setup", timings):
        working_root = runtime_env.working_root
        if working_root is None:
            msg = "RuntimeEnvironment.working_root must be set to create a sandbox"
            raise ValueError(msg)

        if working_root == tempfile.gettempdir():
            sandbox_path_str = os.path.join(working_root, execution_run_id)
        else:
            sandbox_path_str = shard_uri(
                working_root,
                execution_run_id,
                unit.step_number,
                operation_name=operation.name,
            )

        sandbox_path_str, preprocess_dir, execute_dir, postprocess_dir = create_sandbox(
            sandbox_path_str
        )

        log_path = os.path.join(sandbox_path_str, "tool_output.log")
        materialized_dir = os.path.join(sandbox_path_str, "materialized_inputs")
        os.makedirs(materialized_dir, exist_ok=True)

        files_dir: str | None
        if runtime_env.files_root is not None:
            # files_dir is a local sandbox subdirectory. The framework
            # uploads its contents to runtime_env.files_root in
            # post_unit via _upload_files_to_root (cloud = fs.put,
            # local = shutil.move), rewriting external_path on each
            # produced artifact.
            files_dir = os.path.join(sandbox_path_str, "files_outputs")
            os.makedirs(files_dir, exist_ok=True)
        else:
            files_dir = None

        fs = runtime_env.storage.filesystem()
        storage_options = runtime_env.storage.delta_storage_options()

        execution_context = build_creator_execution_context(
            execution_run_id=execution_run_id,
            execution_spec_id=unit.execution_spec_id,
            step_number=unit.step_number,
            timestamp_start=timestamp_start,
            worker_id=worker_id,
            delta_root=runtime_env.delta_root,
            staging_root=runtime_env.staging_root,
            fs=fs,
            storage_options=storage_options,
            operation=operation,
            sandbox_path=sandbox_path_str,
            compute_backend_name=runtime_env.compute_backend_name,
            shared_filesystem=runtime_env.shared_filesystem,
            step_run_id=unit.step_run_id,
            files_root=runtime_env.files_root,
        )
        artifact_store = execution_context.artifact_store

        input_specs = getattr(operation_class, "inputs", {})
        default_hydrate = getattr(operation_class, "hydrate_inputs", True)

        if sources is not None:
            input_artifacts: dict[str, list[Artifact]] = {}
            for role, source in sources.items():
                spec = input_specs.get(role, InputSpec())
                input_artifacts[role] = source.hydrate(
                    artifact_store, spec, default_hydrate
                )
            associated: dict[tuple[str, str], list[Artifact]] = {}
        else:
            input_artifacts, associated = instantiate_inputs(
                original_inputs,
                artifact_store,
                input_specs,
                default_hydrate,
            )
        input_artifacts, materialized_artifact_ids = materialize_inputs(
            input_artifacts,
            input_specs,
            materialized_dir,
            artifact_store,
        )

    # --- preprocess phase ---
    with phase_timer("preprocess", timings):
        preprocess_input = PreprocessInput(
            preprocess_dir=preprocess_dir,
            input_artifacts=input_artifacts,
            _associated=associated,
        )
        prepared_inputs = operation.preprocess(preprocess_input)

    # --- build ExecuteInputs ---
    artifact_execute_inputs: list[ExecuteInput] = []
    artifact_execute_dirs: list[str] = []

    should_split = split_per_artifact and getattr(
        operation, "per_artifact_dispatch", True
    )

    if not should_split:
        # Single ExecuteInput with full prepared_inputs (monolithic)
        artifact_execute_dirs.append(execute_dir)
        artifact_execute_inputs.append(
            ExecuteInput(
                inputs=prepared_inputs,
                execute_dir=execute_dir,
                log_path=log_path,
                files_dir=files_dir,
            )
        )
    else:
        batch_size = unit.get_batch_size() or 1
        for i in range(batch_size):
            artifact_exec_dir = os.path.join(execute_dir, f"artifact_{i}")
            os.makedirs(artifact_exec_dir, exist_ok=True)
            artifact_execute_dirs.append(artifact_exec_dir)

            per_artifact_inputs = _split_prepared_inputs(prepared_inputs, i, batch_size)
            artifact_files_dir: str | None = None
            if files_dir is not None:
                artifact_files_dir = os.path.join(files_dir, f"artifact_{i}")
                os.makedirs(artifact_files_dir, exist_ok=True)

            artifact_execute_inputs.append(
                ExecuteInput(
                    inputs=per_artifact_inputs,
                    execute_dir=artifact_exec_dir,
                    log_path=log_path,
                    files_dir=artifact_files_dir,
                )
            )

    return PreppedUnit(
        unit=unit,
        execution_run_id=execution_run_id,
        timestamp_start=timestamp_start,
        sandbox_path=sandbox_path_str,
        execute_dir=execute_dir,
        postprocess_dir=postprocess_dir,
        log_path=log_path,
        files_dir=files_dir,
        operation=operation,
        input_artifacts=input_artifacts,
        associated=associated,
        materialized_artifact_ids=materialized_artifact_ids,
        execution_context=execution_context,
        timings=timings,
        artifact_execute_inputs=artifact_execute_inputs,
        artifact_execute_dirs=artifact_execute_dirs,
    )


# ---------------------------------------------------------------------------
# post_unit — postprocess + lineage + name derivation + cleanup
# ---------------------------------------------------------------------------


def post_unit(
    prepped: PreppedUnit,
    raw_results: list[Any],
    runtime_env: RuntimeEnvironment,
) -> LifecycleResult:
    """Run postprocess and lineage with reassembled execute results.

    Merges per-artifact execute results back into the shapes that
    postprocess expects, calls postprocess once (batched), then
    runs lineage and finalization.

    Args:
        prepped: State captured by ``prep_unit()``.
        raw_results: One raw result per artifact from execute.
            Exceptions at a given index represent execute failures.
        runtime_env: For sandbox cleanup decision.

    Returns:
        LifecycleResult with artifacts, edges, and timings.
    """
    from artisan.execution.executors.creator import (
        LifecycleResult,
        _PostprocessFailure,
    )

    operation = prepped.operation
    operation_class = type(operation)
    timings = prepped.timings

    # --- postprocess phase ---
    with phase_timer("postprocess", timings):
        memory_outputs, file_outputs = _reassemble_results(
            raw_results, prepped.artifact_execute_dirs
        )

        filesystem_match_map = build_filesystem_match_map(
            prepped.materialized_artifact_ids, file_outputs
        )

        postprocess_input = PostprocessInput(
            file_outputs=file_outputs,
            memory_outputs=memory_outputs,
            input_artifacts=_extract_artifacts_from_input(prepped.input_artifacts),
            step_number=prepped.unit.step_number,
            postprocess_dir=prepped.postprocess_dir,
            _associated=prepped.associated,
        )
        op_result = operation.postprocess(postprocess_input)

        if not op_result.success:
            raise _PostprocessFailure(op_result.error or "Postprocess failed")

        finalized_artifacts = finalize_artifacts(op_result.artifacts)
        validate_artifacts_match_specs(finalized_artifacts, operation_class.outputs)

        # Upload local files_dir bytes to runtime_env.files_root and
        # rewrite external_path on each finalized artifact. Runs inside
        # the postprocess phase_timer, so any _UploadFailure surfaces
        # as a postprocess failure (it subclasses _PostprocessFailure)
        # and the existing except clause at creator.py:204 records it.
        # Because the exception propagates out of post_unit before the
        # sandbox cleanup at lines 429-434, the local files survive
        # automatically for recovery — no explicit preservation code.
        _upload_files_to_root(
            finalized_artifacts,
            files_dir=prepped.files_dir,
            runtime_env=runtime_env,
            execution_run_id=prepped.execution_run_id,
            step_number=prepped.unit.step_number,
            operation_name=operation.name,
            sandbox_path=prepped.sandbox_path,
        )

        augment_match_map_from_artifacts(
            filesystem_match_map,
            prepped.materialized_artifact_ids,
            finalized_artifacts,
        )

    # --- lineage phase ---
    with phase_timer("lineage", timings):
        flat_input_artifacts = _extract_artifacts_from_input(prepped.input_artifacts)
        if op_result.lineage is None:
            lineage = capture_lineage_metadata(
                output_artifacts=finalized_artifacts,
                input_artifacts=flat_input_artifacts,
                output_specs=operation_class.outputs,
                group_by=getattr(operation_class, "group_by", None),
                group_ids=prepped.unit.group_ids,
                filesystem_match_map=filesystem_match_map,
            )
        else:
            validate_lineage_integrity(
                op_result.lineage,
                flat_input_artifacts,
                finalized_artifacts,
            )
            lineage = op_result.lineage

        edge_pairs = build_edges(
            lineage=lineage,
            finalized_artifacts=finalized_artifacts,
            input_artifacts=flat_input_artifacts,
            output_specs=operation_class.outputs,
        )

        validate_lineage_completeness(
            finalized_artifacts,
            operation_class.outputs,
            lineage,
        )
        built_artifacts: dict[str, Artifact] = {}
        for artifact_list in finalized_artifacts.values():
            for artifact in artifact_list:
                if artifact.artifact_id is not None:
                    built_artifacts[artifact.artifact_id] = artifact
        for artifact_list in flat_input_artifacts.values():
            for artifact in artifact_list:
                if artifact.artifact_id is not None:
                    built_artifacts[artifact.artifact_id] = artifact

        artifact_edges = build_artifact_edges_from_dict(
            edge_pairs,
            prepped.execution_run_id,
            built_artifacts,
        )

    # --- name derivation ---
    derive_human_names(
        finalized_artifacts, edge_pairs, flat_input_artifacts, filesystem_match_map
    )

    # Clean up sandbox
    if (
        prepped.sandbox_path is not None  # type: ignore[redundant-expr]  # defensive runtime check
        and not runtime_env.preserve_working
        and os.path.exists(prepped.sandbox_path)
    ):
        shutil.rmtree(prepped.sandbox_path, ignore_errors=True)

    return LifecycleResult(
        input_artifacts=flat_input_artifacts,
        artifacts=finalized_artifacts,
        edges=artifact_edges,
        timings=timings,
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _upload_files_to_root(
    finalized_artifacts: dict[str, list[Artifact]],
    files_dir: str | None,
    runtime_env: RuntimeEnvironment,
    execution_run_id: str,
    step_number: int,
    operation_name: str,
    sandbox_path: str,
) -> None:
    """Relocate local ``files_dir`` bytes to ``runtime_env.files_root``.

    Walks every finalized artifact whose ``external_path`` points
    inside the unit's local ``files_dir`` and moves (local) or
    uploads (cloud) the underlying file to its canonical sharded
    destination under ``files_root``, then rewrites the artifact's
    ``external_path`` in place.

    Multiple artifacts may share one source file (e.g.
    ``AppendableGenerator`` emits N ``AppendableArtifact`` instances
    backed by one JSONL). A ``moved`` dict keyed on source path
    deduplicates so each file moves / uploads at most once; every
    artifact still gets its ``external_path`` rewritten.

    Artifacts whose ``external_path`` is already a cloud URI or
    lives outside ``files_dir`` are left untouched.

    ``Artifact`` is not frozen (``schemas/artifact/base.py:30``), so
    rewrites are direct attribute assignments.

    Args:
        finalized_artifacts: Output of ``finalize_artifacts`` — dict
            of role -> list of finalized Artifact instances.
        files_dir: Unit's local sandbox files directory, or None
            when no operation in this unit produces file outputs.
        runtime_env: Runtime environment carrying ``files_root`` and
            the storage step_runner.
        execution_run_id: Per-execution ID used to compute_provider the
            sharded destination under ``files_root``.
        step_number: Pipeline step number for sharding.
        operation_name: Operation name for sharding.
        sandbox_path: Unit sandbox path; included in the failure
            log so recovery is possible when an upload fails.

    Raises:
        _UploadFailure: When ``shutil.move`` or ``fs.put`` raises.
            As a subclass of ``_PostprocessFailure`` this propagates
            out of ``post_unit`` before the sandbox cleanup runs,
            preserving the local files automatically.
    """
    if files_dir is None or runtime_env.files_root is None:
        return

    from artisan.execution.executors.creator import _UploadFailure

    target_shard = shard_uri(
        runtime_env.files_root,
        execution_run_id,
        step_number,
        operation_name=operation_name,
    )

    fs = runtime_env.storage.filesystem()
    is_local = runtime_env.storage.is_local

    if is_local:
        os.makedirs(target_shard, exist_ok=True)
    else:
        # No-op on most object stores (prefixes are implicit).
        fs.makedirs(target_shard, exist_ok=True)

    # Source local path -> destination URI/path. Dedups the
    # AppendableGenerator shared-file case.
    moved: dict[str, str] = {}

    for artifact_list in finalized_artifacts.values():
        for artifact in artifact_list:
            ext_path = artifact.external_path
            if ext_path is None:
                continue
            if not _is_under_local_dir(ext_path, files_dir):
                continue

            if ext_path not in moved:
                relative = os.path.relpath(ext_path, files_dir)
                destination = (
                    os.path.join(target_shard, relative)
                    if is_local
                    else f"{target_shard}/{relative}"
                )
                try:
                    if is_local:
                        os.makedirs(os.path.dirname(destination), exist_ok=True)
                        shutil.move(ext_path, destination)
                    else:
                        fs.put(ext_path, destination)
                except Exception as exc:
                    logger.warning(
                        "Upload to files_root failed at %s; "
                        "preserving sandbox %s for recovery.",
                        destination,
                        sandbox_path,
                    )
                    msg = f"Upload to {destination} failed: {exc}"
                    raise _UploadFailure(msg) from exc
                moved[ext_path] = destination

            destination = moved[ext_path]
            # Direct mutation: Artifact is not frozen (base.py:30).
            artifact.external_path = destination
            # Local: shutil.move relocated the bytes; point at the
            # new path. Cloud: sandbox still has the bytes until the
            # cleanup at the end of post_unit — keep the local ref
            # so any in-process consumer avoids an fs.get round-trip.
            artifact.materialized_path = destination if is_local else ext_path


def _is_under_local_dir(candidate: str, root: str) -> bool:
    """True iff ``candidate`` is a local path inside ``root``.

    ``os.path.commonpath`` raises on cross-protocol paths, so guard
    against ``s3://...`` strings explicitly.

    Args:
        candidate: Path to test.
        root: Parent directory.

    Returns:
        True when ``candidate`` lives under ``root`` on the local
        filesystem; False otherwise (including cloud URIs).
    """
    if "://" in candidate:
        return False
    try:
        return os.path.commonpath(
            [os.path.abspath(candidate), os.path.abspath(root)]
        ) == os.path.abspath(root)
    except ValueError:
        return False


def _split_prepared_inputs(
    prepared_inputs: dict[str, Any],
    index: int,
    batch_size: int,
) -> dict[str, Any]:
    """Extract per-artifact inputs at the given index.

    Convention: list-valued entries whose length matches the batch
    size are per-artifact and sliced at ``index``. The sliced item
    is wrapped in a single-element list to preserve the list interface
    that operations expect when iterating inputs. All other entries
    (scalars, dicts, lists of different length) are passed through
    unchanged (shared across artifacts).

    Edge case: a shared list that coincidentally has the same length
    as batch_size will be incorrectly sliced. In practice this does
    not arise — shared configuration is returned as dicts or scalars,
    and per-artifact lists are always batch-sized by construction.

    Args:
        prepared_inputs: Output from operation.preprocess().
        index: Artifact index within the unit.
        batch_size: Number of artifacts in the unit. Used to
            distinguish per-artifact lists from shared lists.

    Returns:
        Dict with per-artifact values at ``index``, where sliced
        items are wrapped in single-element lists.
    """
    result: dict[str, Any] = {}
    for key, value in prepared_inputs.items():
        if isinstance(value, list) and len(value) == batch_size:
            result[key] = [value[index]]
        else:
            result[key] = value
    return result


def _reassemble_results(
    per_artifact_results: list[Any],
    artifact_execute_dirs: list[str],
) -> tuple[Any, list[str]]:
    """Merge per-artifact execute results for batched postprocess.

    Reassembles memory_outputs and file_outputs so postprocess sees
    the same data shapes as when execute processes all artifacts at
    once.

    Args:
        per_artifact_results: One raw result per artifact.
            Exceptions at failed indices are filtered out.
        artifact_execute_dirs: Per-artifact execute sub-directory
            paths.

    Returns:
        Tuple of (merged_memory_outputs, file_outputs).
    """
    # Collect file_outputs from all sub-directories
    file_outputs: list[str] = []
    for d in artifact_execute_dirs:
        file_outputs.extend(output_snapshot(d))

    # Filter exceptions, merge memory_outputs
    successes = [r for r in per_artifact_results if not isinstance(r, Exception)]

    if not successes or all(r is None for r in successes):
        return None, file_outputs

    if all(isinstance(r, dict) for r in successes):
        merged: dict[str, Any] = {}
        for key in successes[0]:
            values = [r[key] for r in successes if key in r]
            if all(isinstance(v, list) for v in values):
                merged[key] = [item for v in values for item in v]
            else:
                merged[key] = values
        return merged, file_outputs

    return successes, file_outputs


def _extract_inputs(unit: ExecutionUnit) -> dict[str, list[str]]:
    """Copy input artifact IDs from the execution unit."""
    if not unit.inputs:
        return {}
    return {role: list(ids) for role, ids in unit.inputs.items()}


def _extract_artifacts_from_input(
    input_artifacts: dict[str, list[Artifact]],
) -> dict[str, list[Artifact]]:
    """Shallow-copy the input artifacts dict to avoid mutation."""
    return {role: list(artifacts) for role, artifacts in input_artifacts.items()}


def _validate_operation_outputs(operation_class: type) -> None:
    """Raise ValueError if the operation class has no outputs declared."""
    if getattr(operation_class, "outputs", None) is None:
        msg = (
            f"{operation_class.__name__} must define outputs. "
            "Add a ClassVar like: outputs: ClassVar[dict[str, OutputSpec]] = {}"
        )
        raise ValueError(msg)
