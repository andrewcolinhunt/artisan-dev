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

from artisan.execution.compute.routing import routes_to_endpoint
from artisan.execution.context.builder import build_execution_context
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
from artisan.execution.models.execution_unit import ExecutionUnit
from artisan.execution.recording.recorder import _read_tool_output
from artisan.execution.recording.replay_snapshot import current_replay_builder
from artisan.execution.transport.log_constants import TOOL_OUTPUT_FILENAME
from artisan.execution.utils import finalize_artifacts, generate_execution_run_id
from artisan.operations.base.per_artifact import PerArtifact
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.execution.runtime_environment import RuntimeEnvironment
from artisan.schemas.specs.input_models import (
    ExecuteInput,
    PostprocessInput,
    PreprocessInput,
)
from artisan.utils.filename import strip_extensions
from artisan.utils.path import shard_uri
from artisan.utils.timing import phase_timer

logger = logging.getLogger(__name__)


@dataclass
class PreppedUnit:
    """State captured between prep and post phases.

    Holds per-unit lifecycle state and per-artifact execute inputs.
    Created by ``prep_unit()``, consumed by ``post_unit()``.

    Attributes:
        unit: Original execution unit.
        execution_run_id: Generated run ID for this execution.
        sandbox_path: Root sandbox directory path.
        postprocess_dir: Postprocess directory from sandbox.
        log_path: Tool output log file path.
        files_dir: File reference output directory (or None).
        operation: The original operation instance.
        input_artifacts: Hydrated input artifacts keyed by role.
        associated: Associated artifacts from multi-role inputs.
        materialized_artifact_ids: IDs of materialized input artifacts.
        timings: Phase timings accumulated during prep.
        artifact_execute_inputs: One input per dispatch slot, or a single
            input for generative and monolithic execution.
        artifact_execute_dirs: Per-artifact execute sub-directory
            paths, positionally aligned with artifact_execute_inputs.
    """

    unit: ExecutionUnit
    execution_run_id: str
    sandbox_path: str
    postprocess_dir: str
    log_path: str
    files_dir: str | None
    operation: Any
    input_artifacts: dict[str, list[Artifact]]
    associated: dict[tuple[str, str], list[Artifact]]
    materialized_artifact_ids: set[str]
    timings: dict[str, Any] = field(default_factory=dict)
    artifact_execute_inputs: list[ExecuteInput] = field(default_factory=list)
    artifact_execute_dirs: list[str] = field(default_factory=list)


def prep_unit(
    unit: ExecutionUnit,
    runtime_env: RuntimeEnvironment,
    execution_run_id: str | None = None,
) -> PreppedUnit:
    """Run setup, preprocess, and per-artifact splitting.

    Preprocess output is split into per-artifact ExecuteInputs when the
    operation declares ``per_artifact_dispatch=True`` (the default);
    otherwise a single monolithic ExecuteInput carries the full
    prepared_inputs.

    Args:
        unit: Execution unit specifying the operation and its inputs.
        runtime_env: Paths and runtime configuration.
        execution_run_id: Pre-generated run ID. Generated if None.

    Returns:
        PreppedUnit with ExecuteInputs ready for dispatch.

    Raises:
        ValueError: If working_root is not set.
    """
    timings: dict[str, Any] = {}
    operation = unit.operation
    operation_class = type(operation)
    original_inputs = unit.get_input_artifact_ids()
    _validate_operation_outputs(operation_class)

    timestamp_start = datetime.now(UTC)
    if execution_run_id is None:
        execution_run_id = generate_execution_run_id(
            unit.execution_spec_id, timestamp_start, runtime_env.worker_id
        )

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
        replay_builder = current_replay_builder()
        if replay_builder is not None:
            replay_builder.record_sandbox(sandbox_path_str)

        log_path = os.path.join(sandbox_path_str, TOOL_OUTPUT_FILENAME)
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

        execution_context = build_execution_context(
            execution_run_id=execution_run_id,
            execution_spec_id=unit.execution_spec_id,
            step_number=unit.step_number,
            timestamp_start=timestamp_start,
            runtime_env=runtime_env,
            operation=operation,
            sandbox_path=sandbox_path_str,
            step_run_id=unit.step_run_id,
        )
        artifact_store = execution_context.artifact_store

        input_specs = getattr(operation_class, "inputs", {})
        default_hydrate = getattr(operation_class, "hydrate_inputs", True)

        input_artifacts, associated = instantiate_inputs(
            original_inputs,
            artifact_store,
            input_specs,
            default_hydrate,
            recorded_associated=(
                unit.replay_snapshot.associated
                if unit.replay_of_execution_run_id and unit.replay_snapshot
                else None
            ),
        )
        if replay_builder is not None:
            replay_builder.record_associated(associated)
        input_artifacts, materialized_artifact_ids = materialize_inputs(
            input_artifacts,
            input_specs,
            materialized_dir,
            artifact_store,
            endpoint_routed=routes_to_endpoint(operation),
        )
        external_integrity = _external_integrity_metadata(input_artifacts)

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

    should_split = getattr(operation, "per_artifact_dispatch", True)

    if not should_split:
        # Single ExecuteInput with full prepared_inputs (monolithic).
        # Unwrap PerArtifact markers so execute_function() sees raw lists — the
        # sentinel only signals slicing intent, never reaches op code.
        monolithic_inputs = {
            k: list(v) if isinstance(v, PerArtifact) else v
            for k, v in prepared_inputs.items()
        }
        artifact_execute_dirs.append(execute_dir)
        artifact_execute_inputs.append(
            ExecuteInput(
                inputs=monolithic_inputs,
                execute_dir=execute_dir,
                log_path=log_path,
                metadata={"external_integrity": external_integrity},
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
                    metadata={"external_integrity": external_integrity},
                    files_dir=artifact_files_dir,
                )
            )

    return PreppedUnit(
        unit=unit,
        execution_run_id=execution_run_id,
        sandbox_path=sandbox_path_str,
        postprocess_dir=postprocess_dir,
        log_path=log_path,
        files_dir=files_dir,
        operation=operation,
        input_artifacts=input_artifacts,
        associated=associated,
        materialized_artifact_ids=materialized_artifact_ids,
        timings=timings,
        artifact_execute_inputs=artifact_execute_inputs,
        artifact_execute_dirs=artifact_execute_dirs,
    )


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
        runtime_env: Output storage, integrity verification and sandbox cleanup.

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
        memory_outputs, file_outputs, output_pair_map = _reassemble_results(
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

        flat_input_artifacts = _extract_artifacts_from_input(prepped.input_artifacts)
        draft_names = {
            role: [getattr(artifact, "original_name", None) for artifact in artifacts]
            for role, artifacts in op_result.artifacts.items()
        }
        derive_human_names(
            op_result.artifacts,
            flat_input_artifacts,
            filesystem_match_map,
        )
        finalized_artifacts = finalize_artifacts(op_result.artifacts)
        validate_artifacts_match_specs(finalized_artifacts, operation_class.outputs)
        # Lineage mappings use the postprocessor's occurrence names as structural
        # keys. Keep that view separate because human-name derivation can collapse
        # two distinct occurrences to the same display name.
        lineage_artifacts = {
            role: [
                artifact.model_copy(update={"original_name": draft_name})
                if draft_name is not None
                else artifact
                for artifact, draft_name in zip(
                    artifacts, draft_names[role], strict=True
                )
            ]
            for role, artifacts in finalized_artifacts.items()
        }

        _upload_files_to_root(
            finalized_artifacts,
            files_dir=prepped.files_dir,
            runtime_env=runtime_env,
            execution_run_id=prepped.execution_run_id,
            step_number=prepped.unit.step_number,
            operation_name=operation.name,
            sandbox_path=prepped.sandbox_path,
        )
        for artifact_list in finalized_artifacts.values():
            for artifact in artifact_list:
                if artifact.EXTERNALLY_BACKED:
                    artifact.verify_external_content(
                        fs=runtime_env.storage.filesystem()
                    )

        augment_match_map_from_artifacts(
            filesystem_match_map,
            prepped.materialized_artifact_ids,
            finalized_artifacts,
        )

    # --- lineage phase ---
    with phase_timer("lineage", timings):
        if op_result.lineage is None:
            lineage = capture_lineage_metadata(
                output_artifacts=lineage_artifacts,
                input_artifacts=flat_input_artifacts,
                output_specs=operation_class.outputs,
                group_by=operation.group_by,
                group_ids=prepped.unit.group_ids,
                filesystem_match_map=filesystem_match_map,
                output_pair_map=output_pair_map,
            )
        else:
            validate_lineage_integrity(
                op_result.lineage,
                flat_input_artifacts,
                lineage_artifacts,
            )
            lineage = op_result.lineage

        edge_pairs = build_edges(
            lineage=lineage,
            finalized_artifacts=lineage_artifacts,
        )

        validate_lineage_completeness(
            lineage_artifacts,
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

    # Capture the unit log before sandbox cleanup destroys it — the
    # recorder persists it to the executions table on success.
    tool_output = _read_tool_output(prepped.log_path)

    if not runtime_env.preserve_working and os.path.exists(prepped.sandbox_path):
        shutil.rmtree(prepped.sandbox_path, ignore_errors=True)

    return LifecycleResult(
        input_artifacts=flat_input_artifacts,
        artifacts=finalized_artifacts,
        edges=artifact_edges,
        tool_output=tool_output,
        timings=timings,
    )


def _external_integrity_metadata(
    artifacts: dict[str, list[Artifact]],
) -> dict[str, dict[str, object]]:
    """Return digest contracts for directly transported external URIs."""
    contracts: dict[str, dict[str, object]] = {}
    for artifact_list in artifacts.values():
        for artifact in artifact_list:
            path = artifact.materialized_path
            if not artifact.EXTERNALLY_BACKED or not path or "://" not in path:
                continue
            contract = {
                "content_digest": getattr(artifact, "content_hash", None),
                "size_bytes": getattr(artifact, "size_bytes", None),
            }
            previous = contracts.setdefault(path, contract)
            if previous != contract:
                msg = f"Conflicting integrity metadata for external input {path!r}"
                raise ValueError(msg)
    return contracts


def _upload_files_to_root(
    finalized_artifacts: dict[str, list[Artifact]],
    files_dir: str | None,
    runtime_env: RuntimeEnvironment,
    execution_run_id: str,
    step_number: int,
    operation_name: str,
    sandbox_path: str,
) -> None:
    """Relocate files owned by ``files_dir`` and update artifact locators.

    Move local outputs or upload cloud outputs to the execution's shard under
    ``files_root``. Shared source files move once; existing external locators
    outside ``files_dir`` are unchanged. Failure raises ``_UploadFailure`` before
    sandbox cleanup, leaving remaining local bytes available for recovery.
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

    moved: dict[str, str] = {}

    for artifact_list in finalized_artifacts.values():
        for artifact in artifact_list:
            if not artifact.EXTERNALLY_BACKED:
                continue
            locator_field = next(iter(artifact.LOCATOR_FIELDS))
            ext_path = getattr(artifact, locator_field)
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
            setattr(artifact, locator_field, destination)
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

    Per-artifact data MUST be wrapped in ``PerArtifact(...)`` in
    preprocess; the framework slices the wrapper at ``index`` and re-wraps
    the item in a single-element list (preserves the list interface that
    operations expect when iterating inputs). Raw lists pass through
    unchanged as shared data regardless of length.

    Args:
        prepared_inputs: Output from ``operation.preprocess()``.
        index: Artifact index within the unit.
        batch_size: Number of artifacts in the unit. Used to validate
            ``PerArtifact`` lengths.

    Returns:
        Dict with per-artifact values at ``index``, where ``PerArtifact``
        values are sliced and wrapped in a single-element list.

    Raises:
        ValueError: If a ``PerArtifact`` value's length does not match
            ``batch_size``.
    """
    result: dict[str, Any] = {}
    for key, value in prepared_inputs.items():
        if isinstance(value, PerArtifact):
            if len(value) != batch_size:
                msg = (
                    f"PerArtifact({key!r}) has length {len(value)} but "
                    f"batch_size is {batch_size}."
                )
                raise ValueError(msg)
            result[key] = [value[index]]
        else:
            result[key] = value
    return result


def _reassemble_results(
    per_artifact_results: list[Any],
    artifact_execute_dirs: list[str],
) -> tuple[Any, list[str], dict[str, list[int]]]:
    """Merge per-artifact execute results for batched postprocess.

    Reassembles memory_outputs and file_outputs so postprocess sees
    the same data shapes as when execute processes all artifacts at
    once. Also returns a stem -> ordered slot-index map so lineage capture
    can recover the per-output pair index for grouped multi-input ops
    (fixing the ``primary_id_to_idx`` clobber for repeated primaries
    under CROSS_PRODUCT + ``artifacts_per_unit > 1``).

    Args:
        per_artifact_results: One raw result per artifact.
            Exceptions at failed indices are filtered out.
        artifact_execute_dirs: Per-artifact execute sub-directory
            paths. Index in this list is the pair index.

    Returns:
        Tuple of (merged_memory_outputs, file_outputs, output_pair_map).
        ``output_pair_map`` keys are extension-stripped basenames of emitted
        files (matching ``artifact.original_name`` after draft). Values retain
        every source slot in file-output order so duplicate basenames remain
        occurrence-aligned.
    """
    file_outputs: list[str] = []
    output_pair_map: dict[str, list[int]] = {}
    for slot_idx, d in enumerate(artifact_execute_dirs):
        slot_files = output_snapshot(d)
        for fpath in slot_files:
            stem = strip_extensions(os.path.basename(fpath))
            output_pair_map.setdefault(stem, []).append(slot_idx)
        file_outputs.extend(slot_files)

    # Filter exceptions, merge memory_outputs
    successes = [r for r in per_artifact_results if not isinstance(r, Exception)]

    if not successes or all(r is None for r in successes):
        return None, file_outputs, output_pair_map

    if all(isinstance(r, dict) for r in successes):
        merged: dict[str, Any] = {}
        for key in successes[0]:
            values = [r[key] for r in successes if key in r]
            if all(isinstance(v, list) for v in values):
                merged[key] = [item for v in values for item in v]
            else:
                merged[key] = values
        return merged, file_outputs, output_pair_map

    return successes, file_outputs, output_pair_map


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
