"""Run scientific creator phases on admitted artifacts without store access."""

from __future__ import annotations

import os
from collections.abc import Mapping
from concurrent.futures import CancelledError
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Self

from fsspec import AbstractFileSystem

from artisan.execution.compute.base import ExecuteRouter
from artisan.execution.context.sandbox import create_sandbox, output_snapshot
from artisan.execution.exceptions import AssignmentExecutionError
from artisan.execution.inputs.materialization import (
    materialize_captured_inputs,
    materialize_captured_references,
    requires_materialization,
)
from artisan.execution.lineage.validation import (
    validate_artifacts_match_specs,
    validate_lineage_completeness,
    validate_lineage_integrity,
)
from artisan.execution.recording.commands import sanitize_diagnostic
from artisan.execution.recording.recorder import _read_tool_output
from artisan.execution.scientific_failure import scientific_phase
from artisan.execution.transport.log_constants import TOOL_OUTPUT_FILENAME
from artisan.execution.utils import finalize_artifacts
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.operations.base.per_artifact import PerArtifact
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.execution.curator_result import ArtifactResult
from artisan.schemas.specs.input_models import (
    ExecuteInput,
    FrozenInputContext,
    PostprocessInput,
    PreprocessInput,
)
from artisan.utils.command_supervisor import (
    CommandOwnershipError,
    check_command_scope,
    close_command_scope,
)
from artisan.utils.timing import phase_timer
from artisan.utils.traceback import format_error


@dataclass(frozen=True)
class InvocationPaths:
    """Local directories for one lifecycle, retained by its enclosing owner."""

    sandbox: str | None = None
    preprocess: str | None = None
    execute: str | None = None
    postprocess: str | None = None
    materialized: str | None = None
    log: str | None = None
    files: str | None = None

    @classmethod
    def create(cls, root: Path, *, files_enabled: bool = True) -> Self:
        """Create isolated phase directories without requiring a durable file root."""
        sandbox, preprocess, execute, postprocess = create_sandbox(str(root))
        materialized = root / "materialized_inputs"
        materialized.mkdir(exist_ok=True)
        files = root / "files_outputs" if files_enabled else None
        if files is not None:
            files.mkdir(exist_ok=True)
        return cls(
            sandbox=sandbox,
            preprocess=preprocess,
            execute=execute,
            postprocess=postprocess,
            materialized=str(materialized),
            log=str(root / TOOL_OUTPUT_FILENAME),
            files=str(files) if files is not None else None,
        )


class PostprocessFailure(Exception):
    """Report an operation's unsuccessful postprocess result."""


class ExecuteFailure(Exception):
    """Keep the original execution cause and available command output."""

    def __init__(self, error: str, tool_output: str | None = None) -> None:
        super().__init__(error)
        self.tool_output = tool_output


@dataclass
class PreparedInvocation:
    """Scientific state between preprocessing and result finalization."""

    operation: OperationDefinition
    context: FrozenInputContext
    paths: InvocationPaths
    step_number: int | None
    timings: dict[str, float] = field(default_factory=dict)
    execute_inputs: list[ExecuteInput] = field(default_factory=list)
    execute_dirs: list[str | None] = field(default_factory=list)


def invoke_operation(
    operation: OperationDefinition,
    context: FrozenInputContext,
    *,
    paths: InvocationPaths,
    execute_router: ExecuteRouter,
    batch_size: int,
    materialization_references: Mapping[str, Mapping[str, Artifact]],
    step_number: int | None = None,
    fs: AbstractFileSystem | None = None,
    timings: dict[str, float] | None = None,
) -> ArtifactResult:
    """Run the shared scientific phases within caller-supplied paths and scope."""
    with execute_router.context():
        prepared = prepare_invocation(
            operation,
            context,
            paths,
            batch_size=batch_size,
            materialization_references=materialization_references,
            step_number=step_number,
            fs=fs,
            timings=timings,
        )
        results = execute_invocation(prepared, execute_router)
        return postprocess_invocation(prepared, results, fs=fs)


def prepare_invocation(
    operation: OperationDefinition,
    context: FrozenInputContext,
    paths: InvocationPaths,
    *,
    batch_size: int,
    step_number: int | None = None,
    materialization_references: Mapping[str, Mapping[str, Artifact]],
    fs: AbstractFileSystem | None = None,
    timings: dict[str, float] | None = None,
) -> PreparedInvocation:
    """Materialize captured inputs and preprocess once for an admitted partition."""
    check_command_scope()
    if operation._kind() != "creator":
        msg = "Scientific invocation supports creator operations only"
        raise ValueError(msg)
    if batch_size < 1:
        msg = "An admitted invocation must have a positive batch size"
        raise ValueError(msg)
    _validate_operation_outputs(type(operation))
    _validate_captured_context(operation, context)
    references = dict(materialization_references)
    for (role, index), captured in context.references.items():
        config_id = context.artifacts[role][index].artifact_id
        assert config_id is not None
        references[config_id] = captured
    artifacts = {role: list(values) for role, values in context.artifacts.items()}
    needs_files = requires_materialization(artifacts, operation.inputs, references)
    if paths.materialized is None and needs_files:
        msg = "Operation materialization requires work_dir"
        raise ValueError(msg)
    if any(artifact.is_draft for values in artifacts.values() for artifact in values):
        msg = "Invocation inputs must be finalized artifacts"
        raise ValueError(msg)
    prepared = PreparedInvocation(
        operation, context, paths, step_number, timings if timings is not None else {}
    )
    setup_elapsed = prepared.timings.get("setup", 0.0)
    with phase_timer("setup", prepared.timings):
        if paths.materialized is not None:
            materialize_captured_inputs(
                artifacts,
                operation.inputs,
                paths.materialized,
                references,
                fs=fs,
            )
            typed_references = materialize_captured_references(
                artifacts,
                operation.inputs,
                paths.materialized,
                references,
                fs=fs,
            )
            for (role, index), captured in context.references.items():
                config_id = context.artifacts[role][index].artifact_id
                assert config_id is not None
                for artifact_id, artifact in captured.items():
                    if not artifact.EXTERNAL_REFERENCE:
                        artifact.materialized_path = typed_references[config_id][
                            artifact_id
                        ].materialized_path
    prepared.timings["setup"] += setup_elapsed
    with phase_timer("preprocess", prepared.timings), scientific_phase("preprocess"):
        values = operation.preprocess(
            PreprocessInput(
                preprocess_dir=paths.preprocess,
                input_artifacts=artifacts,
                _context=context,
            )
        )
    check_command_scope()
    metadata = {"external_integrity": _external_integrity_metadata(artifacts)}
    if not operation.per_artifact_dispatch:
        values = {
            key: list(value) if isinstance(value, PerArtifact) else value
            for key, value in values.items()
        }
        prepared.execute_dirs.append(paths.execute)
        prepared.execute_inputs.append(
            ExecuteInput(
                inputs=values,
                execute_dir=paths.execute,
                log_path=paths.log,
                metadata=metadata,
                files_dir=paths.files,
            )
        )
        return prepared
    for index in range(batch_size):
        execute_dir = (
            os.path.join(paths.execute, f"artifact_{index}")
            if paths.execute is not None
            else None
        )
        if execute_dir is not None:
            os.makedirs(execute_dir, exist_ok=True)
        files_dir = (
            os.path.join(paths.files, f"artifact_{index}") if paths.files else None
        )
        if files_dir is not None:
            os.makedirs(files_dir, exist_ok=True)
        prepared.execute_dirs.append(execute_dir)
        prepared.execute_inputs.append(
            ExecuteInput(
                inputs=split_prepared_inputs(values, index, batch_size),
                execute_dir=execute_dir,
                log_path=paths.log,
                metadata=metadata,
                files_dir=files_dir,
            )
        )
    return prepared


def _validate_captured_context(
    operation: OperationDefinition,
    context: FrozenInputContext,
) -> None:
    """Require every declared contextual dependency before calling operation code."""
    for role, artifacts in context.artifacts.items():
        spec = operation.inputs.get(role)
        if spec is None:
            continue
        for index, _artifact in enumerate(artifacts):
            for kind in spec.with_associated:
                if (role, index, kind) not in context.associations:
                    msg = "Associated type was not captured for this input occurrence"
                    raise ValueError(msg)
            if spec.resolve_references and (role, index) not in context.references:
                msg = "Config input has no captured reference context"
                raise ValueError(msg)


def execute_invocation(
    prepared: PreparedInvocation,
    execute_router: ExecuteRouter,
) -> list[Any]:
    """Execute ordered slots through an injected adapter and reject failures."""
    with phase_timer("execute", prepared.timings):
        try:
            raw_results = execute_router.route_execute(
                prepared.operation,
                prepared.execute_inputs,
                prepared.paths.sandbox,
            )
        except (AssignmentExecutionError, CommandOwnershipError, CancelledError):
            raise
        except Exception as exc:
            commands_closed = close_command_scope(exc)
            error = sanitize_diagnostic(format_error(exc))
            if hasattr(exc, "stdout") and exc.stdout:
                tail = "\n".join(exc.stdout.splitlines()[-30:])
                error += f"\n--- tool stdout (last 30 lines) ---\n{tail}"
            tool_output = (
                _read_tool_output(prepared.paths.log) if commands_closed else None
            )
            raise ExecuteFailure(error, tool_output=tool_output) from exc
        # Reject failed entries before postprocessing while retaining the
        # original error and cancellation evidence from the execute router.
        for result in raw_results:
            if isinstance(result, CommandOwnershipError):
                raise result
        failures = [r for r in raw_results if isinstance(r, Exception)]
        if failures:
            for failure in failures:
                if isinstance(failure, CancelledError):
                    raise failure
            if all(
                isinstance(failure, AssignmentExecutionError) for failure in failures
            ):
                raise failures[0]
            commands_closed = close_command_scope(failures[0])
            msg = (
                f"{len(failures)}/{len(raw_results)} artifact executions "
                f"failed; first: {format_error(failures[0])}"
            )
            # Preserve the original structured failure for durable diagnostics.
            raise ExecuteFailure(
                msg,
                tool_output=(
                    _read_tool_output(prepared.paths.log) if commands_closed else None
                ),
            ) from failures[0]
    return raw_results


def postprocess_invocation(
    prepared: PreparedInvocation,
    raw_results: list[Any],
    *,
    fs: AbstractFileSystem | None = None,
) -> ArtifactResult:
    """Postprocess once, close commands, validate lineage and finalize outputs."""
    check_command_scope()
    with phase_timer("postprocess", prepared.timings):
        memory_outputs, file_outputs = reassemble_results(
            raw_results, prepared.execute_dirs
        )
        artifacts = {
            role: list(values) for role, values in prepared.context.artifacts.items()
        }
        with scientific_phase("postprocess"):
            result = prepared.operation.postprocess(
                PostprocessInput(
                    file_outputs=file_outputs,
                    memory_outputs=memory_outputs,
                    input_artifacts=artifacts,
                    step_number=prepared.step_number,
                    postprocess_dir=prepared.paths.postprocess,
                    files_dir=prepared.paths.files,
                    _context=prepared.context,
                )
            )
            if not result.success:
                raise PostprocessFailure(result.error or "Postprocess failed")
        close_command_scope()
        with scientific_phase("output_validation"):
            validate_artifacts_match_specs(result.artifacts, prepared.operation.outputs)
            validate_lineage_integrity(
                result.lineage,
                {
                    role: [
                        prepared.context.ref(role, index=index).artifact_id
                        for index in range(len(values))
                    ]
                    for role, values in artifacts.items()
                },
                result.artifacts,
                prepared.operation.outputs,
                invocation_id=prepared.context.invocation_id,
            )
            validate_lineage_completeness(
                result.artifacts, prepared.operation.outputs, result.lineage
            )
            # The enclosing pipeline owns new drafts before finalization protects
            # their origin. Existing finalized artifacts keep their original owner.
            if prepared.step_number is not None:
                for values in result.artifacts.values():
                    for artifact in values:
                        if artifact.is_draft:
                            artifact.origin_step_number = prepared.step_number
            finalized = finalize_artifacts(result.artifacts)
        for values in finalized.values():
            for artifact in values:
                if artifact.EXTERNALLY_BACKED:
                    artifact.verify_external_content(fs=fs)
        return result.model_copy(update={"artifacts": finalized})


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


def split_prepared_inputs(
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


def reassemble_results(
    per_artifact_results: list[Any],
    artifact_execute_dirs: list[str | None],
) -> tuple[Any, list[str]]:
    """Merge per-artifact execute results for batched postprocess.

    Reassembles memory_outputs and file_outputs so postprocess sees
    the same data shapes as when execute processes all artifacts at
    once. Operations retain their own explicit source references in those
    results or transported files.

    Args:
        per_artifact_results: One successful result per artifact.
            Execution failures must be handled before reassembly.
        artifact_execute_dirs: Per-artifact execute sub-directory
            paths, in dispatch order.

    Returns:
        Tuple of (merged_memory_outputs, file_outputs).
    """
    file_outputs: list[str] = []
    for directory in artifact_execute_dirs:
        if directory is not None:
            file_outputs.extend(output_snapshot(directory))

    if not per_artifact_results or all(r is None for r in per_artifact_results):
        return None, file_outputs

    if all(isinstance(r, dict) for r in per_artifact_results):
        merged: dict[str, Any] = {}
        for key in per_artifact_results[0]:
            values = [r[key] for r in per_artifact_results if key in r]
            if all(isinstance(v, list) for v in values):
                merged[key] = [item for v in values for item in v]
            else:
                merged[key] = values
        return merged, file_outputs

    return per_artifact_results, file_outputs


def _validate_operation_outputs(operation_class: type) -> None:
    """Raise ValueError if the operation class has no outputs declared."""
    if getattr(operation_class, "outputs", None) is None:
        msg = (
            f"{operation_class.__name__} must define outputs. "
            "Add a ClassVar like: outputs: ClassVar[dict[str, OutputSpec]] = {}"
        )
        raise ValueError(msg)
