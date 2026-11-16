"""Import verified candidate-store outputs as independent destination roots."""

from __future__ import annotations

from fsspec import get_filesystem_class
from fsspec.implementations.local import LocalFileSystem

from artisan.errors import ArtifactIntegrityError
from artisan.execution.curator_resources import capture_curator_carrier
from artisan.operations.curator.ingest_pipeline_step import IngestPipelineStep
from artisan.orchestration.engine.current_outputs import read_source_run_outputs
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.execution.curator_result import ArtifactResult
from artisan.storage.core.curator_view import CuratorView, capture_curator_view
from artisan.storage.io.curator_external_reads import curator_carrier_read
from artisan.storage.io.retained_commit import RetainedCommitter
from artisan.storage.io.retained_store import (
    open_retained_store,
    resolve_store_files_root,
)


def import_configured_outputs(
    operation: IngestPipelineStep, *, step_number: int
) -> ArtifactResult:
    """Open the declared source read-only, keeping its credentials independent.

    The omitted local files root is resolved from the current parameters at this
    invocation. External imports still require the curator's resource scope.
    No initialization, recovery or legacy store fallback occurs here.
    """
    params = operation.params
    files_root = _source_files_root(params)
    fs = params.source_storage.filesystem()
    if not fs.exists(params.source_delta_root):
        return ArtifactResult(
            success=False,
            error=f"Source delta root does not exist: {params.source_delta_root}",
        )
    source = open_retained_store(
        params.source_delta_root,
        files_root,
        fs=fs,
        storage_options=params.source_storage.delta_storage_options(),
    )
    return import_candidate_outputs(operation, source, step_number=step_number)


def _source_files_root(params: IngestPipelineStep.Params) -> str:
    if params.source_files_root is None and not params.source_storage.is_local:
        msg = "Cloud source storage requires an explicit source_files_root"
        raise ValueError(msg)
    return resolve_store_files_root(
        params.source_delta_root,
        params.source_files_root,
        protocols=get_filesystem_class(params.source_storage.protocol).protocol,
    )


def import_candidate_outputs(
    operation: IngestPipelineStep,
    source: RetainedCommitter,
    *,
    step_number: int,
) -> ArtifactResult:
    """Redraft sorted unique content with full source copies owned through sealing.

    The explicit source committer supplies its real files root and credentials.
    External imports require the private resource scope of ``run_curator_flow``.
    """
    params = operation.params
    if source.delta_root != params.source_delta_root:
        msg = "Source committer differs from the declared import store"
        raise ValueError(msg)
    selected = read_source_run_outputs(
        source,
        pipeline_run_id=params.source_run_id,
        step_number=params.source_step,
        include_prior_steps=params.include_prior_steps,
    )
    content: dict[tuple[str, str], Artifact] = {}
    contributors = set()
    for step in selected:
        for role in step.outputs:
            for item in role.occurrences:
                artifact = item.artifact
                if params.artifact_type not in {None, artifact.artifact_type}:
                    continue
                assert artifact.artifact_id is not None
                key = (artifact.artifact_type, artifact.artifact_id)
                previous = content.setdefault(key, artifact)
                if previous.to_row() != artifact.to_row():
                    msg = "Selected source content has conflicting protected rows"
                    raise ArtifactIntegrityError(msg)
                contributors.add(step.state.step_run_id)
    if not content:
        mode = "through" if params.include_prior_steps else "at"
        return ArtifactResult(
            success=False,
            error=(
                f"No accepted artifacts found in run {params.source_run_id!r} "
                f"{mode} step {params.source_step}"
                + (
                    f" with type {params.artifact_type!r}"
                    if params.artifact_type is not None
                    else ""
                )
            ),
        )
    view = CuratorView(source, capture_curator_view(source))
    drafts: dict[str, list[Artifact]] = {}
    for (artifact_type, _), artifact in sorted(content.items()):
        localized = _localize(view, artifact)
        drafts.setdefault(artifact_type, []).append(
            operation._to_draft(localized, step_number)
        )
    return ArtifactResult(
        artifacts=drafts,
        lineage={role: [] for role in drafts},
        metadata={
            "ingest_source": {
                "pipeline_run_id": params.source_run_id,
                "source_step": params.source_step,
                "include_prior_steps": params.include_prior_steps,
                "step_run_ids": sorted(contributors),
            }
        },
    )


def _localize(view: CuratorView, artifact: Artifact) -> Artifact:
    protected = artifact.to_row()
    if not artifact.EXTERNALLY_BACKED:
        return artifact
    assert artifact.artifact_id is not None
    locator = next(iter(artifact.LOCATOR_FIELDS))
    uri = getattr(artifact, locator)
    witness = curator_carrier_read(view, artifact.artifact_id, uri)
    path = capture_curator_carrier(
        artifact,
        uri=uri,
        digest=witness.carrier.digest,
        size_bytes=witness.carrier.size_bytes,
        fs=view.committer.fs,
    )
    localized = artifact.model_copy(deep=True, update={locator: str(path)})
    if localized.to_row() != protected:
        msg = "Source localization changed a protected artifact row"
        raise ArtifactIntegrityError(msg)
    localized.verify_external_content(fs=LocalFileSystem())
    return localized
