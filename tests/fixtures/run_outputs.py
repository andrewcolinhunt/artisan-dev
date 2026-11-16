"""Build source-run outputs through real admission, acceptance and terminal selection."""

from __future__ import annotations

from collections.abc import Sequence

from fsspec import AbstractFileSystem
from fsspec.implementations.local import LocalFileSystem

from artisan.schemas.artifact.base import Artifact
from artisan.storage.core.artifact_store import ArtifactStore
from artisan.storage.io.retained_store import create_retained_store
from artisan.utils.path import uri_join, uri_parent
from fixtures.retained_inspection import commit_inspection_attempt


def commit_outputs(
    delta_root: str,
    *,
    run_id: str = "source-run",
    number: int = 0,
    artifacts: Sequence[Artifact] = (),
    output_ids: dict[str, list[str]] | None = None,
    status: str = "succeeded",
    reused_execution_id: str | None = None,
    fs: AbstractFileSystem | None = None,
    storage_options: dict[str, str] | None = None,
    files_root: str | None = None,
) -> tuple[str, str | None]:
    """Select typed supplied or already accepted artifacts in their given order.

    Failed and unfinished attempts have no accepted outputs. A partial attempt
    contains one accepted result and one failed unit, with no failed output rows.
    """
    if fs is None:
        fs = LocalFileSystem()
    if files_root is None:
        if not isinstance(fs, LocalFileSystem):
            msg = "Nonlocal output fixtures require an explicit files root"
            raise ValueError(msg)
        files_root = uri_join(uri_parent(delta_root), "files")
    committer = create_retained_store(
        delta_root, files_root, fs=fs, storage_options=storage_options
    )
    store = ArtifactStore(
        delta_root, fs=fs, storage_options=storage_options, files_root=files_root
    )
    supplied = {artifact.artifact_id: artifact for artifact in artifacts}
    outputs: dict[str, list[Artifact]] = {}
    if output_ids is None:
        outputs = {"output": list(artifacts)}
    else:
        for role, ids in output_ids.items():
            selected = []
            for artifact_id in ids:
                artifact = supplied.get(artifact_id)
                if artifact is None:
                    artifact = store.get_artifact(artifact_id)
                if artifact is None:
                    msg = f"No supplied or accepted artifact {artifact_id}"
                    raise ValueError(msg)
                selected.append(artifact)
            outputs[role] = selected
    return commit_inspection_attempt(
        delta_root,
        committer=committer,
        run_id=run_id,
        number=number,
        outputs=outputs,
        status=status,
        reused_execution_id=reused_execution_id,
    )
