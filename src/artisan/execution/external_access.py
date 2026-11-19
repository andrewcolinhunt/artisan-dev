"""Select and expose ordinary artifacts backed by explicitly owned installations."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path
from types import MappingProxyType
from typing import Literal, Protocol, runtime_checkable

from artisan.errors import StoreIntegrityError
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.external_descriptor import PreparedExternalFiles
from artisan.schemas.execution.assignment import AssignmentAttempt
from artisan.schemas.execution.external_access import (
    ExternalAccessBinding,
    ExternalAccessMount,
    ExternalAccessRecord,
)
from artisan.storage.external_access import verify_prepared_external_files


@runtime_checkable
class ExternalAccessProvider(Protocol):
    """Explicit provider placement, independent of scientific software identity."""

    @property
    def external_files(self) -> Mapping[str, PreparedExternalFiles]: ...

    @property
    def external_provider(self) -> Literal["local", "modal"]: ...

    @property
    def external_mounts(self) -> tuple[ExternalAccessMount, ...]: ...


def freeze_external_files(
    bindings: Mapping[str, PreparedExternalFiles] | None,
) -> Mapping[str, PreparedExternalFiles]:
    """Detach caller configuration without accessing an installation's files."""
    frozen = {}
    for artifact_id, prepared in (bindings or {}).items():
        item = ExternalAccessBinding(
            artifact_id=artifact_id,
            prepared=PreparedExternalFiles.model_validate(prepared.model_dump()),
        )
        frozen[item.artifact_id] = item.prepared
    return MappingProxyType(frozen)


def _external_artifacts(artifacts: Iterable[Artifact]) -> dict[str, Artifact]:
    selected: dict[str, Artifact] = {}
    for artifact in artifacts:
        if not artifact.EXTERNAL_REFERENCE:
            continue
        artifact.finalize()
        assert artifact.artifact_id is not None
        previous = selected.setdefault(artifact.artifact_id, artifact)
        if previous.to_row() != artifact.to_row():
            msg = "One external artifact identity has conflicting descriptors"
            raise ValueError(msg)
    return selected


def select_external_files(
    artifacts: Iterable[Artifact],
    external_files: Mapping[str, PreparedExternalFiles],
) -> tuple[ExternalAccessBinding, ...]:
    """Select exact declared dependencies without probing external payloads."""
    selected = []
    for artifact_id, artifact in sorted(_external_artifacts(artifacts).items()):
        prepared = external_files.get(artifact_id)
        if prepared is None:
            msg = f"Missing explicit external access binding for artifact {artifact_id}"
            raise ValueError(msg)
        binding = ExternalAccessBinding(artifact_id=artifact_id, prepared=prepared)
        if artifact.external_descriptor() != binding.prepared.descriptor:
            msg = f"External binding descriptor differs from artifact {artifact_id}"
            raise ValueError(msg)
        selected.append(binding)
    return tuple(selected)


def bind_external_files(
    artifacts: Iterable[Artifact],
    bindings: Sequence[ExternalAccessBinding],
    *,
    verified_mounts: Sequence[Path] = (),
) -> None:
    """Check prepared installations and attach paths only to supplied live objects."""
    values = tuple(artifacts)
    configured = {item.artifact_id: item.prepared for item in bindings}
    selected = select_external_files(values, configured)
    if tuple(bindings) != selected:
        msg = "External access bindings differ from exact selected artifact membership"
        raise ValueError(msg)
    roots = {
        item.artifact_id: verify_prepared_external_files(
            item.prepared, verified_mounts=verified_mounts
        )
        for item in selected
    }
    for artifact in values:
        if artifact.EXTERNAL_REFERENCE:
            assert artifact.artifact_id is not None
            artifact.bind_external_root(roots[artifact.artifact_id])


def capture_external_access(
    attempt: AssignmentAttempt,
    artifacts: Iterable[Artifact],
    provider: object,
) -> ExternalAccessRecord | None:
    """Freeze selected placement evidence without changing scientific identity."""
    selected = _external_artifacts(artifacts)
    if not selected:
        return None
    if not isinstance(provider, ExternalAccessProvider):
        msg = "Selected provider does not support externally owned artifact access"
        raise ValueError(msg)
    return ExternalAccessRecord(
        attempt=attempt,
        provider=provider.external_provider,
        bindings=select_external_files(selected.values(), provider.external_files),
        mounts=provider.external_mounts,
    )


def verify_external_access(
    attempt: AssignmentAttempt,
    artifacts: Iterable[Artifact],
    record: ExternalAccessRecord | None,
    provider: object,
) -> None:
    """Reject provider substitution, then expose the admitted installations."""
    values = tuple(artifacts)
    expected = capture_external_access(attempt, values, provider)
    if expected != record:
        msg = "External access differs from the exact admitted provider placement"
        raise StoreIntegrityError(msg)
    if record is not None:
        bind_external_files(
            values,
            record.bindings,
            verified_mounts=tuple(mount.target for mount in record.mounts),
        )
