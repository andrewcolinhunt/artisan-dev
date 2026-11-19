"""Freeze typed input evidence before dispatch and reload it after controller exit."""

from __future__ import annotations

import posixpath
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from types import MappingProxyType
from typing import cast

from fsspec import AbstractFileSystem
from fsspec.implementations.local import LocalFileSystem

from artisan.errors import StoreIntegrityError
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.execution_config import ExecutionConfigArtifact
from artisan.schemas.artifact.external import open_external
from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.schemas.composites.plan import ExecutionRecipe
from artisan.schemas.execution.admission import (
    AdmissionRecord,
    AdmissionRecordWithAccess,
    AdmissionReference,
    AnyAdmissionRecord,
    ManagedArtifactBinding,
    ManagedCarrier,
    parse_admission_record,
)
from artisan.schemas.execution.assignment import AssignmentAttempt
from artisan.schemas.execution.external_access import ExternalAccessRecord
from artisan.schemas.execution.retained_result import RetainedTableFile
from artisan.schemas.provenance.occurrence import OccurrenceAddress
from artisan.schemas.provenance.projection import (
    AcceptedProducer,
    BoundaryProjection,
    ProjectedOccurrence,
)
from artisan.storage.core.store_format import assert_retained_store
from artisan.storage.io.bounded_io import CapturedParquet
from artisan.storage.io.publication import (
    publish_immutable_bytes,
    publish_immutable_file,
)
from artisan.storage.io.retained_artifacts import (
    hydrate_artifact_content,
    validate_retained_membership,
)
from artisan.storage.io.table_payload import capture_table_payload, stage_table_payload
from artisan.utils.hashing import (
    canonical_json_bytes,
    compute_content_digest,
    compute_stream_digest,
)
from artisan.utils.path import uri_join

type ProducerVerifier = Callable[[AcceptedProducer, ProjectedOccurrence], None]


@dataclass(frozen=True)
class CapturedAdmission:
    """Validated input records with managed carriers and external access evidence."""

    record: AnyAdmissionRecord
    reference: AdmissionReference
    artifacts: Mapping[OccurrenceAddress, Artifact]
    carriers: Mapping[str, ManagedCarrier]
    external_access: ExternalAccessRecord | None = None


def admission_directory(delta_root: str, attempt: AssignmentAttempt) -> str:
    """Locate one generation's immutable inputs within the controller's store."""
    return uri_join(
        delta_root,
        "_artisan/admissions",
        attempt.assignment.assignment_id,
        str(attempt.generation),
    )


def publish_admission(
    attempt: AssignmentAttempt,
    recipe: ExecutionRecipe,
    boundary: BoundaryProjection,
    artifacts: Mapping[OccurrenceAddress, Artifact],
    *,
    step_number: int,
    delta_root: str,
    files_root: str,
    fs: AbstractFileSystem,
    artifact_fs: AbstractFileSystem | None = None,
    verify_producer: ProducerVerifier | None = None,
    external_access: ExternalAccessRecord | None = None,
) -> AdmissionReference:
    """Publish required typed input evidence without starting work or claiming it.

    The caller records the returned digest in the assignment journal before
    dispatch. Existing managed carriers are verified and reused; other managed
    file inputs are copied once to immutable locations. Externally owned bundles
    retain only descriptors and access evidence. Source objects remain unchanged.
    Optional diagnostics do not participate in this evidence.
    """
    assert_retained_store(delta_root, fs)
    attempt = AssignmentAttempt.model_validate(attempt)
    boundary = BoundaryProjection.model_validate(boundary)
    # Validate the record's scientific shape before copying any payload.
    AdmissionRecord(
        attempt=attempt,
        step_number=step_number,
        recipe=recipe,
        boundary=boundary,
        tables=(),
    )
    _validate_input_contract(recipe, boundary)
    validated = validate_retained_membership(
        boundary, {}, artifacts, artifact_fs=artifact_fs or fs
    )
    _verify_producers(boundary, verify_producer)
    content = _unique_content(validated)
    external_access = _validate_external_content(attempt, content, external_access)
    directory = admission_directory(delta_root, attempt)
    with TemporaryDirectory(prefix="artisan-admission-stage-") as temporary:
        scratch = Path(temporary)
        carriers, bindings = _manage_carriers(
            content, files_root, scratch, fs, artifact_fs
        )
        tables = _stage_input_tables(content, directory, scratch, fs)
        record: AnyAdmissionRecord = AdmissionRecord(
            attempt=attempt,
            step_number=step_number,
            recipe=recipe,
            boundary=boundary,
            tables=tables,
            carriers=carriers,
            external_bindings=bindings,
        )
        if external_access is not None:
            record = AdmissionRecordWithAccess(
                **record.model_dump(exclude={"schema_version"}),
                access_digest=external_access.digest,
                access_artifact_ids=tuple(
                    binding.artifact_id for binding in external_access.bindings
                ),
            )
            publish_immutable_bytes(
                fs,
                uri_join(directory, "external-access.json"),
                canonical_json_bytes(external_access.model_dump(mode="json")),
            )
        encoded = canonical_json_bytes(record.model_dump(mode="json"))
        publish_immutable_bytes(fs, uri_join(directory, "admission.json"), encoded)
    return AdmissionReference(
        attempt=attempt, record_digest=compute_content_digest(encoded)
    )


@contextmanager
def capture_admission(
    reference: AdmissionReference,
    *,
    delta_root: str,
    files_root: str,
    fs: AbstractFileSystem,
    verify_producer: ProducerVerifier | None = None,
) -> Iterator[CapturedAdmission]:
    """Reload the exact journal-bound admission without importing recorded code."""
    reference = AdmissionReference.model_validate(reference)
    record = read_admission_record(reference, delta_root=delta_root, fs=fs)
    directory = admission_directory(delta_root, reference.attempt)
    external_access = _read_external_access(record, directory, fs)
    locations = {}
    for carrier in record.carriers:
        location = uri_join(files_root, carrier.relative_path)
        verify_managed_carrier(carrier, location, fs)
        locations[carrier.digest] = location
    with TemporaryDirectory(prefix="artisan-admission-capture-") as temporary:
        tables = _capture_input_tables(record.tables, directory, Path(temporary), fs)
        bindings = {
            item.artifact_id: item.carrier_digest for item in record.external_bindings
        }
        content = hydrate_artifact_content(tables, locations, bindings, artifact_fs=fs)
        expected = {item.artifact_id for item in record.boundary.occurrences}
        if set(content) != expected:
            msg = "Admitted input tables do not match the exact boundary content"
            raise StoreIntegrityError(msg)
        artifacts = {
            item.address: content[item.artifact_id]
            for item in record.boundary.occurrences
        }
        validate_retained_membership(record.boundary, {}, artifacts, artifact_fs=fs)
        _validate_external_content(record.attempt, content, external_access)
        _verify_producers(record.boundary, verify_producer)
        by_digest = {item.digest: item for item in record.carriers}
        yield CapturedAdmission(
            record,
            reference,
            MappingProxyType(artifacts),
            MappingProxyType(
                {
                    item.artifact_id: by_digest[item.carrier_digest]
                    for item in record.external_bindings
                }
            ),
            external_access,
        )


def read_admission_record(
    reference: AdmissionReference,
    *,
    delta_root: str,
    fs: AbstractFileSystem,
) -> AnyAdmissionRecord:
    """Read exact admitted metadata without granting content or reuse authority.

    This checks the immutable record and input contract only. Acceptance, replay
    or cache use must independently capture content and verify selected producers.
    """
    assert_retained_store(delta_root, fs)
    reference = AdmissionReference.model_validate(reference)
    directory = admission_directory(delta_root, reference.attempt)
    with fs.open(
        uri_join(directory, "admission.json"), "rb", cache_type="none"
    ) as stream:
        encoded = stream.read()
    if compute_content_digest(encoded) != reference.record_digest:
        msg = "Admitted input record digest changed"
        raise StoreIntegrityError(msg)
    record = parse_admission_record(encoded)
    if (
        record.attempt != reference.attempt
        or canonical_json_bytes(record.model_dump(mode="json")) != encoded
    ):
        msg = "Admitted input record has changed ownership or noncanonical encoding"
        raise StoreIntegrityError(msg)
    _validate_input_contract(record.recipe, record.boundary)
    _read_external_access(record, directory, fs)
    return record


def _read_external_access(
    record: AnyAdmissionRecord, directory: str, fs: AbstractFileSystem
) -> ExternalAccessRecord | None:
    expected = {
        item.artifact_id
        for item in record.boundary.occurrences
        if cast(
            type[Artifact], ArtifactTypeDef.get(item.artifact_type).model
        ).EXTERNAL_REFERENCE
    }
    if isinstance(record, AdmissionRecord):
        if expected:
            msg = "External reference artifacts require versioned access evidence"
            raise StoreIntegrityError(msg)
        return None
    with fs.open(
        uri_join(directory, "external-access.json"), "rb", cache_type="none"
    ) as stream:
        encoded = stream.read()
    if compute_content_digest(encoded) != record.access_digest:
        msg = "Admitted external access record digest changed"
        raise StoreIntegrityError(msg)
    access = ExternalAccessRecord.model_validate_json(encoded)
    if (
        canonical_json_bytes(access.model_dump(mode="json")) != encoded
        or access.attempt != record.attempt
        or tuple(binding.artifact_id for binding in access.bindings)
        != record.access_artifact_ids
        or set(record.access_artifact_ids) != expected
    ):
        msg = "External access record does not match exact admitted membership"
        raise StoreIntegrityError(msg)
    return access


def _validate_external_content(
    attempt: AssignmentAttempt,
    content: Mapping[str, Artifact],
    access: ExternalAccessRecord | None,
) -> ExternalAccessRecord | None:
    expected = {
        artifact_id: artifact.external_descriptor()
        for artifact_id, artifact in content.items()
        if artifact.EXTERNAL_REFERENCE
    }
    if access is None:
        if expected:
            msg = "External reference inputs require explicit access bindings"
            raise StoreIntegrityError(msg)
        return None
    access = ExternalAccessRecord.model_validate(access)
    selected = {
        binding.artifact_id: binding.prepared.descriptor for binding in access.bindings
    }
    if access.attempt != attempt or selected != expected:
        msg = "External access bindings differ from admitted artifact descriptors"
        raise StoreIntegrityError(msg)
    return access


def verify_managed_carrier(
    carrier: ManagedCarrier, path: str, fs: AbstractFileSystem
) -> None:
    """Verify complete carrier bytes independently of contained artifact hashes."""
    with fs.open(path, "rb", cache_type="none") as stream:
        observed = compute_stream_digest(stream)
    if observed != (carrier.digest, carrier.size_bytes):
        msg = "Managed carrier bytes changed"
        raise StoreIntegrityError(msg)


def _unique_content(
    artifacts: Mapping[OccurrenceAddress, Artifact],
) -> dict[str, Artifact]:
    content: dict[str, Artifact] = {}
    for artifact in artifacts.values():
        assert artifact.artifact_id is not None
        existing = content.get(artifact.artifact_id)
        if existing is not None and existing.to_row() != artifact.to_row():
            msg = "Admitted occurrences of one content identity have conflicting protected rows"
            raise StoreIntegrityError(msg)
        content[artifact.artifact_id] = artifact
    return content


def _verify_producers(
    boundary: BoundaryProjection, verify: ProducerVerifier | None
) -> None:
    occurrences = {item.address: item for item in boundary.occurrences}
    for attribution in boundary.input_attribution:
        if attribution.producer is None:
            continue
        if verify is None:
            msg = "Selected input producers require accepted-winner verification"
            raise StoreIntegrityError(msg)
        verify(attribution.producer, occurrences[attribution.boundary])


def _validate_input_contract(
    recipe: ExecutionRecipe, boundary: BoundaryProjection
) -> None:
    specs = {item.role: item for item in recipe.inputs}
    roles = boundary.input_roles()
    if (
        roles.keys() - specs.keys()
        or {role for role, spec in specs.items() if spec.required} - roles.keys()
    ):
        msg = "Admitted input roles disagree with the frozen recipe"
        raise StoreIntegrityError(msg)
    occurrences = {item.address: item for item in boundary.occurrences}
    for role, addresses in roles.items():
        for address in addresses:
            if not ArtifactTypes.matches(
                specs[role].artifact_type, occurrences[address].artifact_type
            ):
                msg = "Admitted input artifact type disagrees with the frozen recipe"
                raise StoreIntegrityError(msg)
            for associated_type in specs[role].with_associated:
                boundary.frozen_context.associated(address, associated_type)
            model = ArtifactTypeDef.get(occurrences[address].artifact_type).model
            if issubclass(model, ExecutionConfigArtifact) and (
                specs[role].materialize or specs[role].resolve_references
            ):
                boundary.frozen_context.referenced(address)


def _stage_input_tables(
    content: Mapping[str, Artifact],
    directory: str,
    scratch: Path,
    fs: AbstractFileSystem,
) -> tuple[RetainedTableFile, ...]:
    tables = []
    for kind in sorted({artifact.artifact_type for artifact in content.values()}):
        definition = ArtifactTypeDef.get(kind)
        tables.append(
            stage_table_payload(
                definition.table_path,
                definition.parquet_filename(),
                (
                    artifact.to_row()
                    for _, artifact in sorted(content.items())
                    if artifact.artifact_type == kind
                ),
                definition.polars_schema(),
                directory,
                scratch,
                fs,
            )
        )
    return tuple(sorted(tables, key=lambda item: item.filename))


def _capture_input_tables(
    inventory: tuple[RetainedTableFile, ...],
    directory: str,
    scratch: Path,
    fs: AbstractFileSystem,
) -> dict[str, CapturedParquet]:
    registered = {
        definition.table_path: definition
        for definition in ArtifactTypeDef.get_all().values()
    }
    tables = {}
    for item in inventory:
        definition = registered.get(item.table_path)
        if (
            definition is None
            or item.filename != definition.parquet_filename()
            or item.row_count == 0
        ):
            msg = "Admission contains an unregistered or empty input table"
            raise StoreIntegrityError(msg)
        tables[item.table_path] = capture_table_payload(
            item,
            directory=directory,
            scratch=scratch,
            expected_schema=definition.polars_schema(),
            fs=fs,
        )
    return tables


def _manage_carriers(
    content: Mapping[str, Artifact],
    files_root: str,
    scratch: Path,
    fs: AbstractFileSystem,
    artifact_fs: AbstractFileSystem | None,
) -> tuple[tuple[ManagedCarrier, ...], tuple[ManagedArtifactBinding, ...]]:
    sources: dict[str, tuple[ManagedCarrier, str]] = {}
    carriers: dict[str, ManagedCarrier] = {}
    bindings = []
    for artifact_id, artifact in sorted(content.items()):
        if not artifact.EXTERNALLY_BACKED:
            continue
        locator = next(iter(artifact.LOCATOR_FIELDS))
        source = getattr(artifact, locator)
        if not isinstance(source, str) or not source:
            msg = "Externally backed admitted input has no source location"
            raise StoreIntegrityError(msg)
        if source not in sources:
            sources[source] = _manage_source(
                source, files_root, scratch / f"source-{len(sources)}", fs, artifact_fs
            )
        carrier, location = sources[source]
        copy = artifact.model_copy(deep=True)
        setattr(copy, locator, location)
        copy.verify_external_content(fs=fs)
        carriers[carrier.digest] = carrier
        bindings.append(
            ManagedArtifactBinding(
                artifact_id=artifact_id, carrier_digest=carrier.digest
            )
        )
    return tuple(carriers[digest] for digest in sorted(carriers)), tuple(bindings)


def _manage_source(
    source: str,
    files_root: str,
    scratch: Path,
    fs: AbstractFileSystem,
    artifact_fs: AbstractFileSystem | None,
) -> tuple[ManagedCarrier, str]:
    # A managed file already has an immutable content-addressed location. Reading
    # its bytes is required; another full carrier copy is not.
    name = posixpath.basename(source)
    digest = name.removesuffix(".bin")
    if (
        name.endswith(".bin")
        and len(digest) == 32
        and all(character in "0123456789abcdef" for character in digest)
    ):
        candidate = uri_join(files_root, "carriers", digest[:2], name)
        if source == candidate:
            with fs.open(source, "rb", cache_type="none") as stream:
                actual, size = compute_stream_digest(stream)
            if actual != digest:
                msg = "Managed input carrier differs from its content-addressed path"
                raise StoreIntegrityError(msg)
            return ManagedCarrier(digest=actual, size_bytes=size), source
    with open_external(source, artifact_fs) as stream, scratch.open("xb") as target:
        digest, size = compute_stream_digest(stream, destination=target)
    carrier = ManagedCarrier(digest=digest, size_bytes=size)
    location = uri_join(files_root, carrier.relative_path)
    publish_immutable_file(fs, location, str(scratch), source_fs=LocalFileSystem())
    return carrier, location
