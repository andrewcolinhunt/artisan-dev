"""Read-only retained inspection copies preserving original source assertions."""

from __future__ import annotations

import shutil
from collections.abc import Iterable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from types import MappingProxyType
from typing import Any, Literal

import polars as pl
from fsspec.implementations.local import LocalFileSystem
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from artisan.errors import StoreIntegrityError
from artisan.execution.recording.retained_replay import (
    read_replay_configuration,
    read_replay_diagnostic,
)
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.file_ref import FileRefArtifact
from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.schemas.composites.plan import ExecutionRecipe
from artisan.schemas.enums import TablePath
from artisan.schemas.execution.admission import ManagedArtifactBinding, ManagedCarrier
from artisan.schemas.execution.assignment import (
    ContentDigest,
    DeliveryIdentity,
    SoftwareDigest,
)
from artisan.schemas.execution.curator_admission import (
    CuratorInputAssociation,
    CuratorInputRole,
    CuratorReadSnapshot,
)
from artisan.schemas.execution.curator_definition import CuratorDefinitionContract
from artisan.schemas.execution.retained_replay import (
    RetainedReplayConfiguration,
    RetainedReplayDiagnostic,
    RetainedReplayDiagnosticRecord,
)
from artisan.schemas.execution.retained_result import RetainedTableFile
from artisan.schemas.orchestration.step_lifecycle import StepStatus
from artisan.schemas.provenance.curator_projection import CuratorProjection
from artisan.schemas.provenance.projection import BoundaryProjection
from artisan.storage._publication_filesystem import _PublicationFileSystem
from artisan.storage._publication_policy import PublicationPolicy
from artisan.storage._publication_provenance import (
    curator_artifact_edges,
    curator_execution_edges,
)
from artisan.storage._publication_runs import InspectedRuns
from artisan.storage.core.artifact_store import ArtifactStore
from artisan.storage.core.committed_scan import scan_plan_keys
from artisan.storage.core.curator_reader import CuratorReader, _validate_references
from artisan.storage.core.retained_reader import RetainedReader
from artisan.storage.core.table_schemas import (
    ARTIFACT_EDGES_SCHEMA,
    ARTIFACT_INDEX_SCHEMA,
    EXECUTION_EDGES_SCHEMA,
)
from artisan.storage.io.curator_admission import read_curator_admission
from artisan.storage.io.curator_plan import CuratorCommitPlan
from artisan.storage.io.input_registration import read_registered_inputs
from artisan.storage.io.input_registration_plan import InputRegistrationPlan
from artisan.storage.io.retained_artifacts import (
    hydrate_artifact_content,
    validate_retained_membership,
)
from artisan.storage.io.retained_metadata import (
    read_curator_source_metadata,
    read_terminal_metadata,
)
from artisan.storage.io.retained_plan import RetainedCommitPlan
from artisan.storage.io.retained_read import RetainedReadSnapshot
from artisan.storage.io.table_payload import capture_table_payload, stage_table_payload
from artisan.utils.hashing import (
    canonical_json_bytes,
    compute_content_digest,
    compute_stream_digest,
)
from artisan.utils.path import uri_join

_MANIFEST = "inspection.json"


class _InspectionModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid", frozen=True, revalidate_instances="always"
    )

    @field_validator("schema_version", mode="before", check_fields=False)
    @classmethod
    def _strict_version(cls, value: object) -> object:
        if type(value) is not int:
            msg = "Inspection version must be an integer"
            raise ValueError(msg)
        return value


class InspectedResult(_InspectionModel):
    """Protected original provenance assertions, without import or claim authority."""

    delivery: DeliveryIdentity
    seal_digest: ContentDigest
    admission_digest: ContentDigest
    recipe: ExecutionRecipe
    projection: BoundaryProjection
    replay_configuration: RetainedReplayConfiguration
    replay_diagnostic: RetainedReplayDiagnostic | None = None
    omitted_locators: tuple[Literal["source_seal_uri"]] = ("source_seal_uri",)
    redacted_configuration_fields: tuple[Literal["unavailable_reason"], ...] = ()

    @model_validator(mode="after")
    def _validate_source(self) -> InspectedResult:
        if (
            self.recipe.digest != self.delivery.attempt.recipe_digest
            or self.projection.input_context_digest
            != self.delivery.attempt.input_context_digest
            or self.replay_configuration.delivery != self.delivery
            or self.replay_configuration.seal_digest != self.seal_digest
            or self.replay_configuration.admission_digest != self.admission_digest
        ):
            msg = "Inspection source assertions disagree on original result identity"
            raise ValueError(msg)
        if self.replay_diagnostic is not None:
            RetainedReplayDiagnosticRecord(
                delivery=self.delivery,
                seal_digest=self.seal_digest,
                diagnostic=self.replay_diagnostic,
            )
        return self


class InspectedCurator(_InspectionModel):
    """Original curator assertions, without executable configuration or authority.

    Admission and plan digests identify the original complete records. They are
    not hashes of this deliberately smaller inspection representation.
    """

    step_run_id: ContentDigest
    execution_run_id: ContentDigest
    plan_digest: ContentDigest
    admission_digest: ContentDigest
    operation_name: str = Field(min_length=1, strict=True)
    operation_version: str = Field(min_length=1, strict=True)
    definition: CuratorDefinitionContract | None
    software_digest: SoftwareDigest | None
    inputs: tuple[CuratorInputRole, ...]
    associations: tuple[CuratorInputAssociation, ...]
    view: CuratorReadSnapshot
    projection: CuratorProjection
    success: bool = Field(strict=True)
    replay_of_execution_run_id: str | None
    omitted_fields: tuple[Literal["step_record"]] = ("step_record",)

    @model_validator(mode="after")
    def _validate_source(self) -> InspectedCurator:
        if (self.definition is None) != (self.software_digest is None):
            msg = "Curator inspection requires both definition and software identity"
            raise ValueError(msg)
        if self.definition is not None and (
            self.definition.name != self.operation_name
            or self.definition.version != self.operation_version
        ):
            msg = "Curator inspection definition differs from its operation identity"
            raise ValueError(msg)
        if any(
            item.occurrence.address.invocation_id != self.step_run_id
            for item in self.projection.occurrences
        ):
            msg = "Curator inspection membership belongs to another source"
            raise ValueError(msg)
        if not self.success and self.projection.occurrences:
            msg = "Failed curator inspection cannot publish output content"
            raise ValueError(msg)
        return self


class InspectedRegistration(_InspectionModel):
    """Original completed acquisition assertions, without source URIs or execution.

    The plan digest identifies the original URI-bearing record, not this smaller
    inspection representation. Repeated input identities retain acquisition order.
    """

    step_run_id: ContentDigest
    step_number: int = Field(ge=0, strict=True)
    operation_name: str = Field(min_length=1, strict=True)
    plan_digest: ContentDigest
    input_artifact_ids: tuple[ContentDigest, ...] = Field(min_length=1)
    omitted_locators: tuple[Literal["input_uris"]] = ("input_uris",)


class RetainedInspectionManifest(_InspectionModel):
    """Closed inspection format; its digest is never an original worker seal."""

    domain: Literal["artisan.retained-inspection"] = "artisan.retained-inspection"
    schema_version: Literal[1] = 1
    purpose: Literal["sanitized_inspection"] = "sanitized_inspection"
    sources: tuple[InspectedResult, ...]
    curator_sources: tuple[InspectedCurator, ...]
    registration_sources: tuple[InspectedRegistration, ...]
    artifact_index: RetainedTableFile
    curator_artifact_edges: RetainedTableFile
    runs: InspectedRuns | None = None
    tables: tuple[RetainedTableFile, ...]
    carriers: tuple[ManagedCarrier, ...]
    external_bindings: tuple[ManagedArtifactBinding, ...]
    inspection_digest: ContentDigest

    @model_validator(mode="after")
    def _validate_inventory(self) -> RetainedInspectionManifest:
        sources = [source.delivery.delivery_id for source in self.sources]
        curators = [source.step_run_id for source in self.curator_sources]
        registrations = [source.step_run_id for source in self.registration_sources]
        executions = sources + [
            source.execution_run_id for source in self.curator_sources
        ]
        tables = [table.table_path for table in self.tables]
        files = [table.filename for table in self.tables]
        files.append(self.artifact_index.filename)
        files.append(self.curator_artifact_edges.filename)
        if self.artifact_index.table_path != TablePath.ARTIFACT_INDEX.value:
            msg = "Inspection index has another logical schema"
            raise ValueError(msg)
        if self.curator_artifact_edges.table_path != TablePath.ARTIFACT_EDGES.value:
            msg = "Inspection curator edges have another logical schema"
            raise ValueError(msg)
        if self.runs is not None:
            files.append(self.runs.lifecycle.filename)
        carriers = [carrier.digest for carrier in self.carriers]
        bindings = [binding.artifact_id for binding in self.external_bindings]
        if (
            any(
                values != sorted(set(values))
                for values in (
                    sources,
                    curators,
                    registrations,
                    tables,
                    carriers,
                    bindings,
                )
            )
            or len(files) != len(set(files))
            or len(executions) != len(set(executions))
        ):
            msg = "Inspection inventory must be unique and canonically ordered"
            raise ValueError(msg)
        if {binding.carrier_digest for binding in self.external_bindings} != set(
            carriers
        ):
            msg = "Inspection carrier membership is incomplete"
            raise ValueError(msg)
        if self.inspection_digest != _inspection_digest(
            self.model_dump(mode="json", exclude={"inspection_digest"})
        ):
            msg = "Inspection digest does not match copied evidence"
            raise ValueError(msg)
        if self.runs is not None:
            self.runs.validate_sources(self)
        return self


@dataclass(frozen=True)
class PublicationExport:
    """An inspection directory with no acceptance, cache, repair or resume methods."""

    directory: Path

    def open_store(self) -> ArtifactStore:
        """Open the ordinary query facade with confined, read-only inspection data."""
        from artisan.storage.core.publication_view import PublicationReadScope

        return PublicationReadScope(self).open_store()

    def metadata(self) -> RetainedInspectionManifest:
        """Read closed metadata without loading recorded operation/artifact modules."""
        path = self.directory / _MANIFEST
        encoded = path.read_bytes()
        manifest = RetainedInspectionManifest.model_validate_json(encoded)
        if canonical_json_bytes(manifest.model_dump(mode="json")) != encoded:
            msg = "Inspection manifest is not canonically encoded"
            raise StoreIntegrityError(msg)
        expected = {
            _MANIFEST,
            manifest.artifact_index.filename,
            manifest.curator_artifact_edges.filename,
            *(item.filename for item in manifest.tables),
            *(item.relative_path for item in manifest.carriers),
        }
        if manifest.runs is not None:
            expected.add(manifest.runs.lifecycle.filename)
        actual = {
            str(path.relative_to(self.directory))
            for path in self.directory.rglob("*")
            if path.is_file()
        }
        if (
            any(path.is_symlink() for path in self.directory.rglob("*"))
            or actual != expected
        ):
            msg = "Inspection directory differs from its closed file inventory"
            raise StoreIntegrityError(msg)
        return manifest

    def artifact_index(self) -> pl.DataFrame:
        """Read protected metadata without artifact models or external carrier bytes."""
        return self._artifact_index(self.metadata())

    def curator_artifact_edges(self, *, ordinary: bool = True) -> pl.DataFrame:
        """Read original declared relations without constructing acceptance authority."""
        manifest = self.metadata()
        return self._curator_artifact_edges(
            manifest, self._artifact_index(manifest), ordinary=ordinary
        )

    def curator_execution_edges(self, *, ordinary: bool = True) -> pl.DataFrame:
        """Read losslessly derived original memberships, including failed inputs."""
        manifest = self.metadata()
        self._curator_artifact_edges(
            manifest, self._artifact_index(manifest), ordinary=False
        )
        return curator_execution_edges(manifest.curator_sources, ordinary=ordinary)

    def _curator_artifact_edges(
        self,
        manifest: RetainedInspectionManifest,
        index: pl.DataFrame,
        *,
        ordinary: bool,
    ) -> pl.DataFrame:
        with TemporaryDirectory(prefix="artisan-inspection-edges-") as scratch:
            table = capture_table_payload(
                manifest.curator_artifact_edges,
                directory=str(self.directory),
                scratch=Path(scratch),
                expected_schema=ARTIFACT_EDGES_SCHEMA,
                fs=_PublicationFileSystem(self.directory),
            )
            frame = pl.read_parquet(table.path)
        return curator_artifact_edges(frame, manifest, index, ordinary=ordinary)

    def _artifact_index(self, manifest: RetainedInspectionManifest) -> pl.DataFrame:
        with TemporaryDirectory(prefix="artisan-inspection-index-") as scratch:
            table = capture_table_payload(
                manifest.artifact_index,
                directory=str(self.directory),
                scratch=Path(scratch),
                expected_schema=ARTIFACT_INDEX_SCHEMA,
                fs=_PublicationFileSystem(self.directory),
            )
            index = pl.read_parquet(table.path)
        _validate_artifact_index(manifest, index)
        return index

    @contextmanager
    def artifacts(self) -> Iterator[Mapping[str, Artifact]]:
        """Hydrate already registered schemas through a confined read-only filesystem."""
        manifest = self.metadata()
        index = self._artifact_index(manifest)
        self._curator_artifact_edges(manifest, index, ordinary=False)
        fs = _PublicationFileSystem(self.directory)
        by_path = {item.table_path: item for item in ArtifactTypeDef.get_all().values()}
        with TemporaryDirectory(prefix="artisan-inspection-") as scratch:
            tables = {}
            for item in manifest.tables:
                if item.table_path not in by_path:
                    msg = "Inspection artifact type must be registered by the caller"
                    raise ValueError(msg)
                tables[item.table_path] = capture_table_payload(
                    item,
                    directory=str(self.directory),
                    scratch=Path(scratch),
                    expected_schema=by_path[item.table_path].polars_schema(),
                    fs=fs,
                )
            locations = {}
            for carrier in manifest.carriers:
                path = str(self.directory / carrier.relative_path)
                with fs.open(path, "rb") as stream:
                    if compute_stream_digest(stream) != (
                        carrier.digest,
                        carrier.size_bytes,
                    ):
                        msg = "Inspection carrier bytes changed"
                        raise StoreIntegrityError(msg)
                locations[carrier.digest] = path
            bindings = {
                item.artifact_id: item.carrier_digest
                for item in manifest.external_bindings
            }
            content = hydrate_artifact_content(
                tables, locations, bindings, artifact_fs=fs
            )
            expected = {
                item.artifact_id
                for source in manifest.sources
                for item in source.projection.occurrences
            }
            expected.update(
                item.occurrence.artifact_id
                for source in manifest.curator_sources
                for item in source.projection.occurrences
            )
            expected.update(
                artifact_id
                for source in manifest.registration_sources
                for artifact_id in source.input_artifact_ids
            )
            if set(content) != expected:
                msg = "Inspection content differs from its exact occurrence membership"
                raise StoreIntegrityError(msg)
            for row in index.iter_rows(named=True):
                artifact = content[row["artifact_id"]]
                if row != {
                    "artifact_id": artifact.artifact_id,
                    "artifact_type": artifact.artifact_type,
                    "origin_step_number": artifact.origin_step_number,
                    "metadata": artifact.to_row()["metadata"],
                }:
                    msg = "Inspection index changed protected artifact metadata"
                    raise StoreIntegrityError(msg)
            for source in manifest.sources:
                boundary = source.projection.boundary_addresses()
                inputs = {
                    item.address: content[item.artifact_id]
                    for item in source.projection.occurrences
                    if item.address in boundary
                }
                staged = {
                    item.artifact_id: content[item.artifact_id]
                    for item in source.projection.occurrences
                    if item.address not in boundary
                    and item.artifact_id
                    not in {artifact.artifact_id for artifact in inputs.values()}
                }
                validate_retained_membership(
                    source.projection, staged, inputs, artifact_fs=fs
                )
            for curator in manifest.curator_sources:
                artifacts = {}
                for member in curator.projection.occurrences:
                    occurrence = member.occurrence
                    artifact = content[occurrence.artifact_id]
                    if artifact.artifact_type != occurrence.artifact_type:
                        msg = "Curator inspection content has another artifact type"
                        raise StoreIntegrityError(msg)
                    artifacts[occurrence.address] = artifact
                _validate_references(curator.projection, artifacts)
            by_digest = {item.digest: item for item in manifest.carriers}
            for registration in manifest.registration_sources:
                for artifact_id in set(registration.input_artifact_ids):
                    artifact = content[artifact_id]
                    if not isinstance(artifact, FileRefArtifact):
                        msg = "Registered inspection content must be a file reference"
                        raise StoreIntegrityError(msg)
                    carrier = by_digest[bindings[artifact_id]]
                    if (artifact.content_hash, artifact.size_bytes) != (
                        carrier.digest,
                        carrier.size_bytes,
                    ):
                        msg = "Registered inspection content differs from its carrier"
                        raise StoreIntegrityError(msg)
            yield MappingProxyType(content)


def _validate_artifact_index(
    manifest: RetainedInspectionManifest, index: pl.DataFrame
) -> None:
    """Validate captured index membership without opening payloads or a new read."""
    expected = (
        {
            (item.artifact_id, item.artifact_type)
            for source in manifest.sources
            for item in source.projection.occurrences
        }
        | {
            (item.occurrence.artifact_id, item.occurrence.artifact_type)
            for source in manifest.curator_sources
            for item in source.projection.occurrences
        }
        | {
            (artifact_id, "file_ref")
            for source in manifest.registration_sources
            for artifact_id in source.input_artifact_ids
        }
    )
    if (
        index.height != len(expected)
        or index["artifact_id"].n_unique() != len(expected)
        or set(index.select("artifact_id", "artifact_type").iter_rows()) != expected
    ):
        msg = "Inspection index differs from exact source content membership"
        raise StoreIntegrityError(msg)


def export_retained_publication(
    reader: RetainedReader, destination: Path, *, redact: Sequence[str]
) -> PublicationExport:
    """Copy verified accepted results without copying claim authority or forging seals.

    Scientific content and identity are protected. Original seal locators are
    explicitly omitted; portable managed carrier locations do not change artifact
    identity. Runtime values were never stored: replay retains exact replacement
    obligations or an explicit unavailable configuration.
    """
    policy = PublicationPolicy.create(redact)
    with _owned_destination(reader, destination) as target:
        manifest = _write_inspection(reader, target, policy)
        return _finish_inspection(target, manifest)


@contextmanager
def _owned_destination(reader: RetainedReader, destination: Path) -> Iterator[Path]:
    """Remove only this export's incomplete output on every unsuccessful exit."""
    target = _destination(reader, destination)
    existed = target.exists()
    target.mkdir(parents=True, exist_ok=True)
    try:
        yield target
    except BaseException:
        shutil.rmtree(target)
        if existed:
            target.mkdir()
        raise


def _finish_inspection(
    target: Path, manifest: RetainedInspectionManifest
) -> PublicationExport:
    """Publish the exact manifest and verify its copied content before returning."""
    (target / _MANIFEST).write_bytes(
        canonical_json_bytes(manifest.model_dump(mode="json"))
    )
    publication = PublicationExport(target)
    with publication.artifacts():
        pass
    return publication


def _write_inspection(
    reader: RetainedReader, target: Path, policy: PublicationPolicy
) -> RetainedInspectionManifest:
    sources: list[InspectedResult] = []
    content: dict[str, Artifact] = {}
    bindings: dict[str, ManagedArtifactBinding] = {}
    carriers: dict[str, ManagedCarrier] = {}
    plans = sorted(
        reader.completed_plans(ordinary=False),
        key=lambda plan: plan.receipt.delivery.delivery_id,
    )
    for plan in plans:
        accepted = reader.read(plan.receipt)
        with reader.capture_inputs(plan.receipt) as inputs:
            configuration = read_replay_configuration(
                plan.receipt,
                admission=plan.admission,
                delta_root=reader.committer.delta_root,
                fs=reader.committer.fs,
            )
            redacted: tuple[Literal["unavailable_reason"], ...] = ()
            if configuration.unavailable_reason and policy.matches(
                configuration.unavailable_reason
            ):
                configuration = configuration.model_copy(
                    update={
                        "unavailable_reason": policy.text(
                            configuration.unavailable_reason
                        )
                    }
                )
                redacted = ("unavailable_reason",)
            source = InspectedResult(
                delivery=plan.receipt.delivery,
                seal_digest=plan.receipt.seal_digest,
                admission_digest=plan.admission.record_digest,
                recipe=inputs.record.recipe,
                projection=accepted.projection,
                replay_configuration=configuration,
                replay_diagnostic=read_replay_diagnostic(
                    plan.receipt,
                    delta_root=reader.committer.delta_root,
                    fs=reader.committer.fs,
                ),
                redacted_configuration_fields=redacted,
            )
        policy.protected(
            source.model_dump(mode="json"),
            "retained source identity, recipe and relationships",
        )
        sources.append(source)
        by_uri = {
            uri_join(reader.committer.files_root, item.relative_path): item
            for item in plan.carriers
        }
        _copy_content(
            accepted.artifacts.values(),
            by_uri,
            reader,
            target,
            policy,
            content,
            bindings,
            carriers,
        )
    registration_sources = _write_registrations(
        reader, target, policy, content, bindings, carriers
    )
    curator_sources, curator_edges = _write_curators(
        reader, target, policy, content, bindings, carriers
    )
    with TemporaryDirectory(prefix="artisan-inspection-stage-") as scratch:
        artifact_index = stage_table_payload(
            TablePath.ARTIFACT_INDEX.value,
            "artifact-index.parquet",
            (
                {
                    "artifact_id": key,
                    "artifact_type": artifact.artifact_type,
                    "origin_step_number": artifact.origin_step_number,
                    "metadata": artifact.to_row()["metadata"],
                }
                for key, artifact in sorted(content.items())
            ),
            ARTIFACT_INDEX_SCHEMA,
            str(target),
            Path(scratch),
            LocalFileSystem(),
        )
        tables = []
        for kind in sorted(
            {artifact.artifact_type for artifact in content.values()},
            key=lambda kind: ArtifactTypeDef.get(kind).table_path,
        ):
            definition = ArtifactTypeDef.get(kind)
            tables.append(
                stage_table_payload(
                    definition.table_path,
                    f"content-{compute_content_digest(kind.encode())}.parquet",
                    (
                        artifact.to_row()
                        for _, artifact in sorted(content.items())
                        if artifact.artifact_type == kind
                    ),
                    definition.polars_schema(),
                    str(target),
                    Path(scratch),
                    LocalFileSystem(),
                )
            )
    for plan in plans:
        reader.read(plan.receipt)
    payload = {
        "domain": "artisan.retained-inspection",
        "schema_version": 1,
        "purpose": "sanitized_inspection",
        "sources": [item.model_dump(mode="json") for item in sources],
        "curator_sources": [item.model_dump(mode="json") for item in curator_sources],
        "registration_sources": [
            item.model_dump(mode="json") for item in registration_sources
        ],
        "runs": None,
        "artifact_index": artifact_index.model_dump(mode="json"),
        "curator_artifact_edges": curator_edges.model_dump(mode="json"),
        "tables": [item.model_dump(mode="json") for item in tables],
        "carriers": [carriers[key].model_dump(mode="json") for key in sorted(carriers)],
        "external_bindings": [
            bindings[key].model_dump(mode="json") for key in sorted(bindings)
        ],
    }
    return RetainedInspectionManifest.model_validate(
        {**payload, "inspection_digest": _inspection_digest(payload)}
    )


def _write_registrations(
    reader: RetainedReader,
    target: Path,
    policy: PublicationPolicy,
    content: dict[str, Artifact],
    bindings: dict[str, ManagedArtifactBinding],
    carriers: dict[str, ManagedCarrier],
) -> tuple[InspectedRegistration, ...]:
    sources = []
    with reader.committer._read_snapshot() as snapshot:
        plans = [
            snapshot.completed_control_plan(owner)
            for owner in snapshot.controls.filter(
                (pl.col("state") == "complete")
                & (pl.col("commit_kind") == "input_registration")
            )["logical_commit_id"]
        ]
        selected = sorted(
            (plan for plan in plans if isinstance(plan, InputRegistrationPlan)),
            key=lambda plan: plan.step_run_id,
        )
        for plan in selected:
            artifacts = read_registered_inputs(snapshot, plan)
            source = InspectedRegistration(
                step_run_id=plan.step_run_id,
                step_number=plan.step_number,
                operation_name=plan.operation_name,
                plan_digest=plan.plan_digest,
                input_artifact_ids=tuple(item.artifact_id for item in plan.inputs),
            )
            policy.protected(source.model_dump(mode="json"), "registration assertions")
            sources.append(source)
            _copy_content(
                artifacts.values(),
                {
                    uri_join(reader.committer.files_root, item.relative_path): item
                    for item in plan.carriers
                },
                reader,
                target,
                policy,
                content,
                bindings,
                carriers,
            )
    with reader.committer._read_snapshot() as fresh:
        for plan in selected:
            read_registered_inputs(fresh, plan)
    return tuple(sources)


def _write_curators(
    reader: RetainedReader,
    target: Path,
    policy: PublicationPolicy,
    content: dict[str, Artifact],
    bindings: dict[str, ManagedArtifactBinding],
    carriers: dict[str, ManagedCarrier],
) -> tuple[tuple[InspectedCurator, ...], RetainedTableFile]:
    sources = []
    curator_reader = CuratorReader(reader.committer)
    with reader.committer._read_snapshot() as snapshot:
        plans = [
            snapshot.completed_control_plan(owner)
            for owner in snapshot.controls.filter(pl.col("state") == "complete")[
                "logical_commit_id"
            ]
        ]
        by_uri = {
            uri_join(reader.committer.files_root, item.relative_path): item
            for plan in plans
            if isinstance(
                plan, (RetainedCommitPlan, CuratorCommitPlan, InputRegistrationPlan)
            )
            for item in plan.carriers
        }
        selected = sorted(
            (plan for plan in plans if isinstance(plan, CuratorCommitPlan)),
            key=lambda plan: plan.step_run_id,
        )
        for plan in selected:
            terminal = read_terminal_metadata(snapshot, plan)
            if plan.table(TablePath.EXECUTIONS.value) is None:
                if terminal.status not in {StepStatus.FAILED, StepStatus.CANCELLED}:
                    msg = "Curator completion lacks its required execution"
                    raise StoreIntegrityError(msg)
                _validate_execution_membership(snapshot, plan, ())
                continue
            accepted = curator_reader._read_result(
                plan.step_run_id, plan.plan_digest, snapshot
            )
            _, execution = read_curator_source_metadata(snapshot, plan, terminal)
            admission = read_curator_admission(
                plan.admission,
                delta_root=reader.committer.delta_root,
                fs=reader.committer.fs,
            )
            source = InspectedCurator(
                step_run_id=plan.step_run_id,
                execution_run_id=execution["execution_run_id"],
                plan_digest=plan.plan_digest,
                admission_digest=plan.admission.admission_digest,
                operation_name=admission.operation_name,
                operation_version=admission.operation_version,
                definition=admission.definition,
                software_digest=admission.software_digest,
                inputs=admission.inputs,
                associations=admission.associations,
                view=admission.view,
                projection=accepted.projection,
                success=execution["success"],
                replay_of_execution_run_id=admission.record.replay_of_execution_run_id,
            )
            policy.protected(
                source.model_dump(mode="json"), "curator source assertions"
            )
            sources.append(source)
            _validate_execution_membership(snapshot, plan, (source,))
            _copy_content(
                accepted.artifacts.values(),
                by_uri,
                reader,
                target,
                policy,
                content,
                bindings,
                carriers,
            )
        with TemporaryDirectory(prefix="artisan-inspection-curator-edges-") as scratch:
            edges = stage_table_payload(
                TablePath.ARTIFACT_EDGES.value,
                "curator-artifact-edges.parquet",
                _curator_edge_rows(snapshot, selected, policy),
                ARTIFACT_EDGES_SCHEMA,
                str(target),
                Path(scratch),
                LocalFileSystem(),
            )
    for plan in selected:
        if plan.table(TablePath.EXECUTIONS.value) is None:
            with reader.committer._read_snapshot() as fresh:
                read_terminal_metadata(fresh, plan)
        else:
            curator_reader.read_result(plan.step_run_id, plan.plan_digest)
    return tuple(sources), edges


def _curator_edge_rows(
    snapshot: RetainedReadSnapshot,
    plans: Sequence[CuratorCommitPlan],
    policy: PublicationPolicy,
) -> Iterator[dict[str, Any]]:
    for plan in plans:
        for row in _curator_effect_rows(snapshot, plan, TablePath.ARTIFACT_EDGES):
            policy.protected(row, "original curator provenance")
            yield row


def _curator_effect_rows(
    read: RetainedReadSnapshot,
    plan: CuratorCommitPlan,
    path: TablePath,
) -> Iterator[dict[str, Any]]:
    physical = read.verify_effect(plan, path.value)
    effect = plan.table(path.value)
    if effect is None:
        return
    selected = scan_plan_keys(plan, effect, physical, read.controls)
    for frame in physical.iter_frames(rows_by_file=selected.matched):
        yield from frame.drop("logical_commit_id").iter_rows(named=True)


def _validate_execution_membership(
    read: RetainedReadSnapshot,
    plan: CuratorCommitPlan,
    sources: Sequence[InspectedCurator],
) -> None:
    actual = pl.DataFrame(
        _curator_effect_rows(read, plan, TablePath.EXECUTION_EDGES),
        schema=EXECUTION_EDGES_SCHEMA,
    ).sort(list(EXECUTION_EDGES_SCHEMA))
    if not actual.equals(curator_execution_edges(sources, ordinary=False)):
        msg = "Inspection curator membership differs from original execution edges"
        raise StoreIntegrityError(msg)


def _copy_content(
    artifacts: Iterable[Artifact],
    by_uri: Mapping[str, ManagedCarrier],
    reader: RetainedReader,
    target: Path,
    policy: PublicationPolicy,
    content: dict[str, Artifact],
    bindings: dict[str, ManagedArtifactBinding],
    carriers: dict[str, ManagedCarrier],
) -> None:
    for artifact in artifacts:
        assert artifact.artifact_id is not None
        row = artifact.to_row()
        policy.protected(row, "artifact content and metadata")
        previous = content.setdefault(artifact.artifact_id, artifact)
        if previous.to_row() != row:
            msg = "Inspection sources disagree on protected artifact rows"
            raise StoreIntegrityError(msg)
        if artifact.EXTERNALLY_BACKED:
            uri = getattr(artifact, next(iter(artifact.LOCATOR_FIELDS)))
            carrier = by_uri[uri]
            binding = ManagedArtifactBinding(
                artifact_id=artifact.artifact_id, carrier_digest=carrier.digest
            )
            if bindings.setdefault(artifact.artifact_id, binding) != binding:
                msg = "Inspection sources disagree on an artifact carrier"
                raise StoreIntegrityError(msg)
            if carrier.digest not in carriers:
                _copy_carrier(reader, carrier, target, policy)
                carriers[carrier.digest] = carrier


def _copy_carrier(
    reader: RetainedReader,
    carrier: ManagedCarrier,
    target: Path,
    policy: PublicationPolicy,
) -> None:
    destination = target / carrier.relative_path
    destination.parent.mkdir(parents=True, exist_ok=True)
    with (
        reader.committer.fs.open(
            uri_join(reader.committer.files_root, carrier.relative_path),
            "rb",
            cache_type="none",
        ) as source,
        destination.open("xb") as output,
    ):
        if compute_stream_digest(source, destination=output) != (
            carrier.digest,
            carrier.size_bytes,
        ):
            msg = "Source carrier changed during inspection export"
            raise StoreIntegrityError(msg)
    overlap = max((len(value.encode()) for value in policy.redact), default=1) - 1
    tail = b""
    with destination.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            policy.protected(tail + chunk, "external artifact bytes")
            tail = (tail + chunk)[-overlap:] if overlap else b""


def _destination(reader: RetainedReader, destination: Path) -> Path:
    target = destination.absolute()
    if any(path.is_symlink() for path in (target, *target.parents)):
        msg = "Inspection destination cannot traverse symbolic links"
        raise ValueError(msg)
    target = target.resolve()
    for source in (reader.committer.delta_root, reader.committer.files_root):
        if "://" not in source or source.startswith(("file://", "local://")):
            original = Path(LocalFileSystem._strip_protocol(source)).resolve()
            if original.is_relative_to(target) or target.is_relative_to(original):
                msg = "Inspection destination overlaps its source"
                raise ValueError(msg)
    if target.exists() and (not target.is_dir() or any(target.iterdir())):
        msg = "Inspection destination must be new or empty"
        raise ValueError(msg)
    return target


def _inspection_digest(payload: dict[str, object]) -> str:
    return compute_content_digest(
        b"artisan.retained-inspection-v1\0" + canonical_json_bytes(payload)
    )
