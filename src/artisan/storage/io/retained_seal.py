"""Seal and capture one complete retained delivery without accepting it."""

from __future__ import annotations

import posixpath
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from types import MappingProxyType

import polars as pl
from fsspec import AbstractFileSystem

from artisan.errors import StoreIntegrityError
from artisan.execution.lineage.validation import validate_artifacts_match_specs
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.schemas.composites.plan import ExecutionRecipe
from artisan.schemas.execution.assignment import SealedDelivery, SuccessfulDelivery
from artisan.schemas.execution.retained_result import (
    RetainedResultSeal,
)
from artisan.schemas.provenance.occurrence import OccurrenceAddress
from artisan.schemas.provenance.projection import BoundaryProjection
from artisan.schemas.specs.output_spec import OutputSpec
from artisan.storage.core.projection_schemas import PROJECTION_TABLES
from artisan.storage.io.bounded_io import (
    CapturedParquet,
    iter_parquet_frames,
)
from artisan.storage.io.external_carriers import (
    capture_external_carriers,
    stage_external_carriers,
)
from artisan.storage.io.projection_encoding import decode_projection, encode_projection
from artisan.storage.io.publication import (
    is_publication_temporary,
    publish_immutable_bytes,
)
from artisan.storage.io.retained_artifacts import (
    hydrate_artifact_content,
    retained_content,
    validate_retained_membership,
)
from artisan.storage.io.table_payload import capture_table_payload, stage_table_payload
from artisan.utils.hashing import (
    canonical_json_bytes,
    compute_content_digest,
)
from artisan.utils.path import uri_join

RETAINED_SEAL_FILENAME = "retained-result.json"


@dataclass(frozen=True)
class CapturedRetainedDelivery:
    """Verified private files and artifacts, valid only within the capture scope."""

    seal: RetainedResultSeal
    receipt: SealedDelivery
    projection: BoundaryProjection
    artifacts: Mapping[OccurrenceAddress, Artifact]
    tables: Mapping[str, CapturedParquet]
    carriers: Mapping[str, str]


def stage_retained_result(
    staging_root: str,
    completion: SuccessfulDelivery,
    projection: BoundaryProjection,
    artifacts: Mapping[OccurrenceAddress, Artifact],
    *,
    fs: AbstractFileSystem,
    artifact_fs: AbstractFileSystem | None = None,
) -> SealedDelivery:
    """Publish complete delivery-owned files first and the immutable seal last.

    Commands must already be closed while the session's artifact files remain
    available. This does not publish global artifact locations or claim a winner.
    """
    completion = SuccessfulDelivery.model_validate(completion)
    projection = BoundaryProjection.model_validate(projection)
    delivery = completion.delivery
    directory = uri_join(staging_root, delivery.delivery_id)
    seal_uri = uri_join(directory, RETAINED_SEAL_FILENAME)
    if fs.exists(seal_uri):
        msg = "Retained delivery is already sealed"
        raise StoreIntegrityError(msg)
    frames = encode_projection(projection, delivery=delivery)
    content = retained_content(projection, artifacts)
    with TemporaryDirectory(prefix="artisan-retained-stage-") as scratch:
        carriers, bindings = stage_external_carriers(
            sorted(content.items()), directory, Path(scratch), fs, artifact_fs
        )
        tables = [
            stage_table_payload(
                table.table_path,
                table.filename,
                frames[table.name].iter_rows(named=True),
                dict(table.schema),
                directory,
                Path(scratch),
                fs,
            )
            for table in PROJECTION_TABLES
        ]
        for kind in sorted({artifact.artifact_type for artifact in content.values()}):
            definition = ArtifactTypeDef.get(kind)
            filename = definition.parquet_filename()
            if filename in {item.filename for item in tables}:
                msg = (
                    "Artifact staging filename conflicts with a retained metadata table"
                )
                raise StoreIntegrityError(msg)
            tables.append(
                stage_table_payload(
                    definition.table_path,
                    filename,
                    (
                        artifact.to_row()
                        for _, artifact in sorted(content.items())
                        if artifact.artifact_type == kind
                    ),
                    definition.polars_schema(),
                    directory,
                    Path(scratch),
                    fs,
                )
            )
        seal = RetainedResultSeal(
            completion=completion,
            projection_digest=_projection_digest(projection),
            tables=tuple(sorted(tables, key=lambda item: item.filename)),
            carriers=carriers,
            external_bindings=bindings,
        )
        encoded = canonical_json_bytes(seal.model_dump(mode="json"))
        publish_immutable_bytes(fs, seal_uri, encoded)
    return SealedDelivery(
        delivery=delivery,
        seal_uri=seal_uri,
        seal_digest=compute_content_digest(encoded),
    )


@contextmanager
def capture_retained_delivery(
    receipt: SealedDelivery,
    boundary_artifacts: Mapping[OccurrenceAddress, Artifact],
    *,
    expected_recipe: ExecutionRecipe,
    fs: AbstractFileSystem,
    artifact_fs: AbstractFileSystem | None = None,
) -> Iterator[CapturedRetainedDelivery]:
    """Capture and verify a sealed delivery without publication or acceptance."""
    receipt = SealedDelivery.model_validate(receipt)
    if expected_recipe.digest != receipt.delivery.attempt.recipe_digest:
        msg = "Retained delivery does not match the admitted recipe"
        raise StoreIntegrityError(msg)
    directory = posixpath.dirname(receipt.seal_uri)
    if (
        posixpath.basename(receipt.seal_uri) != RETAINED_SEAL_FILENAME
        or posixpath.basename(directory) != receipt.delivery.delivery_id
    ):
        msg = "Retained seal locator does not match its delivery identity"
        raise StoreIntegrityError(msg)
    with fs.open(receipt.seal_uri, "rb", cache_type="none") as stream:
        encoded = stream.read()
    if compute_content_digest(encoded) != receipt.seal_digest:
        msg = "Retained seal digest changed"
        raise StoreIntegrityError(msg)
    seal = RetainedResultSeal.model_validate_json(encoded)
    if canonical_json_bytes(seal.model_dump(mode="json")) != encoded:
        msg = "Retained seal encoding is not canonical"
        raise StoreIntegrityError(msg)
    if seal.completion.delivery != receipt.delivery:
        msg = "Retained seal has different delivery ownership"
        raise StoreIntegrityError(msg)
    _validate_inventory(directory, seal, fs)
    with TemporaryDirectory(prefix="artisan-retained-capture-") as scratch:
        tables = _capture_tables(directory, seal, Path(scratch), fs)
        carriers = capture_external_carriers(
            directory, seal.carriers, Path(scratch), fs
        )
        metadata = {
            table.name: _collect(tables[table.table_path])
            for table in PROJECTION_TABLES
        }
        projection = decode_projection(metadata, delivery=receipt.delivery)
        if _projection_digest(projection) != seal.projection_digest:
            msg = "Retained projection digest changed"
            raise StoreIntegrityError(msg)
        content = hydrate_artifact_content(
            tables,
            carriers,
            {item.artifact_id: item.filename for item in seal.external_bindings},
        )
        artifacts = validate_retained_membership(
            projection, content, boundary_artifacts, artifact_fs=artifact_fs or fs
        )
        _validate_outputs(expected_recipe, projection, artifacts)
        yield CapturedRetainedDelivery(
            seal,
            receipt,
            projection,
            MappingProxyType(artifacts),
            MappingProxyType(tables),
            MappingProxyType(carriers),
        )


def _validate_inventory(
    directory: str, seal: RetainedResultSeal, fs: AbstractFileSystem
) -> None:
    expected = (
        {item.filename for item in seal.tables}
        | {item.filename for item in seal.carriers}
        | {RETAINED_SEAL_FILENAME}
    )
    actual = {
        posixpath.basename(str(path).rstrip("/"))
        for path in fs.ls(directory, detail=False)
    }
    if expected - actual or any(
        not is_publication_temporary(name) for name in actual - expected
    ):
        msg = "Retained seal has missing or unexpected staging objects"
        raise StoreIntegrityError(msg)
    required = {table.table_path: table.filename for table in PROJECTION_TABLES}
    by_path = {item.table_path: item.filename for item in seal.tables}
    if any(by_path.get(path) != filename for path, filename in required.items()):
        msg = "Retained seal omits registered projection tables"
        raise StoreIntegrityError(msg)


def _capture_tables(
    directory: str, seal: RetainedResultSeal, scratch: Path, fs: AbstractFileSystem
) -> dict[str, CapturedParquet]:
    registered = {
        table.table_path: (table.filename, dict(table.schema))
        for table in PROJECTION_TABLES
    }
    registered.update(
        {
            definition.table_path: (
                definition.parquet_filename(),
                definition.polars_schema(),
            )
            for definition in ArtifactTypeDef.get_all().values()
        }
    )
    tables = {}
    projection_paths = {table.table_path for table in PROJECTION_TABLES}
    for item in seal.tables:
        expected = registered.get(item.table_path)
        if expected is None or item.filename != expected[0]:
            msg = "Retained seal names an unregistered table or filename"
            raise StoreIntegrityError(msg)
        if item.table_path not in projection_paths and item.row_count == 0:
            msg = "Retained seal contains an unnecessary empty artifact table"
            raise StoreIntegrityError(msg)
        captured = capture_table_payload(
            item,
            directory=directory,
            scratch=scratch,
            expected_schema=expected[1],
            fs=fs,
        )
        tables[item.table_path] = captured
    return tables


def _collect(captured: CapturedParquet) -> pl.DataFrame:
    frames = list(iter_parquet_frames(captured.path))
    return pl.concat(frames) if frames else pl.DataFrame(schema=captured.schema)


def _projection_digest(projection: BoundaryProjection) -> str:
    return compute_content_digest(
        canonical_json_bytes(projection.model_dump(mode="json"))
    )


def _validate_outputs(
    recipe: ExecutionRecipe,
    projection: BoundaryProjection,
    artifacts: Mapping[OccurrenceAddress, Artifact],
) -> None:
    specs = {
        item.role: OutputSpec(artifact_type=item.artifact_type, required=item.required)
        for item in recipe.outputs
    }
    outputs = {
        group.role: [artifacts[address] for address in group.occurrences]
        for group in projection.exports
    }
    validate_artifacts_match_specs(outputs, specs)
