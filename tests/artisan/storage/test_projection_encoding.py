"""Registered retained evidence survives physical storage without order loss."""

from __future__ import annotations

import io

import polars as pl
import pytest
from fixtures.retained_readers import Backend

from artisan.errors import StoreIntegrityError
from artisan.schemas.execution.assignment import (
    AssignmentAttempt,
    AssignmentIdentity,
    DeliveryIdentity,
)
from artisan.schemas.provenance.occurrence import OccurrenceAddress
from artisan.schemas.provenance.projection import (
    AcceptedCuratorOccurrence,
    AcceptedOccurrence,
    BoundaryInputAttribution,
    BoundaryProjection,
    FrozenAssociation,
    FrozenBoundaryContext,
    FrozenReferences,
    OccurrenceRelation,
    ProjectedOccurrence,
    RoleOccurrences,
)
from artisan.storage.core.projection_schemas import PROJECTION_TABLES
from artisan.storage.core.table_schemas import (
    get_natural_key,
    get_physical_schema_for_path,
)
from artisan.storage.io.delta_files import DeltaSnapshot, append_table_files
from artisan.storage.io.projection_encoding import decode_projection, encode_projection
from artisan.storage.io.publication import publish_immutable_bytes


def _projection(*, empty: bool = False) -> BoundaryProjection:
    if empty:
        return BoundaryProjection(
            boundary_inputs=(),
            occurrences=(),
            input_attribution=(),
            exports=(RoleOccurrences(role="optional", occurrences=()),),
        )
    source, config, context, first, second, support, exported = (
        OccurrenceAddress(invocation_id=owner, role="data", index=0)
        for owner in ("source", "config", "context", "A", "B", "support", "exported")
    )
    pairs = ((source, first), (source, second), (source, support), (support, exported))
    return BoundaryProjection(
        boundary_inputs=(
            RoleOccurrences(role="data", occurrences=(source,)),
            RoleOccurrences(role="config", occurrences=(config,)),
            RoleOccurrences(role="optional", occurrences=()),
        ),
        occurrences=tuple(
            ProjectedOccurrence(
                address=address, artifact_id=identity * 32, artifact_type=kind
            )
            for address, identity, kind in (
                (source, "a", "csv"),
                (config, "b", "config"),
                (context, "c", "metric"),
                (first, "d", "csv"),
                (second, "d", "csv"),
                (support, "e", "csv"),
                (exported, "f", "config"),
            )
        ),
        input_attribution=(
            BoundaryInputAttribution(boundary=context, producer=None),
            BoundaryInputAttribution(
                boundary=source,
                producer=AcceptedOccurrence(
                    delivery_id="1" * 32, seal_digest="2" * 32, address=first
                ),
            ),
            BoundaryInputAttribution(boundary=config, producer=None),
        ),
        exports=(
            RoleOccurrences(role="data", occurrences=(second, first, second)),
            RoleOccurrences(role="config", occurrences=(exported,)),
            RoleOccurrences(role="optional", occurrences=()),
        ),
        support=(support,),
        parents=tuple(
            OccurrenceRelation(source=left, target=right) for left, right in pairs
        ),
        associations=tuple(
            OccurrenceRelation(source=left, target=right) for left, right in pairs
        ),
        references=(OccurrenceRelation(source=exported, target=support),),
        frozen_context=FrozenBoundaryContext(
            associations=(
                FrozenAssociation(
                    primary=source, artifact_type="metric", members=(context, context)
                ),
                FrozenAssociation(primary=config, artifact_type="metric", members=()),
            ),
            references=(
                FrozenReferences(
                    source=config, members=(source,), direct_parents=(source, context)
                ),
            ),
        ),
    )


def _delivery(projection: BoundaryProjection) -> DeliveryIdentity:
    return DeliveryIdentity.new(
        AssignmentAttempt(
            assignment=AssignmentIdentity(step_run_id="a" * 32, unit_index=0),
            attempt=0,
            generation=0,
            recipe_digest="b" * 32,
            software_digest="c" * 64,
            input_context_digest=projection.input_context_digest,
        )
    )


@pytest.mark.parametrize("owner", ["retained", "curator", "empty"])
def test_projection_roundtrips_registered_parquet_and_delta(
    backend_fs: Backend, owner: str
) -> None:
    fs, storage, root = backend_fs
    projection = _projection(empty=owner == "empty")
    if owner == "curator":
        projection = projection.model_copy(
            update={
                "input_attribution": tuple(
                    item.model_copy(
                        update={
                            "producer": AcceptedCuratorOccurrence(
                                step_run_id="1" * 32,
                                plan_digest="2" * 32,
                                address=item.producer.address,
                            )
                        }
                    )
                    if item.producer is not None
                    else item
                    for item in projection.input_attribution
                )
            }
        )
    delivery = _delivery(projection)
    encoded = encode_projection(projection, delivery=delivery)
    parquet, delta = {}, {}
    for table in PROJECTION_TABLES:
        frame = encoded[table.name].reverse()
        content = io.BytesIO()
        frame.write_parquet(content)
        path = f"{root}/staging/{table.filename}"
        publish_immutable_bytes(fs, path, content.getvalue())
        with fs.open(path, "rb") as stream:
            parquet[table.name] = pl.read_parquet(stream)
        schema = get_physical_schema_for_path(table.table_path)
        assert get_natural_key(table.table_path) == table.natural_key
        path = f"{root}/tables/{table.table_path}"
        pl.DataFrame(schema=schema).write_delta(
            path, storage_options=storage.delta_storage_options()
        )
        snapshot = DeltaSnapshot(
            path, schema, fs=fs, storage_options=storage.delta_storage_options()
        )
        physical = frame.with_columns(pl.lit("commit").alias("logical_commit_id"))
        append_table_files(
            snapshot,
            [physical],
            step_number=0,
            statistics_columns=list(table.natural_key),
        )
        fresh = DeltaSnapshot(
            path, schema, fs=fs, storage_options=storage.delta_storage_options()
        )
        frames = [item for _, _, item in fresh.iter_located()]
        delta[table.name] = (
            pl.concat(frames) if frames else pl.DataFrame(schema=schema)
        ).drop("logical_commit_id")
    assert decode_projection(parquet, delivery=delivery) == projection
    assert decode_projection(delta, delivery=delivery) == projection


@pytest.mark.parametrize(
    "change",
    [
        "unknown-kind",
        "missing-table",
        "foreign-owner",
        "gap",
        "duplicate",
        "extra-role",
        "lost-empty-capture",
        "partial-producer",
        "unknown-producer-kind",
        "unknown-column",
        "null-list",
        "null-position",
        "missing-reference-parents",
        "missing-references",
        "wrong-reference-parent-owner",
    ],
)
def test_projection_rejects_changed_or_discarded_evidence(change: str) -> None:
    projection = _projection()
    delivery = _delivery(projection)
    tables = encode_projection(projection, delivery=delivery)
    if change == "unknown-kind":
        tables["relations"] = tables["relations"].with_columns(
            pl.lit("unknown").alias("kind")
        )
    elif change == "missing-table":
        del tables["context"]
    elif change == "foreign-owner":
        tables["memberships"] = tables["memberships"].with_columns(
            pl.lit("f" * 32).alias("delivery_id")
        )
    elif change == "gap":
        tables["memberships"] = tables["memberships"].with_columns(
            (pl.col("position") + 1).alias("position")
        )
    elif change == "duplicate":
        tables["roles"] = pl.concat([tables["roles"], tables["roles"].head(1)])
    elif change == "extra-role":
        tables["memberships"] = tables["memberships"].with_columns(
            pl.lit("unknown").alias("role")
        )
    elif change == "lost-empty-capture":
        tables["context"] = tables["context"].filter(pl.col("members").list.len() > 0)
    elif change in ("missing-reference-parents", "missing-references"):
        kind = "reference_parents" if change.endswith("parents") else "references"
        tables["context"] = tables["context"].filter(pl.col("kind") != kind)
    elif change == "wrong-reference-parent-owner":
        tables["context"] = tables["context"].with_columns(
            pl.when(pl.col("kind") == "reference_parents")
            .then(pl.lit(projection.occurrences[0].address.key))
            .otherwise(pl.col("primary_key"))
            .alias("primary_key")
        )
    elif change == "partial-producer":
        tables["occurrences"] = tables["occurrences"].with_columns(
            pl.lit(None, dtype=pl.String).alias("producer_owner_digest")
        )
    elif change == "unknown-producer-kind":
        tables["occurrences"] = tables["occurrences"].with_columns(
            pl.when(pl.col("producer_kind").is_not_null())
            .then(pl.lit("unknown"))
            .otherwise(pl.col("producer_kind"))
            .alias("producer_kind")
        )
    elif change == "null-list":
        tables["context"] = tables["context"].with_columns(
            pl.lit(None, dtype=pl.List(pl.String)).alias("members")
        )
    elif change == "null-position":
        tables["roles"] = tables["roles"].with_columns(
            pl.lit(None, dtype=pl.Int64).alias("position")
        )
    else:
        tables["context"] = tables["context"].with_columns(
            pl.lit(True).alias("trusted")
        )
    with pytest.raises((ValueError, StoreIntegrityError)):
        decode_projection(tables, delivery=delivery)
