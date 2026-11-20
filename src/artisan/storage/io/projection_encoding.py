"""Lossless physical encoding of the immutable retained-boundary contract."""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

import polars as pl

from artisan.errors import StoreIntegrityError
from artisan.schemas.execution.assignment import DeliveryIdentity
from artisan.schemas.provenance.occurrence import OccurrenceAddress
from artisan.schemas.provenance.projection import (
    AcceptedCuratorOccurrence,
    AcceptedOccurrence,
    AcceptedProducer,
    BoundaryInputAttribution,
    BoundaryProjection,
    FrozenAssociation,
    FrozenBoundaryContext,
    FrozenConstant,
    FrozenReferences,
    OccurrenceRelation,
    ProjectedOccurrence,
    RoleOccurrences,
)
from artisan.storage.core.projection_schemas import PROJECTION_TABLES


def encode_projection(
    projection: BoundaryProjection, *, delivery: DeliveryIdentity
) -> dict[str, pl.DataFrame]:
    """Encode explicit positions; never depend on Parquet or Delta row order."""
    projection = BoundaryProjection.model_validate(projection)
    delivery = DeliveryIdentity.model_validate(delivery)
    if projection.input_context_digest != delivery.attempt.input_context_digest:
        msg = "Retained result changed the admitted input context"
        raise StoreIntegrityError(msg)
    rows: dict[str, list[dict[str, Any]]] = {
        table.name: [] for table in PROJECTION_TABLES
    }
    attribution = {
        item.boundary: (index, item.producer)
        for index, item in enumerate(projection.input_attribution)
    }
    for position, item in enumerate(projection.occurrences):
        input_position, producer = attribution.get(item.address, (None, None))
        rows["occurrences"].append(
            {
                "position": position,
                "occurrence_key": item.address.key,
                "artifact_id": item.artifact_id,
                "artifact_type": item.artifact_type,
                "input_position": input_position,
                **_encode_producer(producer),
            }
        )
    _encode_membership(projection, rows)
    for kind in ("parents", "associations", "references"):
        rows["relations"].extend(
            {
                "kind": kind,
                "position": index,
                "source_key": item.source.key,
                "target_key": item.target.key,
            }
            for index, item in enumerate(getattr(projection, kind))
        )
    for position, association in enumerate(projection.frozen_context.associations):
        rows["context"].append(
            {
                "kind": "associations",
                "position": position,
                "primary_key": association.primary.key,
                "artifact_type": association.artifact_type,
                "members": [member.key for member in association.members],
            }
        )
    for position, capture in enumerate(projection.frozen_context.references):
        for kind, members in (
            ("references", capture.members),
            ("reference_parents", capture.direct_parents),
        ):
            rows["context"].append(
                {
                    "kind": kind,
                    "position": position,
                    "primary_key": capture.source.key,
                    "artifact_type": "",
                    "members": [member.key for member in members],
                }
            )
    for position, constant in enumerate(projection.frozen_context.constants):
        rows["context"].append(
            {
                "kind": "constants_v2",
                "position": position,
                "primary_key": json.dumps(
                    [list(constant.call_path), constant.role], separators=(",", ":")
                ),
                "artifact_type": "",
                "members": [address.key for address in constant.occurrences],
            }
        )
    return {
        table.name: pl.DataFrame(
            [{"delivery_id": delivery.delivery_id, **row} for row in rows[table.name]],
            schema=dict(table.schema),
        )
        for table in PROJECTION_TABLES
    }


def _encode_producer(producer: AcceptedProducer | None) -> dict[str, str | None]:
    if isinstance(producer, AcceptedOccurrence):
        kind, owner, digest = "retained", producer.delivery_id, producer.seal_digest
    elif isinstance(producer, AcceptedCuratorOccurrence):
        kind, owner, digest = "curator", producer.step_run_id, producer.plan_digest
    else:
        kind, owner, digest = None, None, None
    return {
        "producer_kind": kind,
        "producer_owner_id": owner,
        "producer_owner_digest": digest,
        "producer_occurrence_key": producer.address.key if producer else None,
    }


def _encode_membership(
    projection: BoundaryProjection, rows: dict[str, list[dict[str, Any]]]
) -> None:
    for kind, groups in (
        ("input", projection.boundary_inputs),
        ("export", projection.exports),
    ):
        for position, group in enumerate(groups):
            rows["roles"].append(
                {"kind": kind, "position": position, "role": group.role}
            )
            rows["memberships"].extend(
                {
                    "kind": kind,
                    "role": group.role,
                    "position": index,
                    "occurrence_key": address.key,
                }
                for index, address in enumerate(group.occurrences)
            )
    rows["memberships"].extend(
        {
            "kind": "support",
            "role": "",
            "position": index,
            "occurrence_key": address.key,
        }
        for index, address in enumerate(projection.support)
    )


def decode_projection(
    tables: Mapping[str, pl.DataFrame], *, delivery: DeliveryIdentity
) -> BoundaryProjection:
    """Validate and reconstruct all evidence, rejecting any discarded table row."""
    delivery = DeliveryIdentity.model_validate(delivery)
    _validate_tables(tables, delivery)
    occurrences = tables["occurrences"].sort("position")
    projection = BoundaryProjection(
        boundary_inputs=_decode_roles(tables, "input"),
        exports=_decode_roles(tables, "export"),
        occurrences=tuple(
            ProjectedOccurrence(
                address=_address(row["occurrence_key"]),
                artifact_id=row["artifact_id"],
                artifact_type=row["artifact_type"],
            )
            for row in occurrences.iter_rows(named=True)
        ),
        input_attribution=_decode_attribution(occurrences),
        support=tuple(
            _address(key)
            for key in tables["memberships"]
            .filter(pl.col("kind") == "support")
            .sort("position")["occurrence_key"]
        ),
        parents=_decode_relations(tables["relations"], "parents"),
        associations=_decode_relations(tables["relations"], "associations"),
        references=_decode_relations(tables["relations"], "references"),
        frozen_context=_decode_context(tables["context"]),
    )
    expected = encode_projection(projection, delivery=delivery)
    for table in PROJECTION_TABLES:
        keys = list(table.natural_key)
        if not expected[table.name].sort(keys).equals(tables[table.name].sort(keys)):
            msg = f"Noncanonical retained projection table {table.name!r}"
            raise StoreIntegrityError(msg)
    return projection


def _validate_tables(
    tables: Mapping[str, pl.DataFrame], delivery: DeliveryIdentity
) -> None:
    if set(tables) != {table.name for table in PROJECTION_TABLES}:
        msg = "Retained projection requires its exact registered table set"
        raise StoreIntegrityError(msg)
    for table in PROJECTION_TABLES:
        frame = tables[table.name]
        if frame.schema != dict(table.schema):
            msg = f"Retained projection table {table.name!r} has the wrong schema"
            raise StoreIntegrityError(msg)
        nullable = (
            {
                "input_position",
                "producer_kind",
                "producer_owner_id",
                "producer_owner_digest",
                "producer_occurrence_key",
            }
            if table.name == "occurrences"
            else set()
        )
        required = [name for name in table.schema if name not in nullable]
        if any(frame.select(required).null_count().row(0)):
            msg = f"Retained projection table {table.name!r} has null required values"
            raise StoreIntegrityError(msg)
        if frame.height and set(frame["delivery_id"]) != {delivery.delivery_id}:
            msg = "Retained projection contains a foreign delivery owner"
            raise StoreIntegrityError(msg)


def _decode_roles(
    tables: Mapping[str, pl.DataFrame], kind: str
) -> tuple[RoleOccurrences, ...]:
    members = tables["memberships"]
    return tuple(
        RoleOccurrences(
            role=role,
            occurrences=tuple(
                _address(key)
                for key in members.filter(
                    (pl.col("kind") == kind) & (pl.col("role") == role)
                ).sort("position")["occurrence_key"]
            ),
        )
        for role in tables["roles"]
        .filter(pl.col("kind") == kind)
        .sort("position")["role"]
    )


def _decode_attribution(
    occurrences: pl.DataFrame,
) -> tuple[BoundaryInputAttribution, ...]:
    return tuple(
        BoundaryInputAttribution(
            boundary=_address(row["occurrence_key"]),
            producer=_decode_producer(row),
        )
        for row in occurrences.filter(pl.col("input_position").is_not_null())
        .sort("input_position")
        .iter_rows(named=True)
    )


def _decode_producer(row: dict[str, Any]) -> AcceptedProducer | None:
    fields = (
        "producer_kind",
        "producer_owner_id",
        "producer_owner_digest",
        "producer_occurrence_key",
    )
    if all(row[name] is None for name in fields):
        return None
    if any(row[name] is None for name in fields):
        msg = "Accepted producer ownership must be complete"
        raise StoreIntegrityError(msg)
    address = _address(row["producer_occurrence_key"])
    if row["producer_kind"] == "retained":
        return AcceptedOccurrence(
            delivery_id=row["producer_owner_id"],
            seal_digest=row["producer_owner_digest"],
            address=address,
        )
    if row["producer_kind"] == "curator":
        return AcceptedCuratorOccurrence(
            step_run_id=row["producer_owner_id"],
            plan_digest=row["producer_owner_digest"],
            address=address,
        )
    msg = "Unknown accepted producer ownership domain"
    raise StoreIntegrityError(msg)


def _decode_relations(frame: pl.DataFrame, kind: str) -> tuple[OccurrenceRelation, ...]:
    return tuple(
        OccurrenceRelation(
            source=_address(row["source_key"]), target=_address(row["target_key"])
        )
        for row in frame.filter(pl.col("kind") == kind)
        .sort("position")
        .iter_rows(named=True)
    )


def _decode_context(frame: pl.DataFrame) -> FrozenBoundaryContext:
    references = frame.filter(pl.col("kind") == "references").sort("position")
    parents = frame.filter(pl.col("kind") == "reference_parents").sort("position")
    if not references.select("position", "primary_key").equals(
        parents.select("position", "primary_key")
    ):
        msg = "Captured references require matching original-parent evidence"
        raise StoreIntegrityError(msg)
    constants = tuple(
        _decode_constant(row)
        for row in frame.filter(pl.col("kind") == "constants_v2")
        .sort("position")
        .iter_rows(named=True)
    )
    return FrozenBoundaryContext(
        schema_version=2 if constants else 1,
        constants=constants,
        associations=tuple(
            FrozenAssociation(
                primary=_address(row["primary_key"]),
                artifact_type=row["artifact_type"],
                members=tuple(_address(key) for key in row["members"]),
            )
            for row in frame.filter(pl.col("kind") == "associations")
            .sort("position")
            .iter_rows(named=True)
        ),
        references=tuple(
            FrozenReferences(
                source=_address(row["primary_key"]),
                members=tuple(_address(key) for key in row["members"]),
                direct_parents=tuple(_address(key) for key in parent_keys),
            )
            for row, parent_keys in zip(
                references.iter_rows(named=True), parents["members"], strict=True
            )
        ),
    )


def _decode_constant(row: dict[str, Any]) -> FrozenConstant:
    key = json.loads(row["primary_key"])
    if not isinstance(key, list) or len(key) != 2 or not isinstance(key[0], list):
        msg = "Constant input key must contain its call path and role"
        raise StoreIntegrityError(msg)
    return FrozenConstant(
        call_path=tuple(key[0]),
        role=key[1],
        occurrences=tuple(_address(value) for value in row["members"]),
    )


def _address(value: object) -> OccurrenceAddress:
    if not isinstance(value, str):
        msg = "Occurrence join key must be a canonical address string"
        raise StoreIntegrityError(msg)
    components = json.loads(value)
    if not isinstance(components, list) or len(components) != 3:
        msg = "Occurrence join key must contain invocation, role and index"
        raise StoreIntegrityError(msg)
    address = OccurrenceAddress(
        invocation_id=components[0], role=components[1], index=components[2]
    )
    if address.key != value:
        msg = "Occurrence join key is not canonical"
        raise StoreIntegrityError(msg)
    return address
