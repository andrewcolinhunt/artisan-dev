"""Physical contracts for retained occurrence evidence."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any

import polars as pl


@dataclass(frozen=True)
class ProjectionTable:
    """One delivery-owned metadata table and its exact retry key."""

    name: str
    schema: Mapping[str, Any]
    natural_key: tuple[str, ...]

    @property
    def table_path(self) -> str:
        """Return the private registered table path."""
        return f"provenance/retained_{self.name}"

    @property
    def filename(self) -> str:
        """Return its immutable staging filename."""
        return f"retained_{self.name}.parquet"


def _table(
    name: str, columns: dict[str, Any], natural_key: tuple[str, ...]
) -> ProjectionTable:
    return ProjectionTable(
        name,
        MappingProxyType({"delivery_id": pl.String, **columns}),
        ("delivery_id", *natural_key),
    )


PROJECTION_TABLES: tuple[ProjectionTable, ...] = (
    _table(
        "occurrences",
        {
            "position": pl.Int64,
            "occurrence_key": pl.String,
            "artifact_id": pl.String,
            "artifact_type": pl.String,
            "input_position": pl.Int64,
            "producer_kind": pl.String,
            "producer_owner_id": pl.String,
            "producer_owner_digest": pl.String,
            "producer_occurrence_key": pl.String,
        },
        ("occurrence_key",),
    ),
    _table(
        "roles",
        {"kind": pl.String, "position": pl.Int64, "role": pl.String},
        ("kind", "position"),
    ),
    _table(
        "memberships",
        {
            "kind": pl.String,
            "role": pl.String,
            "position": pl.Int64,
            "occurrence_key": pl.String,
        },
        ("kind", "role", "position"),
    ),
    _table(
        "relations",
        {
            "kind": pl.String,
            "position": pl.Int64,
            "source_key": pl.String,
            "target_key": pl.String,
        },
        ("kind", "position"),
    ),
    _table(
        "context",
        {
            "kind": pl.String,
            "position": pl.Int64,
            "primary_key": pl.String,
            "artifact_type": pl.String,
            "members": pl.List(pl.String),
        },
        ("kind", "position"),
    ),
)

PROJECTION_TABLES_BY_PATH: Mapping[str, ProjectionTable] = MappingProxyType(
    {table.table_path: table for table in PROJECTION_TABLES}
)
