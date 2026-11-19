"""Read-only table visibility shared by stores and inspection bundles."""

from __future__ import annotations

from contextlib import AbstractContextManager
from typing import Protocol

import polars as pl
from fsspec import AbstractFileSystem

from artisan.schemas.enums import TablePath


class QueryView(Protocol):
    """One fixed query's visible tables, without storage mutation authority."""

    @property
    def files_root(self) -> str | None: ...

    def validate_binding(
        self, root: str, fs: AbstractFileSystem, storage_options: dict[str, str]
    ) -> None: ...

    def scan(self, table: str | TablePath) -> pl.LazyFrame: ...

    def artifact_edges(self, *, direct: bool) -> pl.LazyFrame: ...

    def step_names(self, pipeline_run_id: str | None) -> dict[int, str]: ...

    def accepted_artifact_ids_by_step(
        self, artifact_type: str
    ) -> dict[int, set[str]]: ...


class QueryScope(Protocol):
    """Own fresh visibility and temporary captures for each outer query."""

    @property
    def files_root(self) -> str | None: ...

    @property
    def view(self) -> QueryView | None: ...

    def validate_binding(
        self, root: str, fs: AbstractFileSystem, storage_options: dict[str, str]
    ) -> None: ...

    def activate(self) -> AbstractContextManager[None]: ...
