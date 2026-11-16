"""Structural contracts shared by exact logical-commit effects."""

from __future__ import annotations

from typing import Any, Protocol


class TableEffect(Protocol):
    """Immutable keys and canonical values of one planned table effect."""

    @property
    def table_path(self) -> str: ...

    @property
    def natural_key(self) -> tuple[str, ...]: ...

    @property
    def row_keys(self) -> tuple[tuple[Any, ...], ...]: ...

    @property
    def row_count(self) -> int: ...

    @property
    def table_plan_key(self) -> str: ...


class LogicalEffect(Protocol):
    """Validated plan identity and exact owned-row evidence.

    Store domains validate their own serialized plans and authorization before
    passing them to the shared writer. Reused content can retain a completed
    prior owner's origin; rows owned by this plan must match ``expected_origin``.
    """

    @property
    def logical_commit_id(self) -> str: ...

    @property
    def commit_kind(self) -> str: ...

    @property
    def step_run_id(self) -> str: ...

    @property
    def recovery_batch_id(self) -> str | None: ...

    @property
    def step_number(self) -> int: ...

    @property
    def plan_digest(self) -> str: ...

    def table(self, table_path: str) -> TableEffect | None: ...

    def expected_origin(self, table_path: str, key: tuple[Any, ...]) -> int | None: ...
