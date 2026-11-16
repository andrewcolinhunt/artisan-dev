"""Verify selected registered content against one captured storage version."""

from __future__ import annotations

from typing import cast

import polars as pl

from artisan.errors import StoreIntegrityError
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.schemas.enums import TablePath
from artisan.storage.core.committed_scan import verify_snapshot_effect
from artisan.storage.core.curator_view import CuratorView
from artisan.storage.core.table_schemas import get_physical_schema_for_path
from artisan.storage.io.commit_contract import LogicalEffect
from artisan.storage.io.delta_files import DeltaSnapshot
from artisan.utils.path import uri_join


class RegisteredContent:
    """Validate only selected registered rows, retaining authority within this call."""

    def __init__(self, view: CuratorView) -> None:
        self.view = view
        self.artifacts: dict[str, Artifact] = {}
        self._plans: dict[str, LogicalEffect] = {}
        self._snapshots: dict[str, DeltaSnapshot] = {}
        self._verified: set[tuple[str, str]] = set()

    def load(self, artifact_ids: set[str]) -> None:
        missing = artifact_ids - self.artifacts.keys()
        if not missing:
            return
        index = self._rows(TablePath.ARTIFACT_INDEX.value, missing)
        for kind in sorted(set(index["artifact_type"])):
            definition = ArtifactTypeDef.get(kind)
            ids = set(index.filter(pl.col("artifact_type") == kind)["artifact_id"])
            rows = self._rows(definition.table_path, ids)
            model = cast(type[Artifact], definition.model)
            for row in rows.iter_rows(named=True):
                artifact = model.from_row(row)
                plan = self._plans[row["logical_commit_id"]]
                if artifact.origin_step_number != plan.expected_origin(
                    definition.table_path, (artifact.artifact_id,)
                ):
                    msg = "Registered input changed its protected original origin"
                    raise StoreIntegrityError(msg)
                self.artifacts[row["artifact_id"]] = artifact

    def _rows(self, path: str, ids: set[str]) -> pl.DataFrame:
        rows = (
            self.view.physical(path).filter(pl.col("artifact_id").is_in(ids)).collect()
        )
        if set(rows["artifact_id"]) != ids or rows.height != len(ids):
            msg = "Registered input has missing or duplicate registered content rows"
            raise StoreIntegrityError(msg)
        committer = self.view.committer
        for owner in rows["logical_commit_id"].unique():
            control = self.view.controls.filter(pl.col("logical_commit_id") == owner)
            if control.height != 1 or control["state"][0] != "complete":
                msg = "Registered input lacks a completed physical owner in its captured view"
                raise StoreIntegrityError(msg)
            if owner not in self._plans:
                self._plans[owner] = committer._control_plan(control.row(0, named=True))
            if (owner, path) not in self._verified:
                if path not in self._snapshots:
                    captured = self.view.snapshot.table(path)
                    assert captured is not None
                    self._snapshots[path] = DeltaSnapshot(
                        uri_join(committer.delta_root, path),
                        get_physical_schema_for_path(path),
                        fs=committer.fs,
                        storage_options=committer.storage_options,
                        version=captured.version,
                    )
                verify_snapshot_effect(
                    self._plans[owner],
                    path,
                    self._snapshots[path],
                    self.view.controls,
                )
                self._verified.add((owner, path))
        return rows
