"""Fixed completion-aware table views for ordinary store-bound curator queries."""

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl
from fsspec import AbstractFileSystem

from artisan.errors import StoreIntegrityError
from artisan.schemas.enums import TablePath
from artisan.schemas.execution.curator_admission import (
    CuratorAdmission,
    CuratorCompletedControl,
    CuratorReadSnapshot,
    CuratorTableVersion,
)
from artisan.schemas.execution.curator_cache import CuratorCacheExclusion
from artisan.storage.core.committed_scan import _visibility_expression
from artisan.storage.core.curator_membership import (
    ACCEPTED_STEP_VIEW,
    accepted_step_frame,
    owning_pipeline_run,
)
from artisan.storage.core.curator_provenance import (
    RetainedProjections,
    artifact_edges,
    capture_retained_projections,
    execution_edges,
)
from artisan.storage.core.store_format import assert_retained_store
from artisan.storage.core.table_schemas import (
    FRAMEWORK_SCHEMAS,
    get_physical_schema_for_path,
)
from artisan.storage.io.curator_admission import read_curator_admission
from artisan.storage.io.curator_plan import CuratorCommitPlan
from artisan.storage.io.input_registration_plan import REGISTRATION_TABLES
from artisan.storage.io.retained_commit import RetainedCommitter
from artisan.storage.io.retained_plan import retained_table_paths
from artisan.storage.io.retained_read import RetainedReadSnapshot
from artisan.utils.path import uri_join

if TYPE_CHECKING:
    from artisan.storage.cache.curator_dependencies import CuratorReadRecorder


def capture_curator_view(
    committer: RetainedCommitter, *, _read: RetainedReadSnapshot | None = None
) -> CuratorReadSnapshot:
    """Freeze complete owners and registered versions under the controller lock."""
    if _read is None:
        with committer._read_snapshot() as read:
            return capture_curator_view(committer, _read=read)
    _read.require_committer(committer)
    controls = _read.controls.filter(pl.col("state") == "complete")
    paths = set(retained_table_paths()) | {table.value for table in FRAMEWORK_SCHEMAS}
    versions = []
    for table_path in sorted(paths):
        path = uri_join(committer.delta_root, table_path)
        if committer.fs.exists(uri_join(path, "_delta_log")):
            versions.append(
                CuratorTableVersion(
                    table_path=table_path,
                    version=_read.snapshot(table_path).delta.version(),
                )
            )
    return CuratorReadSnapshot(
        tables=tuple(versions),
        controls=tuple(
            CuratorCompletedControl.model_validate(
                {name: row[name] for name in CuratorCompletedControl.model_fields}
            )
            for row in controls.sort("logical_commit_id").iter_rows(named=True)
        ),
    )


class CuratorView:
    """Read a fixed visible row set without hydrating unrelated artifact content."""

    def __init__(
        self,
        committer: RetainedCommitter,
        snapshot: CuratorReadSnapshot,
        *,
        _read: RetainedReadSnapshot | None = None,
    ) -> None:
        self.committer = committer
        self.snapshot = CuratorReadSnapshot.model_validate(snapshot)
        if _read is not None:
            _read.require_committer(committer)
        self._read = _read
        current = committer._controls() if _read is None else _read.controls
        for owner in self.snapshot.controls:
            found = current.filter(
                pl.col("logical_commit_id") == owner.logical_commit_id
            )
            if found.height != 1:
                msg = "Frozen curator owner is no longer present"
                raise StoreIntegrityError(msg)
            row = found.row(0, named=True)
            if row["state"] != "complete" or any(
                row[name] != value for name, value in owner.model_dump().items()
            ):
                msg = "Frozen curator owner differs from its completed plan"
                raise StoreIntegrityError(msg)
        self.controls = current.filter(
            pl.col("logical_commit_id").is_in(
                [owner.logical_commit_id for owner in self.snapshot.controls]
            )
        )
        self._projections: RetainedProjections | None = None
        self._read_recorder: CuratorReadRecorder | None = None
        self._admission: CuratorAdmission | None = None
        self._pipeline_run_id: str | None = None

    @property
    def files_root(self) -> str:
        """Return the managed carrier root bound to this fixed view."""
        return self.committer.files_root

    def _at(self, snapshot: CuratorReadSnapshot) -> CuratorView:
        """Validate another exact view within the same finite read, when present."""
        return CuratorView(self.committer, snapshot, _read=self._read)

    def bind_admission(self, admission: CuratorAdmission) -> None:
        """Bind query defaults to immutable invocation ownership before observation."""
        if self.snapshot != admission.view or (
            self._admission is not None and self._admission != admission
        ):
            msg = "Curator query context differs from its exact admission"
            raise StoreIntegrityError(msg)
        self._admission = admission

    def accepted_artifact_ids_by_step(self, artifact_type: str) -> dict[int, set[str]]:
        """Return accepted exports in the bound run, preserving repeated membership."""
        rows = (
            self.scan(ACCEPTED_STEP_VIEW)
            .filter(pl.col("artifact_type") == artifact_type)
            .select("step_number", "artifact_id")
            .collect()
        )
        selected: dict[int, set[str]] = {}
        for number, artifact_id in rows.iter_rows():
            selected.setdefault(number, set()).add(artifact_id)
        return selected

    def exclude_cache(self, reason: CuratorCacheExclusion) -> None:
        """Record an unsupported authored dependency without preventing execution."""
        if self._read_recorder is not None:
            self._read_recorder.exclusions.add(reason)

    def _observe(self, table_path: str, *, direct: bool = False) -> None:
        if self._read_recorder is not None:
            self._read_recorder.views.add((table_path, direct))

    def validate_binding(
        self, root: str, fs: AbstractFileSystem, storage_options: dict[str, str]
    ) -> None:
        """Reject a facade that would combine this visibility with another store."""
        if (
            root != self.committer.delta_root
            or fs is not self.committer.fs
            or storage_options != self.committer.storage_options
        ):
            msg = "Curator query view belongs to another store binding"
            raise StoreIntegrityError(msg)
        assert_retained_store(root, fs)

    def step_names(self, pipeline_run_id: str | None) -> dict[int, str]:
        """Read current names only from the captured lifecycle table version."""
        from artisan.orchestration.engine.step_tracker import StepTracker

        if pipeline_run_id is None and self._admission is not None:
            rows = (
                self.scan(ACCEPTED_STEP_VIEW)
                .filter(pl.col("step_number").is_not_null())
                .select("step_number", "step_name")
                .unique()
                .collect()
            )
            return dict(rows.iter_rows())
        rows = self.scan(TablePath.STEPS).collect()
        if rows.is_empty():
            return {}
        if pipeline_run_id is None:
            pipeline_run_id = rows.sort("timestamp")["pipeline_run_id"][-1]
        selected = rows.filter(pl.col("pipeline_run_id") == pipeline_run_id)
        states = []
        for group in selected.partition_by("step_run_id", maintain_order=True):
            history = group.unique().sort("state_sequence")
            if history["state_sequence"].n_unique() != history.height:
                msg = "Frozen curator lifecycle contains conflicting snapshots"
                raise StoreIntegrityError(msg)
            snapshots = history.to_dicts()
            StepTracker._validate_history(snapshots)
            states.append(StepTracker._validated_snapshot(snapshots[-1]))
        return {
            state.step_number: state.step_name
            for state in StepTracker._latest_by_position(states)
        }

    def scan(self, table: str | TablePath) -> pl.LazyFrame:
        """Scan registered rows at the captured version and completion boundary."""
        table_path = table.value if isinstance(table, TablePath) else table
        self._observe(table_path)
        if table_path == ACCEPTED_STEP_VIEW:
            if self._admission is not None and self._pipeline_run_id is None:
                self._pipeline_run_id = owning_pipeline_run(self, self._admission)
            return accepted_step_frame(self).lazy()
        if table_path == TablePath.LOGICAL_COMMITS.value:
            return self.controls.lazy()
        if table_path == TablePath.ARTIFACT_EDGES.value:
            return self.artifact_edges(direct=False)
        if table_path == TablePath.EXECUTION_EDGES.value:
            return pl.concat(
                [
                    self._visible(table_path),
                    execution_edges(
                        {
                            key: projection
                            for key, (_, projection) in self.projections().items()
                        }
                    ).lazy(),
                ]
            ).unique()
        return self._visible(table_path)

    def projections(self) -> RetainedProjections:
        """Capture immutable metadata once for this invocation's fixed query view."""
        if self._projections is None:
            self._projections = capture_retained_projections(self)
        return self._projections

    def artifact_edges(self, *, direct: bool) -> pl.LazyFrame:
        """Keep original direct links distinct from collapsed ancestry shortcuts."""
        self._observe(TablePath.ARTIFACT_EDGES.value, direct=direct)
        return pl.concat(
            [
                self._visible(TablePath.ARTIFACT_EDGES.value),
                artifact_edges(
                    {
                        key: projection
                        for key, (_, projection) in self.projections().items()
                    },
                    direct=direct,
                ).lazy(),
            ]
        ).unique()

    def _visible(self, table_path: str) -> pl.LazyFrame:
        physical = self.physical(table_path)
        if (
            table_path not in REGISTRATION_TABLES
            and "logical_commit_id" in physical.collect_schema()
        ):
            registrations = self.controls.filter(
                pl.col("commit_kind") == "input_registration"
            )["logical_commit_id"]
            if (
                physical.filter(
                    pl.col("logical_commit_id").is_in(registrations.implode())
                )
                .select(pl.len())
                .collect()
                .item()
            ):
                msg = "Raw registration owns an undeclared curator table effect"
                raise StoreIntegrityError(msg)
        visible = physical.filter(_visibility_expression(table_path, self.controls))
        if table_path in {
            TablePath.ARTIFACT_EDGES.value,
            TablePath.EXECUTION_EDGES.value,
        }:
            visible = visible.filter(
                ~pl.col("logical_commit_id").is_in(self.diagnostic_curator_owners())
            )
        return (
            visible.drop("logical_commit_id")
            if "logical_commit_id" in physical.collect_schema()
            else visible
        )

    def diagnostic_curator_owners(self) -> tuple[str, ...]:
        """Separate diagnostic graph evidence from ordinary content discovery."""
        owners = []
        for row in self.controls.filter(
            pl.col("commit_kind") == "step_result"
        ).iter_rows(named=True):
            plan = (
                self.committer._control_plan(row)
                if self._read is None
                else self._read.completed_control_plan(row["logical_commit_id"])
            )
            if isinstance(plan, CuratorCommitPlan):
                admission = read_curator_admission(
                    plan.admission,
                    delta_root=self.committer.delta_root,
                    fs=self.committer.fs,
                )
                if admission.record.replay_of_execution_run_id is not None:
                    owners.append(plan.logical_commit_id)
        return tuple(owners)

    def physical(self, table_path: str) -> pl.LazyFrame:
        """Read registered physical metadata or content at its captured version."""
        schema = get_physical_schema_for_path(table_path)
        captured = self.snapshot.table(table_path)
        if captured is None:
            return pl.DataFrame(schema=schema).lazy()
        scan = (
            pl.scan_delta(
                uri_join(self.committer.delta_root, table_path),
                version=captured.version,
                storage_options=self.committer.storage_options,
            )
            if self._read is None
            else pl.scan_delta(
                self._read.snapshot(table_path, version=captured.version).delta,
            )
        )
        if dict(scan.collect_schema()) != schema:
            msg = "Frozen curator table has an unexpected registered schema"
            raise StoreIntegrityError(msg)
        return scan
