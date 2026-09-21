"""Crash-safe logical commits for Artisan Delta tables."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import arro3.core as ac
import polars as pl
from deltalake import DeltaTable, WriterProperties, write_deltalake
from fsspec import AbstractFileSystem

from artisan.errors import CommitError, StoreIntegrityError
from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.schemas.enums import TablePath
from artisan.schemas.orchestration.step_lifecycle import (
    TERMINAL_STEP_STATUSES,
    StepStatus,
)
from artisan.storage.core.committed_scan import (
    read_committed,
    read_logical_commits,
    verify_plan_effect,
)
from artisan.storage.core.store_format import (
    assert_store_format,
    assert_store_manifest,
    prepare_store_initialization,
    publish_store_manifest,
)
from artisan.storage.core.table_schemas import (
    FRAMEWORK_SCHEMAS,
    LOGICAL_COMMITS_SCHEMA,
    NON_PARTITIONED_TABLES,
    get_physical_schema,
    get_physical_schema_for_path,
    is_global_artifact_table,
)
from artisan.storage.io.commit_plan import (
    CommitPlan,
    PlannedTable,
    StagedPlanEvidence,
    canonical_table_plan_key,
    comparable_effect_rows,
    read_commit_plan,
    read_plan_evidence,
)
from artisan.storage.io.staging import StagingManager
from artisan.utils.path import uri_join

DEFAULT_WRITER_PROPERTIES = WriterProperties(compression="ZSTD")


def _normalize_table(table: str | TablePath) -> str:
    return table.value if isinstance(table, TablePath) else table


def _table_name(table_path: str) -> str:
    return table_path.rsplit("/", 1)[-1]


@dataclass(frozen=True)
class PreparedCommitEvidence:
    """Validated staged snapshot, usable once by the writer that prepared it."""

    staged: StagedPlanEvidence
    delta_root: str
    token: object


class DeltaCommitter:
    """Commit one immutable plan in dependency order and complete it last."""

    def __init__(
        self,
        delta_base_path: str,
        staging_manager: StagingManager,
        *,
        fs: AbstractFileSystem,
        storage_options: dict[str, str] | None = None,
        files_root: str | None = None,
    ) -> None:
        self.delta_base_path = delta_base_path
        self.staging_manager = staging_manager
        self._fs = fs
        self._storage_options = storage_options or {}
        self._files_root = files_root
        self._prepared: dict[object, PreparedCommitEvidence] = {}

    def prepare_logical(
        self,
        plan: CommitPlan,
        *,
        staged: StagedPlanEvidence | None = None,
        source_rows: pl.DataFrame | None = None,
        committed_frames: dict[str, pl.DataFrame] | None = None,
    ) -> PreparedCommitEvidence:
        """Validate exact staged evidence for one immediate application.

        A new plan may be prepared before publication. Applying the prepared
        evidence still requires the identical durable plan under this root.
        """
        assert_store_manifest(self.delta_base_path, self._fs)
        staged = staged or read_plan_evidence(
            plan, self.staging_manager.staging_dir, self._fs
        )
        if (
            staged.plan != plan
            or staged.staging_root != self.staging_manager.staging_dir
        ):
            msg = "Prepared staging evidence does not match this plan/root"
            raise StoreIntegrityError(msg)
        if plan.commit_kind == "execution_recovery":
            self._validate_recovery(
                plan, staged, source_rows=source_rows, committed_frames=committed_frames
            )
        else:
            self._reject_terminal_attempt(plan, source_rows=source_rows)
        prepared = PreparedCommitEvidence(staged, self.delta_base_path, object())
        self._prepared[prepared.token] = prepared
        return prepared

    def commit_logical(
        self,
        plan: CommitPlan,
        *,
        preserve_staging: bool = False,
        prepared: PreparedCommitEvidence | None = None,
    ) -> dict[str, int]:
        """Apply one immutable batch, proving its effects before completion.

        Prepared evidence is optional and can be consumed only once by this
        writer. Retries prepare the persisted plan again. Already completed
        plans are verified only when retained staging needs cleanup.
        """
        assert_store_manifest(self.delta_base_path, self._fs)
        self._require_persisted_plan(plan)
        if prepared is not None:
            self._consume_prepared(plan, prepared)
        controls = self._controls()
        control = self._control_for(plan, controls)
        if control is not None and control["state"] == "complete":
            if not preserve_staging and self._has_staging(plan):
                self._cleanup_plan(plan)
            return {}
        if control is not None and control["state"] == "abandoned":
            msg = f"Logical commit {plan.logical_commit_id} is abandoned"
            raise StoreIntegrityError(msg)
        if prepared is None:
            prepared = self.prepare_logical(plan)
            self._consume_prepared(plan, prepared)
        frames = prepared.staged.frames
        if control is None:
            try:
                controls = self._insert_planned(plan)
            except Exception as exc:
                msg = f"Commit {plan.logical_commit_id} failed at control planning"
                raise CommitError(
                    plan.logical_commit_id,
                    TablePath.LOGICAL_COMMITS.value,
                    plan.plan_digest,
                    [],
                    self._plan_objects(plan),
                    msg,
                ) from exc
            self._checkpoint("planned", plan, None)

        results: dict[str, int] = {}
        verified: list[str] = []
        for table in plan.tables:
            try:
                written = self._commit_table_effect(
                    plan, table, frames[table.table_path], controls
                )
                verified.append(table.table_path)
                if written:
                    results[_table_name(table.table_path)] = written
                self._checkpoint("table", plan, table.table_path)
            except Exception as exc:
                msg = f"Commit {plan.logical_commit_id} failed at {table.table_path}"
                raise CommitError(
                    plan.logical_commit_id,
                    table.table_path,
                    table.table_plan_key,
                    verified,
                    [file.relative_path for file in table.files],
                    msg,
                ) from exc
        try:
            self._complete(plan)
            self._checkpoint("complete", plan, None)
        except Exception as exc:
            msg = f"Commit {plan.logical_commit_id} failed at logical completion"
            raise CommitError(
                plan.logical_commit_id,
                TablePath.LOGICAL_COMMITS.value,
                plan.plan_digest,
                verified,
                self._plan_objects(plan),
                msg,
            ) from exc
        if not preserve_staging:
            self._delete_planned_files(plan)
        return results

    def _consume_prepared(
        self, plan: CommitPlan, prepared: PreparedCommitEvidence
    ) -> None:
        issued = self._prepared.pop(prepared.token, None)
        if (
            issued is not prepared
            or prepared.delta_root != self.delta_base_path
            or prepared.staged.plan != plan
            or prepared.staged.staging_root != self.staging_manager.staging_dir
        ):
            msg = "Prepared commit evidence is stale or belongs to another writer/plan/root"
            raise StoreIntegrityError(msg)

    def _has_staging(self, plan: CommitPlan) -> bool:
        return any(
            self._fs.exists(
                uri_join(self.staging_manager.staging_dir, file.relative_path)
            )
            for table in plan.tables
            for file in table.files
        )

    def _require_persisted_plan(self, plan: CommitPlan) -> None:
        persisted = read_commit_plan(
            self.delta_base_path,
            self._fs,
            plan.step_run_id,
            plan.commit_kind,
            plan.recovery_batch_id,
        )
        if persisted != plan:
            msg = f"Persisted plan disagrees for {plan.logical_commit_id}"
            raise StoreIntegrityError(msg)

    def _reject_terminal_attempt(
        self, plan: CommitPlan, *, source_rows: pl.DataFrame | None = None
    ) -> None:
        """Prevent an incomplete plan from reviving an already terminal attempt."""
        if source_rows is None:
            source_rows = read_committed(
                self.delta_base_path,
                TablePath.STEPS,
                fs=self._fs,
                storage_options=self._storage_options,
            )
        rows = source_rows.filter(pl.col("step_run_id") == plan.step_run_id)
        if rows.is_empty():
            return
        latest = rows.sort("state_sequence").row(-1, named=True)
        try:
            status = StepStatus(latest["status"])
        except (TypeError, ValueError) as exc:
            msg = f"Step attempt {plan.step_run_id} has an invalid visible status"
            raise StoreIntegrityError(msg) from exc
        if status in TERMINAL_STEP_STATUSES:
            msg = (
                f"Refusing to commit {plan.logical_commit_id}: step attempt is "
                f"already {status.value}"
            )
            raise StoreIntegrityError(msg)
        if plan.commit_kind == "step_result" and status is not StepStatus.RUNNING:
            msg = (
                f"Refusing to commit {plan.logical_commit_id}: step attempt must be "
                f"running, found {status.value}"
            )
            raise StoreIntegrityError(msg)

    def _validate_recovery(
        self,
        plan: CommitPlan,
        staged: StagedPlanEvidence,
        *,
        source_rows: pl.DataFrame | None = None,
        committed_frames: dict[str, pl.DataFrame] | None = None,
    ) -> None:
        """Validate all recovered workers through one shared source/artifact view."""
        from artisan.storage.core.run_scope import validate_staged_executions

        if source_rows is None:
            source_rows = self._read_physical(TablePath.STEPS.value)
        validate_staged_executions(
            self.delta_base_path,
            staged.frames,
            step_run_id=plan.step_run_id,
            step_number=plan.step_number,
            operation_name=plan.operation_name,
            source_rows=source_rows,
            per_execution_artifact_ids=staged.per_execution_artifact_ids,
            committed_frames=committed_frames,
            fs=self._fs,
            storage_options=self._storage_options,
            files_root=self._files_root,
        )

    @staticmethod
    def _control_for(
        plan: CommitPlan,
        controls: pl.DataFrame,
    ) -> dict[str, Any] | None:
        matches = controls.filter(pl.col("logical_commit_id") == plan.logical_commit_id)
        if matches.is_empty():
            return None
        row = matches.row(0, named=True)
        if (
            row["commit_kind"] != plan.commit_kind
            or row["step_run_id"] != plan.step_run_id
            or row["plan_digest"] != plan.plan_digest
            or row["recovery_batch_id"] != plan.recovery_batch_id
        ):
            msg = f"Control row disagrees with plan {plan.logical_commit_id}"
            raise StoreIntegrityError(msg)
        return row

    def _controls(self) -> pl.DataFrame:
        return read_logical_commits(
            self.delta_base_path,
            fs=self._fs,
            storage_options=self._storage_options,
        )

    def _insert_planned(self, plan: CommitPlan) -> pl.DataFrame:
        control = pl.DataFrame(
            [
                {
                    "logical_commit_id": plan.logical_commit_id,
                    "commit_kind": plan.commit_kind,
                    "step_run_id": plan.step_run_id,
                    "recovery_batch_id": plan.recovery_batch_id,
                    "state": "planned",
                    "plan_digest": plan.plan_digest,
                    "created_at": datetime.now(UTC),
                    "completed_at": None,
                    "abandon_reason": None,
                }
            ],
            schema=LOGICAL_COMMITS_SCHEMA,
        )
        self._append(control, TablePath.LOGICAL_COMMITS.value)
        controls = self._controls()
        row = self._control_for(plan, controls)
        if row is None or row["state"] != "planned":
            msg = f"Planned control row was not durable for {plan.logical_commit_id}"
            raise StoreIntegrityError(msg)
        return controls

    def _commit_table_effect(
        self,
        plan: CommitPlan,
        table: PlannedTable,
        expected: pl.DataFrame,
        controls: pl.DataFrame,
    ) -> int:
        physical = self._read_physical(table.table_path)
        missing = self._missing_rows(plan, table, expected, physical, controls)
        if not missing.is_empty():
            self._append(
                self._inject_owner(missing, table.table_path, plan.logical_commit_id),
                table.table_path,
            )
            physical = self._read_physical(table.table_path)
        verify_plan_effect(plan, table.table_path, physical, controls)
        return missing.height

    def _missing_rows(
        self,
        plan: CommitPlan,
        table: PlannedTable,
        expected: pl.DataFrame,
        physical: pl.DataFrame,
        controls: pl.DataFrame,
    ) -> pl.DataFrame:
        keys = list(table.natural_key)
        keyed = physical.join(
            expected.select(keys),
            on=keys,
            how="inner",
            nulls_equal=True,
        )
        self._reject_conflicting_values(
            table.table_path, table.natural_key, expected, keyed
        )
        if table.table_path == TablePath.CACHE_REUSE.value:
            owned = physical.filter(pl.col("current_step_run_id") == plan.step_run_id)
        else:
            owned = physical.filter(
                pl.col("logical_commit_id") == plan.logical_commit_id
            )
            if keyed["logical_commit_id"].null_count():
                msg = f"Table {table.table_path!r} contains unowned rows"
                raise StoreIntegrityError(msg)
        extras = owned.join(
            expected.select(keys), on=keys, how="anti", nulls_equal=True
        )
        if not extras.is_empty():
            msg = f"Commit {plan.logical_commit_id} owns unplanned {table.table_path} rows"
            raise StoreIntegrityError(msg)
        if (
            "origin_step_number" in owned
            and not owned.filter(
                pl.col("origin_step_number").is_null()
                | (pl.col("origin_step_number") != plan.step_number)
            ).is_empty()
        ):
            msg = (
                f"Artifact origin disagrees with owning commit {plan.logical_commit_id}"
            )
            raise StoreIntegrityError(msg)
        if table.table_path == TablePath.CACHE_REUSE.value:
            satisfied = keyed
            if 0 < satisfied.height < expected.height:
                self._raise_partial(table.table_path)
        elif is_global_artifact_table(table.table_path):
            complete = set(
                controls.filter(pl.col("state") == "complete")[
                    "logical_commit_id"
                ].to_list()
            )
            satisfied = keyed.filter(
                (pl.col("logical_commit_id") == plan.logical_commit_id)
                | pl.col("logical_commit_id").is_in(complete)
            )
        else:
            foreign = keyed.filter(
                pl.col("logical_commit_id") != plan.logical_commit_id
            )
            if not foreign.is_empty():
                msg = f"Natural key already belongs to another commit in {table.table_path}"
                raise StoreIntegrityError(msg)
            satisfied = keyed.filter(
                pl.col("logical_commit_id") == plan.logical_commit_id
            )
            if 0 < satisfied.height < expected.height:
                self._raise_partial(table.table_path)
        if not satisfied.group_by(keys).len().filter(pl.col("len") != 1).is_empty():
            msg = f"Duplicate natural key in table {table.table_path!r}"
            raise StoreIntegrityError(msg)
        missing = expected.join(
            satisfied.select(keys).unique(),
            on=keys,
            how="anti",
            nulls_equal=True,
        )
        if missing.is_empty():
            self._verify_table_plan_key(plan, table, expected)
        return missing

    @staticmethod
    def _reject_conflicting_values(
        table_path: str,
        natural_key: tuple[str, ...],
        expected: pl.DataFrame,
        keyed: pl.DataFrame,
    ) -> None:
        if keyed.is_empty():
            return
        expected = comparable_effect_rows(table_path, expected)
        columns = expected.columns
        ownerless = keyed.select(columns).unique(maintain_order=True)
        intended = expected.join(
            keyed.select(list(natural_key)).unique(),
            on=list(natural_key),
            how="inner",
            nulls_equal=True,
        )
        if not _frames_equal(ownerless, intended, sort_by=list(natural_key)):
            msg = f"Natural key has conflicting values in {table_path}"
            raise StoreIntegrityError(msg)

    @staticmethod
    def _verify_table_plan_key(
        plan: CommitPlan,
        table: PlannedTable,
        expected: pl.DataFrame,
    ) -> None:
        if (
            canonical_table_plan_key(
                plan.logical_commit_id,
                table.table_path,
                expected,
            )
            != table.table_plan_key
        ):
            msg = f"Table plan key disagrees for {table.table_path}"
            raise StoreIntegrityError(msg)

    @staticmethod
    def _raise_partial(table_path: str) -> None:
        msg = f"Only part of the planned effect exists in {table_path}"
        raise StoreIntegrityError(msg)

    @staticmethod
    def _inject_owner(
        frame: pl.DataFrame,
        table_path: str,
        logical_commit_id: str,
    ) -> pl.DataFrame:
        if table_path == TablePath.CACHE_REUSE.value:
            return frame
        return frame.with_columns(
            pl.lit(logical_commit_id).cast(pl.String).alias("logical_commit_id")
        )

    def _complete(self, plan: CommitPlan) -> None:
        table = DeltaTable(
            self._table_path(TablePath.LOGICAL_COMMITS.value),
            storage_options=self._storage_options,
        )
        predicate = (
            f"logical_commit_id = '{plan.logical_commit_id}' AND state = 'planned'"
        )
        completed_at = datetime.now(UTC).isoformat()
        metrics = table.update(
            predicate=predicate,
            updates={
                "state": "'complete'",
                "completed_at": f"CAST('{completed_at}' AS TIMESTAMP)",
            },
        )
        if metrics.get("num_updated_rows") != 1:
            msg = f"Control completion was not conditional for {plan.logical_commit_id}"
            raise StoreIntegrityError(msg)
        control = self._control_for(plan, self._controls())
        if control is None or control["state"] != "complete":
            msg = f"Completion was not durable for {plan.logical_commit_id}"
            raise StoreIntegrityError(msg)

    def _validate_complete(
        self, plan: CommitPlan, *, controls: pl.DataFrame | None = None
    ) -> None:
        """Verify only this batch before a later retained-staging cleanup."""
        controls = self._controls() if controls is None else controls
        control = self._control_for(plan, controls)
        if control is None or control["state"] != "complete":
            msg = (
                f"Refusing verification before completion for {plan.logical_commit_id}"
            )
            raise StoreIntegrityError(msg)
        for table in plan.tables:
            verify_plan_effect(
                plan, table.table_path, self._read_physical(table.table_path), controls
            )

    def _cleanup_plan(self, plan: CommitPlan) -> None:
        """Reverify a completed batch before cleaning it in a later operation."""
        self._validate_complete(plan)
        self._delete_planned_files(plan)

    def _delete_planned_files(self, plan: CommitPlan) -> None:
        """Delete unchanged evidence immediately after local completion proof."""
        self.staging_manager.cleanup_plan(
            [file for table in plan.tables for file in table.files]
        )

    @staticmethod
    def _plan_objects(plan: CommitPlan) -> list[str]:
        return [file.relative_path for table in plan.tables for file in table.files]

    def _read_physical(self, table_path: str) -> pl.DataFrame:
        try:
            frame = pl.scan_delta(
                self._table_path(table_path),
                storage_options=self._storage_options,
            ).collect()
        except Exception as exc:
            msg = f"Unreadable Delta table {table_path!r}"
            raise StoreIntegrityError(msg) from exc
        if dict(frame.schema) != get_physical_schema_for_path(table_path):
            msg = f"Delta table {table_path!r} has an unexpected schema"
            raise StoreIntegrityError(msg)
        return frame

    def _append(self, frame: pl.DataFrame, table_path: str) -> None:
        expected = get_physical_schema_for_path(table_path)
        frame = frame.cast(pl.Schema(expected))
        if dict(frame.schema) != expected:
            msg = f"Refusing a schema-changing write to {table_path!r}"
            raise StoreIntegrityError(msg)
        write_deltalake(
            self._table_path(table_path),
            _delta_arrow(frame),
            mode="append",
            storage_options=self._storage_options,
            writer_properties=DEFAULT_WRITER_PROPERTIES,
        )

    @staticmethod
    def _checkpoint(
        boundary: str,
        plan: CommitPlan,
        table_path: str | None,
    ) -> None:
        """Fault-injection seam exercised by crash-recovery tests."""

    def initialize_tables(self) -> None:
        """Create the exact release table set and publish its manifest last."""
        publish_manifest = prepare_store_initialization(
            self.delta_base_path,
            self._fs,
            self._storage_options,
        )
        for table in FRAMEWORK_SCHEMAS:
            path = self._table_path(table.value)
            if not self._fs.exists(path):
                self._create_empty(
                    pl.DataFrame(schema=get_physical_schema(table)),
                    table.value,
                )
        for definition in ArtifactTypeDef.get_all().values():
            path = self._table_path(definition.table_path)
            if not self._fs.exists(path):
                schema = {**definition.polars_schema(), "logical_commit_id": pl.String}
                self._create_empty(pl.DataFrame(schema=schema), definition.table_path)
        if publish_manifest:
            publish_store_manifest(self.delta_base_path, self._fs)

    def _create_empty(self, frame: pl.DataFrame, table_path: str) -> None:
        non_partitioned = {table.value for table in NON_PARTITIONED_TABLES}
        options: dict[str, Any] = {"writer_properties": DEFAULT_WRITER_PROPERTIES}
        if table_path not in non_partitioned:
            options["partition_by"] = ["origin_step_number"]
        frame.write_delta(
            self._table_path(table_path),
            mode="overwrite",
            delta_write_options=options,
            storage_options=self._storage_options,
        )

    def compact_table(
        self,
        table: str | TablePath,
        z_order_columns: list[str] | None = None,
        step_number: int | None = None,
    ) -> dict[str, int]:
        """Compact a table, optionally restricting a partition or Z-ordering.

        Callers must schedule maintenance after logical commits finish.
        """
        table_path = _normalize_table(table)
        assert_store_format(self.delta_base_path, self._fs, self._storage_options)
        delta = DeltaTable(
            self._table_path(table_path),
            storage_options=self._storage_options,
        )
        non_partitioned = {member.value for member in NON_PARTITIONED_TABLES}
        partition_filters = None
        if step_number is not None and table_path not in non_partitioned:
            partition_filters = [("origin_step_number", "=", str(step_number))]
        if z_order_columns:
            result = delta.optimize.z_order(
                columns=z_order_columns,
                partition_filters=partition_filters,
            )
        else:
            result = delta.optimize.compact(partition_filters=partition_filters)
        return {
            "files_added": result.get("numFilesAdded", 0),
            "files_removed": result.get("numFilesRemoved", 0),
        }

    def compact_all_tables(
        self,
        z_order: bool = True,
        step_number: int | None = None,
    ) -> dict[str, dict[str, int]]:
        """Compact every data table, propagating the first maintenance failure."""
        config = {
            definition.table_path: ["artifact_id"]
            for definition in ArtifactTypeDef.get_all().values()
        }
        config.update(
            {
                TablePath.EXECUTIONS.value: ["execution_spec_id"],
                TablePath.ARTIFACT_INDEX.value: ["artifact_id"],
                TablePath.ARTIFACT_LOCATIONS.value: ["artifact_id", "uri"],
                TablePath.ARTIFACT_EDGES.value: [
                    "source_artifact_id",
                    "target_artifact_id",
                ],
                TablePath.EXECUTION_EDGES.value: ["execution_run_id"],
                TablePath.CACHE_REUSE.value: ["current_step_run_id"],
                TablePath.STEPS.value: ["step_spec_id"],
            }
        )
        results: dict[str, dict[str, int]] = {}
        for table_path, columns in config.items():
            stats = self.compact_table(
                table_path,
                z_order_columns=columns if z_order else None,
                step_number=step_number,
            )
            if stats["files_added"] or stats["files_removed"]:
                results[_table_name(table_path)] = stats
        return results

    def vacuum_table(self, table: str | TablePath, retention_hours: int = 168) -> None:
        """Run Delta's vacuum eligibility check without deleting files.

        This maintenance hook discards the dry-run candidate list. Actual
        reclamation is not implemented.
        """
        assert_store_format(self.delta_base_path, self._fs, self._storage_options)
        delta = DeltaTable(
            self._table_path(_normalize_table(table)),
            storage_options=self._storage_options,
        )
        delta.vacuum(
            retention_hours=retention_hours,
            enforce_retention_duration=False,
        )

    def _table_path(self, table_path: str) -> str:
        return uri_join(self.delta_base_path, table_path)


def _frames_equal(
    left: pl.DataFrame,
    right: pl.DataFrame,
    *,
    sort_by: list[str],
) -> bool:
    return left.sort(sort_by).equals(
        right.select(left.columns).sort(sort_by),
        null_equal=True,
    )


def _delta_arrow(frame: pl.DataFrame) -> ac.Table:
    """Use Arrow UTF-8, not string-view, for delta-rs predicate compatibility."""
    table = ac.Table.from_arrow(frame)
    columns: list[ac.ChunkedArray] = []
    fields: list[ac.Field] = []
    for index, name in enumerate(table.column_names):
        column = table.column(index)
        if ac.DataType.is_string_view(column.type):
            column = column.cast(ac.DataType.string())
        columns.append(column)
        fields.append(ac.Field(name, column.type, nullable=True))
    return ac.Table.from_arrays(columns, schema=ac.Schema(fields))
