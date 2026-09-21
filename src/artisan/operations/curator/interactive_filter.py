"""Interactive notebook filter: explore metrics, set thresholds, commit.

Provides a load-explore-filter-visualize-commit workflow. Unlike Filter
(which requires upfront criteria), InteractiveFilter lets users inspect
metric distributions before committing to thresholds.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import polars as pl
from fsspec import AbstractFileSystem

from artisan.operations.curator.filter import (
    Criterion,
    _assemble_diagnostics,
    _build_criterion_frame,
    _build_funnel,
    _build_metric_namespace,
    _build_metric_sources,
    _compute_funnel_counts,
    _criterion_stats,
    _criterion_to_expr,
    _empty_metric_pairs,
    _MetricSelection,
    _resolve_metric_step,
)
from artisan.provenance.traversal import walk_forward
from artisan.schemas.artifact.metric import MetricArtifact
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.schemas.enums import TablePath
from artisan.schemas.execution.command_record import CommandRecording
from artisan.schemas.execution.replay import ReplaySnapshot
from artisan.schemas.orchestration.step_lifecycle import StepDisposition, StepStatus
from artisan.schemas.orchestration.step_result import StepResult
from artisan.schemas.orchestration.step_start_record import StepStartRecord
from artisan.storage.core.artifact_store import ArtifactStore
from artisan.storage.core.committed_scan import read_committed
from artisan.utils.dataframes import encode_metric_value
from artisan.utils.dicts import flatten_dict
from artisan.utils.hashing import digest_utf8
from artisan.utils.path import uri_join

if TYPE_CHECKING:
    from artisan.orchestration.engine.step_tracker import StepTracker


@dataclass
class FilterSummary:
    """Per-criterion statistics and cumulative funnel with HTML rendering.

    Attributes:
        criteria (pl.DataFrame): Per-criterion pass rates and value statistics.
        funnel (pl.DataFrame): Cumulative elimination at each criterion stage.
    """

    criteria: pl.DataFrame
    funnel: pl.DataFrame
    _header: str

    def _repr_html_(self) -> str:
        """Render criteria and funnel tables as HTML for Jupyter."""
        parts = [f"<h4>{self._header}</h4>"]
        parts.append("<h5>Per-criterion statistics</h5>")
        parts.append(self.criteria._repr_html_())
        parts.append("<h5>Cumulative funnel</h5>")
        parts.append(self.funnel._repr_html_())
        return "\n".join(parts)

    def __repr__(self) -> str:
        return f"FilterSummary({self._header})"


class InteractiveFilter:
    """Explore metric distributions, set thresholds, and commit as a step.

    Args:
        delta_root: Root directory of the Delta Lake store.
        fs: Filesystem for reading and writing the store. Defaults to a
            local filesystem when None.
        storage_options: Backend storage options forwarded to the artifact
            store and Delta scans (e.g. cloud credentials). Defaults to None.
    """

    def __init__(
        self,
        delta_root: str,
        *,
        fs: AbstractFileSystem | None = None,
        storage_options: dict[str, str] | None = None,
    ) -> None:
        from fsspec.implementations.local import LocalFileSystem

        self._delta_root = delta_root
        self._fs = fs if fs is not None else LocalFileSystem()
        self._storage_options = storage_options
        self._store = ArtifactStore(
            self._delta_root,
            fs=self._fs,
            storage_options=self._storage_options,
        )
        self._wide_df: pl.DataFrame | None = None
        self._tidy_df: pl.DataFrame | None = None
        self._criteria: list[Criterion] = []
        self._pipeline_run_id: str | None = None
        self._primary_artifact_ids: set[str] = set()
        self._metric_pairs = _empty_metric_pairs()
        self._metric_steps = pl.DataFrame(
            schema={"metric_id": pl.String, "step_number": pl.Int64}
        )
        self._step_names: dict[int, str] = {}
        self._evaluation_df: pl.DataFrame | None = None
        self._bound_criteria: list[Criterion] = []
        self._resolved_steps: list[int | None] = []
        self._total_metrics_discovered: int = 0
        self._metric_sources: list[dict[str, Any]] = []

    # ------------------------------------------------------------------
    # Load
    # ------------------------------------------------------------------

    def load(
        self,
        step_numbers: list[int] | None = None,
        *,
        artifact_type: str | None = None,
        pipeline_run_id: str | None = None,
    ) -> None:
        """Load artifacts and their derived metrics from the Delta store.

        Discovers descendant metrics via forward provenance walk, then builds
        tidy and wide DataFrames. Wide DataFrame uses raw field names matching
        Filter's ``_build_metric_namespace`` approach.

        Args:
            step_numbers: Only load primary artifacts from these steps.
                None means all matching artifacts.
            artifact_type: Only load primary artifacts of this type
                (e.g. "data"). None means all non-metric artifacts.
            pipeline_run_id: Run whose accepted artifacts and metrics to load.
                None selects the newest run from the steps table.

        Raises:
            ValueError: If no artifacts found or no metrics found.
        """
        self._evaluation_df = None
        self._wide_df = None
        self._tidy_df = None
        index_path = uri_join(self._delta_root, TablePath.ARTIFACT_INDEX)
        if not self._fs.exists(index_path):
            msg = f"Artifact index not found at {index_path}"
            raise ValueError(msg)

        self._pipeline_run_id = pipeline_run_id or self._detect_pipeline_run_id()
        execution_ids: set[str] | None = None
        if self._pipeline_run_id is not None:
            from artisan.storage.core.run_scope import (
                load_accepted_outputs,
                load_execution_membership,
            )

            outputs = load_accepted_outputs(
                self._delta_root,
                fs=self._fs,
                storage_options=self._storage_options,
                pipeline_run_id=self._pipeline_run_id,
            )
            all_index = outputs.select(
                "artifact_id",
                "artifact_type",
                pl.col("current_step_number").alias("origin_step_number"),
            ).unique()
            membership = load_execution_membership(
                self._delta_root,
                fs=self._fs,
                storage_options=self._storage_options,
                pipeline_run_id=self._pipeline_run_id,
            )
            execution_ids = set(membership["execution_run_id"].to_list())
        else:
            all_index = read_committed(
                self._delta_root,
                TablePath.ARTIFACT_INDEX,
                fs=self._fs,
                storage_options=self._storage_options,
            ).select(["artifact_id", "artifact_type", "origin_step_number"])

        if artifact_type is not None:
            primary_mask = all_index["artifact_type"] == artifact_type
        else:
            primary_mask = all_index["artifact_type"] != "metric"
        if step_numbers is not None:
            primary_mask = primary_mask & all_index["origin_step_number"].is_in(
                step_numbers
            )
        primary_df = all_index.filter(primary_mask)

        if primary_df.is_empty():
            msg = "No primary artifacts found"
            if step_numbers is not None:
                msg += f" for step_numbers={step_numbers}"
            raise ValueError(msg)

        primary_ids = set(primary_df["artifact_id"].to_list())
        self._primary_artifact_ids = primary_ids

        # ── Metric discovery via forward provenance walk ──
        # Use ALL artifact IDs for step range (not just primaries) so metrics
        # at higher steps are included in the edge scan.
        step_range = self._store.provenance.get_step_range(
            all_index["artifact_id"].to_list()
        )
        if step_range is None:
            msg = "No metrics found derived from the primary artifacts"
            raise ValueError(msg)

        step_min, step_max = step_range
        edges = self._store.provenance.load_edges_df(
            step_min,
            step_max,
            include_target_type=True,
            execution_ids=execution_ids,
        )

        if edges.is_empty():
            msg = "No metrics found derived from the primary artifacts"
            raise ValueError(msg)

        walk_result = walk_forward(
            sources=primary_df.select("artifact_id"),
            edges=edges,
            target_type="metric",
        )

        if walk_result.is_empty():
            msg = "No metrics found derived from the primary artifacts"
            raise ValueError(msg)

        metric_pairs = walk_result.rename(
            {"source_id": "passthrough_id", "target_id": "metric_id"}
        )

        self._total_metrics_discovered = metric_pairs["metric_id"].n_unique()
        self._metric_pairs = metric_pairs
        # Accepted current-step membership can differ from a cached metric's origin.
        self._metric_steps = (
            all_index.filter(pl.col("artifact_type") == "metric")
            .select(
                pl.col("artifact_id").alias("metric_id"),
                pl.col("origin_step_number").alias("step_number"),
            )
            .unique()
        )
        self._step_names = self._store.provenance.load_step_name_map(
            self._pipeline_run_id
        )

        # ── Build wide DataFrame via _build_metric_namespace ──
        wide_df, _ = _build_metric_namespace(
            primary_df, metric_pairs, self._store, self._pipeline_run_id
        )
        self._wide_df = wide_df.rename({"passthrough_id": "artifact_id"})

        # ── Build tidy DataFrame for exploration ──
        all_found_metric_ids = set(metric_pairs["metric_id"].unique().to_list())
        self._metric_sources = _build_metric_sources(
            all_found_metric_ids, self._store, self._pipeline_run_id
        )
        metric_artifacts = self._store.get_artifacts_by_type(
            list(all_found_metric_ids), "metric"
        )

        metric_step_map = self._store.provenance.load_step_map(all_found_metric_ids)
        step_name_map = self._store.provenance.load_step_name_map(self._pipeline_run_id)

        # Build primary->metrics mapping from metric_pairs
        primary_to_metrics: dict[str, list[str]] = {}
        for row in metric_pairs.iter_rows(named=True):
            primary_to_metrics.setdefault(row["passthrough_id"], []).append(
                row["metric_id"]
            )

        rows: list[dict[str, Any]] = []
        for pid in primary_ids:
            for mid in primary_to_metrics.get(pid, []):
                metric = metric_artifacts.get(mid)
                if not isinstance(metric, MetricArtifact):
                    continue
                try:
                    values = metric.values
                except (ValueError, json.JSONDecodeError):
                    continue

                step_num = metric_step_map.get(mid)
                step_name = (
                    step_name_map.get(step_num, "unknown")
                    if step_num is not None
                    else "unknown"
                )

                for metric_name, raw_value in flatten_dict(values).items():
                    scalar, compound = encode_metric_value(raw_value)
                    rows.append(
                        {
                            "artifact_id": pid,
                            "step_number": step_num,
                            "step_name": step_name,
                            "metric_name": metric_name,
                            "metric_value": scalar,
                            "metric_compound": compound,
                        }
                    )

        if not rows:
            msg = "No metric values could be extracted"
            raise ValueError(msg)

        tidy_schema = {
            "artifact_id": pl.String,
            "step_number": pl.Int32,
            "step_name": pl.String,
            "metric_name": pl.String,
            "metric_value": pl.String,
            "metric_compound": pl.String,
        }
        self._tidy_df = pl.DataFrame(rows, schema=tidy_schema)

    def _detect_pipeline_run_id(self) -> str | None:
        """Return the newest run selected by the authoritative state reader."""
        from artisan.orchestration.engine.step_tracker import StepTracker

        states = StepTracker(
            self._delta_root,
            storage_options=self._storage_options,
            fs=self._fs,
        ).load_current_states()
        if not states:
            return None
        return states[0].pipeline_run_id

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def wide_df(self) -> pl.DataFrame:
        """Wide-format DataFrame with one row per artifact, metrics as columns.

        Raises:
            ValueError: If data has not been loaded.
        """
        if self._wide_df is None:
            msg = "No data loaded. Call load() first."
            raise ValueError(msg)
        return self._wide_df

    @property
    def tidy_df(self) -> pl.DataFrame:
        """Tidy (long) DataFrame with one row per (artifact, metric_name).

        Raises:
            ValueError: If data has not been loaded.
        """
        if self._tidy_df is None:
            msg = "No data loaded. Call load() first."
            raise ValueError(msg)
        return self._tidy_df

    @property
    def criteria(self) -> list[Criterion]:
        """Current filter criteria (may be empty)."""
        return list(self._criteria)

    # ------------------------------------------------------------------
    # Set criteria
    # ------------------------------------------------------------------

    def set_criteria(self, criteria: list[dict[str, Any]]) -> None:
        """Set filter criteria, validating metric names against loaded data.

        Args:
            criteria: Criterion dicts with metric, operator, value, and optional
                step name or step_number selectors.

        Raises:
            ValueError: If data not loaded, a metric name doesn't exist
                as a column in wide_df, or a metric name is ambiguous
                across multiple steps without disambiguation.
        """
        if self._wide_df is None:
            msg = "No data loaded. Call load() first."
            raise ValueError(msg)

        parsed = [Criterion(**c) for c in criteria]

        available = [c for c in self._wide_df.columns if c != "artifact_id"]
        for crit in parsed:
            if crit.metric not in available:
                msg = (
                    f"Metric '{crit.metric}' not found in loaded data. "
                    f"Available columns: {sorted(available)}"
                )
                raise ValueError(msg)

        frame, bound, resolved = self._bind_criteria(parsed)
        self._criteria = parsed
        self._evaluation_df = frame
        self._bound_criteria = bound
        self._resolved_steps = resolved

    def _bind_criteria(
        self, criteria: list[Criterion]
    ) -> tuple[pl.DataFrame, list[Criterion], list[int | None]]:
        """Select values within the loaded run before collapsing metric fields."""
        selections = {}
        for criterion in criteria:
            selector = (criterion.step, criterion.step_number)
            if selector in selections:
                continue
            resolved = _resolve_metric_step(*selector, self._step_names)
            pairs = self._metric_pairs
            if selector != (None, None):
                if resolved is None:
                    pairs = _empty_metric_pairs()
                else:
                    metric_ids = self._metric_steps.filter(
                        pl.col("step_number") == resolved
                    ).select("metric_id")
                    pairs = pairs.join(metric_ids, on="metric_id", how="semi")
            selections[selector] = _MetricSelection(pairs, resolved)
        return _build_criterion_frame(
            self.wide_df.select("artifact_id"),
            criteria,
            selections,
            self._store,
            self._pipeline_run_id,
            metric_steps=self._metric_steps,
        )

    def _evaluation_frame(self) -> pl.DataFrame:
        """Return current bindings, rebuilding after a new load."""
        if self._wide_df is None:
            msg = "No data loaded. Call load() first."
            raise ValueError(msg)
        if not self._criteria:
            msg = "No criteria set. Call set_criteria() first."
            raise ValueError(msg)
        if self._evaluation_df is None:
            self.set_criteria([criterion.model_dump() for criterion in self._criteria])
        assert self._evaluation_df is not None
        return self._evaluation_df

    # ------------------------------------------------------------------
    # Filtered results
    # ------------------------------------------------------------------

    @property
    def filtered_ids(self) -> list[str]:
        """Artifact IDs that pass all criteria.

        Raises:
            ValueError: If data not loaded or criteria not set.
        """
        frame = self._evaluation_frame()
        expressions = [
            _criterion_to_expr(c).fill_null(False) for c in self._bound_criteria
        ]
        return frame.filter(pl.all_horizontal(expressions))["passthrough_id"].to_list()

    @property
    def filtered_wide_df(self) -> pl.DataFrame:
        """Wide DataFrame filtered to rows passing all criteria.

        Raises:
            ValueError: If data not loaded or criteria not set.
        """
        return self.wide_df.filter(pl.col("artifact_id").is_in(self.filtered_ids))

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------

    def summary(self) -> FilterSummary:
        """Compute per-criterion statistics and cumulative funnel.

        Returns:
            FilterSummary with criteria and funnel DataFrames.

        Raises:
            ValueError: If data not loaded or criteria not set.
        """
        wide = self._evaluation_frame()
        total = wide.height

        # Per-criterion stats
        crit_rows: list[dict[str, Any]] = []
        for crit, bound in zip(self._criteria, self._bound_criteria, strict=True):
            pass_count, stats = _criterion_stats(wide, bound)
            crit_rows.append(
                {
                    "metric": crit.metric,
                    "operator": crit.operator,
                    "threshold": crit.value,
                    "pass": pass_count,
                    "total": total,
                    "rate": round(pass_count / total * 100, 1) if total else 0.0,
                    "min": stats["min"] if stats else None,
                    "mean": stats["mean"] if stats else None,
                    "max": stats["max"] if stats else None,
                }
            )

        criteria_df = pl.DataFrame(crit_rows)

        funnel_rows = _build_funnel(
            self._criteria, _compute_funnel_counts(wide, self._bound_criteria, total)
        )
        funnel_df = pl.DataFrame(funnel_rows)

        passed = funnel_rows[-1]["count"]
        rate = round(passed / total * 100, 1) if total else 0.0
        header = f"{passed} / {total} pass ({rate}%)"

        return FilterSummary(criteria=criteria_df, funnel=funnel_df, _header=header)

    # ------------------------------------------------------------------
    # Plot
    # ------------------------------------------------------------------

    def plot(self, **kwargs: Any) -> Any:
        """Plot per-criterion histograms with threshold lines.

        Args:
            **kwargs: Forwarded to ``plt.subplots()``.

        Returns:
            matplotlib Figure with one subplot per criterion.

        Raises:
            ValueError: If data not loaded or criteria not set.
        """
        wide = self._evaluation_frame()

        import matplotlib.pyplot as plt

        n = len(self._criteria)
        fig, axes = plt.subplots(1, n, figsize=(5 * n, 4), squeeze=False, **kwargs)

        for i, crit in enumerate(self._criteria):
            ax = axes[0][i]
            values = [
                v
                for v in wide[self._bound_criteria[i].metric].to_list()
                if isinstance(v, (int, float))
            ]
            if values:
                ax.hist(values, bins=min(20, len(values)), edgecolor="black", alpha=0.7)
            ax.axvline(
                crit.value,
                color="red",
                linestyle="--",
                label=f"{crit.operator} {crit.value}",
            )
            ax.set_title(crit.metric)
            ax.set_xlabel("Value")
            ax.set_ylabel("Count")
            ax.legend()

        fig.tight_layout()
        plt.close(fig)
        return fig

    # ------------------------------------------------------------------
    # Commit
    # ------------------------------------------------------------------

    def commit(self, step_name: str = "interactive_filter") -> StepResult:
        """Commit the filtered result as a pipeline step.

        Writes step, execution, and execution_edge records to the Delta store,
        making the filter result available for downstream pipeline steps via
        ``result.output("passthrough")``.

        Args:
            step_name: Name for the committed step.

        Returns:
            StepResult for downstream wiring.

        Raises:
            ValueError: If data not loaded, criteria not set, or no artifacts
                pass the filter.
        """
        if self._wide_df is None:
            msg = "No data loaded. Call load() first."
            raise ValueError(msg)
        if not self._criteria:
            msg = "No criteria set. Call set_criteria() first."
            raise ValueError(msg)

        filtered = self.filtered_ids
        if not filtered:
            msg = "No artifacts pass the current criteria. Adjust thresholds."
            raise ValueError(msg)

        now = datetime.now(UTC)
        timestamp_str = now.isoformat()

        step_number = self._next_step_number()

        pipeline_run_id = self._pipeline_run_id or str(uuid.uuid4())

        criteria_json = json.dumps(
            [c.model_dump() for c in self._criteria], sort_keys=True
        )
        sorted_input_ids = ",".join(sorted(self._primary_artifact_ids))

        step_spec_id = digest_utf8(f"{step_name}|{criteria_json}|{sorted_input_ids}")
        step_run_id = uuid.uuid4().hex
        execution_spec_id = digest_utf8(f"filter|{sorted_input_ids}|{criteria_json}")
        execution_run_id = digest_utf8(f"{execution_spec_id}|{timestamp_str}")

        # Build and record step start
        start_record = StepStartRecord(
            step_run_id=step_run_id,
            step_spec_id=step_spec_id,
            step_number=step_number,
            step_name=step_name,
            operation_class="artisan.operations.curator.filter.Filter",
            params_json=json.dumps(
                {"criteria": [c.model_dump() for c in self._criteria]}
            ),
            input_refs_json="{}",
            compute_backend="local",
            compute_options_json="{}",
            output_roles_json='["passthrough"]',
            output_types_json='{"passthrough": "any"}',
        )

        from artisan.orchestration.engine.step_tracker import StepTracker

        tracker = StepTracker(
            self._delta_root,
            pipeline_run_id,
            storage_options=self._storage_options,
            fs=self._fs,
        )
        tracker.create_attempt(start_record)
        tracker.transition(
            step_run_id,
            StepStatus.PENDING,
            StepStatus.RUNNING,
            step_spec_id=step_spec_id,
        )

        try:
            return self._commit_running_attempt(
                tracker=tracker,
                step_name=step_name,
                step_number=step_number,
                step_spec_id=step_spec_id,
                step_run_id=step_run_id,
                execution_spec_id=execution_spec_id,
                execution_run_id=execution_run_id,
                filtered=filtered,
                now=now,
            )
        except Exception as exc:
            current = tracker.current_state(step_run_id)
            if current.status != StepStatus.RUNNING:
                raise
            error = f"{type(exc).__name__}: {exc}"
            failed = StepResult(
                step_name=step_name,
                step_number=step_number,
                status=StepStatus.FAILED,
                error=error,
                step_run_id=step_run_id,
            )
            tracker.transition(
                step_run_id,
                StepStatus.RUNNING,
                StepStatus.FAILED,
                step_spec_id=step_spec_id,
                result=failed,
            )
            raise

    def _commit_running_attempt(
        self,
        *,
        tracker: StepTracker,
        step_name: str,
        step_number: int,
        step_spec_id: str,
        step_run_id: str,
        execution_spec_id: str,
        execution_run_id: str,
        filtered: list[str],
        now: datetime,
    ) -> StepResult:
        """Commit one interactive selection and its terminal snapshot."""
        from artisan.execution.recording.recorder import record_passthrough
        from artisan.operations.curator.filter import Filter
        from artisan.schemas.execution.execution_context import ExecutionContext
        from artisan.storage.io.commit import DeltaCommitter
        from artisan.storage.io.commit_plan import (
            prepare_commit_evidence,
            publish_commit_plan,
        )
        from artisan.storage.io.staging import StagingManager

        diagnostics = self._build_diagnostics(filtered)
        operation = Filter()
        staging_root = uri_join(self._delta_root, "_staging")
        execution_context = ExecutionContext(
            execution_run_id=execution_run_id,
            execution_spec_id=execution_spec_id,
            step_number=step_number,
            timestamp_start=now,
            worker_id=0,
            artifact_store=self._store,
            staging_root=staging_root,
            fs=self._fs,
            operation_name=type(operation).name,
            operation=operation,
            sandbox_path=None,
            compute_backend="local",
            shared_filesystem=False,
            step_run_id=step_run_id,
        )
        record_passthrough(
            command_recording=CommandRecording.empty(),
            replay_snapshot=ReplaySnapshot.unavailable("manual_interactive_commit"),
            replay_of_execution_run_id=None,
            execution_context=execution_context,
            passthrough={"passthrough": filtered},
            lineage_edges=None,
            inputs={"passthrough": list(self._primary_artifact_ids)},
            timestamp_end=now,
            params={"criteria": [c.model_dump() for c in self._criteria]},
            result_metadata={"diagnostics": diagnostics},
        )
        staging_manager = StagingManager(staging_root, self._fs)
        committer = DeltaCommitter(
            self._delta_root,
            staging_manager,
            fs=self._fs,
            storage_options=self._storage_options,
        )
        result = StepResult(
            step_name=step_name,
            step_number=step_number,
            status=StepStatus.SUCCEEDED,
            disposition=StepDisposition.EXECUTED,
            total_count=len(filtered),
            succeeded_count=len(filtered),
            failed_count=0,
            output_roles=frozenset(["passthrough"]),
            output_types={"passthrough": ArtifactTypes.ANY},
            metadata={"diagnostics": diagnostics},
            step_run_id=step_run_id,
        )
        candidate = tracker.prepare_terminal_candidate(
            step_run_id,
            StepStatus.RUNNING,
            StepStatus.SUCCEEDED,
            step_spec_id=step_spec_id,
            result=result,
        )
        staging_manager.stage_orchestrator_dataframe(
            candidate,
            TablePath.STEPS.value,
            commit_kind="step_result",
            step_run_id=step_run_id,
            step_number=step_number,
            operation_name=type(operation).name,
        )
        staged = prepare_commit_evidence(
            staging_root=staging_root,
            fs=self._fs,
            commit_kind="step_result",
            step_run_id=step_run_id,
            step_number=step_number,
            operation_name=type(operation).name,
            execution_run_ids=(execution_run_id,),
        )
        prepared = committer.prepare_logical(staged.plan, staged=staged)
        publish_commit_plan(self._delta_root, self._fs, staged.plan)
        committer.commit_logical(staged.plan, prepared=prepared)
        return tracker.current_state(step_run_id).to_step_result()

    def _build_diagnostics(self, filtered: list[str]) -> dict[str, Any]:
        """Build v4 diagnostics dict matching Filter's format.

        Args:
            filtered: List of artifact IDs that passed all criteria.

        Returns:
            Diagnostics dict with v4 structure.
        """
        wide = self._evaluation_frame()
        total = wide.height
        criteria_diags: list[dict[str, Any]] = []
        for crit, bound, resolved in zip(
            self._criteria, self._bound_criteria, self._resolved_steps, strict=True
        ):
            pass_count, stats = _criterion_stats(wide, bound)

            criteria_diags.append(
                {
                    "metric": crit.metric,
                    "operator": crit.operator,
                    "value": crit.value,
                    "pass_count": pass_count,
                    "resolved_from_step": resolved,
                    "stats": stats or {},
                }
            )

        funnel = _build_funnel(
            self._criteria, _compute_funnel_counts(wide, self._bound_criteria, total)
        )

        return _assemble_diagnostics(
            total_input=total,
            total_evaluated=total,
            total_metrics_discovered=self._total_metrics_discovered,
            total_passed=len(filtered),
            metric_sources=self._metric_sources,
            criteria=criteria_diags,
            funnel=funnel,
            interactive=True,
        )

    def _next_step_number(self) -> int:
        """Determine the next step number from the steps table."""
        steps_path = uri_join(self._delta_root, TablePath.STEPS)
        if not self._fs.exists(steps_path):
            return 0

        from artisan.orchestration.engine.step_tracker import StepTracker

        states = StepTracker(
            self._delta_root,
            storage_options=self._storage_options,
            fs=self._fs,
        ).load_all_current_states()
        max_val = max((state.step_number for state in states), default=None)
        return (max_val + 1) if max_val is not None else 0
