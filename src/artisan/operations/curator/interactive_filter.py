"""Interactive notebook filter: explore metrics, set thresholds, commit.

Provides a load-explore-filter-visualize-commit workflow. Unlike Filter
(which requires upfront criteria), InteractiveFilter lets users inspect
metric distributions before committing to thresholds.
"""

from __future__ import annotations

import json
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
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
from artisan.schemas.orchestration.step_result import StepResult
from artisan.storage.core.artifact_store import ArtifactStore
from artisan.utils.dataframes import encode_metric_value
from artisan.utils.dicts import flatten_dict
from artisan.utils.path import uri_join

if TYPE_CHECKING:
    from artisan.schemas.execution.curator_admission import CuratorReadSnapshot
    from artisan.storage.core.curator_view import CuratorView
    from artisan.storage.core.retained_reader import AcceptedArtifact


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
        files_root: Managed carrier root; required for nonlocal/custom storage.
        storage_options: Backend storage options forwarded to the artifact
            store and Delta scans (e.g. cloud credentials). Defaults to None.
    """

    def __init__(
        self,
        delta_root: str,
        *,
        fs: AbstractFileSystem | None = None,
        storage_options: dict[str, str] | None = None,
        files_root: str | None = None,
    ) -> None:
        from fsspec.implementations.local import LocalFileSystem

        self._delta_root = str(delta_root)
        self._fs = fs if fs is not None else LocalFileSystem()
        self._storage_options = storage_options
        from artisan.storage.io.retained_store import open_configured_store

        self._committer = open_configured_store(
            self._delta_root,
            files_root=files_root,
            fs=self._fs,
            storage_options=storage_options,
        )
        self._files_root = self._committer.files_root
        self._snapshot: CuratorReadSnapshot | None = None
        self._source_attempts: tuple[str, ...] = ()
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
                (e.g. "csv"). None means all non-metric artifacts.
            pipeline_run_id: Run whose accepted artifacts and metrics to load.
                None selects the newest run from the steps table.

        Raises:
            ValueError: If no artifacts found or no metrics found.
        """
        self._evaluation_df = None
        self._wide_df = None
        self._tidy_df = None
        self._snapshot = None
        self._source_attempts = ()
        from artisan.orchestration.engine.retained_artifact_query import (
            _references_for_states,
        )
        from artisan.orchestration.engine.retained_execution_query import (
            _execution_membership,
        )
        from artisan.orchestration.engine.retained_step_tracker import (
            RetainedStepTracker,
        )
        from artisan.storage.core.curator_view import CuratorView, capture_curator_view
        from artisan.storage.core.execution_source import (
            CuratorExecutionSource,
            ScientificExecutionSource,
        )

        with self._committer._read_snapshot() as read:
            tracker = RetainedStepTracker(self._committer, "")
            _, attempts = tracker._read_lifecycle(read)
            states = tracker._select_current_states(attempts, pipeline_run_id)
            if not states:
                msg = "No primary artifacts found"
                raise ValueError(msg)
            self._pipeline_run_id = states[0].pipeline_run_id
            refs = _references_for_states(
                read, states, pipeline_run_id=self._pipeline_run_id
            )
            all_index = pl.DataFrame(
                [
                    {
                        "artifact_id": ref.artifact_id,
                        "artifact_type": ref.artifact_type,
                        "origin_step_number": ref.current_step_number,
                    }
                    for ref in refs
                ],
                schema={
                    "artifact_id": pl.String,
                    "artifact_type": pl.String,
                    "origin_step_number": pl.Int64,
                },
            ).unique()
            execution_ids = set()
            for member in _execution_membership(read, states):
                if isinstance(member.source, CuratorExecutionSource):
                    execution_ids.add(
                        str(member.source.execution_record["execution_run_id"])
                    )
                elif isinstance(member.source, ScientificExecutionSource):
                    execution_ids.add(member.source.plan.receipt.delivery.delivery_id)
            snapshot = capture_curator_view(self._committer, _read=read)
            store = self._view_store(CuratorView(self._committer, snapshot, _read=read))
            self._load_data(
                all_index, execution_ids, store, step_numbers, artifact_type
            )
            self._source_attempts = tuple(
                state.step_run_id
                for state in states
                if any(
                    ref.current_step_number == state.step_number
                    and ref.artifact_id in self._primary_artifact_ids
                    for ref in refs
                )
            )
            self._snapshot = snapshot

    def _load_data(
        self,
        all_index: pl.DataFrame,
        execution_ids: set[str],
        store: ArtifactStore,
        step_numbers: list[int] | None,
        artifact_type: str | None,
    ) -> None:
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

        step_range = store.provenance.get_step_range(all_index["artifact_id"].to_list())
        if step_range is None:
            msg = "No metrics found derived from the primary artifacts"
            raise ValueError(msg)

        step_min, step_max = step_range
        edges = store.provenance.load_edges_df(
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
        self._metric_steps = (
            all_index.filter(pl.col("artifact_type") == "metric")
            .select(
                pl.col("artifact_id").alias("metric_id"),
                pl.col("origin_step_number").alias("step_number"),
            )
            .unique()
        )
        self._step_names = store.provenance.load_step_name_map(self._pipeline_run_id)

        wide_df, _ = _build_metric_namespace(
            primary_df, metric_pairs, store, self._pipeline_run_id
        )
        self._wide_df = wide_df.rename({"passthrough_id": "artifact_id"})

        all_found_metric_ids = set(metric_pairs["metric_id"].unique().to_list())
        self._metric_sources = _build_metric_sources(
            all_found_metric_ids, store, self._pipeline_run_id
        )
        metric_artifacts = store.get_artifacts_by_type(
            list(all_found_metric_ids), "metric"
        )

        metric_step_map = store.provenance.load_step_map(all_found_metric_ids)
        step_name_map = store.provenance.load_step_name_map(self._pipeline_run_id)

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

    def _view_store(self, view: CuratorView) -> ArtifactStore:
        return ArtifactStore(
            self._delta_root,
            fs=self._fs,
            storage_options=self._storage_options,
            files_root=self._files_root,
            _view=view,
        )

    @contextmanager
    def _loaded_store(self) -> Iterator[ArtifactStore]:
        """Revalidate the frozen load boundary without advancing its table versions."""
        from artisan.storage.core.curator_view import CuratorView

        if self._snapshot is None:
            msg = "No data loaded. Call load() first."
            raise ValueError(msg)
        with self._committer._read_snapshot() as read:
            yield self._view_store(
                CuratorView(self._committer, self._snapshot, _read=read)
            )

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
        with self._loaded_store() as store:
            return _build_criterion_frame(
                self.wide_df.select("artifact_id"),
                criteria,
                selections,
                store,
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

    def summary(self) -> FilterSummary:
        """Compute per-criterion statistics and cumulative funnel.

        Returns:
            FilterSummary with criteria and funnel DataFrames.

        Raises:
            ValueError: If data not loaded or criteria not set.
        """
        wide = self._evaluation_frame()
        total = wide.height

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

    def commit(self, step_name: str = "interactive_filter") -> StepResult:
        """Commit the filtered result as a pipeline step.

        Writes step, execution, and execution_edge records to the Delta store,
        making the filter result available for downstream pipeline steps via
        ``result.output("passthrough")``. A selection with no passing artifacts
        records the criteria and an empty output.

        Args:
            step_name: Name for the committed step.

        Returns:
            StepResult for downstream wiring.

        Raises:
            ValueError: If data not loaded or criteria not set.
        """
        if self._wide_df is None:
            msg = "No data loaded. Call load() first."
            raise ValueError(msg)
        if not self._criteria:
            msg = "No criteria set. Call set_criteria() first."
            raise ValueError(msg)

        filtered = self.filtered_ids

        from artisan.execution.assignment_transport import AssignmentDispatch
        from artisan.operations.curator._interactive_selection import (
            InteractiveSelection,
        )
        from artisan.orchestration.engine.retained_step_tracker import (
            RetainedStepTracker,
        )
        from artisan.orchestration.pipeline_manager import (
            PipelineManager,
            _recorded_defaults,
        )
        from artisan.schemas.execution.storage_config import StorageConfig
        from artisan.storage.core.store_format import assert_writable_store
        from artisan.utils.application_modules import resolve_application_modules

        assert_writable_store(self._delta_root, self._fs)
        protocol = self._fs.protocol
        protocol = protocol if isinstance(protocol, str) else protocol[0]
        storage = StorageConfig(
            protocol=protocol,
            options=dict(self._fs.storage_options),
            delta_options=dict(self._storage_options or {}),
        )
        reconstructed = storage.filesystem()
        if (
            type(reconstructed) is not type(self._fs)
            or reconstructed.protocol != self._fs.protocol
            or reconstructed.storage_options != self._fs.storage_options
            or self._fs.storage_args
        ):
            msg = "Interactive commit requires a reconstructable filesystem binding"
            raise ValueError(msg)
        modules = resolve_application_modules()
        assert self._pipeline_run_id is not None
        tracker = RetainedStepTracker(self._committer, self._pipeline_run_id)
        with self._committer._read_snapshot() as read:
            _, attempts = tracker._read_lifecycle(read)
        defaults = _recorded_defaults(
            [
                state
                for state in attempts
                if state.pipeline_run_id == self._pipeline_run_id
            ],
            modules,
        )
        params = InteractiveSelection.Params(
            selected_ids=filtered,
            criteria=self.criteria,
            diagnostics=self._build_diagnostics(filtered),
        )
        dispatch = AssignmentDispatch(
            self._delta_root,
            self._files_root,
            uri_join(self._delta_root, "_staging"),
            storage,
            Path(tempfile.gettempdir()).resolve(),
            application_modules=modules,
        )
        with PipelineManager(
            self._committer,
            dispatch,
            pipeline_run_id=self._pipeline_run_id,
            recorded_defaults=defaults,
            skip_cache=True,
        ) as manager:
            return manager.submit(
                InteractiveSelection,
                params=params,
                inputs={"passthrough": self._selected_inputs()},
                name=step_name,
            ).result()

    def _selected_inputs(self) -> list[AcceptedArtifact]:
        """Read the exact attempts loaded by the UI, preserving repeated producers."""
        from artisan.orchestration.engine.current_outputs import (
            read_current_step_outputs,
        )

        return [
            member
            for attempt in self._source_attempts
            for group in read_current_step_outputs(self._committer, attempt)
            for member in group.occurrences
            if member.artifact.artifact_id in self._primary_artifact_ids
        ]

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
