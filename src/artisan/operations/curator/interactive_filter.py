"""Interactive notebook filter: explore metrics, set thresholds, commit.

Provides a load-explore-filter-visualize-commit workflow. Unlike Filter
(which requires upfront criteria), InteractiveFilter lets users inspect
metric distributions before committing to thresholds.
"""

from __future__ import annotations

import json
import operator as _op
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import polars as pl

from artisan.operations.curator.filter import Criterion
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.schemas.enums import TablePath
from artisan.schemas.orchestration.step_result import StepResult
from artisan.schemas.orchestration.step_start_record import StepStartRecord
from artisan.storage.core.artifact_store import ArtifactStore
from artisan.storage.core.table_schemas import (
    EXECUTION_EDGES_SCHEMA,
    EXECUTIONS_SCHEMA,
)
from artisan.storage.provenance_utils import trace_derived_artifacts
from artisan.utils.dataframes import pivot_metrics_wide, to_float
from artisan.utils.dicts import flatten_dict
from artisan.utils.hashing import compute_artifact_id

# Python operator mapping for per-value comparisons
_OPERATORS: dict[str, Callable] = {
    "gt": _op.gt,
    "ge": _op.ge,
    "lt": _op.lt,
    "le": _op.le,
    "eq": _op.eq,
    "ne": _op.ne,
}

# Polars operator mapping for filter expressions
_PL_OPERATORS: dict[str, str] = {
    "gt": "gt",
    "ge": "ge",
    "lt": "lt",
    "le": "le",
    "eq": "eq",
    "ne": "ne",
}


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
    """

    def __init__(self, delta_root: str | Path) -> None:
        self._delta_root = Path(delta_root)
        self._store = ArtifactStore(self._delta_root)
        self._wide_df: pl.DataFrame | None = None
        self._tidy_df: pl.DataFrame | None = None
        self._criteria: list[Criterion] = []
        self._pipeline_run_id: str | None = None
        self._primary_artifact_ids: set[str] = set()

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

        Traces the provenance graph forward from primary artifacts to find
        all derived metrics, then builds tidy and wide DataFrames.

        Args:
            step_numbers: Only load primary artifacts from these steps.
                None means all matching artifacts.
            artifact_type: Only load primary artifacts of this type
                (e.g. "data"). None means all non-metric artifacts.
            pipeline_run_id: Pipeline run ID for step name resolution.
                None auto-detects from the steps table.

        Raises:
            ValueError: If no artifacts found or no metrics found.
        """
        index_path = self._delta_root / TablePath.ARTIFACT_INDEX
        if not index_path.exists():
            msg = f"Artifact index not found at {index_path}"
            raise ValueError(msg)

        # Load primary artifact IDs
        all_index = (
            pl.scan_delta(str(index_path))
            .select(["artifact_id", "artifact_type", "origin_step_number"])
            .collect()
        )

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

        # Load all metric IDs
        metric_ids = self._store.load_artifact_ids_by_type("metric")
        if not metric_ids:
            msg = "No metric artifacts found in the store"
            raise ValueError(msg)

        # Trace forward provenance to find metrics derived from primaries.
        # A metric may be reachable from multiple primaries (e.g. a step-0
        # artifact reaches metrics via step-5 outputs). Assign each metric
        # only to its nearest primary (highest step number) to avoid duplicates.
        forward_map = self._store.load_forward_provenance_map()
        primary_step_map = dict(
            zip(
                primary_df["artifact_id"].to_list(),
                primary_df["origin_step_number"].to_list(),
                strict=True,
            )
        )

        # First pass: collect all claims {metric_id: [(primary_id, step)]}
        metric_claimants: dict[str, list[tuple[str, int]]] = {}
        for pid in primary_ids:
            derived = trace_derived_artifacts(pid, forward_map, metric_ids)
            step = primary_step_map[pid]
            for mid in derived:
                metric_claimants.setdefault(mid, []).append((pid, step))

        # Second pass: assign each metric to the nearest primary only
        primary_to_metrics: dict[str, list[str]] = {}
        all_found_metric_ids: set[str] = set()

        for mid, claimants in metric_claimants.items():
            # Pick the primary with the highest step number (nearest ancestor)
            winner_pid, _ = max(claimants, key=lambda x: x[1])
            primary_to_metrics.setdefault(winner_pid, []).append(mid)
            all_found_metric_ids.add(mid)

        if not all_found_metric_ids:
            msg = "No metrics found derived from the primary artifacts"
            raise ValueError(msg)

        # Bulk-load metric artifacts
        metric_artifacts = self._store.get_artifacts_by_type(
            list(all_found_metric_ids), "metric"
        )

        # Resolve step info
        metric_step_map = self._store.load_step_number_map(all_found_metric_ids)
        step_name_map = self._store.load_step_name_map(pipeline_run_id)

        # Detect pipeline_run_id
        if pipeline_run_id:
            self._pipeline_run_id = pipeline_run_id
        else:
            self._pipeline_run_id = self._detect_pipeline_run_id()

        # Build tidy DataFrame
        rows: list[dict[str, Any]] = []
        for pid in primary_ids:
            for mid in primary_to_metrics.get(pid, []):
                metric = metric_artifacts.get(mid)
                if metric is None:
                    continue
                try:
                    values = metric.values
                except (ValueError, json.JSONDecodeError):
                    continue

                step_num = metric_step_map.get(mid)
                step_name = (
                    step_name_map.get(step_num, "unknown") if step_num else "unknown"
                )

                for metric_name, raw_value in flatten_dict(values).items():
                    rows.append(
                        {
                            "artifact_id": pid,
                            "step_number": step_num,
                            "step_name": step_name,
                            "metric_name": metric_name,
                            "metric_value": to_float(raw_value),
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
            "metric_value": pl.Float64,
        }
        self._tidy_df = pl.DataFrame(rows, schema=tidy_schema)

        # Build wide DataFrame with qualified column names
        self._wide_df = pivot_metrics_wide(self._tidy_df)

    def _detect_pipeline_run_id(self) -> str | None:
        """Try to detect the pipeline_run_id from the steps table."""
        steps_path = self._delta_root / TablePath.STEPS
        if not steps_path.exists():
            return None
        result = (
            pl.scan_delta(str(steps_path))
            .sort("timestamp", descending=True)
            .limit(1)
            .select("pipeline_run_id")
            .collect()
        )
        if result.is_empty():
            return None
        return result.item(0, 0)

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
            criteria: List of criterion dicts with keys: metric, operator, value.

        Raises:
            ValueError: If data not loaded or a metric name doesn't exist
                as a column in wide_df.
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

        self._criteria = parsed

    # ------------------------------------------------------------------
    # Filtered results
    # ------------------------------------------------------------------

    @property
    def filtered_ids(self) -> list[str]:
        """Artifact IDs that pass all criteria.

        Raises:
            ValueError: If data not loaded or criteria not set.
        """
        return self.filtered_wide_df["artifact_id"].to_list()

    @property
    def filtered_wide_df(self) -> pl.DataFrame:
        """Wide DataFrame filtered to rows passing all criteria.

        Raises:
            ValueError: If data not loaded or criteria not set.
        """
        if self._wide_df is None:
            msg = "No data loaded. Call load() first."
            raise ValueError(msg)
        if not self._criteria:
            msg = "No criteria set. Call set_criteria() first."
            raise ValueError(msg)

        mask = pl.lit(True)
        for crit in self._criteria:
            col = pl.col(crit.metric)
            op_name = _PL_OPERATORS[crit.operator]
            mask = mask & getattr(col, op_name)(crit.value)

        return self._wide_df.filter(mask)

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
        if self._wide_df is None:
            msg = "No data loaded. Call load() first."
            raise ValueError(msg)
        if not self._criteria:
            msg = "No criteria set. Call set_criteria() first."
            raise ValueError(msg)

        wide = self._wide_df
        total = wide.height

        # Per-criterion stats
        crit_rows: list[dict[str, Any]] = []
        for crit in self._criteria:
            col_data = wide[crit.metric].drop_nulls()
            numeric = col_data.cast(pl.Float64, strict=False).drop_nulls()

            op_func = _OPERATORS[crit.operator]
            pass_count = sum(
                1
                for v in col_data.to_list()
                if v is not None and op_func(v, crit.value)
            )

            row: dict[str, Any] = {
                "metric": crit.metric,
                "operator": crit.operator,
                "threshold": crit.value,
                "pass": pass_count,
                "total": total,
                "rate": round(pass_count / total * 100, 1) if total else 0.0,
            }

            if numeric.len() > 0:
                row["min"] = numeric.min()
                row["median"] = numeric.median()
                row["max"] = numeric.max()
            else:
                row["min"] = None
                row["median"] = None
                row["max"] = None

            crit_rows.append(row)

        criteria_df = pl.DataFrame(crit_rows)

        # Cumulative funnel
        funnel_rows: list[dict[str, Any]] = [
            {"step": "All", "remaining": total, "eliminated": 0}
        ]
        surviving = wide
        for crit in self._criteria:
            col = pl.col(crit.metric)
            op_name = _PL_OPERATORS[crit.operator]
            prev_count = surviving.height
            surviving = surviving.filter(getattr(col, op_name)(crit.value))
            funnel_rows.append(
                {
                    "step": f"+ {crit.metric} {crit.operator} {crit.value}",
                    "remaining": surviving.height,
                    "eliminated": prev_count - surviving.height,
                }
            )

        funnel_df = pl.DataFrame(funnel_rows)

        passed = surviving.height
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
        if self._wide_df is None:
            msg = "No data loaded. Call load() first."
            raise ValueError(msg)
        if not self._criteria:
            msg = "No criteria set. Call set_criteria() first."
            raise ValueError(msg)

        import matplotlib.pyplot as plt

        n = len(self._criteria)
        fig, axes = plt.subplots(1, n, figsize=(5 * n, 4), squeeze=False, **kwargs)

        for i, crit in enumerate(self._criteria):
            ax = axes[0][i]
            values = [
                v
                for v in self._wide_df[crit.metric].to_list()
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

        # Determine step_number
        step_number = self._next_step_number()

        # Pipeline run ID
        pipeline_run_id = self._pipeline_run_id or str(uuid.uuid4())

        # Deterministic IDs
        criteria_json = json.dumps(
            [c.model_dump() for c in self._criteria], sort_keys=True
        )
        sorted_input_ids = ",".join(sorted(self._primary_artifact_ids))

        step_spec_id = compute_artifact_id(
            f"{step_name}|{criteria_json}|{sorted_input_ids}".encode()
        )
        step_run_id = compute_artifact_id(f"{step_spec_id}|{timestamp_str}".encode())
        execution_spec_id = compute_artifact_id(
            f"filter|{sorted_input_ids}|{criteria_json}".encode()
        )
        execution_run_id = compute_artifact_id(
            f"{execution_spec_id}|{timestamp_str}".encode()
        )

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

        tracker = StepTracker(self._delta_root, pipeline_run_id)
        tracker.record_step_start(start_record)

        # Build diagnostics summary
        summary = self.summary()
        diagnostics = {
            "version": 2,
            "interactive": True,
            "total_input": len(self._primary_artifact_ids),
            "total_passed": len(filtered),
            "criteria": summary.criteria.to_dicts(),
            "funnel": summary.funnel.to_dicts(),
        }

        # Write execution record
        exec_row = pl.DataFrame(
            [
                {
                    "execution_run_id": execution_run_id,
                    "execution_spec_id": execution_spec_id,
                    "origin_step_number": step_number,
                    "operation_name": "filter",
                    "params": json.dumps(
                        {"criteria": [c.model_dump() for c in self._criteria]}
                    ),
                    "user_overrides": "{}",
                    "timestamp_start": now,
                    "timestamp_end": now,
                    "source_worker": 0,
                    "compute_backend": "local",
                    "success": True,
                    "error": None,
                    "metadata": json.dumps({"diagnostics": diagnostics}),
                }
            ],
            schema=EXECUTIONS_SCHEMA,
        )

        from artisan.storage.io.commit import DeltaCommitter

        committer = DeltaCommitter(self._delta_root, self._delta_root)
        committer.commit_dataframe(exec_row, TablePath.EXECUTIONS, deduplicate=False)

        # Write execution edges
        edge_rows: list[dict[str, Any]] = []
        for aid in self._primary_artifact_ids:
            edge_rows.append(
                {
                    "execution_run_id": execution_run_id,
                    "direction": "input",
                    "role": "passthrough",
                    "artifact_id": aid,
                }
            )
        for aid in filtered:
            edge_rows.append(
                {
                    "execution_run_id": execution_run_id,
                    "direction": "output",
                    "role": "passthrough",
                    "artifact_id": aid,
                }
            )
        edges_df = pl.DataFrame(edge_rows, schema=EXECUTION_EDGES_SCHEMA)
        committer.commit_dataframe(
            edges_df, TablePath.EXECUTION_EDGES, deduplicate=False
        )

        # Build and record step result
        result = StepResult(
            step_name=step_name,
            step_number=step_number,
            success=True,
            total_count=len(self._primary_artifact_ids),
            succeeded_count=len(filtered),
            failed_count=0,
            output_roles=frozenset(["passthrough"]),
            output_types={"passthrough": ArtifactTypes.ANY},
            metadata={"diagnostics": diagnostics},
        )
        tracker.record_step_completed(start_record, result)

        return result

    def _next_step_number(self) -> int:
        """Determine the next step number from the steps table."""
        steps_path = self._delta_root / TablePath.STEPS
        if not steps_path.exists():
            return 0

        result = (
            pl.scan_delta(str(steps_path))
            .select(pl.col("step_number").max().alias("max_step"))
            .collect()
        )
        max_val = result.item(0, 0)
        return (max_val + 1) if max_val is not None else 0
