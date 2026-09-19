"""Curator operation that filters artifacts by structured metric criteria.

Discovers descendant metrics via forward provenance walk, evaluates AND'd
criteria, and returns the IDs of passthrough artifacts that pass all criteria.
"""

from __future__ import annotations

import logging
from collections import Counter
from dataclasses import dataclass
from enum import StrEnum, auto
from typing import TYPE_CHECKING, Any, ClassVar, Literal, cast

import polars as pl
from pydantic import BaseModel, Field

from artisan.operations.base.operation_definition import OperationDefinition
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.schemas.execution.curator_result import PassthroughResult
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec

if TYPE_CHECKING:
    from artisan.storage.core.artifact_store import ArtifactStore

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------


def _criterion_to_expr(c: Criterion) -> pl.Expr:
    """Convert a Criterion to a Polars boolean expression.

    Args:
        c: Criterion with metric (column name), operator, and value.

    Returns:
        Polars expression that evaluates to True when the criterion is met.
    """
    col = pl.col(c.metric)
    match c.operator:
        case "gt":
            return col > c.value
        case "ge":
            return col >= c.value
        case "lt":
            return col < c.value
        case "le":
            return col <= c.value
        case "eq":
            return col == c.value
        case "ne":
            return col != c.value


def _flatten_struct_columns(frame: pl.DataFrame) -> pl.DataFrame:
    """Recursively unnest all Struct columns into dot-separated flat columns.

    Args:
        frame: DataFrame potentially containing Struct-typed columns.

    Returns:
        DataFrame with all Struct columns flattened to dot-separated names.
    """
    result = frame
    changed = True
    while changed:
        changed = False
        for col_name in result.columns:
            col_dtype = result[col_name].dtype
            if isinstance(col_dtype, pl.Struct):
                fields = col_dtype.fields
                prefixed_names = [f"{col_name}.{f.name}" for f in fields]
                result = result.with_columns(
                    pl.col(col_name).struct.rename_fields(prefixed_names)
                ).unnest(col_name)
                changed = True
                break
    return result


class Criterion(BaseModel):
    """Compare a metric field, optionally from a selected pipeline step.

    Attributes:
        metric: Metric field name, using dots for nested fields.
        operator: Comparison applied to each selected value.
        value: Numeric, string, or Boolean comparison value.
        step: Step name; ambiguous names also require ``step_number``.
        step_number: Step number to select. Missing values fail the criterion.
    """

    metric: str
    operator: Literal["gt", "ge", "lt", "le", "eq", "ne"]
    value: float | int | str | bool
    step: str | None = None
    step_number: int | None = None


@dataclass(frozen=True)
class _MetricSelection:
    """Keep a selector's discovered pairs and resolved step together."""

    pairs: pl.DataFrame
    step_number: int | None = None


def _empty_metric_pairs() -> pl.DataFrame:
    """Return an empty metric-discovery result with stable column types."""
    return pl.DataFrame(schema={"passthrough_id": pl.String, "metric_id": pl.String})


def _resolve_metric_step(
    step: str | None, step_number: int | None, step_names: dict[int, str]
) -> int | None:
    """Resolve a step selector; return None when no step matches."""
    if step is None:
        return step_number
    if step_number is not None:
        return step_number if step_names.get(step_number) == step else None
    matching = [number for number, name in step_names.items() if name == step]
    if len(matching) > 1:
        msg = (
            f"Step name '{step}' matches multiple step numbers: "
            f"{sorted(matching)}. Use step_number to disambiguate."
        )
        raise ValueError(msg)
    return matching[0] if matching else None


def _build_metric_namespace(
    passthrough_df: pl.DataFrame,
    metric_pairs: pl.DataFrame,
    artifact_store: ArtifactStore,
    pipeline_run_id: str | None = None,
    metric_steps: pl.DataFrame | None = None,
) -> tuple[pl.DataFrame, dict[str, Any] | None]:
    """Hydrate metrics and build wide DataFrame for evaluation.

    Args:
        passthrough_df: DataFrame with passthrough artifact_id column.
        metric_pairs: DataFrame with [passthrough_id, metric_id].
        artifact_store: Store for metric loading.
        pipeline_run_id: If given, restrict step-name resolution to this
            pipeline run. None uses names from the most recent run.
        metric_steps: Optional current membership with ``metric_id`` and
            ``step_number`` columns. None uses artifact origin steps.

    Returns:
        Tuple of (wide DataFrame with passthrough_id + metric columns,
        step_info mapping field names to their source step numbers,
        or None if no metrics were found). The dict also contains
        a special ``_step_names`` key mapping step numbers to step names.
    """
    base_df = passthrough_df.select(pl.col("artifact_id").alias("passthrough_id"))

    if metric_pairs.is_empty():
        return base_df, None

    unique_metric_ids = metric_pairs["metric_id"].unique().to_list()
    metrics_df = artifact_store.load_metrics_df(unique_metric_ids)

    if metrics_df.is_empty():
        return base_df, None

    # Decode JSON content: Binary -> Utf8 -> struct -> unnest -> flatten
    parsed_series = metrics_df["content"].cast(pl.Utf8).str.json_decode()
    decoded = (
        metrics_df.select("artifact_id")
        .with_columns(parsed_series.alias("_parsed"))
        .unnest("_parsed")
    )
    decoded = _flatten_struct_columns(decoded)

    value_columns = [c for c in decoded.columns if c != "artifact_id"]

    if metric_steps is None:
        origins = artifact_store.provenance.load_step_map(set(unique_metric_ids))
        steps_by_metric = {mid: {number} for mid, number in origins.items()}
    else:
        steps_by_metric = {}
        for mid, number in metric_steps.select("metric_id", "step_number").iter_rows():
            steps_by_metric.setdefault(mid, set()).add(number)
    step_name_map = artifact_store.provenance.load_step_name_map(pipeline_run_id)

    # Keep names available for ambiguity evidence accumulated across chunks.
    step_info: dict[str, Any] = {"_step_names": step_name_map}

    for col in value_columns:
        step_info[col] = set()
        for mid in unique_metric_ids:
            if mid in steps_by_metric:
                row = decoded.filter(pl.col("artifact_id") == mid)
                if not row.is_empty() and col in row.columns:
                    val = row[col][0]
                    if val is not None:
                        step_info[col].update(steps_by_metric[mid])

    # Join metrics to passthrough via metric_pairs
    mapping = metric_pairs.select(
        pl.col("passthrough_id"),
        pl.col("metric_id").alias("artifact_id"),
    )
    values = mapping.join(decoded, on="artifact_id", how="left").drop("artifact_id")

    # Aggregate: take first non-null per passthrough_id
    agg_exprs = [
        pl.col(c).drop_nulls().first().alias(c)
        for c in value_columns
        if c in values.columns
    ]
    if agg_exprs:
        agg = values.group_by("passthrough_id").agg(agg_exprs)
        base_df = base_df.join(agg, on="passthrough_id", how="left")

    return base_df, step_info


def _build_metric_sources(
    metric_ids: set[str],
    artifact_store: ArtifactStore,
    pipeline_run_id: str | None = None,
) -> list[dict[str, Any]]:
    """Summarize distinct metric artifacts by producing step."""
    if not metric_ids:
        return []

    step_number_map = artifact_store.provenance.load_step_map(metric_ids)
    step_name_map = artifact_store.provenance.load_step_name_map(pipeline_run_id)
    counts = Counter(step_number_map.values())
    return [
        {
            "step_number": step_number,
            "step_name": step_name_map.get(step_number, ""),
            "metric_count": counts[step_number],
        }
        for step_number in sorted(counts)
    ]


def _check_collision(field: str, step_info: dict[str, Any]) -> None:
    """Raise ValueError if a field comes from multiple steps.

    Args:
        field: Metric field name to check.
        step_info: Mapping from field names to sets of step numbers.

    Raises:
        ValueError: When the field appears in metrics from multiple steps.
    """
    if field not in step_info or field.startswith("_"):
        return

    step_nums = step_info[field]
    if len(step_nums) <= 1:
        return

    step_names = step_info.get("_step_names", {})
    lines = [f"Field '{field}' found in metrics from multiple steps:"]
    for sn in sorted(step_nums):
        name = step_names.get(sn, "unknown")
        lines.append(f'  - step {sn} ("{name}")')
    lines.append("Add 'step' or 'step_number' to disambiguate:")
    lines.append(f'  {{"metric": "{field}", "step_number": {min(step_nums)}, ...}}')
    raise ValueError("\n".join(lines))


def _build_criterion_frame(
    primary: pl.DataFrame,
    criteria: list[Criterion],
    selections: dict[tuple[str | None, int | None], _MetricSelection],
    artifact_store: ArtifactStore,
    pipeline_run_id: str | None = None,
    observed_steps: dict[str, set[int]] | None = None,
    metric_steps: pl.DataFrame | None = None,
) -> tuple[pl.DataFrame, list[Criterion], list[int | None]]:
    """Bind each criterion to its selected values without exposing private columns.

    Selector namespaces stay separate until each comparison has its own column.
    ``observed_steps`` carries unqualified ambiguity evidence across chunks.
    ``metric_steps`` supplies current membership when artifact origins differ.
    """
    observed = observed_steps if observed_steps is not None else {}
    unique_primary = primary.select("artifact_id").unique(maintain_order=True)
    primary_ids = unique_primary["artifact_id"].to_list()
    frame = primary.select(pl.col("artifact_id").alias("passthrough_id"))
    namespaces: dict[
        tuple[str | None, int | None], tuple[pl.DataFrame, dict[str, Any] | None]
    ] = {}
    bound: list[Criterion] = []
    resolved: list[int | None] = []
    for index, criterion in enumerate(criteria):
        selector = (criterion.step, criterion.step_number)
        selection = selections[selector]
        if selector not in namespaces:
            pairs = selection.pairs.filter(pl.col("passthrough_id").is_in(primary_ids))
            namespaces[selector] = _build_metric_namespace(
                unique_primary, pairs, artifact_store, pipeline_run_id, metric_steps
            )
        wide, step_info = namespaces[selector]
        step_number = selection.step_number
        if selector == (None, None) and step_info is not None:
            steps = observed.setdefault(criterion.metric, set())
            steps.update(step_info.get(criterion.metric, set()))
            _check_collision(
                criterion.metric,
                {criterion.metric: steps, "_step_names": step_info["_step_names"]},
            )
            step_number = next(iter(steps)) if len(steps) == 1 else None
        column = f"__criterion_{index}"
        value = (
            pl.col(criterion.metric)
            if criterion.metric in wide.columns
            else pl.lit(None)
        )
        values = wide.select("passthrough_id", value.alias(column))
        # The right side is unique; repeated primary occurrences keep their order.
        frame = frame.join(
            values, on="passthrough_id", how="left", maintain_order="left"
        )
        bound.append(criterion.model_copy(update={"metric": column}))
        resolved.append(step_number)
    return frame, bound, resolved


def _compute_funnel_counts(
    wide: pl.DataFrame, criteria: list[Criterion], total: int
) -> list[int]:
    """Compute cumulative AND pass counts for the funnel from a wide frame.

    Args:
        wide: Wide DataFrame with one row per artifact and metric columns.
        criteria: Criteria applied cumulatively (AND).
        total: Count for the "All evaluated" stage (index 0).

    Returns:
        Counts where index 0 is ``total`` and index ``i + 1`` is the number of
        rows passing the first ``i + 1`` criteria.
    """
    counts = [total]
    mask = pl.lit(True)
    for crit in criteria:
        mask = mask & _criterion_to_expr(crit).fill_null(False)
        counts.append(wide.filter(mask).height)
    return counts


def _build_funnel(criteria: list[Criterion], counts: list[int]) -> list[dict[str, Any]]:
    """Build cumulative-funnel rows from progressive pass counts.

    Args:
        criteria: One criterion per funnel stage after "All evaluated".
        counts: Cumulative counts; ``counts[0]`` is all evaluated and
            ``counts[i + 1]`` is the count after applying criterion ``i``.

    Returns:
        Funnel rows with label, count, and per-stage eliminated delta. The
        first row (``All evaluated``) has no ``eliminated`` key.
    """
    funnel: list[dict[str, Any]] = [{"label": "All evaluated", "count": counts[0]}]
    for i, crit in enumerate(criteria):
        count = counts[i + 1]
        prev_count = funnel[-1]["count"]
        funnel.append(
            {
                "label": f"+ {crit.metric} {crit.operator} {crit.value}",
                "count": count,
                "eliminated": prev_count - count,
            }
        )
    return funnel


def _criterion_stats(
    wide: pl.DataFrame, crit: Criterion
) -> tuple[int, dict[str, float] | None]:
    """Compute a criterion's pass count and numeric min/max/mean stats.

    Args:
        wide: Wide DataFrame with metric columns.
        crit: Criterion whose metric column to summarize.

    Returns:
        Tuple of (pass count, stats dict with min/max/mean, or None when the
        metric column holds no numeric values).
    """
    pass_count = wide.select(_criterion_to_expr(crit).fill_null(False).sum()).item()
    numeric = _numeric_values(wide[crit.metric])
    if numeric.len() == 0:
        return pass_count, None
    minimum = cast(float, numeric.min())
    maximum = cast(float, numeric.max())
    mean = cast(float, numeric.mean())
    return pass_count, {
        "min": minimum,
        "max": maximum,
        "mean": round(mean, 6),
    }


def _numeric_values(values: pl.Series) -> pl.Series:
    """Select values usable in numeric summaries without changing comparisons."""
    return values.drop_nulls().cast(pl.Float64, strict=False).drop_nulls()


def _assemble_diagnostics(
    *,
    total_input: int,
    total_evaluated: int,
    total_metrics_discovered: int,
    total_passed: int,
    metric_sources: list[dict[str, Any]],
    criteria: list[dict[str, Any]],
    funnel: list[dict[str, Any]],
    interactive: bool = False,
) -> dict[str, Any]:
    """Assemble the v4 diagnostics dict shared by Filter and InteractiveFilter.

    Args:
        total_input: Passthrough artifacts entering the filter.
        total_evaluated: Artifacts actually evaluated.
        total_metrics_discovered: Distinct metrics discovered.
        total_passed: Artifacts passing all criteria.
        metric_sources: Per-step metric-source descriptors.
        criteria: Per-criterion diagnostics rows.
        funnel: Cumulative AND funnel rows.
        interactive: Whether the diagnostics come from InteractiveFilter.

    Returns:
        The v4 diagnostics dict.
    """
    diagnostics: dict[str, Any] = {"version": 4}
    if interactive:
        diagnostics["interactive"] = True
    diagnostics.update(
        {
            "total_input": total_input,
            "total_evaluated": total_evaluated,
            "total_metrics_discovered": total_metrics_discovered,
            "total_passed": total_passed,
            "metric_sources": metric_sources,
            "criteria": criteria,
            "funnel": funnel,
        }
    )
    return diagnostics


class _DiagnosticsAccumulator:
    """Accumulate filter diagnostics across evaluation chunks.

    Maintains running counts, min/max, sum/count for mean, and a progressive
    AND funnel -- all mergeable without holding per-value data in memory.
    """

    def __init__(self, criteria: list[Criterion]) -> None:
        self.total_evaluated: int = 0
        self.total_normally_passed: int = 0

        self._pass_counts: list[int] = [0] * len(criteria)
        self._mins: list[float] = [float("inf")] * len(criteria)
        self._maxs: list[float] = [float("-inf")] * len(criteria)
        self._sums: list[float] = [0.0] * len(criteria)
        self._non_null_counts: list[int] = [0] * len(criteria)

        # Funnel: progressive AND counts (one slot per criterion + 1 for "all")
        self._funnel_counts: list[int] = [0] * (len(criteria) + 1)

    def update(
        self,
        wide_chunk: pl.DataFrame,
        bool_exprs: list[pl.Expr],
        bound_criteria: list[Criterion],
    ) -> None:
        """Ingest one chunk and update running statistics and funnel counts."""
        self.total_evaluated += wide_chunk.height

        passed_count = wide_chunk.filter(pl.all_horizontal(bool_exprs)).height
        self.total_normally_passed += passed_count

        for i, crit in enumerate(bound_criteria):
            col_name = crit.metric
            expr = _criterion_to_expr(crit).fill_null(False)
            self._pass_counts[i] += wide_chunk.select(expr.sum()).item()

            numeric = _numeric_values(wide_chunk[col_name])
            if numeric.len() > 0:
                col_min = cast(float, numeric.min())
                col_max = cast(float, numeric.max())
                self._mins[i] = min(self._mins[i], col_min)
                self._maxs[i] = max(self._maxs[i], col_max)
                self._sums[i] += float(numeric.sum())
                self._non_null_counts[i] += numeric.len()

        counts = _compute_funnel_counts(wide_chunk, bound_criteria, wide_chunk.height)
        for i, count in enumerate(counts):
            self._funnel_counts[i] += count

    def finalize(
        self,
        criteria: list[Criterion],
        resolved_steps: list[int | None],
        metric_sources: list[dict[str, Any]],
        total_input: int,
        total_passed: int,
        total_metrics_discovered: int,
    ) -> dict[str, Any]:
        """Produce the final v4 diagnostics dict from accumulated statistics."""
        criteria_diagnostics = []
        for i, crit in enumerate(criteria):
            stats: dict[str, Any] = {}
            if self._non_null_counts[i] > 0:
                mean_val = self._sums[i] / self._non_null_counts[i]
                stats = {
                    "min": self._mins[i],
                    "max": self._maxs[i],
                    "mean": round(mean_val, 6),
                }

            criteria_diagnostics.append(
                {
                    "metric": crit.metric,
                    "operator": crit.operator,
                    "value": crit.value,
                    "pass_count": self._pass_counts[i],
                    "resolved_from_step": resolved_steps[i],
                    "stats": stats,
                }
            )

        return _assemble_diagnostics(
            total_input=total_input,
            total_evaluated=self.total_evaluated,
            total_metrics_discovered=total_metrics_discovered,
            total_passed=total_passed,
            metric_sources=metric_sources,
            criteria=criteria_diagnostics,
            funnel=_build_funnel(criteria, self._funnel_counts),
        )


class Filter(OperationDefinition):
    """Filter artifacts by evaluating structured criteria against matched metrics.

    Discovers descendant metrics via forward provenance walk from passthrough
    artifacts. Criteria are always metric field names — step name/number on
    Criterion disambiguates when field names collide across metric sources.
    """

    # ---------- Metadata ----------
    name = "filter"
    description = (
        "Filter artifacts by evaluating structured criteria against matched metrics"
    )

    # ---------- Inputs ----------
    class InputRole(StrEnum):
        passthrough = auto()

    inputs: ClassVar[dict[str, InputSpec]] = {
        InputRole.passthrough: InputSpec(
            artifact_type=ArtifactTypes.ANY,
            required=True,
            description="Artifacts to filter (any type)",
        ),
    }

    # ---------- Outputs ----------
    class OutputRole(StrEnum):
        passthrough = auto()

    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.passthrough: OutputSpec(
            artifact_type=ArtifactTypes.ANY,
            required=False,
            description="Artifacts that passed all filter criteria",
        )
    }

    # ---------- Behavior ----------
    hydrate_inputs: ClassVar[bool] = False

    # ---------- Parameters ----------
    class Params(BaseModel):
        """Filter operation parameters.

        Attributes:
            criteria (list[Criterion]): AND'd filter criteria to evaluate.
            passthrough_failures (bool): If True, pass all artifacts
                through regardless of criteria (diagnostics still computed).
            chunk_size (int): Number of passthrough artifacts per
                hydration/evaluation chunk.
        """

        criteria: list[Criterion] = []
        passthrough_failures: bool = False
        chunk_size: int = Field(default=100_000, gt=0)

    params: Params = Params()

    # ---------- Lifecycle ----------
    def execute_curator(
        self,
        inputs: dict[str, pl.DataFrame],
        step_number: int,
        artifact_store: ArtifactStore,
    ) -> PassthroughResult:
        """Evaluate filter criteria and return passing artifact IDs.

        Args:
            inputs: Role names to DataFrames with ``artifact_id``.
            step_number: Current filter step number; upper bound for the
                forward provenance walk that discovers descendant metrics.
            artifact_store: Store for lineage matching and metric hydration.

        Returns:
            PassthroughResult with artifact IDs that passed all criteria.

        Raises:
            ValueError: If ``passthrough`` role is missing, or a criterion's
                metric field collides across multiple steps without
                disambiguation.
        """
        if "passthrough" not in inputs:
            msg = f"Filter requires a 'passthrough' input role. Got: {list(inputs.keys())}"
            raise ValueError(msg)

        passthrough_df = inputs["passthrough"]

        if passthrough_df.is_empty():
            return PassthroughResult(success=True, passthrough={"passthrough": []})

        # No criteria -> all passthrough artifacts pass
        if not self.params.criteria:
            passthrough_ids = passthrough_df["artifact_id"].to_list()
            diag = _assemble_diagnostics(
                total_input=len(passthrough_ids),
                total_evaluated=len(passthrough_ids),
                total_metrics_discovered=0,
                total_passed=len(passthrough_ids),
                metric_sources=[],
                criteria=[],
                funnel=_build_funnel([], [len(passthrough_ids)]),
            )
            if self.params.passthrough_failures:
                diag["passthrough_failures"] = True
            return PassthroughResult(
                success=True,
                passthrough={"passthrough": passthrough_ids},
                metadata={"diagnostics": diag},
            )

        selections = self._select_metrics(passthrough_df, artifact_store, step_number)
        metric_ids = {
            metric_id
            for selection in selections.values()
            for metric_id in selection.pairs["metric_id"].to_list()
        }
        metric_sources = _build_metric_sources(metric_ids, artifact_store)
        accumulator = _DiagnosticsAccumulator(self.params.criteria)
        all_passed_ids: list[str] = []
        resolved_steps: list[int | None] = [None] * len(self.params.criteria)
        observed_steps: dict[str, set[int]] = {}

        for chunk_start in range(0, passthrough_df.height, self.params.chunk_size):
            chunk = passthrough_df.slice(chunk_start, self.params.chunk_size)
            wide, bound, chunk_steps = _build_criterion_frame(
                chunk,
                self.params.criteria,
                selections,
                artifact_store,
                observed_steps=observed_steps,
            )
            resolved_steps = [
                current if current is not None else previous
                for previous, current in zip(resolved_steps, chunk_steps, strict=True)
            ]
            bool_exprs = [_criterion_to_expr(c).fill_null(False) for c in bound]
            passed = wide.filter(pl.all_horizontal(bool_exprs))
            all_passed_ids.extend(
                chunk["artifact_id"].to_list()
                if self.params.passthrough_failures
                else passed["passthrough_id"].to_list()
            )
            accumulator.update(wide, bool_exprs, bound)

        diagnostics = accumulator.finalize(
            criteria=self.params.criteria,
            resolved_steps=resolved_steps,
            metric_sources=metric_sources,
            total_input=passthrough_df.height,
            total_passed=(
                len(all_passed_ids)
                if not self.params.passthrough_failures
                else accumulator.total_normally_passed
            ),
            total_metrics_discovered=len(metric_ids),
        )

        if self.params.passthrough_failures:
            diagnostics["passthrough_failures"] = True
            n_failed = accumulator.total_evaluated - accumulator.total_normally_passed
            n_non_evaluable = passthrough_df.height - accumulator.total_evaluated
            if n_failed or n_non_evaluable:
                logger.warning(
                    "Filter (passthrough_failures mode): %d/%d failed criteria, "
                    "%d non-evaluable — all %d artifacts passed through",
                    n_failed,
                    accumulator.total_evaluated,
                    n_non_evaluable,
                    passthrough_df.height,
                )

        return PassthroughResult(
            success=True,
            passthrough={"passthrough": all_passed_ids},
            metadata={"diagnostics": diagnostics},
        )

    def _select_metrics(
        self,
        passthrough: pl.DataFrame,
        artifact_store: ArtifactStore,
        step_number: int,
    ) -> dict[tuple[str | None, int | None], _MetricSelection]:
        """Discover each distinct selector without merging its metric sources."""
        selections = {}
        step_names = artifact_store.provenance.load_step_name_map()
        for criterion in self.params.criteria:
            selector = (criterion.step, criterion.step_number)
            if selector in selections:
                continue
            if selector == (None, None):
                pairs = self._discover_descendant_metrics(
                    passthrough, artifact_store, step_number
                )
                resolved_step = None
            else:
                resolved_step = _resolve_metric_step(*selector, step_names)
                pairs = (
                    self._discover_step_metrics(
                        passthrough, artifact_store, resolved_step
                    )
                    if resolved_step is not None
                    else _empty_metric_pairs()
                )
            selections[selector] = _MetricSelection(pairs, resolved_step)
        return selections

    def _discover_descendant_metrics(
        self,
        passthrough_df: pl.DataFrame,
        artifact_store: ArtifactStore,
        step_number: int,
    ) -> pl.DataFrame:
        """Forward walk from passthrough to find descendant metrics.

        Args:
            passthrough_df: DataFrame with passthrough artifact_id column.
            artifact_store: Store for edge/step loading.
            step_number: Current filter step number (upper bound for edge
                loading — descendant metrics are at higher steps than the
                passthrough artifacts).

        Returns:
            DataFrame with columns [passthrough_id, metric_id].
        """
        empty = _empty_metric_pairs()

        from artisan.provenance.traversal import walk_forward

        step_range = artifact_store.provenance.get_step_range(
            passthrough_df["artifact_id"].to_list()
        )
        if step_range is None:
            return empty

        step_min, _ = step_range
        edges = artifact_store.provenance.load_edges_df(
            step_min, step_number, include_target_type=True
        )

        if edges.is_empty():
            return empty

        walk_result = walk_forward(
            sources=passthrough_df,
            edges=edges,
            target_type="metric",
        )

        if walk_result.is_empty():
            return empty

        return walk_result.select(
            pl.col("source_id").alias("passthrough_id"),
            pl.col("target_id").alias("metric_id"),
        )

    def _discover_step_metrics(
        self,
        passthrough_df: pl.DataFrame,
        artifact_store: ArtifactStore,
        step_number: int,
    ) -> pl.DataFrame:
        """Walk backward from the selected step's metrics to passthrough artifacts."""
        from artisan.provenance.traversal import walk_backward

        empty = _empty_metric_pairs()
        # Load metric IDs from the target step
        metric_ids = artifact_store.provenance.load_artifact_ids_by_type(
            "metric", step_numbers=[step_number]
        )

        if not metric_ids:
            return empty

        metrics_df = pl.DataFrame({"artifact_id": sorted(metric_ids)})

        # Get step range covering both passthrough and metrics
        all_ids = (
            passthrough_df["artifact_id"].to_list()
            + metrics_df["artifact_id"].to_list()
        )
        step_range = artifact_store.provenance.get_step_range(all_ids)
        if step_range is None:
            return empty

        step_min, step_max = step_range
        edges = artifact_store.provenance.load_edges_df(step_min, step_max)

        walk_result = walk_backward(
            candidates=metrics_df,
            targets=passthrough_df,
            edges=edges,
        )

        if walk_result.is_empty():
            return empty

        return walk_result.select(
            pl.col("target_id").alias("passthrough_id"),
            pl.col("candidate_id").alias("metric_id"),
        )
