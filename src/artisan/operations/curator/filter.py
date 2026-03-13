"""Curator operation that filters artifacts by structured metric criteria.

Discovers descendant metrics via forward provenance walk, evaluates AND'd
criteria, and returns the IDs of passthrough artifacts that pass all criteria.
"""

from __future__ import annotations

import logging
from enum import StrEnum, auto
from typing import TYPE_CHECKING, Any, ClassVar, Literal

import polars as pl
from pydantic import BaseModel

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
    """A single filter criterion."""

    metric: str
    operator: Literal["gt", "ge", "lt", "le", "eq", "ne"]
    value: float | int | str | bool
    step: str | None = None
    step_number: int | None = None


def _build_metric_namespace(
    passthrough_df: pl.DataFrame,
    metric_pairs: pl.DataFrame,
    artifact_store: ArtifactStore,
) -> tuple[pl.DataFrame, dict[str, set[int]] | None]:
    """Hydrate metrics and build wide DataFrame for evaluation.

    Args:
        passthrough_df: DataFrame with passthrough artifact_id column.
        metric_pairs: DataFrame with [passthrough_id, metric_id].
        artifact_store: Store for metric loading.

    Returns:
        Tuple of (wide DataFrame with passthrough_id + metric columns,
        step_info mapping field names to sets of step numbers that
        produce them — None if no metrics found).
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

    # Enrich with step info
    step_number_map = artifact_store.provenance.load_step_map(set(unique_metric_ids))
    step_name_map = artifact_store.provenance.load_step_name_map()

    # Build step_info: {field_name: {step_numbers}}
    step_info: dict[str, Any] = {"_step_names": {}}
    for mid, sn in step_number_map.items():
        step_info["_step_names"][sn] = step_name_map.get(sn, "")

    for col in value_columns:
        step_info[col] = set()
        for mid in unique_metric_ids:
            if mid in step_number_map:
                row = decoded.filter(pl.col("artifact_id") == mid)
                if not row.is_empty() and col in row.columns:
                    val = row[col][0]
                    if val is not None:
                        step_info[col].add(step_number_map[mid])

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


class _DiagnosticsAccumulator:
    """Accumulate filter diagnostics across evaluation chunks.

    Maintains running counts, min/max, sum/count for mean, and a progressive
    AND funnel -- all mergeable without holding per-value data in memory.
    """

    def __init__(self, criteria: list[Criterion]) -> None:
        self.criteria = criteria
        self.total_evaluated: int = 0
        self.total_normally_passed: int = 0

        self._pass_counts: list[int] = [0] * len(criteria)
        self._mins: list[float] = [float("inf")] * len(criteria)
        self._maxs: list[float] = [float("-inf")] * len(criteria)
        self._sums: list[float] = [0.0] * len(criteria)
        self._non_null_counts: list[int] = [0] * len(criteria)

        # Funnel: progressive AND counts (one slot per criterion + 1 for "all")
        self._funnel_counts: list[int] = [0] * (len(criteria) + 1)

    def update(self, wide_chunk: pl.DataFrame, bool_exprs: list[pl.Expr]) -> None:
        """Ingest one chunk and update running statistics and funnel counts."""
        self.total_evaluated += wide_chunk.height

        passed_count = wide_chunk.filter(pl.all_horizontal(bool_exprs)).height
        self.total_normally_passed += passed_count

        for i, crit in enumerate(self.criteria):
            col_name = crit.metric
            if col_name not in wide_chunk.columns:
                continue

            expr = _criterion_to_expr(crit).fill_null(False)
            self._pass_counts[i] += wide_chunk.select(expr.sum()).item()

            col = wide_chunk[col_name]
            non_null = col.drop_nulls()
            if non_null.len() > 0:
                self._mins[i] = min(self._mins[i], non_null.min())
                self._maxs[i] = max(self._maxs[i], non_null.max())
                self._sums[i] += non_null.sum()
                self._non_null_counts[i] += non_null.len()

        # Funnel: progressive AND
        self._funnel_counts[0] += wide_chunk.height
        mask = pl.lit(True)
        for i, crit in enumerate(self.criteria):
            expr = _criterion_to_expr(crit).fill_null(False)
            mask = mask & expr
            self._funnel_counts[i + 1] += wide_chunk.filter(mask).height

    def finalize(
        self,
        criteria: list[Criterion],
        resolved_steps: list[int | None],
        metric_sources: list[dict[str, Any]],
        total_input: int,
        total_passed: int,
        total_metrics_discovered: int,
    ) -> dict:
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

        # Funnel from accumulated counts
        funnel: list[dict[str, Any]] = [
            {"label": "All evaluated", "count": self._funnel_counts[0]}
        ]
        for i, crit in enumerate(criteria):
            count = self._funnel_counts[i + 1]
            prev_count = funnel[-1]["count"]
            funnel.append(
                {
                    "label": f"+ {crit.metric} {crit.operator} {crit.value}",
                    "count": count,
                    "eliminated": prev_count - count,
                }
            )

        return {
            "version": 4,
            "total_input": total_input,
            "total_evaluated": self.total_evaluated,
            "total_metrics_discovered": total_metrics_discovered,
            "total_passed": total_passed,
            "metric_sources": metric_sources,
            "criteria": criteria_diagnostics,
            "funnel": funnel,
        }


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
        chunk_size: int = 100_000

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
            step_number: Current pipeline step number (unused).
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
            diag: dict = {
                "version": 4,
                "total_input": len(passthrough_ids),
                "total_evaluated": len(passthrough_ids),
                "total_metrics_discovered": 0,
                "total_passed": len(passthrough_ids),
                "metric_sources": [],
                "criteria": [],
                "funnel": [{"label": "All evaluated", "count": len(passthrough_ids)}],
            }
            if self.params.passthrough_failures:
                diag["passthrough_failures"] = True
            return PassthroughResult(
                success=True,
                passthrough={"passthrough": passthrough_ids},
                metadata={"diagnostics": diag},
            )

        # ── Phase 1: Discover metrics ──

        default_criteria = [
            c for c in self.params.criteria if c.step is None and c.step_number is None
        ]
        step_targeted_criteria = [
            c
            for c in self.params.criteria
            if c.step is not None or c.step_number is not None
        ]

        # Forward walk for default criteria
        metric_pairs = pl.DataFrame(
            schema={"passthrough_id": pl.String, "metric_id": pl.String}
        )
        if default_criteria:
            metric_pairs = self._discover_descendant_metrics(
                passthrough_df, artifact_store, step_number
            )

        # Backward walk for step-targeted criteria
        step_metric_pairs = pl.DataFrame(
            schema={"passthrough_id": pl.String, "metric_id": pl.String}
        )
        if step_targeted_criteria:
            unique_targets = {(c.step, c.step_number) for c in step_targeted_criteria}
            parts: list[pl.DataFrame] = []
            for step_name, step_num in unique_targets:
                part = self._discover_step_metrics(
                    passthrough_df, artifact_store, step_name, step_num
                )
                if not part.is_empty():
                    parts.append(part)
            if parts:
                step_metric_pairs = pl.concat(parts).unique()

        # Combine all metric pairs
        all_pairs_parts: list[pl.DataFrame] = []
        if not metric_pairs.is_empty():
            all_pairs_parts.append(metric_pairs)
        if not step_metric_pairs.is_empty():
            all_pairs_parts.append(step_metric_pairs)

        all_metric_pairs = (
            pl.concat(all_pairs_parts).unique()
            if all_pairs_parts
            else pl.DataFrame(
                schema={"passthrough_id": pl.String, "metric_id": pl.String}
            )
        )

        total_metrics_discovered = (
            all_metric_pairs["metric_id"].n_unique()
            if not all_metric_pairs.is_empty()
            else 0
        )

        # ── Phase 2: Chunked hydration + evaluation ──

        bool_exprs = [
            _criterion_to_expr(c).fill_null(False) for c in self.params.criteria
        ]
        accumulator = _DiagnosticsAccumulator(self.params.criteria)
        all_passed_ids: list[str] = []
        resolved_steps: list[int | None] = [None] * len(self.params.criteria)
        metric_sources_map: dict[int, dict[str, Any]] = {}

        for chunk_start in range(0, passthrough_df.height, self.params.chunk_size):
            chunk_pt = passthrough_df.slice(chunk_start, self.params.chunk_size)
            chunk_ids = chunk_pt["artifact_id"]

            # Filter metric pairs to this chunk
            chunk_pairs = (
                all_metric_pairs.filter(pl.col("passthrough_id").is_in(chunk_ids))
                if not all_metric_pairs.is_empty()
                else all_metric_pairs
            )

            # Build metric namespace
            wide_chunk, step_info = self._build_metric_namespace(
                chunk_pt, chunk_pairs, artifact_store
            )

            # Collision detection for default criteria
            if default_criteria and step_info is not None:
                for i, crit in enumerate(self.params.criteria):
                    if crit.step is not None or crit.step_number is not None:
                        continue
                    self._check_collision(crit.metric, step_info)
                    # Resolve step for diagnostics
                    if crit.metric in step_info:
                        steps_for_field = step_info[crit.metric]
                        if len(steps_for_field) == 1:
                            resolved_steps[i] = next(iter(steps_for_field))

            # Resolve steps for step-targeted criteria
            for i, crit in enumerate(self.params.criteria):
                if crit.step is None and crit.step_number is None:
                    continue
                resolved_steps[i] = crit.step_number

            # Collect metric source info from step_info
            if step_info is not None:
                for field, step_nums in step_info.items():
                    for sn in step_nums:
                        if sn not in metric_sources_map:
                            metric_sources_map[sn] = {
                                "step_number": sn,
                                "step_name": step_info.get("_step_names", {}).get(
                                    sn, ""
                                ),
                                "metric_count": 0,
                            }

            # Add null columns for missing criteria references
            for c in self.params.criteria:
                if c.metric not in wide_chunk.columns:
                    wide_chunk = wide_chunk.with_columns(pl.lit(None).alias(c.metric))

            # Evaluate criteria
            passed_chunk = wide_chunk.filter(pl.all_horizontal(bool_exprs))

            # Accumulate results
            if self.params.passthrough_failures:
                all_passed_ids.extend(chunk_ids.to_list())
            else:
                passed_ordered = chunk_pt.join(
                    passed_chunk.select(pl.col("passthrough_id").alias("artifact_id")),
                    on="artifact_id",
                    how="semi",
                )
                all_passed_ids.extend(passed_ordered["artifact_id"].to_list())

            accumulator.update(wide_chunk, bool_exprs)

        # ── Build final result ──

        metric_sources = sorted(
            metric_sources_map.values(), key=lambda x: x["step_number"]
        )

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
            total_metrics_discovered=total_metrics_discovered,
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
        empty = pl.DataFrame(
            schema={"passthrough_id": pl.String, "metric_id": pl.String}
        )

        from artisan.provenance.traversal import walk_forward

        step_range = artifact_store.provenance.get_step_range(
            passthrough_df["artifact_id"]
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
        step: str | None,
        step_number: int | None,
    ) -> pl.DataFrame:
        """Backward walk to find metrics from a specific step.

        Args:
            passthrough_df: DataFrame with passthrough artifact_id column.
            artifact_store: Store for edge/step loading.
            step: Step name for targeting.
            step_number: Step number for targeting.

        Returns:
            DataFrame with columns [passthrough_id, metric_id].
        """
        empty = pl.DataFrame(
            schema={"passthrough_id": pl.String, "metric_id": pl.String}
        )

        from artisan.provenance.traversal import walk_backward

        # Resolve step identity
        step_name_map = artifact_store.provenance.load_step_name_map()
        target_step_number = step_number

        if step is not None and step_number is None:
            matching = [sn for sn, name in step_name_map.items() if name == step]
            if not matching:
                return empty
            if len(matching) > 1:
                msg = (
                    f"Step name '{step}' matches multiple step numbers: "
                    f"{sorted(matching)}. Use step_number to disambiguate."
                )
                raise ValueError(msg)
            target_step_number = matching[0]
        elif step is not None and step_number is not None:
            actual_name = step_name_map.get(step_number)
            if actual_name != step:
                return empty

        if target_step_number is None:
            return empty

        # Load metric IDs from the target step
        metric_ids = artifact_store.provenance.load_artifact_ids_by_type(
            "metric", step_numbers=[target_step_number]
        )

        if not metric_ids:
            return empty

        metrics_df = pl.DataFrame({"artifact_id": sorted(metric_ids)})

        # Get step range covering both passthrough and metrics
        all_ids = pl.concat([passthrough_df["artifact_id"], metrics_df["artifact_id"]])
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

    def _build_metric_namespace(
        self,
        passthrough_df: pl.DataFrame,
        metric_pairs: pl.DataFrame,
        artifact_store: ArtifactStore,
    ) -> tuple[pl.DataFrame, dict[str, set[int]] | None]:
        """Hydrate metrics and build wide DataFrame for evaluation.

        Args:
            passthrough_df: DataFrame with passthrough artifact_id column.
            metric_pairs: DataFrame with [passthrough_id, metric_id].
            artifact_store: Store for metric loading.

        Returns:
            Tuple of (wide DataFrame with passthrough_id + metric columns,
            step_info mapping field names to sets of step numbers that
            produce them — None if no metrics found).
        """
        return _build_metric_namespace(passthrough_df, metric_pairs, artifact_store)

    @staticmethod
    def _check_collision(field: str, step_info: dict[str, Any]) -> None:
        """Raise ValueError if a field comes from multiple steps.

        Args:
            field: Metric field name to check.
            step_info: Mapping from field names to sets of step numbers.

        Raises:
            ValueError: When the field appears in metrics from multiple steps.
        """
        _check_collision(field, step_info)
