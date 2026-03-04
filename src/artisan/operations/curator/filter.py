"""Curator operation that filters artifacts by structured metric criteria.

Accepts a ``passthrough`` stream plus named metric streams, pairs them via
provenance walk, and evaluates AND'd criteria. Returns the IDs of artifacts
that pass all criteria.
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
                unnested = result.unnest(col_name)
                renames = {
                    f.name: f"{col_name}.{f.name}"
                    for f in fields
                    if f.name in unnested.columns
                }
                result = unnested.rename(renames)
                changed = True
                break
    return result


class Criterion(BaseModel):
    """A single filter criterion."""

    metric: str
    operator: Literal["gt", "ge", "lt", "le", "eq", "ne"]
    value: float | int | str | bool


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
        display_criteria: list[Criterion],
        all_criteria: list[Criterion],
        resolution_list: list[str],
        total_input: int,
        total_passed: int,
        total_explicit_matched: int,
        total_implicit_resolved: int,
    ) -> dict:
        """Produce the final v3 diagnostics dict from accumulated statistics."""
        criteria_diagnostics = []
        for i, (display_crit, lookup_crit) in enumerate(
            zip(display_criteria, all_criteria, strict=True)
        ):
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
                    "metric": display_crit.metric,
                    "operator": display_crit.operator,
                    "value": display_crit.value,
                    "pass_count": self._pass_counts[i],
                    "resolution": resolution_list[i],
                    "stats": stats,
                }
            )

        # Funnel from accumulated counts
        funnel: list[dict[str, Any]] = [
            {"label": "All evaluated", "count": self._funnel_counts[0]}
        ]
        for i, display_crit in enumerate(display_criteria):
            count = self._funnel_counts[i + 1]
            prev_count = funnel[-1]["count"]
            funnel.append(
                {
                    "label": f"+ {display_crit.metric} {display_crit.operator} {display_crit.value}",
                    "count": count,
                    "eliminated": prev_count - count,
                }
            )

        return {
            "version": 3,
            "total_input": total_input,
            "total_evaluated": self.total_evaluated,
            "total_explicit_matched": total_explicit_matched,
            "total_implicit_resolved": total_implicit_resolved,
            "total_passed": total_passed,
            "criteria": criteria_diagnostics,
            "funnel": funnel,
        }


class Filter(OperationDefinition):
    """Filter artifacts by evaluating structured criteria against matched metrics.

    Accepts a ``passthrough`` stream plus any number of named metric streams.
    Each passthrough artifact is paired with its metrics via provenance walk,
    then evaluated against AND'd criteria. Only artifacts passing every
    criterion appear in the output.
    """

    # ---------- Metadata ----------
    name = "filter"
    description = (
        "Filter artifacts by evaluating structured criteria against matched metrics"
    )

    # ---------- Inputs ----------
    class InputRole(StrEnum):
        passthrough = auto()  # additional metric roles are user-defined

    inputs: ClassVar[dict[str, InputSpec]] = {
        InputRole.passthrough: InputSpec(
            artifact_type=ArtifactTypes.ANY,
            required=True,
            description="Artifacts to filter (any type)",
            with_associated=(ArtifactTypes.METRIC,),
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
    runtime_defined_inputs: ClassVar[bool] = True
    hydrate_inputs: ClassVar[bool] = False
    independent_input_streams: ClassVar[bool] = True

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
        step_number: int,  # noqa: ARG002
        artifact_store: ArtifactStore,
    ) -> PassthroughResult:
        """Evaluate filter criteria and return passing artifact IDs.

        Phase 1 matches metrics to passthrough artifacts via provenance walk.
        Phase 2 hydrates and evaluates criteria in bounded chunks.

        Args:
            inputs: Role names to DataFrames with ``artifact_id``.
            step_number: Current pipeline step number (unused).
            artifact_store: Store for lineage matching and metric hydration.

        Returns:
            PassthroughResult with artifact IDs that passed all criteria.

        Raises:
            ValueError: If ``passthrough`` role is missing, no metric roles
                exist and no implicit criteria are set, or criteria reference
                unknown roles.
        """
        if "passthrough" not in inputs:
            msg = f"Filter requires a 'passthrough' input role. Got: {list(inputs.keys())}"
            raise ValueError(msg)

        passthrough_df = inputs["passthrough"]
        metric_role_dfs = {r: df for r, df in inputs.items() if r != "passthrough"}

        implicit_criteria, explicit_criteria = self._partition_criteria()

        if not metric_role_dfs and not implicit_criteria:
            msg = "Filter requires at least one metric input role or implicit criteria"
            raise ValueError(msg)

        if passthrough_df.is_empty():
            return PassthroughResult(success=True, passthrough={"passthrough": []})

        # No criteria -> all passthrough artifacts pass
        if not self.params.criteria:
            passthrough_ids = passthrough_df["artifact_id"].to_list()
            diag: dict = {
                "version": 3,
                "total_input": len(passthrough_ids),
                "total_evaluated": len(passthrough_ids),
                "total_explicit_matched": 0,
                "total_implicit_resolved": 0,
                "total_passed": len(passthrough_ids),
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

        # Guard: "_implicit" is reserved as a synthetic role key
        if "_implicit" in metric_role_dfs:
            msg = (
                "Metric role name '_implicit' is reserved for implicit metric "
                "resolution. Please rename the input role."
            )
            raise ValueError(msg)

        # Validate explicit criteria against provided metric roles
        if explicit_criteria:
            self._validate_criteria(metric_role_dfs, explicit_criteria)

        # ── Phase 1: Full matching (DataFrame-native, memory efficient) ──

        matched_df = self._match_explicit(
            passthrough_df, metric_role_dfs, artifact_store
        )
        total_explicit_matched = (
            matched_df["passthrough_id"].n_unique() if not matched_df.is_empty() else 0
        )

        implicit_df = (
            artifact_store.get_descendant_ids_df(
                passthrough_df["artifact_id"],
                target_artifact_type=ArtifactTypes.METRIC,
            )
            if implicit_criteria
            else pl.DataFrame(
                schema={
                    "source_artifact_id": pl.String,
                    "target_artifact_id": pl.String,
                }
            )
        )
        total_implicit_resolved = (
            implicit_df["source_artifact_id"].n_unique()
            if not implicit_df.is_empty()
            else 0
        )

        # Build criteria lists
        all_criteria, display_criteria, resolution_list = self._build_criteria_lists(
            explicit_criteria, implicit_criteria
        )
        exprs = [_criterion_to_expr(c) for c in all_criteria]
        bool_exprs = [e.fill_null(False) for e in exprs]

        # ── Phase 2: Chunked hydration + evaluation ──

        accumulator = _DiagnosticsAccumulator(all_criteria)
        all_passed_ids: list[str] = []

        for chunk_start in range(0, passthrough_df.height, self.params.chunk_size):
            chunk_pt = passthrough_df.slice(chunk_start, self.params.chunk_size)
            chunk_ids = chunk_pt["artifact_id"]

            # Filter pre-computed matches to this chunk
            chunk_matched = (
                matched_df.filter(pl.col("passthrough_id").is_in(chunk_ids))
                if not matched_df.is_empty()
                else matched_df
            )
            chunk_implicit = (
                implicit_df.filter(pl.col("source_artifact_id").is_in(chunk_ids))
                if not implicit_df.is_empty()
                else implicit_df
            )

            # Hydrate chunk -> wide DataFrame
            wide_chunk = self._hydrate_as_wide_df(
                chunk_pt, chunk_matched, chunk_implicit, artifact_store
            )

            # Add null columns for missing criteria references
            for c in all_criteria:
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

            # Accumulate diagnostics from this chunk
            accumulator.update(wide_chunk, bool_exprs)

        # ── Build final result ──

        diagnostics = accumulator.finalize(
            display_criteria=display_criteria,
            all_criteria=all_criteria,
            resolution_list=resolution_list,
            total_input=passthrough_df.height,
            total_passed=(
                len(all_passed_ids)
                if not self.params.passthrough_failures
                else accumulator.total_normally_passed
            ),
            total_explicit_matched=total_explicit_matched,
            total_implicit_resolved=total_implicit_resolved,
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

    def _partition_criteria(self) -> tuple[list[Criterion], list[Criterion]]:
        """Split criteria into implicit (bare field) and explicit (role.field).

        Returns:
            Tuple of (implicit_criteria, explicit_criteria).
        """
        implicit, explicit = [], []
        for c in self.params.criteria:
            if "." in c.metric:
                explicit.append(c)
            else:
                implicit.append(c)
        return implicit, explicit

    def _build_criteria_lists(
        self,
        explicit_criteria: list[Criterion],
        implicit_criteria: list[Criterion],
    ) -> tuple[list[Criterion], list[Criterion], list[str]]:
        """Build unified criteria with column name mapping.

        Args:
            explicit_criteria: Criteria with role.field format.
            implicit_criteria: Criteria with bare field names.

        Returns:
            Tuple of (all_criteria, display_criteria, resolution_list).
        """
        all_criteria: list[Criterion] = []
        display_criteria: list[Criterion] = []
        resolution_list: list[str] = []

        for c in explicit_criteria:
            all_criteria.append(c)
            display_criteria.append(c)
            resolution_list.append("explicit")
        for c in implicit_criteria:
            all_criteria.append(
                Criterion(
                    metric=f"_implicit.{c.metric}",
                    operator=c.operator,
                    value=c.value,
                )
            )
            display_criteria.append(c)
            resolution_list.append("implicit")

        return all_criteria, display_criteria, resolution_list

    def _match_explicit(
        self,
        passthrough_df: pl.DataFrame,
        metric_role_dfs: dict[str, pl.DataFrame],
        artifact_store: ArtifactStore,
    ) -> pl.DataFrame:
        """Match metric roles to passthrough via provenance walk.

        Args:
            passthrough_df: DataFrame with passthrough artifact_id column.
            metric_role_dfs: Dict of role name to metric DataFrames.
            artifact_store: Store for provenance edge loading.

        Returns:
            DataFrame with columns [passthrough_id, role, metric_id].
        """
        empty = pl.DataFrame(
            schema={
                "passthrough_id": pl.String,
                "role": pl.String,
                "metric_id": pl.String,
            }
        )

        if not metric_role_dfs:
            return empty

        from artisan.execution.inputs.lineage_matching import (
            walk_provenance_to_targets,
        )

        # Determine step range for scoped edge loading
        all_ids = passthrough_df["artifact_id"]
        for df in metric_role_dfs.values():
            all_ids = pl.concat([all_ids, df["artifact_id"]])

        step_range = artifact_store.get_step_range(all_ids)
        if step_range is None:
            return empty

        step_min, step_max = step_range
        edges = artifact_store.load_provenance_edges_df(step_min, step_max)

        role_dfs: list[pl.DataFrame] = []
        for role, metric_df in metric_role_dfs.items():
            walk_result = walk_provenance_to_targets(
                candidates=metric_df,
                targets=passthrough_df,
                edges=edges,
            )
            if not walk_result.is_empty():
                role_df = walk_result.select(
                    pl.col("target_id").alias("passthrough_id"),
                    pl.lit(role).alias("role"),
                    pl.col("candidate_id").alias("metric_id"),
                )
                role_dfs.append(role_df)

        if not role_dfs:
            return empty

        return pl.concat(role_dfs)

    def _hydrate_as_wide_df(
        self,
        passthrough_df: pl.DataFrame,
        matched_df: pl.DataFrame,
        implicit_df: pl.DataFrame,
        artifact_store: ArtifactStore,
    ) -> pl.DataFrame:
        """Build a wide DataFrame with metric values joined per role.

        Loads metric content via ``artifact_store.load_metrics_df``, decodes
        JSON in Polars, flattens nested structs, and joins per role with
        column prefixes.

        Args:
            passthrough_df: DataFrame with passthrough artifact_id column.
            matched_df: DataFrame with [passthrough_id, role, metric_id].
            implicit_df: DataFrame with [source_artifact_id, target_artifact_id].
            artifact_store: Store for metric loading.

        Returns:
            Wide DataFrame with ``passthrough_id`` and prefixed metric columns.

        Raises:
            ValueError: On implicit field name collisions.
        """
        base_df = passthrough_df.select(pl.col("artifact_id").alias("passthrough_id"))

        # Collect all metric IDs across both paths
        all_metric_ids: list[pl.Series] = []
        if not matched_df.is_empty():
            all_metric_ids.append(matched_df["metric_id"])
        if not implicit_df.is_empty():
            all_metric_ids.append(implicit_df["target_artifact_id"])

        if not all_metric_ids:
            return base_df

        combined_ids = pl.concat(all_metric_ids).unique().to_list()
        if not combined_ids:
            return base_df

        # Bulk-load all metrics at once
        metrics_df = artifact_store.load_metrics_df(combined_ids)
        if metrics_df.is_empty():
            return base_df

        # Decode JSON content: Binary -> Utf8 -> struct -> unnest -> flatten
        parsed_series = metrics_df["content"].cast(pl.Utf8).str.json_decode()
        decoded = (
            metrics_df.select("artifact_id")
            .with_columns(parsed_series.alias("_parsed"))
            .unnest("_parsed")
        )
        decoded = _flatten_struct_columns(decoded)

        value_columns = [c for c in decoded.columns if c != "artifact_id"]

        # --- Explicit roles ---
        if not matched_df.is_empty():
            roles = matched_df["role"].unique().to_list()
            for role in roles:
                role_mapping = matched_df.filter(pl.col("role") == role).select(
                    pl.col("passthrough_id"),
                    pl.col("metric_id").alias("artifact_id"),
                )
                role_values = role_mapping.join(decoded, on="artifact_id", how="left")
                role_values = role_values.drop("artifact_id")
                rename_map = {
                    c: f"{role}.{c}" for c in value_columns if c in role_values.columns
                }
                role_values = role_values.rename(rename_map)
                base_df = base_df.join(role_values, on="passthrough_id", how="left")

        # --- Implicit path ---
        if not implicit_df.is_empty():
            impl_mapping = implicit_df.select(
                pl.col("source_artifact_id").alias("passthrough_id"),
                pl.col("target_artifact_id").alias("artifact_id"),
            )
            impl_values = impl_mapping.join(decoded, on="artifact_id", how="left")
            impl_values = impl_values.drop("artifact_id")

            # Detect field collisions
            dup_check = impl_mapping.join(decoded, on="artifact_id", how="left")
            for col in value_columns:
                if col not in dup_check.columns:
                    continue
                non_null = dup_check.filter(pl.col(col).is_not_null())
                dupes = (
                    non_null.group_by("passthrough_id")
                    .agg(pl.col(col).count().alias("_cnt"))
                    .filter(pl.col("_cnt") > 1)
                )
                if dupes.height > 0:
                    first_pt = dupes["passthrough_id"][0]
                    msg = (
                        f"Implicit metric field collision for passthrough "
                        f"artifact '{first_pt}': field '{col}' "
                        f"found in multiple associated metrics. Use explicit "
                        f"metric roles to disambiguate: "
                        f'{{"metric": "role.{col}", ...}}'
                    )
                    raise ValueError(msg)

            # Aggregate: take first non-null per passthrough_id
            agg_exprs = [
                pl.col(c).drop_nulls().first().alias(c)
                for c in value_columns
                if c in impl_values.columns
            ]
            if agg_exprs:
                impl_agg = impl_values.group_by("passthrough_id").agg(agg_exprs)
                rename_map = {
                    c: f"_implicit.{c}"
                    for c in impl_agg.columns
                    if c != "passthrough_id"
                }
                impl_agg = impl_agg.rename(rename_map)
                base_df = base_df.join(impl_agg, on="passthrough_id", how="left")

        return base_df

    def _validate_criteria(
        self,
        metric_roles: dict[str, pl.DataFrame | list],
        criteria: list[Criterion] | None = None,
    ) -> None:
        """Validate that all criteria reference known input roles.

        Args:
            metric_roles: Dict of metric role names to their DataFrames or lists.
            criteria: Criteria to validate. Defaults to self.params.criteria.

        Raises:
            ValueError: If a criterion has invalid metric format or unknown role.
        """
        criteria = criteria if criteria is not None else self.params.criteria
        known_roles = set(metric_roles.keys())
        for criterion in criteria:
            role, _, field = criterion.metric.partition(".")
            if not field:
                msg = f"Criterion metric must be 'role.field' format, got: '{criterion.metric}'"
                raise ValueError(msg)
            if role not in known_roles:
                msg = (
                    f"Criterion references unknown role '{role}'. "
                    f"Known metric roles: {sorted(known_roles)}"
                )
                raise ValueError(msg)
