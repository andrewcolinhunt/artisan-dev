"""PipelineTimings — DataFrame-first API for pipeline timing analysis.

Usage:
    # From delta lake (post-hoc analysis)
    timings = PipelineTimings.from_delta(delta_root)
    timings.step_timings()
    timings.execution_timings(step_number=0)
    timings.execution_stats(step_number=0)
    fig = timings.plot_steps()
    fig = timings.plot_execution_stats()

    # From raw data dict (e.g., in tests)
    timings = PipelineTimings(data)
"""

from __future__ import annotations

import json
import os
from typing import Any

import polars as pl

from artisan.schemas.enums import TablePath
from artisan.utils.path import uri_join


class PipelineTimings:
    """DataFrame-first API for pipeline timing analysis.

    Attributes:
        data (dict[str, Any]): Raw timing data dict with "pipeline_run_id"
            and "steps" keys.

    Raises:
        ValueError: If data is empty or missing required keys.
    """

    def __init__(self, data: dict[str, Any]) -> None:
        """Initialize with a timing data dict.

        Args:
            data: Must contain a "steps" key with step timing records.
        """
        if not data:
            msg = "Timing data cannot be empty"
            raise ValueError(msg)
        if "steps" not in data:
            msg = "Timing data must contain 'steps' key"
            raise ValueError(msg)
        self._data = data

    @property
    def data(self) -> dict[str, Any]:
        """Raw timing data dict."""
        return self._data

    @classmethod
    def from_delta(
        cls,
        delta_root: str,
        pipeline_run_id: str | None = None,
        storage_options: dict[str, str] | None = None,
    ) -> PipelineTimings:
        """Load timing data from steps and executions delta tables.

        Args:
            delta_root: Path to the delta lake root directory.
            pipeline_run_id: Pipeline run ID to filter by. If None, uses the
                latest pipeline run.
            storage_options: Delta-rs storage options for cloud backends.

        Returns:
            PipelineTimings instance.

        Raises:
            FileNotFoundError: If steps table doesn't exist.
            ValueError: If no completed steps found.
        """
        steps_path = uri_join(delta_root, TablePath.STEPS)
        if not os.path.exists(steps_path):
            msg = f"steps table not found at {steps_path}"
            raise FileNotFoundError(msg)

        # Read completed steps
        scanner = pl.scan_delta(steps_path, storage_options=storage_options).filter(
            pl.col("status") == "completed"
        )
        if pipeline_run_id is not None:
            scanner = scanner.filter(pl.col("pipeline_run_id") == pipeline_run_id)

        steps_df = (
            scanner.sort("step_number")
            .select(
                "pipeline_run_id",
                "step_number",
                "step_name",
                "duration_seconds",
                "metadata",
            )
            .collect()
        )

        if steps_df.is_empty():
            msg = "No completed steps found"
            raise ValueError(msg)

        # Resolve pipeline_run_id from first row if not provided
        run_id = pipeline_run_id or steps_df["pipeline_run_id"][0]

        # Filter to only this pipeline run
        if pipeline_run_id is None:
            steps_df = steps_df.filter(pl.col("pipeline_run_id") == run_id)

        # Read executions if available
        exec_path = uri_join(delta_root, TablePath.EXECUTIONS)
        exec_df = None
        if os.path.exists(exec_path):
            exec_scanner = pl.scan_delta(
                exec_path, storage_options=storage_options
            ).filter(
                pl.col("success") == True  # noqa: E712
            )
            exec_df = (
                exec_scanner.sort("origin_step_number")
                .select(
                    "execution_run_id",
                    "origin_step_number",
                    "operation_name",
                    "metadata",
                )
                .collect()
            )

        # Build structured data
        steps = []
        for row in steps_df.iter_rows(named=True):
            step_timings = _parse_timings(row["metadata"])
            step_num = row["step_number"]

            # Gather executions for this step
            executions = []
            if exec_df is not None:
                step_execs = exec_df.filter(pl.col("origin_step_number") == step_num)
                for exec_row in step_execs.iter_rows(named=True):
                    exec_timings = _parse_timings(exec_row["metadata"])
                    executions.append(
                        {
                            "execution_run_id": exec_row["execution_run_id"],
                            "operation_name": exec_row["operation_name"],
                            "timings": exec_timings or {},
                        }
                    )

            steps.append(
                {
                    "step_number": step_num,
                    "step_name": row["step_name"],
                    "duration_seconds": row["duration_seconds"],
                    "timings": step_timings or {},
                    "executions": executions,
                }
            )

        return cls({"pipeline_run_id": run_id, "steps": steps})

    # ------------------------------------------------------------------
    # DataFrames
    # ------------------------------------------------------------------

    def step_timings(self) -> pl.DataFrame:
        """Step-level timings as a DataFrame.

        Returns:
            DataFrame with one row per step: step_number, step_name,
            duration_seconds, plus one column per timing phase.
        """
        rows = []
        for step in self._data["steps"]:
            row: dict[str, Any] = {
                "step_number": step["step_number"],
                "step_name": step["step_name"],
                "duration_seconds": step["duration_seconds"],
            }
            for phase, value in step["timings"].items():
                if isinstance(value, float):
                    row[phase] = value
            rows.append(row)
        return pl.DataFrame(rows)

    def execution_timings(self, step_number: int) -> pl.DataFrame:
        """Execution-level timings for a single step.

        Args:
            step_number: The step to get execution timings for.

        Returns:
            DataFrame with one row per execution: step_number, execution_run_id,
            operation_name, plus one column per timing phase.

        Raises:
            ValueError: If step_number not found.
        """
        step = self._find_step(step_number)
        rows = []
        for ex in step["executions"]:
            row: dict[str, Any] = {
                "step_number": step["step_number"],
                "execution_run_id": ex["execution_run_id"],
                "operation_name": ex["operation_name"],
            }
            for phase, value in ex["timings"].items():
                if isinstance(value, float):
                    row[phase] = value
            rows.append(row)
        return pl.DataFrame(rows)

    def execution_stats(self, step_number: int) -> pl.DataFrame:
        """Summary statistics for execution-level phase timings of a step.

        Args:
            step_number: The step to compute stats for.

        Returns:
            DataFrame with columns: phase, mean, std, min, max.

        Raises:
            ValueError: If step_number not found or has no executions.
        """
        exec_df = self.execution_timings(step_number)
        if exec_df.is_empty():
            msg = f"Step {step_number} has no executions"
            raise ValueError(msg)

        phase_cols = [
            c
            for c in exec_df.columns
            if c not in ("step_number", "execution_run_id", "operation_name")
            and exec_df[c].dtype in (pl.Float64, pl.Float32)
        ]

        rows = []
        for phase in phase_cols:
            col = exec_df[phase]
            rows.append(
                {
                    "phase": phase,
                    "mean": col.mean(),
                    "std": col.std() or 0.0,
                    "min": col.min(),
                    "max": col.max(),
                }
            )
        return pl.DataFrame(rows)

    # ------------------------------------------------------------------
    # Plots
    # ------------------------------------------------------------------

    def plot_steps(
        self,
        step_numbers: list[int] | None = None,
        **kwargs: Any,
    ) -> Any:
        """Plot stacked horizontal bar chart of step-level phase timings.

        Requires matplotlib. One bar per step, segments colored by phase.

        Args:
            step_numbers: Steps to include. If None, includes all steps.
            **kwargs: Forwarded to ``plt.subplots`` (e.g. ``figsize``).

        Returns:
            matplotlib Figure.
        """
        import matplotlib.pyplot as plt

        steps = self._data["steps"]
        if step_numbers is not None:
            steps = [s for s in steps if s["step_number"] in step_numbers]
        if not steps:
            fig, ax = plt.subplots(1, 1, **kwargs)
            ax.text(0.5, 0.5, "No steps", ha="center", va="center")
            plt.close(fig)
            return fig

        # Collect all phase names (excluding "total") in order
        all_phases = _collect_phase_names(step["timings"] for step in steps)

        labels = [f"{s['step_number']}: {s['step_name']}" for s in steps]
        phase_data: dict[str, list[float]] = {p: [] for p in all_phases}
        for step in steps:
            for p in all_phases:
                phase_data[p].append(step["timings"].get(p, 0.0))

        fig, ax = plt.subplots(
            figsize=kwargs.pop("figsize", (10, max(2, len(steps) * 0.8))),
            **kwargs,
        )

        y_pos = range(len(labels))
        lefts = [0.0] * len(labels)
        colors = _get_phase_colors(all_phases)

        for phase in all_phases:
            values = phase_data[phase]
            ax.barh(y_pos, values, left=lefts, label=phase, color=colors[phase])
            lefts = [left + v for left, v in zip(lefts, values, strict=False)]

        ax.set_yticks(y_pos)
        ax.set_yticklabels(labels)
        ax.set_xlabel("Time (seconds)")
        ax.set_title("Step Phase Timings")
        ax.legend(loc="lower right", fontsize="small")
        ax.invert_yaxis()
        fig.tight_layout()
        plt.close(fig)
        return fig

    def plot_execution_stats(self, **kwargs: Any) -> Any:
        """Plot stacked horizontal bar chart of mean execution phase timings.

        One bar per step, segments colored by mean execution phase time.
        Requires matplotlib.

        Args:
            **kwargs: Forwarded to ``plt.subplots`` (e.g. ``figsize``).

        Returns:
            matplotlib Figure.
        """
        import matplotlib.pyplot as plt

        steps = self._data["steps"]
        # Only include steps that have executions
        steps_with_execs = [s for s in steps if s["executions"]]

        if not steps_with_execs:
            fig, ax = plt.subplots(1, 1, **kwargs)
            ax.text(0.5, 0.5, "No executions", ha="center", va="center")
            plt.close(fig)
            return fig

        # Collect mean timings per step
        all_phases = _collect_phase_names(
            ex["timings"] for s in steps_with_execs for ex in s["executions"]
        )

        labels = [f"{s['step_number']}: {s['step_name']}" for s in steps_with_execs]
        phase_data: dict[str, list[float]] = {p: [] for p in all_phases}

        for step in steps_with_execs:
            stats_df = self.execution_stats(step["step_number"])
            stats_map = dict(zip(stats_df["phase"], stats_df["mean"], strict=False))
            for p in all_phases:
                phase_data[p].append(stats_map.get(p, 0.0))

        n_bars = len(labels)
        fig, ax = plt.subplots(
            figsize=kwargs.pop("figsize", (10, max(2, n_bars * 0.8))),
            **kwargs,
        )

        y_pos = range(n_bars)
        lefts = [0.0] * n_bars
        colors = _get_phase_colors(all_phases)

        for phase in all_phases:
            values = phase_data[phase]
            ax.barh(y_pos, values, left=lefts, label=phase, color=colors[phase])
            lefts = [left + v for left, v in zip(lefts, values, strict=False)]

        ax.set_yticks(y_pos)
        ax.set_yticklabels(labels)
        ax.set_xlabel("Time (seconds)")
        ax.set_title("Mean Execution Phase Timings")
        ax.legend(loc="lower right", fontsize="small")
        ax.invert_yaxis()
        fig.tight_layout()
        plt.close(fig)
        return fig

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _find_step(self, step_number: int) -> dict[str, Any]:
        """Return the step dict matching step_number, or raise."""
        for step in self._data["steps"]:
            if step["step_number"] == step_number:
                return step
        msg = f"Step {step_number} not found in timing data"
        raise ValueError(msg)


# ======================================================================
# Module-level helpers
# ======================================================================


def _parse_timings(metadata_json: str | None) -> dict[str, Any] | None:
    """Parse timings from a metadata JSON string."""
    if not metadata_json:
        return None
    try:
        data = json.loads(metadata_json)
        return data.get("timings")
    except (json.JSONDecodeError, TypeError):
        return None


def _collect_phase_names(timings_iter: Any) -> list[str]:
    """Collect unique phase names (excluding 'total') preserving insertion order."""
    seen: dict[str, None] = {}
    for timings in timings_iter:
        for key in timings:
            if key != "total" and isinstance(timings[key], float) and key not in seen:
                seen[key] = None
    return list(seen.keys())


def _get_phase_colors(phases: list[str]) -> dict[str, str]:
    """Assign colors to phases from a predefined palette."""
    palette = [
        "#4e79a7",  # blue
        "#f28e2b",  # orange
        "#e15759",  # red
        "#76b7b2",  # teal
        "#59a14f",  # green
        "#edc948",  # yellow
        "#b07aa1",  # purple
        "#ff9da7",  # pink
        "#9c755f",  # brown
        "#bab0ac",  # gray
    ]
    return {phase: palette[i % len(palette)] for i, phase in enumerate(phases)}
