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
from typing import Any

import polars as pl
from fsspec import AbstractFileSystem

from artisan.errors import StoreIntegrityError
from artisan.schemas.enums import TablePath
from artisan.schemas.execution.command_record import CommandRecording
from artisan.storage.core.store_format import assert_store_format
from artisan.utils.path import uri_join


class PipelineTimings:
    """DataFrame-first API for pipeline timing analysis.

    Attributes:
        data (dict[str, Any]): Raw timing data dict. Requires a "steps" key;
            "pipeline_run_id" is optional.

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
        fs: AbstractFileSystem | None = None,
    ) -> PipelineTimings:
        """Load timing data from steps and executions delta tables.

        Args:
            delta_root: Path to the delta lake root directory.
            pipeline_run_id: Pipeline run ID to filter by. If None, uses the
                latest pipeline run.
            storage_options: Delta-rs storage options for cloud backends.
            fs: Filesystem for existence checks. Local if None.

        Returns:
            PipelineTimings instance.

        Raises:
            FileNotFoundError: If steps table doesn't exist.
            ValueError: If no usable terminal steps are found.
        """
        if fs is None:
            from fsspec.implementations.local import LocalFileSystem

            fs = LocalFileSystem()
        assert_store_format(delta_root, fs, storage_options)
        steps_path = uri_join(delta_root, TablePath.STEPS)
        if not fs.exists(steps_path):
            msg = f"steps table not found at {steps_path}"
            raise FileNotFoundError(msg)

        from artisan.orchestration.engine.step_tracker import StepTracker
        from artisan.schemas.orchestration.step_lifecycle import StepStatus

        states = StepTracker(
            delta_root,
            storage_options=storage_options,
            fs=fs,
        ).load_current_states(pipeline_run_id)
        states = [
            state
            for state in states
            if state.status in {StepStatus.SUCCEEDED, StepStatus.PARTIAL}
        ]
        if not states:
            msg = "No usable terminal steps found"
            raise ValueError(msg)
        run_id = states[0].pipeline_run_id

        from artisan.storage.core.run_scope import load_execution_membership

        exec_df = load_execution_membership(
            delta_root,
            fs=fs,
            storage_options=storage_options,
            pipeline_run_id=run_id,
        ).filter(pl.col("success") & ~pl.col("cache_hit"))

        # Build structured data
        steps = []
        for state in states:
            step_timings = state.metadata.get("timings", {})
            step_num = state.step_number

            # Gather executions for this step
            executions = []
            step_execs = exec_df.filter(
                pl.col("current_step_run_id") == state.step_run_id
            )
            for exec_row in step_execs.iter_rows(named=True):
                exec_timings = _parse_timings(exec_row["metadata"])
                executions.append(
                    {
                        "execution_run_id": exec_row["execution_run_id"],
                        "operation_name": exec_row["operation_name"],
                        "timings": exec_timings or {},
                        "command_recording": _validate_command_recording(
                            exec_row.get("command_recording"),
                            exec_row["execution_run_id"],
                        ).model_dump(mode="json"),
                    }
                )

            steps.append(
                {
                    "step_number": step_num,
                    "step_name": state.step_name,
                    "duration_seconds": state.duration_seconds,
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

    def command_timings(self, step_number: int) -> pl.DataFrame:
        """Return observed Popen durations and explicit missing-evidence markers.

        Launch time is already included in execution phases; do not add it to
        stacked phase totals. Like execution_timings, stored timing selection
        includes only fresh successful executions from usable terminal steps.
        Failed attempts remain available through inspect_commands.

        Args:
            step_number: Step whose selected executions should be flattened.

        Returns:
            Typed frame ordered by invocation and command sequence within each
            execution, preserving availability reasons and both omission counts.

        Raises:
            ValueError: The step does not exist.
            StoreIntegrityError: Canonical command evidence is missing or invalid.
        """
        step = self._find_step(step_number)
        rows: list[dict[str, Any]] = []
        for execution in step["executions"]:
            recording = _validate_command_recording(
                execution.get("command_recording"), execution["execution_run_id"]
            )
            common = {
                "step_number": step_number,
                "execution_run_id": execution["execution_run_id"],
                "operation_name": execution["operation_name"],
                "recording_status": recording.status,
                "omitted_commands": recording.omitted_commands,
                "omitted_missing_invocations": recording.omitted_missing_invocations,
                "unavailable_reason": recording.unavailable_reason,
            }
            rows.extend(common | entry for entry in _command_timing_entries(recording))
        return pl.DataFrame(rows, schema=_COMMAND_TIMINGS_SCHEMA)

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
        steps = self._data["steps"]
        if step_numbers is not None:
            steps = [s for s in steps if s["step_number"] in step_numbers]

        # Collect all phase names (excluding "total") in order
        all_phases = _collect_phase_names(step["timings"] for step in steps)

        labels = [f"{s['step_number']}: {s['step_name']}" for s in steps]
        phase_data: dict[str, list[float]] = {p: [] for p in all_phases}
        for step in steps:
            for p in all_phases:
                phase_data[p].append(step["timings"].get(p, 0.0))

        return self._plot_stacked_phases(
            labels,
            phase_data,
            all_phases,
            "Step Phase Timings",
            "No steps",
            **kwargs,
        )

    def plot_execution_stats(self, **kwargs: Any) -> Any:
        """Plot stacked horizontal bar chart of mean execution phase timings.

        One bar per step, segments colored by mean execution phase time.
        Requires matplotlib.

        Args:
            **kwargs: Forwarded to ``plt.subplots`` (e.g. ``figsize``).

        Returns:
            matplotlib Figure.
        """
        # Only include steps that have executions
        steps_with_execs = [s for s in self._data["steps"] if s["executions"]]

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

        return self._plot_stacked_phases(
            labels,
            phase_data,
            all_phases,
            "Mean Execution Phase Timings",
            "No executions",
            **kwargs,
        )

    def _plot_stacked_phases(
        self,
        labels: list[str],
        phase_data: dict[str, list[float]],
        all_phases: list[str],
        title: str,
        empty_message: str,
        **kwargs: Any,
    ) -> Any:
        """Render a stacked horizontal bar chart of phase timings.

        Args:
            labels: One y-axis label per bar. Empty renders the placeholder.
            phase_data: Phase name -> per-bar values (aligned with ``labels``).
            all_phases: Phase names in stacking/legend order.
            title: Axis title.
            empty_message: Text drawn when ``labels`` is empty.
            **kwargs: Forwarded to ``plt.subplots`` (e.g. ``figsize``).

        Returns:
            matplotlib Figure.
        """
        import matplotlib.pyplot as plt

        if not labels:
            fig, ax = plt.subplots(1, 1, **kwargs)
            ax.text(0.5, 0.5, empty_message, ha="center", va="center")
            plt.close(fig)
            return fig

        fig, ax = plt.subplots(
            figsize=kwargs.pop("figsize", (10, max(2, len(labels) * 0.8))),
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
        ax.set_title(title)
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
                result: dict[str, Any] = step
                return result
        msg = f"Step {step_number} not found in timing data"
        raise ValueError(msg)


# ======================================================================
# Module-level helpers
# ======================================================================


_COMMAND_TIMINGS_SCHEMA = {
    "step_number": pl.Int32,
    "execution_run_id": pl.String,
    "operation_name": pl.String,
    "recording_status": pl.String,
    "omitted_commands": pl.Int64,
    "omitted_missing_invocations": pl.Int64,
    "unavailable_reason": pl.String,
    "entry_type": pl.String,
    "invocation": pl.Int64,
    "sequence": pl.Int64,
    "missing_reason": pl.String,
    "location": pl.String,
    "outcome": pl.String,
    "launch_seconds": pl.Float64,
}


def _validate_command_recording(value: Any, execution_run_id: str) -> CommandRecording:
    """Use the canonical model without exposing malformed diagnostic contents."""
    try:
        if isinstance(value, str):
            return CommandRecording.model_validate_json(value)
        return CommandRecording.model_validate(value)
    except (ValueError, TypeError):
        msg = f"Invalid canonical command recording for execution {execution_run_id!r}"
        raise StoreIntegrityError(msg) from None


def _command_timing_entries(recording: CommandRecording) -> list[dict[str, Any]]:
    """Flatten commands and availability markers without copying command text."""
    entries = [
        {
            "entry_type": "command",
            "invocation": command.invocation,
            "sequence": command.sequence,
            "location": command.location,
            "outcome": command.outcome,
            "launch_seconds": command.launch_seconds,
        }
        for command in recording.commands
    ]
    entries.extend(
        {
            "entry_type": "missing_invocation",
            "invocation": marker.invocation,
            "missing_reason": marker.reason,
        }
        for marker in recording.missing_invocations
    )
    entries.sort(
        key=lambda row: (
            row["invocation"],
            row["entry_type"] != "command",
            row.get("sequence", 0),
        )
    )
    return entries or [{"entry_type": "empty_recording"}]


def _parse_timings(metadata_json: str | None) -> dict[str, Any] | None:
    """Parse timings from a metadata JSON string."""
    if not metadata_json:
        return None
    try:
        data = json.loads(metadata_json)
        timings: dict[str, Any] | None = data.get("timings")
        return timings
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
