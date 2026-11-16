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
import math
from typing import TYPE_CHECKING, Any

import polars as pl
from fsspec import AbstractFileSystem
from polars.datatypes import DataType, DataTypeClass

from artisan.errors import StoreIntegrityError
from artisan.schemas.execution.command_record import CommandRecording

if TYPE_CHECKING:
    from artisan.inspection.models import ExecutionCommands


class PipelineTimings:
    """DataFrame-first API for pipeline timing analysis.

    Attributes:
        data (dict[str, Any]): Raw timing data dict. Requires a "steps" key;
            "pipeline_run_id" is optional. Execution command data uses the
            existing ExecutionCommands envelope under "commands"; scientific
            invocation timing observations remain nested under "invocations".

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
        *,
        files_root: str | None = None,
    ) -> PipelineTimings:
        """Load timing data from steps and executions delta tables.

        Args:
            delta_root: Path to the delta lake root directory.
            pipeline_run_id: Pipeline run ID to filter by. If None, uses the
                latest pipeline run.
            storage_options: Delta-rs storage options for cloud backends.
            fs: Filesystem for existence checks. Local if None.
            files_root: Managed carrier root; required for nonlocal/custom storage.

        Returns:
            PipelineTimings instance.

        Raises:
            FileNotFoundError: If steps table doesn't exist.
            ValueError: If no usable terminal steps are found.
        """
        from artisan.storage.io.retained_store import open_configured_store
        from artisan.visualization.retained_timing import read_retained_timings

        return read_retained_timings(
            open_configured_store(
                delta_root,
                files_root=files_root,
                fs=fs,
                storage_options=storage_options,
            ),
            pipeline_run_id=pipeline_run_id,
        )

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
        return pl.DataFrame(rows, infer_schema_length=None)

    def execution_timings(self, step_number: int) -> pl.DataFrame:
        """Execution-level timings for a single step.

        Args:
            step_number: The step to get execution timings for.

        Returns:
            One row per outer execution with its identity, nullable observed
            duration, diagnostic coverage and nested invocation observations,
            plus any actually recorded outer timing phases.

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
                "unit_index": ex.get("unit_index"),
                "duration_seconds": ex.get("duration_seconds"),
                "diagnostics_status": ex.get("diagnostics_status"),
                "omitted_invocations": ex.get("omitted_invocations"),
                "invocations": ex.get("invocations", []),
            }
            for phase, value in ex["timings"].items():
                if isinstance(value, float):
                    row[phase] = value
            rows.append(row)
        return pl.DataFrame(
            rows,
            infer_schema_length=None,
            schema_overrides={"duration_seconds": pl.Float64},
        )

    def command_timings(self, step_number: int) -> pl.DataFrame:
        """Return observed Popen durations and explicit missing-evidence markers.

        Launch time is already included in execution phases; do not add it to
        stacked phase totals. Like execution_timings, stored timing selection
        includes only fresh successful executions from usable terminal steps.
        Failed attempts remain available through inspect_commands.

        Args:
            step_number: Step whose selected executions should be flattened.

        Returns:
            Typed frame preserving scientific invocation-group order and local
            command indices. Delivery coverage, absent observations and empty
            recordings remain distinct; no command text is copied.

        Raises:
            ValueError: The step does not exist.
            StoreIntegrityError: Canonical command evidence is missing or invalid.
        """
        step = self._find_step(step_number)
        rows: list[dict[str, Any]] = []
        for execution in step["executions"]:
            commands = _validate_execution_commands(
                execution.get("commands"), execution["execution_run_id"]
            )
            rows.extend(
                _command_rows(step_number, execution["operation_name"], commands)
            )
        return pl.DataFrame(rows, schema=_COMMAND_TIMINGS_SCHEMA)

    def execution_stats(self, step_number: int) -> pl.DataFrame:
        """Summary statistics for execution-level phase timings of a step.

        Args:
            step_number: The step to compute stats for.

        Returns:
            Phase, mean, std, min, max, observed_count and total_count columns.
            Missing phases are excluded from numeric samples; no observations
            produce a typed empty frame. Outer durations are not phase samples.

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
            if c
            not in (
                "step_number",
                "execution_run_id",
                "operation_name",
                "duration_seconds",
            )
            and exec_df[c].dtype in (pl.Float64, pl.Float32)
        ]

        rows = []
        for phase in phase_cols:
            col = exec_df[phase]
            rows.append(
                {
                    "phase": phase,
                    "mean": col.mean(),
                    "std": 0.0 if col.count() == 1 else col.std(),
                    "observed_count": col.count(),
                    "total_count": len(col),
                    "min": col.min(),
                    "max": col.max(),
                }
            )
        return pl.DataFrame(rows, schema=_EXECUTION_STATS_SCHEMA)

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
        phase_data: dict[str, list[float | None]] = {p: [] for p in all_phases}
        for step in steps:
            for p in all_phases:
                phase_data[p].append(step["timings"].get(p))

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
        phase_data: dict[str, list[float | None]] = {p: [] for p in all_phases}

        for step in steps_with_execs:
            stats_df = self.execution_stats(step["step_number"])
            complete = stats_df.filter(
                pl.col("observed_count") == pl.col("total_count")
            )
            stats_map = dict(zip(complete["phase"], complete["mean"], strict=False))
            for p in all_phases:
                phase_data[p].append(stats_map.get(p))

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
        phase_data: dict[str, list[float | None]],
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
        observed = [
            index
            for index in y_pos
            if all_phases
            and all(phase_data[phase][index] is not None for phase in all_phases)
        ]
        lefts = [0.0] * len(observed)
        colors = _get_phase_colors(all_phases)

        for phase in all_phases:
            values = [phase_data[phase][index] for index in observed]
            ax.barh(observed, values, left=lefts, label=phase, color=colors[phase])
            lefts = [
                left + value
                for left, value in zip(lefts, values, strict=True)
                if value is not None
            ]

        for index in y_pos:
            if index not in observed:
                ax.text(0, index, "Timing unavailable", va="center")

        ax.set_yticks(y_pos)
        ax.set_yticklabels(labels)
        ax.set_xlabel("Time (seconds)")
        ax.set_title(title)
        if observed:
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


_COMMAND_TIMINGS_SCHEMA: dict[str, DataType | DataTypeClass] = {
    "step_number": pl.Int32,
    "execution_run_id": pl.String,
    "operation_name": pl.String,
    "coverage_status": pl.String,
    "omitted_invocations": pl.Int64,
    "invocation_id": pl.String,
    "call_path": pl.List(pl.String),
    "invocation_operation": pl.String,
    "invocation_version": pl.String,
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


_EXECUTION_STATS_SCHEMA = {
    "phase": pl.String,
    "mean": pl.Float64,
    "std": pl.Float64,
    "min": pl.Float64,
    "max": pl.Float64,
    "observed_count": pl.Int64,
    "total_count": pl.Int64,
}


def _validate_execution_commands(
    value: object, execution_run_id: str
) -> ExecutionCommands:
    from artisan.inspection.models import ExecutionCommands

    try:
        commands = ExecutionCommands.model_validate(value)
        if commands.execution_run_id != execution_run_id:
            raise ValueError
    except (ValueError, TypeError):
        msg = f"Invalid canonical command recording for execution {execution_run_id!r}"
        raise StoreIntegrityError(msg) from None
    return commands


def _command_rows(
    step_number: int, operation: str, commands: ExecutionCommands
) -> list[dict[str, Any]]:
    common = {
        "step_number": step_number,
        "execution_run_id": commands.execution_run_id,
        "operation_name": operation,
        "coverage_status": commands.status,
        "omitted_invocations": commands.omitted_invocations,
    }
    rows: list[dict[str, Any]] = []
    if commands.recording is not None:
        rows.extend(common | entry for entry in _recording_entries(commands.recording))
    for invocation in commands.invocations:
        identity = {
            "invocation_id": invocation.invocation_id,
            "call_path": list(invocation.call_path),
            "invocation_operation": invocation.operation_name,
            "invocation_version": invocation.operation_version,
        }
        rows.extend(
            common | identity | entry
            for entry in _recording_entries(invocation.recording)
        )
    if commands.recording is None and (
        not commands.invocations or commands.omitted_invocations
    ):
        rows.append(common | {"entry_type": "delivery_coverage"})
    return rows


def _recording_entries(recording: CommandRecording) -> list[dict[str, Any]]:
    coverage = {
        "recording_status": recording.status,
        "omitted_commands": recording.omitted_commands,
        "omitted_missing_invocations": recording.omitted_missing_invocations,
        "unavailable_reason": recording.unavailable_reason,
    }
    return [coverage | entry for entry in _command_timing_entries(recording)]


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


def _parse_timings(metadata_json: object) -> dict[str, float]:
    """Read optional phase observations; malformed present evidence is explicit."""
    if metadata_json is None:
        return {}
    try:
        if not isinstance(metadata_json, str):
            raise TypeError
        data = json.loads(metadata_json)
        if not isinstance(data, dict):
            raise TypeError
    except (ValueError, TypeError):
        msg = "Execution timing metadata is malformed"
        raise StoreIntegrityError(msg) from None
    return _timing_values(data.get("timings"))


def _timing_values(value: object) -> dict[str, float]:
    """Keep actual finite nonnegative timings without filling missing phases."""
    if value is None:
        return {}
    if not isinstance(value, dict) or any(
        not isinstance(key, str)
        or type(seconds) not in {int, float}
        or not math.isfinite(seconds)
        or seconds < 0
        for key, seconds in value.items()
    ):
        msg = "Timing phase observations are malformed"
        raise StoreIntegrityError(msg)
    return {key: float(seconds) for key, seconds in value.items()}


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
