"""Tests for PipelineTimings visualization class."""

from __future__ import annotations

import pytest
from fixtures.cache_isolation_store import build_cache_isolation_store

from artisan.errors import IncompatibleStoreError
from artisan.visualization.timing import PipelineTimings


@pytest.fixture
def sample_timing_data():
    """A complete timing data dict with two steps and multiple executions."""
    return {
        "pipeline_run_id": "test-run-001",
        "steps": [
            {
                "step_number": 0,
                "step_name": "data_generator_with_metrics",
                "duration_seconds": 1.5,
                "timings": {
                    "resolve_inputs": 0.01,
                    "batch_and_cache": 0.05,
                    "execute": 1.2,
                    "verify_staging": 0.0,
                    "commit": 0.08,
                    "compact": 0.02,
                    "total": 1.36,
                },
                "executions": [
                    {
                        "execution_run_id": "exec-aaaa-1111-2222-333344445555",
                        "operation_name": "data_generator_with_metrics",
                        "timings": {
                            "setup": 0.03,
                            "preprocess": 0.01,
                            "execute": 0.5,
                            "postprocess": 0.02,
                            "lineage": 0.01,
                            "record": 0.04,
                            "total": 0.61,
                        },
                    },
                    {
                        "execution_run_id": "exec-bbbb-1111-2222-333344445555",
                        "operation_name": "data_generator_with_metrics",
                        "timings": {
                            "setup": 0.02,
                            "preprocess": 0.01,
                            "execute": 0.4,
                            "postprocess": 0.01,
                            "lineage": 0.01,
                            "record": 0.03,
                            "total": 0.48,
                        },
                    },
                ],
            },
            {
                "step_number": 1,
                "step_name": "filter",
                "duration_seconds": 0.3,
                "timings": {
                    "resolve_inputs": 0.02,
                    "cache_check": 0.01,
                    "execute": 0.15,
                    "commit": 0.05,
                    "total": 0.23,
                },
                "executions": [
                    {
                        "execution_run_id": "exec-cccc-1111-2222-333344445555",
                        "operation_name": "filter",
                        "timings": {
                            "setup": 0.02,
                            "execute": 0.1,
                            "record": 0.02,
                            "total": 0.14,
                        },
                    },
                ],
            },
        ],
    }


class TestPipelineTimingsInit:
    """Tests for constructor validation."""

    def test_empty_data_raises(self):
        with pytest.raises(ValueError, match="empty"):
            PipelineTimings({})

    def test_missing_steps_raises(self):
        with pytest.raises(ValueError, match="steps"):
            PipelineTimings({"pipeline_run_id": "test"})

    def test_valid_data_accepted(self, sample_timing_data):
        timings = PipelineTimings(sample_timing_data)
        assert timings.data is sample_timing_data

    def test_data_property(self, sample_timing_data):
        timings = PipelineTimings(sample_timing_data)
        assert timings.data["pipeline_run_id"] == "test-run-001"
        assert len(timings.data["steps"]) == 2


class TestStepTimings:
    """Tests for step_timings()."""

    def test_correct_shape(self, sample_timing_data):
        timings = PipelineTimings(sample_timing_data)
        result = timings.step_timings()
        assert result.shape[0] == 2
        assert "step_number" in result.columns
        assert "step_name" in result.columns
        assert "duration_seconds" in result.columns

    def test_contains_phase_columns(self, sample_timing_data):
        timings = PipelineTimings(sample_timing_data)
        result = timings.step_timings()
        assert "total" in result.columns
        assert "execute" in result.columns

    def test_values_match(self, sample_timing_data):
        timings = PipelineTimings(sample_timing_data)
        result = timings.step_timings()
        assert result["step_name"][0] == "data_generator_with_metrics"
        assert result["total"][0] == pytest.approx(1.36)


class TestExecutionTimings:
    """Tests for execution_timings()."""

    def test_correct_shape(self, sample_timing_data):
        timings = PipelineTimings(sample_timing_data)
        result = timings.execution_timings(step_number=0)
        assert result.shape[0] == 2

    def test_step_1_shape(self, sample_timing_data):
        timings = PipelineTimings(sample_timing_data)
        result = timings.execution_timings(step_number=1)
        assert result.shape[0] == 1

    def test_contains_required_columns(self, sample_timing_data):
        timings = PipelineTimings(sample_timing_data)
        result = timings.execution_timings(step_number=0)
        assert "step_number" in result.columns
        assert "execution_run_id" in result.columns
        assert "operation_name" in result.columns
        assert "execute" in result.columns

    def test_values_match(self, sample_timing_data):
        timings = PipelineTimings(sample_timing_data)
        result = timings.execution_timings(step_number=0)
        assert result["operation_name"][0] == "data_generator_with_metrics"
        assert result["execute"][0] == pytest.approx(0.5)

    def test_invalid_step_raises(self, sample_timing_data):
        timings = PipelineTimings(sample_timing_data)
        with pytest.raises(ValueError, match="Step 99 not found"):
            timings.execution_timings(step_number=99)


class TestExecutionStats:
    """Tests for execution_stats()."""

    def test_correct_columns(self, sample_timing_data):
        timings = PipelineTimings(sample_timing_data)
        result = timings.execution_stats(step_number=0)
        assert list(result.columns) == ["phase", "mean", "std", "min", "max"]

    def test_correct_phases(self, sample_timing_data):
        timings = PipelineTimings(sample_timing_data)
        result = timings.execution_stats(step_number=0)
        phases = result["phase"].to_list()
        assert "setup" in phases
        assert "execute" in phases
        assert "record" in phases

    def test_mean_values(self, sample_timing_data):
        timings = PipelineTimings(sample_timing_data)
        result = timings.execution_stats(step_number=0)
        # execute: mean of 0.5 and 0.4 = 0.45
        execute_row = result.filter(result["phase"] == "execute")
        assert execute_row["mean"][0] == pytest.approx(0.45)
        # setup: mean of 0.03 and 0.02 = 0.025
        setup_row = result.filter(result["phase"] == "setup")
        assert setup_row["mean"][0] == pytest.approx(0.025)

    def test_min_max_values(self, sample_timing_data):
        timings = PipelineTimings(sample_timing_data)
        result = timings.execution_stats(step_number=0)
        execute_row = result.filter(result["phase"] == "execute")
        assert execute_row["min"][0] == pytest.approx(0.4)
        assert execute_row["max"][0] == pytest.approx(0.5)

    def test_single_execution_std_zero(self, sample_timing_data):
        timings = PipelineTimings(sample_timing_data)
        result = timings.execution_stats(step_number=1)
        # Step 1 has one execution, std should be 0
        for row in result.iter_rows(named=True):
            assert row["std"] == pytest.approx(0.0)

    def test_no_executions_raises(self):
        data = {
            "pipeline_run_id": "test",
            "steps": [
                {
                    "step_number": 0,
                    "step_name": "empty_step",
                    "duration_seconds": 0.0,
                    "timings": {},
                    "executions": [],
                }
            ],
        }
        timings = PipelineTimings(data)
        with pytest.raises(ValueError, match="no executions"):
            timings.execution_stats(step_number=0)

    def test_invalid_step_raises(self, sample_timing_data):
        timings = PipelineTimings(sample_timing_data)
        with pytest.raises(ValueError, match="Step 99 not found"):
            timings.execution_stats(step_number=99)


class TestPipelineTimingsPlot:
    """Tests for plot_steps() and plot_execution_stats() figure cleanup."""

    def test_plot_steps_closes_figure(self, sample_timing_data):
        """Verify plot_steps returns a Figure and does not leave it registered."""
        import matplotlib.pyplot as plt

        timings = PipelineTimings(sample_timing_data)
        initial_figs = len(plt.get_fignums())
        fig = timings.plot_steps()
        assert fig is not None
        assert hasattr(fig, "savefig")
        assert len(plt.get_fignums()) == initial_figs
        plt.close("all")

    def test_plot_steps_filters_by_step_numbers(self, sample_timing_data):
        """Verify step_numbers filters which steps are plotted."""
        import matplotlib.pyplot as plt

        timings = PipelineTimings(sample_timing_data)
        fig = timings.plot_steps(step_numbers=[0])
        ax = fig.axes[0]
        labels = [t.get_text() for t in ax.get_yticklabels()]
        assert len(labels) == 1
        assert "0:" in labels[0]
        plt.close("all")

    def test_plot_execution_stats_closes_figure(self, sample_timing_data):
        """Verify plot_execution_stats returns a Figure and does not leave it registered."""
        import matplotlib.pyplot as plt

        timings = PipelineTimings(sample_timing_data)
        initial_figs = len(plt.get_fignums())
        fig = timings.plot_execution_stats()
        assert fig is not None
        assert hasattr(fig, "savefig")
        assert len(plt.get_fignums()) == initial_figs
        plt.close("all")

    def test_plot_steps_empty(self):
        """Verify empty steps returns a figure without error."""
        import matplotlib.pyplot as plt

        data = {"pipeline_run_id": "test", "steps": []}
        timings = PipelineTimings(data)
        initial_figs = len(plt.get_fignums())
        fig = timings.plot_steps()
        assert fig is not None
        assert len(plt.get_fignums()) == initial_figs
        plt.close("all")

    def test_plot_execution_stats_empty(self):
        """Verify empty executions returns a figure without error."""
        import matplotlib.pyplot as plt

        data = {
            "pipeline_run_id": "test",
            "steps": [
                {
                    "step_number": 0,
                    "step_name": "empty",
                    "duration_seconds": 0.0,
                    "timings": {},
                    "executions": [],
                }
            ],
        }
        timings = PipelineTimings(data)
        initial_figs = len(plt.get_fignums())
        fig = timings.plot_execution_stats()
        assert fig is not None
        assert len(plt.get_fignums()) == initial_figs
        plt.close("all")


class TestPipelineTimingsFromDelta:
    """Tests for from_delta() loading."""

    def test_missing_path_raises(self, tmp_path):
        with pytest.raises(IncompatibleStoreError, match="missing manifest"):
            PipelineTimings.from_delta(tmp_path)

    def test_run_excludes_other_and_reused_execution_timings(self, tmp_path):
        store = build_cache_isolation_store(tmp_path)

        timings = PipelineTimings.from_delta(
            store.root,
            pipeline_run_id=store.current_run,
        )

        assert timings.execution_timings(0)["execution_run_id"].to_list() == [
            store.current_data_execution
        ]
        assert timings.execution_timings(5).is_empty()
        all_execution_ids = {
            row["execution_run_id"]
            for step in timings._data["steps"]
            for row in step["executions"]
        }
        assert store.source_metric_execution not in all_execution_ids
        assert store.other_data_execution not in all_execution_ids

    def test_latest_run_uses_lifecycle_time_not_step_number(self, tmp_path):
        store = build_cache_isolation_store(tmp_path)

        timings = PipelineTimings.from_delta(store.root)

        assert timings.data["pipeline_run_id"] == store.other_run


def _timing_command(invocation=0, sequence=0, **changes):
    """Build explicit canonical subprocess evidence for timing assertions."""
    return {
        "invocation": invocation,
        "sequence": sequence,
        "location": "local",
        "requested_argv": ["tool"],
        "argv": ["tool"],
        "cwd": "/tmp",
        "tool": None,
        "environment": {
            "type": "LocalEnvironmentSpec",
            "identity": {},
            "variable_names": [],
        },
        "outcome": "succeeded",
        "returncode": 0,
        "redacted_fields": [],
        "required_environment": [],
        "launch_seconds": 0.123456789,
    } | changes


def _timing_recording(**changes):
    from artisan.schemas.execution.command_record import CommandRecording

    return CommandRecording.empty().model_dump(mode="json") | changes


def _command_timings_data(*recordings):
    return {
        "steps": [
            {
                "step_number": 0,
                "step_name": "op",
                "duration_seconds": 3.0,
                "timings": {"execute": 3.0},
                "executions": [
                    {
                        "execution_run_id": f"execution-{index}",
                        "operation_name": "op",
                        "timings": {"execute": 1.0},
                        "command_recording": recording,
                    }
                    for index, recording in enumerate(recordings)
                ],
            }
        ]
    }


def test_command_timings_preserves_typed_order_and_partial_evidence():
    import polars as pl

    recording = _timing_recording(
        status="partial",
        commands=[
            _timing_command(),
            _timing_command(1, 0, location="endpoint"),
            _timing_command(
                1,
                1,
                location="endpoint",
                outcome="launch_failed",
                returncode=None,
                launch_seconds=None,
            ),
        ],
        missing_invocations=[
            {"invocation": 1, "reason": "transport_failure"},
            {"invocation": 2, "reason": "cancelled"},
        ],
        omitted_commands=3,
        omitted_missing_invocations=4,
    )
    timing = PipelineTimings(_command_timings_data(recording))
    frame = timing.command_timings(0)
    assert frame.schema == {
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
    assert frame.select("entry_type", "invocation", "sequence").rows() == [
        ("command", 0, 0),
        ("command", 1, 0),
        ("command", 1, 1),
        ("missing_invocation", 1, None),
        ("missing_invocation", 2, None),
    ]
    assert frame["launch_seconds"].to_list() == [
        0.123456789,
        0.123456789,
        None,
        None,
        None,
    ]
    assert frame["missing_reason"].to_list() == [
        None,
        None,
        None,
        "transport_failure",
        "cancelled",
    ]
    assert frame["omitted_commands"].to_list() == [3] * 5
    assert frame["omitted_missing_invocations"].to_list() == [4] * 5
    assert "argv" not in frame.columns
    assert timing.execution_stats(0)["phase"].to_list() == ["execute"]
    assert timing.step_timings()["execute"].to_list() == [3.0]
    assert (
        PipelineTimings(_command_timings_data()).command_timings(0).schema
        == frame.schema
    )


def test_command_timings_distinguishes_empty_omitted_and_unavailable():
    from artisan.schemas.execution.command_record import CommandRecording

    frames = PipelineTimings(
        _command_timings_data(
            _timing_recording(),
            _timing_recording(
                status="partial", omitted_commands=4, omitted_missing_invocations=2
            ),
            CommandRecording.unavailable().model_dump(mode="json"),
            _timing_recording(
                status="unavailable",
                missing_invocations=[{"invocation": 0, "reason": "missing_recording"}],
            ),
        )
    ).command_timings(0)
    assert frames["entry_type"].to_list() == ["empty_recording"] * 3 + [
        "missing_invocation"
    ]
    assert frames["recording_status"].to_list() == [
        "complete",
        "partial",
        "unavailable",
        "unavailable",
    ]
    assert frames["unavailable_reason"].to_list() == [
        None,
        None,
        "worker_evidence_unavailable",
        None,
    ]
    assert frames["launch_seconds"].null_count() == 4
    assert frames["invocation"].to_list() == [None, None, None, 0]


@pytest.mark.parametrize(
    "invalid",
    [
        None,
        {},
        "sensitive malformed JSON",
        _timing_recording(commands=[_timing_command(launch_seconds=float("nan"))]),
        _timing_recording(commands=[_timing_command(launch_seconds=float("inf"))]),
        _timing_recording(commands=[_timing_command(launch_seconds=-1.0)]),
        _timing_recording(commands=[_timing_command(launch_seconds="1.0")]),
        _timing_recording(omitted_commands="1"),
        _timing_recording(status="unavailable"),
    ],
)
def test_command_timings_rejects_invalid_evidence_without_leaking_json(invalid):
    from artisan.errors import StoreIntegrityError

    timing = PipelineTimings(_command_timings_data(invalid))
    with pytest.raises(StoreIntegrityError, match="execution-0") as error:
        timing.command_timings(0)
    assert "sensitive" not in str(error.value)


def test_command_timings_requires_raw_evidence_and_valid_step():
    from artisan.errors import StoreIntegrityError

    data = _command_timings_data(_timing_recording())
    data["steps"][0]["executions"][0].pop("command_recording")
    with pytest.raises(StoreIntegrityError):
        PipelineTimings(data).command_timings(0)
    with pytest.raises(ValueError, match="Step 99 not found"):
        PipelineTimings(data).command_timings(99)


def test_command_timings_from_delta_retains_only_selected_fresh_evidence(tmp_path):
    store = build_cache_isolation_store(tmp_path)
    timing = PipelineTimings.from_delta(store.root, pipeline_run_id=store.current_run)
    assert timing.command_timings(0)["execution_run_id"].to_list() == [
        store.current_data_execution
    ]
    assert timing.command_timings(0)["entry_type"].to_list() == ["empty_recording"]
    assert timing.command_timings(5).is_empty()


@pytest.mark.parametrize("invalid", [None, "not-json", "{}"])
def test_from_delta_rejects_invalid_canonical_recording(tmp_path, monkeypatch, invalid):
    import polars as pl

    from artisan.errors import StoreIntegrityError
    from artisan.storage.core import run_scope

    store = build_cache_isolation_store(tmp_path)
    original = run_scope.load_execution_membership

    def invalid_evidence(*args, **kwargs):
        return original(*args, **kwargs).with_columns(
            pl.lit(invalid, dtype=pl.String).alias("command_recording")
        )

    monkeypatch.setattr(run_scope, "load_execution_membership", invalid_evidence)
    with pytest.raises(StoreIntegrityError, match=store.current_data_execution):
        PipelineTimings.from_delta(store.root, pipeline_run_id=store.current_run)
