"""Tests for PipelineTimings visualization class."""

from __future__ import annotations

import pytest

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
        with pytest.raises(FileNotFoundError):
            PipelineTimings.from_delta(tmp_path)
