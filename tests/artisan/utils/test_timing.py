"""Tests for artisan.utils.timing module."""

from __future__ import annotations

import time

from artisan.utils.timing import phase_timer


class TestPhaseTimer:
    """Tests for the phase_timer context manager."""

    def test_records_elapsed_time(self):
        """phase_timer should record a positive elapsed time."""
        timings: dict = {}
        with phase_timer("test_phase", timings):
            time.sleep(0.01)

        assert "test_phase" in timings
        assert timings["test_phase"] > 0

    def test_records_multiple_phases(self):
        """Multiple phase_timer calls should accumulate in the same dict."""
        timings: dict = {}
        with phase_timer("phase_a", timings):
            time.sleep(0.01)
        with phase_timer("phase_b", timings):
            time.sleep(0.01)

        assert "phase_a" in timings
        assert "phase_b" in timings
        assert len(timings) == 2

    def test_zero_work_phase(self):
        """A phase with no real work should still record a non-negative time."""
        timings: dict = {}
        with phase_timer("empty", timings):
            pass

        assert "empty" in timings
        assert timings["empty"] >= 0

    def test_timing_precision(self):
        """Timings should be rounded to 4 decimal places."""
        timings: dict = {}
        with phase_timer("precise", timings):
            time.sleep(0.001)

        value_str = str(timings["precise"])
        if "." in value_str:
            decimals = len(value_str.split(".")[1])
            assert decimals <= 4

    def test_phase_timer_propagates_exceptions(self):
        """Exceptions inside phase_timer should propagate without recording."""
        timings: dict = {}
        try:
            with phase_timer("failing", timings):
                msg = "test error"
                raise ValueError(msg)
        except ValueError:
            pass

        # The timing should NOT be recorded since the exception prevents
        # the post-yield code from running
        assert "failing" not in timings
