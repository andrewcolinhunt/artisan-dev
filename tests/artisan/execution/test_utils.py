"""Tests for execution run identifiers."""

from __future__ import annotations

from datetime import UTC, datetime

from artisan.execution.utils import generate_execution_run_id


class TestGenerateExecutionRunId:
    """Tests for generate_execution_run_id()."""

    def test_returns_32_char_hex(self):
        """Returns 32-character hexadecimal string."""
        run_id = generate_execution_run_id(
            spec_id="a" * 32,
            timestamp=datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC),
            worker_id=0,
        )

        assert len(run_id) == 32
        assert all(c in "0123456789abcdef" for c in run_id)

    def test_deterministic(self):
        """Same inputs produce same output."""
        ts = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)

        run_id_1 = generate_execution_run_id("a" * 32, ts, 0)
        run_id_2 = generate_execution_run_id("a" * 32, ts, 0)

        assert run_id_1 == run_id_2

    def test_different_spec_id_different_hash(self):
        """Different spec_id produces different hash."""
        ts = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)

        run_id_1 = generate_execution_run_id("a" * 32, ts, 0)
        run_id_2 = generate_execution_run_id("b" * 32, ts, 0)

        assert run_id_1 != run_id_2

    def test_different_timestamp_different_hash(self):
        """Different timestamp produces different hash."""
        ts_1 = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
        ts_2 = datetime(2024, 1, 1, 12, 0, 1, tzinfo=UTC)

        run_id_1 = generate_execution_run_id("a" * 32, ts_1, 0)
        run_id_2 = generate_execution_run_id("a" * 32, ts_2, 0)

        assert run_id_1 != run_id_2

    def test_different_worker_id_different_hash(self):
        """Different worker_id produces different hash."""
        ts = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)

        run_id_1 = generate_execution_run_id("a" * 32, ts, 0)
        run_id_2 = generate_execution_run_id("a" * 32, ts, 1)

        assert run_id_1 != run_id_2
