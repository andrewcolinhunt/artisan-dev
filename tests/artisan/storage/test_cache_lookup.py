"""Tests for cache lookup via Delta Lake."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import polars as pl
import pytest

from artisan.schemas.enums import CacheValidationReason
from artisan.schemas.execution.cache_result import CacheHit, CacheMiss
from artisan.storage.cache.cache_lookup import cache_lookup
from artisan.storage.core.table_schemas import (
    EXECUTION_EDGES_SCHEMA,
    EXECUTIONS_SCHEMA,
)


@pytest.fixture
def executions_path(tmp_path: Path) -> Path:
    """Create executions and execution_edges tables with test data."""
    records_path = tmp_path / "orchestration/executions"
    provenance_path = tmp_path / "provenance/execution_edges"

    now = datetime.now()

    # Executions (no inputs/outputs columns)
    records_data = {
        "execution_run_id": ["run_success", "run_failed"],
        "execution_spec_id": ["spec_success", "spec_failed"],
        "step_run_id": [None, None],
        "origin_step_number": [1, 1],
        "operation_name": ["relax", "relax"],
        "params": ["{}", "{}"],
        "user_overrides": ["{}", "{}"],
        "timestamp_start": [now, now],
        "timestamp_end": [now, now],
        "source_worker": [0, 0],
        "compute_backend": ["local", "local"],
        "success": [True, False],
        "error": [None, "Failed"],
        "tool_output": [None, None],
        "worker_log": [None, None],
        "metadata": ["{}", "{}"],
    }

    df = pl.DataFrame(records_data, schema=EXECUTIONS_SCHEMA)
    df.write_delta(str(records_path), mode="overwrite")

    # Execution edges (normalized input/output edges)
    provenance_data = {
        "execution_run_id": ["run_success", "run_success"],
        "direction": ["input", "output"],
        "role": ["data", "processed"],
        "artifact_id": ["input123", "output456"],
    }

    prov_df = pl.DataFrame(provenance_data, schema=EXECUTION_EDGES_SCHEMA)
    prov_df.write_delta(str(provenance_path), mode="overwrite")

    return records_path


def test_cache_hit(executions_path: Path) -> None:
    """Cache hit returns inputs/outputs from successful execution."""
    result = cache_lookup(executions_path, "spec_success")

    assert isinstance(result, CacheHit)
    assert result.execution_spec_id == "spec_success"
    assert len(result.inputs) == 1
    assert len(result.outputs) == 1


def test_cache_miss_no_execution(tmp_path: Path) -> None:
    """Cache miss when no execution exists."""
    result = cache_lookup(tmp_path / "nonexistent", "any_spec")

    assert isinstance(result, CacheMiss)
    assert result.reason == CacheValidationReason.NO_PREVIOUS_EXECUTION


def test_cache_miss_failed_execution(executions_path: Path) -> None:
    """Cache miss when execution exists but failed."""
    result = cache_lookup(executions_path, "spec_failed")

    assert isinstance(result, CacheMiss)
    assert result.reason == CacheValidationReason.EXECUTION_FAILED


def test_cache_miss_unknown_spec_id(executions_path: Path) -> None:
    """Cache miss when spec_id not found in existing table."""
    result = cache_lookup(executions_path, "nonexistent_spec")

    assert isinstance(result, CacheMiss)
    assert result.reason == CacheValidationReason.NO_PREVIOUS_EXECUTION


def test_cache_lookup_returns_most_recent_on_multiple_successes(
    tmp_path: Path,
) -> None:
    """When multiple successful executions exist, return most recent."""
    records_path = tmp_path / "orchestration/executions"
    provenance_path = tmp_path / "provenance/execution_edges"

    earlier = datetime(2024, 1, 1, 10, 0, 0)
    later = datetime(2024, 1, 1, 12, 0, 0)

    records_data = {
        "execution_run_id": ["run_old", "run_new"],
        "execution_spec_id": ["same_spec", "same_spec"],
        "step_run_id": [None, None],
        "origin_step_number": [1, 1],
        "operation_name": ["op", "op"],
        "params": ["{}", "{}"],
        "user_overrides": ["{}", "{}"],
        "timestamp_start": [earlier, later],
        "timestamp_end": [earlier, later],
        "source_worker": [0, 0],
        "compute_backend": ["local", "local"],
        "success": [True, True],
        "error": [None, None],
        "tool_output": [None, None],
        "worker_log": [None, None],
        "metadata": ["{}", "{}"],
    }

    df = pl.DataFrame(records_data, schema=EXECUTIONS_SCHEMA)
    df.write_delta(str(records_path), mode="overwrite")

    # Provenance for both executions
    provenance_data = {
        "execution_run_id": ["run_old", "run_old", "run_new", "run_new"],
        "direction": ["input", "output", "input", "output"],
        "role": ["x", "y", "x", "y"],
        "artifact_id": ["a", "old_output", "a", "new_output"],
    }

    prov_df = pl.DataFrame(provenance_data, schema=EXECUTION_EDGES_SCHEMA)
    prov_df.write_delta(str(provenance_path), mode="overwrite")

    result = cache_lookup(records_path, "same_spec")

    assert isinstance(result, CacheHit)
    assert result.execution_run_id == "run_new"


def test_cache_hit_enables_output_reference_resolution(
    executions_path: Path,
) -> None:
    """Cache hit provides outputs for OutputReference resolution.

    When a cache hit occurs, downstream steps use OutputReference(source_step, role)
    to find artifact IDs. The CacheHit contains inputs/outputs that provides the
    mapping that enables this resolution.

    Key behavior verified:
    - Cache hit returns inputs/outputs with role -> artifact_id mappings
    - No new ExecutionRecord needed - artifacts exist from original execution
    """
    result = cache_lookup(executions_path, "spec_success")

    assert isinstance(result, CacheHit)

    # The outputs can be used for OutputReference resolution
    # OutputReference(source_step=N, role="processed") -> artifact_id
    assert len(result.outputs) == 1
    assert any(
        o["role"] == "processed" and o["artifact_id"] == "output456"
        for o in result.outputs
    )


def test_cache_miss_reasons_for_different_scenarios(tmp_path: Path) -> None:
    """CacheMiss.reason distinguishes between no execution and failed execution.

    This helps the executor decide:
    - NO_PREVIOUS_EXECUTION: New execution needed (first time)
    - EXECUTION_FAILED: Retry needed (previous attempt failed)

    Note: On cache miss, the executor proceeds to:
    1. Materialization (stream artifacts to working directory)
    2. Execute operation
    3. Record execution
    """
    # Non-existent table -> NO_PREVIOUS_EXECUTION
    result = cache_lookup(tmp_path / "nonexistent", "any_spec")
    assert isinstance(result, CacheMiss)
    assert result.reason == CacheValidationReason.NO_PREVIOUS_EXECUTION


class TestCacheHitSchema:
    """Tests for CacheHit with new inputs/outputs schema."""

    def test_cache_hit_has_inputs_outputs(self) -> None:
        """CacheHit uses inputs/outputs instead of input_output_pairs."""
        hit = CacheHit(
            execution_run_id="e" * 32,
            execution_spec_id="s" * 32,
            inputs=[{"role": "data", "artifact_id": "a" * 32}],
            outputs=[{"role": "processed", "artifact_id": "b" * 32}],
        )

        assert hit.inputs == [{"role": "data", "artifact_id": "a" * 32}]
        assert hit.outputs == [{"role": "processed", "artifact_id": "b" * 32}]

    def test_cache_hit_no_input_output_pairs(self) -> None:
        """CacheHit doesn't have old input_output_pairs attribute."""
        hit = CacheHit(
            execution_run_id="e" * 32,
            execution_spec_id="s" * 32,
            inputs=[],
            outputs=[],
        )

        assert not hasattr(hit, "input_output_pairs")

    def test_cache_hit_no_parents_children(self) -> None:
        """CacheHit uses inputs/outputs, not parents/children."""
        hit = CacheHit(
            execution_run_id="e" * 32,
            execution_spec_id="s" * 32,
            inputs=[],
            outputs=[],
        )

        assert not hasattr(hit, "parents")
        assert not hasattr(hit, "children")
