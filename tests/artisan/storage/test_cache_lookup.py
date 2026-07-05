"""Tests for cache lookup via Delta Lake."""

from __future__ import annotations

from datetime import datetime

import pytest
from fixtures.execution_records import executions_df

from artisan.schemas.enums import CacheValidationReason
from artisan.schemas.execution.cache_result import CacheHit, CacheMiss
from artisan.storage.cache.cache_lookup import cache_lookup


@pytest.fixture
def cache_env(backend_fs):
    """Yield ``(executions_path, fs, storage_options, root)`` per step_runner.

    Seeds ``orchestration/executions`` with one success + one failure so
    consumers test the three primary outcomes (hit / miss-failed /
    miss-unknown-spec) without having to re-seed.
    """
    fs, storage, root = backend_fs
    opts = storage.delta_storage_options()
    executions_path = f"{root}/orchestration/executions"

    now = datetime.now()
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
    executions_df(**records_data).write_delta(
        executions_path, mode="overwrite", storage_options=opts
    )

    return executions_path, fs, opts, root


class TestCacheLookup:
    """Cache lookup behavior across successful, failed, and absent executions.

    Runs against both local and s3 backends via the ``cache_env`` fixture.
    """

    def test_cache_hit(self, cache_env):
        """Cache hit returns the matching successful execution."""
        executions_path, fs, opts, _root = cache_env
        result = cache_lookup(executions_path, "spec_success", fs, storage_options=opts)

        assert isinstance(result, CacheHit)
        assert result.execution_spec_id == "spec_success"
        assert result.execution_run_id == "run_success"

    def test_cache_miss_no_execution(self, backend_fs):
        """Cache miss when no execution exists."""
        fs, storage, root = backend_fs
        result = cache_lookup(
            f"{root}/nonexistent",
            "any_spec",
            fs,
            storage_options=storage.delta_storage_options(),
        )

        assert isinstance(result, CacheMiss)
        assert result.reason == CacheValidationReason.NO_PREVIOUS_EXECUTION

    def test_cache_miss_failed_execution(self, cache_env):
        """Cache miss when execution exists but failed."""
        executions_path, fs, opts, _root = cache_env
        result = cache_lookup(executions_path, "spec_failed", fs, storage_options=opts)

        assert isinstance(result, CacheMiss)
        assert result.reason == CacheValidationReason.EXECUTION_FAILED

    def test_cache_miss_unknown_spec_id(self, cache_env):
        """Cache miss when spec_id not found in existing table."""
        executions_path, fs, opts, _root = cache_env
        result = cache_lookup(
            executions_path, "nonexistent_spec", fs, storage_options=opts
        )

        assert isinstance(result, CacheMiss)
        assert result.reason == CacheValidationReason.NO_PREVIOUS_EXECUTION

    def test_cache_lookup_returns_most_recent_on_multiple_successes(self, backend_fs):
        """When multiple successful executions exist, return most recent."""
        fs, storage, root = backend_fs
        opts = storage.delta_storage_options()
        executions_path = f"{root}/orchestration/executions"

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
        executions_df(**records_data).write_delta(
            executions_path, mode="overwrite", storage_options=opts
        )

        result = cache_lookup(executions_path, "same_spec", fs, storage_options=opts)

        assert isinstance(result, CacheHit)
        assert result.execution_run_id == "run_new"

    def test_cache_miss_reasons_for_different_scenarios(self, backend_fs):
        """CacheMiss.reason distinguishes between no execution and failed execution.

        This helps the executor decide:
        - NO_PREVIOUS_EXECUTION: New execution needed (first time)
        - EXECUTION_FAILED: Retry needed (previous attempt failed)

        Note: On cache miss, the executor proceeds to:
        1. Materialization (stream artifacts to working directory)
        2. Execute operation
        3. Record execution
        """
        fs, storage, root = backend_fs
        # Non-existent table -> NO_PREVIOUS_EXECUTION
        result = cache_lookup(
            f"{root}/nonexistent",
            "any_spec",
            fs,
            storage_options=storage.delta_storage_options(),
        )
        assert isinstance(result, CacheMiss)
        assert result.reason == CacheValidationReason.NO_PREVIOUS_EXECUTION


class TestCacheHitSchema:
    """Tests for the CacheHit identifier-only schema."""

    def test_cache_hit_fields(self) -> None:
        """CacheHit carries only the run and spec identifiers."""
        hit = CacheHit(
            execution_run_id="e" * 32,
            execution_spec_id="s" * 32,
        )

        assert hit.execution_run_id == "e" * 32
        assert hit.execution_spec_id == "s" * 32

    def test_cache_hit_no_input_output_fields(self) -> None:
        """CacheHit no longer exposes inputs/outputs."""
        hit = CacheHit(
            execution_run_id="e" * 32,
            execution_spec_id="s" * 32,
        )

        assert not hasattr(hit, "inputs")
        assert not hasattr(hit, "outputs")


class TestCacheLookupBackendParametrized:
    """Smoke test cache_lookup against both [local, s3] backends.

    Kept alongside the promoted ``TestCacheLookup`` class as an
    additional integration-level round-trip guard.
    """

    def test_cache_hit_round_trip(self, backend_fs):
        """A successful execution produces a CacheHit on either step_runner."""
        fs, storage, root = backend_fs
        delta_root = f"{root}/delta"
        executions_path = f"{delta_root}/orchestration/executions"
        storage_options = storage.delta_storage_options()

        now = datetime.now()
        records_df = executions_df(
            execution_run_id=["run_success"],
            execution_spec_id=["spec_success"],
            step_run_id=[None],
            origin_step_number=[1],
            operation_name=["relax"],
            params=["{}"],
            user_overrides=["{}"],
            timestamp_start=[now],
            timestamp_end=[now],
            source_worker=[0],
            compute_backend=["local"],
            success=[True],
            error=[None],
            tool_output=[None],
            worker_log=[None],
            metadata=["{}"],
        )
        records_df.write_delta(
            executions_path, mode="overwrite", storage_options=storage_options
        )

        result = cache_lookup(
            executions_path,
            "spec_success",
            fs,
            storage_options=storage_options,
        )

        assert isinstance(result, CacheHit)
        assert result.execution_run_id == "run_success"
