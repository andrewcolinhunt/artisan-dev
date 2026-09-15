"""Tests for cache lookup via Delta Lake."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from fixtures.execution_records import executions_df
from fixtures.logical_commit_store import commit_test_step

from artisan.schemas.enums import CacheValidationReason, TablePath
from artisan.schemas.execution.cache_result import CacheHit, CacheMiss
from artisan.storage.cache.cache_lookup import cache_lookup


def _seed_executions(
    backend_fs,
    records_data: dict[str, list[object]],
) -> tuple[str, object, dict[str, str]]:
    """Commit execution fixtures through the format-2 persistence boundary."""
    fs, storage, root = backend_fs
    options = storage.delta_storage_options()
    delta_root = f"{root}/delta"
    staging_root = f"{root}/staging"
    step_run_id = "c" * 32
    records_data["step_run_id"] = [step_run_id] * len(records_data["execution_run_id"])
    succeeded = sum(bool(value) for value in records_data["success"])
    total = len(records_data["success"])
    status = "succeeded" if succeeded == total else "partial"
    terminal = {
        "step_run_id": step_run_id,
        "step_spec_id": "d" * 32,
        "pipeline_run_id": "cache-test-run",
        "step_number": 1,
        "step_name": "cache-test",
        "status": status,
        "state_sequence": 2,
        "disposition": "executed",
        "cancellation_status": None,
        "operation_class": "tests.CacheOperation",
        "params_json": "{}",
        "input_refs_json": "{}",
        "compute_backend": "local",
        "compute_options_json": "{}",
        "output_roles_json": "[]",
        "output_types_json": "{}",
        "total_count": total,
        "succeeded_count": succeeded,
        "failed_count": total - succeeded,
        "timestamp": datetime.now(UTC),
        "duration_seconds": 1.0,
        "error": None if succeeded == total else "one execution failed",
        "metadata": None,
    }
    commit_test_step(
        delta_root,
        staging_root,
        [terminal],
        {TablePath.EXECUTIONS.value: executions_df(**records_data)},
        fs=fs,
        storage_options=options,
    )
    return delta_root, fs, options


@pytest.fixture
def cache_env(backend_fs):
    """Yield ``(delta_root, fs, storage_options)`` per storage backend.

    Seeds ``orchestration/executions`` with one success + one failure so
    consumers test the three primary outcomes (hit / miss-failed /
    miss-unknown-spec) without having to re-seed.
    """
    now = datetime.now()
    records_data = {
        "execution_run_id": ["run_success", "run_failed"],
        "execution_spec_id": ["spec_success", "spec_failed"],
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
    return _seed_executions(backend_fs, records_data)


class TestCacheLookup:
    """Cache lookup behavior across successful, failed, and absent executions.

    Runs against both local and s3 backends via the ``cache_env`` fixture.
    """

    def test_cache_hit(self, cache_env):
        """Cache hit returns the matching successful execution."""
        delta_root, fs, opts = cache_env
        result = cache_lookup(delta_root, "spec_success", fs, storage_options=opts)

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
        delta_root, fs, opts = cache_env
        result = cache_lookup(delta_root, "spec_failed", fs, storage_options=opts)

        assert isinstance(result, CacheMiss)
        assert result.reason == CacheValidationReason.EXECUTION_FAILED

    def test_cache_miss_unknown_spec_id(self, cache_env):
        """Cache miss when spec_id not found in existing table."""
        delta_root, fs, opts = cache_env
        result = cache_lookup(delta_root, "nonexistent_spec", fs, storage_options=opts)

        assert isinstance(result, CacheMiss)
        assert result.reason == CacheValidationReason.NO_PREVIOUS_EXECUTION

    def test_cache_lookup_returns_most_recent_on_multiple_successes(self, backend_fs):
        """When multiple successful executions exist, return most recent."""
        earlier = datetime(2024, 1, 1, 10, 0, 0)
        later = datetime(2024, 1, 1, 12, 0, 0)

        records_data = {
            "execution_run_id": ["run_old", "run_new"],
            "execution_spec_id": ["same_spec", "same_spec"],
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
        delta_root, fs, opts = _seed_executions(backend_fs, records_data)
        result = cache_lookup(delta_root, "same_spec", fs, storage_options=opts)

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
        now = datetime.now()
        records_data = {
            "execution_run_id": ["run_success"],
            "execution_spec_id": ["spec_success"],
            "origin_step_number": [1],
            "operation_name": ["relax"],
            "params": ["{}"],
            "user_overrides": ["{}"],
            "timestamp_start": [now],
            "timestamp_end": [now],
            "source_worker": [0],
            "compute_backend": ["local"],
            "success": [True],
            "error": [None],
            "tool_output": [None],
            "worker_log": [None],
            "metadata": ["{}"],
        }
        delta_root, fs, storage_options = _seed_executions(backend_fs, records_data)

        result = cache_lookup(
            delta_root,
            "spec_success",
            fs,
            storage_options=storage_options,
        )

        assert isinstance(result, CacheHit)
        assert result.execution_run_id == "run_success"
