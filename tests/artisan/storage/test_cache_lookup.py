"""Tests for cache lookup via Delta Lake."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import Mock

import pytest
from fixtures.execution_records import executions_df
from fixtures.logical_commit_store import commit_test_step

from artisan.schemas.enums import TablePath
from artisan.schemas.execution.cache_result import CacheHit
from artisan.storage.cache.cache_lookup import cache_lookup
from artisan.storage.core.committed_scan import scan_committed


def _seed_executions(
    backend_fs,
    records_data: dict[str, list[object]],
) -> tuple[str, object, dict[str, str]]:
    """Commit execution fixtures through the logical persistence boundary."""
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


@pytest.fixture
def execution_scan(monkeypatch: pytest.MonkeyPatch) -> Mock:
    """Observe the cache's committed execution scans without replacing storage."""
    scan = Mock(wraps=scan_committed)
    monkeypatch.setattr("artisan.storage.cache.cache_lookup.scan_committed", scan)
    return scan


class TestCacheLookup:
    """Cache lookup behavior across successful, failed, and absent executions.

    Runs against both local and s3 backends via the ``cache_env`` fixture.
    """

    def test_cache_hit(self, cache_env, execution_scan):
        """Cache hit returns the matching successful execution."""
        delta_root, fs, opts = cache_env
        result = cache_lookup(delta_root, "spec_success", fs, storage_options=opts)

        assert isinstance(result, CacheHit)
        assert result.execution_spec_id == "spec_success"
        assert result.execution_run_id == "run_success"
        execution_scan.assert_called_once()

    def test_cache_miss_no_execution(self, backend_fs, execution_scan):
        """Cache miss when no execution exists."""
        fs, storage, root = backend_fs
        result = cache_lookup(
            f"{root}/nonexistent",
            "any_spec",
            fs,
            storage_options=storage.delta_storage_options(),
        )

        assert result is None
        execution_scan.assert_not_called()

    def test_cache_miss_failed_execution(self, cache_env, execution_scan):
        """Cache miss when execution exists but failed."""
        delta_root, fs, opts = cache_env
        result = cache_lookup(delta_root, "spec_failed", fs, storage_options=opts)

        assert result is None
        execution_scan.assert_called_once()

    def test_cache_miss_unknown_spec_id(self, cache_env, execution_scan):
        """Cache miss when spec_id not found in existing table."""
        delta_root, fs, opts = cache_env
        result = cache_lookup(delta_root, "nonexistent_spec", fs, storage_options=opts)

        assert result is None
        execution_scan.assert_called_once()

    @pytest.mark.parametrize("later_success", [True, False])
    def test_cache_lookup_returns_most_recent_success(self, backend_fs, later_success):
        """A later failure never hides an earlier reusable success."""
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
            "success": [True, later_success],
            "error": [None, None if later_success else "Failed"],
            "tool_output": [None, None],
            "worker_log": [None, None],
            "metadata": ["{}", "{}"],
        }
        delta_root, fs, opts = _seed_executions(backend_fs, records_data)
        result = cache_lookup(delta_root, "same_spec", fs, storage_options=opts)

        assert isinstance(result, CacheHit)
        assert result.execution_run_id == ("run_new" if later_success else "run_old")


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
    """Test cache lookup through committed storage on both backends."""

    def test_cache_hit_round_trip(self, backend_fs):
        """Return the successful execution on either storage backend."""
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
