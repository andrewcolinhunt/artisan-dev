"""Tests for orchestration/engine/inputs.py."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import patch

import polars as pl
import pytest
from fixtures.execution_records import executions_df
from fixtures.store_format import commit_test_tables, publish_test_store
from fsspec.implementations.local import LocalFileSystem

from artisan.errors import IncompatibleStoreError
from artisan.orchestration.engine.inputs import resolve_output_reference
from artisan.schemas.enums import TablePath
from artisan.schemas.orchestration.output_reference import OutputReference


@pytest.fixture(autouse=True)
def _supported_store(tmp_path):
    publish_test_store(str(tmp_path), LocalFileSystem())


def _create_executions_df(**overrides) -> pl.DataFrame:
    """Create a test executions DataFrame with default values."""
    defaults = {
        "execution_run_id": ["e" * 32],
        "execution_spec_id": ["s" * 32],
        "step_run_id": [None],
        "origin_step_number": [0],
        "operation_name": ["TestOp"],
        "timestamp_start": [datetime.now(UTC)],
        "timestamp_end": [datetime.now(UTC)],
        "source_worker": [0],
        "success": [True],
        "error": [None],
        "params": ["{}"],
        "user_overrides": ["{}"],
        "compute_backend": ["local"],
        "tool_output": [None],
        "worker_log": [None],
        "metadata": ["{}"],
    }
    defaults.update(overrides)
    return executions_df(**defaults)


def _commit_executions(tmp_path, df: pl.DataFrame) -> None:
    """Commit execution rows with current commit ownership."""
    step_run_id = "seed-step"
    commit_test_tables(
        str(tmp_path),
        str(tmp_path / "staging"),
        LocalFileSystem(),
        {
            TablePath.EXECUTIONS.value: df.with_columns(
                pl.lit(step_run_id).alias("step_run_id")
            )
        },
        step_run_id=step_run_id,
    )


class TestResolveOutputReferenceEmptyUpstream:
    """Tests for empty upstream returns empty list instead of raising."""

    def test_empty_executions_table_returns_empty(self, tmp_path):
        """A coordinated store with no executions returns an empty list."""
        ref = OutputReference(source_step=0, role="data")
        result = resolve_output_reference(ref, str(tmp_path), fs=LocalFileSystem())
        assert result == []

    def test_unsupported_store_fails_before_scan(self, tmp_path):
        """The manifest gate runs before any Delta table lookup."""
        (tmp_path / "_artisan" / "store.json").unlink()
        ref = OutputReference(source_step=0, role="data")

        with (
            patch("artisan.orchestration.engine.inputs.pl.scan_delta") as scan,
            pytest.raises(IncompatibleStoreError, match="missing manifest"),
        ):
            resolve_output_reference(ref, str(tmp_path), fs=LocalFileSystem())

        scan.assert_not_called()

    def test_empty_execution_edges_table_returns_empty(self, tmp_path):
        """A committed execution with no output edges returns an empty list."""
        df = _create_executions_df()
        _commit_executions(tmp_path, df)

        ref = OutputReference(source_step=0, role="data")
        result = resolve_output_reference(ref, str(tmp_path), fs=LocalFileSystem())
        assert result == []

    def test_no_successful_executions_returns_empty(self, tmp_path):
        """When all executions failed, return [] (not raise)."""
        df = _create_executions_df(success=[False], error=["some error"])
        _commit_executions(tmp_path, df)

        ref = OutputReference(source_step=0, role="data")
        result = resolve_output_reference(ref, str(tmp_path), fs=LocalFileSystem())
        assert result == []


class TestStorageOptionsForwarding:
    """storage_options should be forwarded to scan_delta."""

    def test_scan_delta_receives_storage_options(self, tmp_path):
        """resolve_output_reference forwards storage_options to pl.scan_delta."""
        df = _create_executions_df()
        _commit_executions(tmp_path, df)

        opts = {"key": "val"}
        ref = OutputReference(source_step=0, role="data")

        with patch(
            "artisan.orchestration.engine.inputs.pl.scan_delta",
            wraps=pl.scan_delta,
        ) as mock_scan:
            resolve_output_reference(
                ref, str(tmp_path), fs=LocalFileSystem(), storage_options=opts
            )
            mock_scan.assert_called()
            _, kwargs = mock_scan.call_args_list[0]
            assert kwargs.get("storage_options") == opts
