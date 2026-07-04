"""Tests for execution/staging/recorder.py — failure recording."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import polars as pl
import pytest
from fsspec.implementations.local import LocalFileSystem

from artisan.execution.staging.parquet_writer import StagingResult
from artisan.execution.staging.recorder import record_execution_failure


def _make_execution_context(tmp_path: Path) -> MagicMock:
    """Create a mock ExecutionContext with required attributes."""
    from fsspec.implementations.local import LocalFileSystem

    staging_dir = tmp_path / "staging"
    staging_dir.mkdir(parents=True, exist_ok=True)
    ctx = MagicMock()
    ctx.staging_root = str(staging_dir)
    ctx.fs = LocalFileSystem()
    ctx.execution_run_id = "a" * 32
    ctx.execution_spec_id = "b" * 32
    ctx.operation_name = "test_op"
    ctx.step_number = 0
    ctx.timestamp_start = datetime.now(UTC)
    ctx.worker_id = 0
    ctx.compute_backend = "local"
    return ctx


class TestRecordExecutionFailure:
    """Tests for record_execution_failure()."""

    def test_returns_success_false(self, tmp_path):
        """Failure recording must set success=False on the StagingResult."""
        ctx = _make_execution_context(tmp_path)
        result = record_execution_failure(
            execution_context=ctx,
            error="something broke",
            inputs={},
            timestamp_end=datetime.now(UTC),
        )
        assert isinstance(result, StagingResult)
        assert result.success is False

    def test_populates_error(self, tmp_path):
        """Failure recording must propagate the error string."""
        ctx = _make_execution_context(tmp_path)
        result = record_execution_failure(
            execution_context=ctx,
            error="ValueError: bad input",
            inputs={},
            timestamp_end=datetime.now(UTC),
        )
        assert result.error == "ValueError: bad input"

    def test_populates_execution_run_id(self, tmp_path):
        """Failure recording must set the execution_run_id."""
        ctx = _make_execution_context(tmp_path)
        result = record_execution_failure(
            execution_context=ctx,
            error="err",
            inputs={},
            timestamp_end=datetime.now(UTC),
        )
        assert result.execution_run_id == "a" * 32

    def test_double_fault_returns_combined_error(self, tmp_path):
        """If staging itself fails, return a combined error (no raise)."""
        ctx = _make_execution_context(tmp_path)

        with patch(
            "artisan.execution.staging.parquet_writer._stage_execution",
            side_effect=OSError("disk full"),
        ):
            result = record_execution_failure(
                execution_context=ctx,
                error="original error",
                inputs={},
                timestamp_end=datetime.now(UTC),
            )

        assert result.success is False
        assert "original error" in result.error
        assert "disk full" in result.error
        assert "staging the failure record failed" in result.error

    def test_double_fault_still_has_execution_run_id(self, tmp_path):
        """Double-fault result still carries the execution_run_id."""
        ctx = _make_execution_context(tmp_path)

        with patch(
            "artisan.execution.staging.parquet_writer._stage_execution",
            side_effect=RuntimeError("boom"),
        ):
            result = record_execution_failure(
                execution_context=ctx,
                error="original",
                inputs={},
                timestamp_end=datetime.now(UTC),
            )

        assert result.execution_run_id == "a" * 32


class TestPassthroughStagedRowsGolden:
    """Characterization: staged rows for the curator passthrough path.

    Pins the exact executions / execution_edges / artifact_edges rows that
    the passthrough recording produces, so the delegation refactor
    (``_handle_passthrough_result`` -> ``record_passthrough``) can be proven
    byte-identical. Exercises ``_handle_passthrough_result`` (the stable
    entry that survives the refactor) and reads the staged Parquet back.
    """

    def _run(self, tmp_path: Path):
        from artisan.execution.executors.curator import _handle_passthrough_result
        from artisan.operations.curator.filter import Filter
        from artisan.schemas.artifact.provenance import ArtifactProvenanceEdge
        from artisan.schemas.execution.curator_result import PassthroughResult

        staging_root = tmp_path / "staging"
        staging_root.mkdir(parents=True, exist_ok=True)

        ctx = MagicMock()
        ctx.staging_root = str(staging_root)
        ctx.fs = LocalFileSystem()
        ctx.execution_run_id = "r" * 32
        ctx.execution_spec_id = "s" * 32
        ctx.operation_name = "filter"
        ctx.step_number = 3
        ctx.timestamp_start = datetime(2026, 1, 1, tzinfo=UTC)
        ctx.worker_id = 0
        ctx.compute_backend = "local"
        ctx.shared_filesystem = False
        ctx.step_run_id = None

        edge = ArtifactProvenanceEdge(
            execution_run_id="0" * 32,  # sentinel; recorder stamps the real one
            source_artifact_id="a" * 32,
            target_artifact_id="b" * 32,
            source_artifact_type="data",
            target_artifact_type="data",
            source_role="passthrough",
            target_role="passthrough",
            group_id=None,
            step_boundary=True,
        )
        result = PassthroughResult(
            passthrough={"passthrough": ["b" * 32]},
            lineage_edges=[edge],
            metadata={"k": "v"},
        )

        _handle_passthrough_result(
            result=result,
            operation=Filter(),
            execution_context=ctx,
            inputs={"passthrough": ["a" * 32]},
            timestamp_end=datetime(2026, 1, 1, 0, 0, 1, tzinfo=UTC),
            user_overrides={"foo": "bar"},
        )
        return staging_root

    @staticmethod
    def _read_only(staging_root: Path, filename: str) -> pl.DataFrame:
        matches = list(staging_root.rglob(filename))
        assert len(matches) == 1, f"expected one {filename}, found {matches}"
        return pl.read_parquet(matches[0])

    def test_executions_row_golden(self, tmp_path):
        staging_root = self._run(tmp_path)
        row = self._read_only(staging_root, "executions.parquet").to_dicts()[0]
        assert row == {
            "execution_run_id": "r" * 32,
            "execution_spec_id": "s" * 32,
            "step_run_id": None,
            "origin_step_number": 3,
            "operation_name": "filter",
            "params": (
                '{"criteria": [], "passthrough_failures": false, "chunk_size": 100000}'
            ),
            "user_overrides": '{"foo": "bar"}',
            "timestamp_start": datetime(2026, 1, 1, tzinfo=UTC),
            "timestamp_end": datetime(2026, 1, 1, 0, 0, 1, tzinfo=UTC),
            "source_worker": 0,
            "compute_backend": "local",
            "success": True,
            "error": None,
            "tool_output": None,
            "worker_log": None,
            "metadata": '{"k": "v"}',
        }

    def test_execution_edges_rows_golden(self, tmp_path):
        staging_root = self._run(tmp_path)
        rows = self._read_only(staging_root, "execution_edges.parquet").to_dicts()
        assert rows == [
            {
                "execution_run_id": "r" * 32,
                "direction": "input",
                "role": "passthrough",
                "artifact_id": "a" * 32,
            },
            {
                "execution_run_id": "r" * 32,
                "direction": "output",
                "role": "passthrough",
                "artifact_id": "b" * 32,
            },
        ]

    def test_artifact_edges_row_golden_stamped(self, tmp_path):
        staging_root = self._run(tmp_path)
        rows = self._read_only(staging_root, "artifact_edges.parquet").to_dicts()
        # Sentinel run id ("0"*32) is replaced with the execution's run id.
        assert rows == [
            {
                "execution_run_id": "r" * 32,
                "source_artifact_id": "a" * 32,
                "target_artifact_id": "b" * 32,
                "source_artifact_type": "data",
                "target_artifact_type": "data",
                "source_role": "passthrough",
                "target_role": "passthrough",
                "group_id": None,
                "step_boundary": True,
            }
        ]


@pytest.fixture(
    params=[
        pytest.param("local"),
        pytest.param("s3", marks=pytest.mark.s3),
    ]
)
def backend_fs(request, tmp_path):
    """Yield ``(fs, uri_prefix)`` for both local and s3 backends.

    Inlined here because ``tests/artisan/execution/`` does not share the
    storage-layer ``backend_fs`` fixture. ``s3_fs`` is resolved lazily
    via ``request.getfixturevalue`` so the local-only run never
    instantiates MinIO via testcontainers (which leaks a Docker UNIX
    socket on session teardown when the daemon isn't reachable). S3
    params skip cleanly when MinIO is unavailable.
    """
    if request.param == "local":
        return LocalFileSystem(), str(tmp_path)
    fs, _, uri_prefix = request.getfixturevalue("s3_fs")
    return fs, uri_prefix


class TestStagingRecorderBackendParametrized:
    """Smoke round-trip of failure recording against both [local, s3] backends."""

    def test_record_execution_failure_round_trip(self, backend_fs):
        """Failure record stages to executions.parquet on both backends."""
        fs, root = backend_fs
        staging_root = f"{root}/staging"
        fs.makedirs(staging_root, exist_ok=True)

        ctx = MagicMock()
        ctx.staging_root = staging_root
        ctx.fs = fs
        ctx.execution_run_id = "a" * 32
        ctx.execution_spec_id = "b" * 32
        ctx.operation_name = "smoke_op"
        ctx.step_number = 0
        ctx.timestamp_start = datetime.now(UTC)
        ctx.worker_id = 0
        ctx.compute_backend = "local"
        ctx.shared_filesystem = False
        ctx.step_run_id = None

        result = record_execution_failure(
            execution_context=ctx,
            error="smoke failure",
            inputs={},
            timestamp_end=datetime.now(UTC),
        )

        assert isinstance(result, StagingResult)
        assert result.success is False
        assert result.error == "smoke failure"
        assert result.staging_path is not None

        executions_uri = f"{result.staging_path}/executions.parquet"
        assert fs.exists(executions_uri)
        with fs.open(executions_uri, "rb") as f:
            df = pl.read_parquet(f)
        assert df["execution_run_id"][0] == "a" * 32
        assert df["success"][0] is False
        assert df["error"][0] == "smoke failure"
