"""Tests for staging_verification.py - NFS-aware staging file verification."""

from __future__ import annotations

import time
from pathlib import Path

import pytest

from artisan.storage.io.staging_verification import (
    REQUIRED_STAGING_FILE,
    await_staging_files,
    compute_expected_staging_paths,
    verify_file_exists_nfs,
    verify_staging_directory,
)
from artisan.utils.path import shard_uri


class TestVerifyFileExistsNfs:
    """Tests for verify_file_exists_nfs()."""

    def test_existing_file_returns_true(self, tmp_path):
        """Returns True for existing file."""
        test_file = tmp_path / "test.txt"
        test_file.write_text("content")

        result = verify_file_exists_nfs(test_file)

        assert result is True

    def test_nonexistent_file_returns_false(self, tmp_path):
        """Returns False for non-existent file."""
        nonexistent = tmp_path / "does_not_exist.txt"

        result = verify_file_exists_nfs(nonexistent)

        assert result is False

    def test_nonexistent_parent_returns_false(self, tmp_path):
        """Returns False when parent directory doesn't exist."""
        nonexistent = tmp_path / "missing_parent" / "file.txt"

        result = verify_file_exists_nfs(nonexistent)

        assert result is False

    def test_empty_file_returns_true(self, tmp_path):
        """Returns True for empty file (read(1) returns b'', no exception)."""
        test_file = tmp_path / "empty.txt"
        test_file.write_bytes(b"")

        result = verify_file_exists_nfs(test_file)

        assert result is True


class TestComputeExpectedStagingPaths:
    """Tests for compute_expected_staging_paths()."""

    def test_computes_sharded_paths(self, tmp_path):
        """Computes correct sharded paths for execution_run_ids."""
        execution_run_ids = [
            "abcdef1234567890abcdef1234567890",
            "12345678901234567890123456789012",
        ]

        paths = compute_expected_staging_paths(tmp_path, execution_run_ids)

        assert len(paths) == 2
        # Verify sharding matches shard_uri()
        assert paths[0] == Path(shard_uri(str(tmp_path), execution_run_ids[0]))
        assert paths[1] == Path(shard_uri(str(tmp_path), execution_run_ids[1]))
        # Verify structure: root/ab/cd/full_id
        assert paths[0] == tmp_path / "ab" / "cd" / execution_run_ids[0]
        assert paths[1] == tmp_path / "12" / "34" / execution_run_ids[1]

    def test_empty_list_returns_empty(self, tmp_path):
        """Empty list returns empty list."""
        paths = compute_expected_staging_paths(tmp_path, [])

        assert paths == []


class TestVerifyStagingDirectory:
    """Tests for verify_staging_directory()."""

    def test_directory_with_required_file_succeeds(self, tmp_path):
        """Returns (True, []) when required file exists."""
        staging_dir = tmp_path / "staging" / "ab" / "cd" / "run_id"
        staging_dir.mkdir(parents=True)
        (staging_dir / REQUIRED_STAGING_FILE).write_bytes(b"data")

        success, missing = verify_staging_directory(staging_dir)

        assert success is True
        assert missing == []

    def test_directory_missing_required_file_fails(self, tmp_path):
        """Returns (False, [file]) when required file is missing."""
        staging_dir = tmp_path / "staging" / "ab" / "cd" / "run_id"
        staging_dir.mkdir(parents=True)
        # Directory exists but no executions.parquet

        success, missing = verify_staging_directory(staging_dir)

        assert success is False
        assert REQUIRED_STAGING_FILE in missing

    def test_nonexistent_directory_fails(self, tmp_path):
        """Returns (False, [reason]) when directory doesn't exist."""
        nonexistent_dir = tmp_path / "does_not_exist"

        success, missing = verify_staging_directory(nonexistent_dir)

        assert success is False
        assert len(missing) == 1
        assert "not found" in missing[0]


class TestAwaitStagingFiles:
    """Tests for await_staging_files()."""

    def test_empty_execution_run_ids_returns_immediately(self, tmp_path):
        """No-op when execution_run_ids is empty."""
        # Should not raise, should return quickly
        await_staging_files(
            staging_root=tmp_path,
            execution_run_ids=[],
            timeout_seconds=1.0,
        )

    def test_all_files_present_returns_immediately(self, tmp_path):
        """Returns quickly when all files are already present."""
        execution_run_id = "abcdef1234567890abcdef1234567890"

        # Create staging directory with required file
        staging_dir = Path(shard_uri(str(tmp_path), execution_run_id))
        staging_dir.mkdir(parents=True)
        (staging_dir / REQUIRED_STAGING_FILE).write_bytes(b"data")

        start = time.monotonic()
        await_staging_files(
            staging_root=tmp_path,
            execution_run_ids=[execution_run_id],
            timeout_seconds=10.0,
        )
        elapsed = time.monotonic() - start

        # Should complete quickly (not wait for timeout)
        assert elapsed < 2.0

    def test_files_appear_after_delay_succeeds(self, tmp_path):
        """Succeeds when files appear during polling."""
        execution_run_id = "abcdef1234567890abcdef1234567890"
        staging_dir = Path(shard_uri(str(tmp_path), execution_run_id))

        def create_file_after_delay():
            """Simulate file appearing after a short delay."""
            time.sleep(0.5)
            staging_dir.mkdir(parents=True, exist_ok=True)
            (staging_dir / REQUIRED_STAGING_FILE).write_bytes(b"data")

        import threading

        # Start thread to create file after delay
        thread = threading.Thread(target=create_file_after_delay)
        thread.start()

        try:
            # Should succeed after file appears
            await_staging_files(
                staging_root=tmp_path,
                execution_run_ids=[execution_run_id],
                timeout_seconds=5.0,
                poll_interval_seconds=0.2,
            )
        finally:
            thread.join()

    def test_timeout_raises_error_with_details(self, tmp_path):
        """Raises TimeoutError with helpful message when files never appear."""
        execution_run_ids = [
            "abcdef1234567890abcdef1234567890",
            "12345678901234567890123456789012",
        ]

        with pytest.raises(TimeoutError) as exc_info:
            await_staging_files(
                staging_root=tmp_path,
                execution_run_ids=execution_run_ids,
                timeout_seconds=0.5,
                poll_interval_seconds=0.1,
            )

        error_msg = str(exc_info.value)
        # Check error message contains helpful info
        assert "not visible after" in error_msg
        assert "Missing 2/2" in error_msg
        assert "abcdef" in error_msg
        assert "SLURM worker logs" in error_msg

    def test_partial_files_still_raises_timeout(self, tmp_path):
        """Raises TimeoutError even when some files are present."""
        execution_run_ids = [
            "abcdef1234567890abcdef1234567890",
            "12345678901234567890123456789012",
        ]

        # Create only the first file
        staging_dir = Path(shard_uri(str(tmp_path), execution_run_ids[0]))
        staging_dir.mkdir(parents=True)
        (staging_dir / REQUIRED_STAGING_FILE).write_bytes(b"data")

        with pytest.raises(TimeoutError) as exc_info:
            await_staging_files(
                staging_root=tmp_path,
                execution_run_ids=execution_run_ids,
                timeout_seconds=0.5,
                poll_interval_seconds=0.1,
            )

        error_msg = str(exc_info.value)
        # Only the second one should be missing
        assert "Missing 1/2" in error_msg
        assert "12345678" in error_msg

    def test_multiple_files_all_verified(self, tmp_path):
        """Verifies all execution_run_ids when multiple provided."""
        execution_run_ids = [
            "abcdef1234567890abcdef1234567890",
            "12345678901234567890123456789012",
            "deadbeef12345678deadbeef12345678",
        ]

        # Create all staging directories with required files
        for run_id in execution_run_ids:
            staging_dir = Path(shard_uri(str(tmp_path), run_id))
            staging_dir.mkdir(parents=True)
            (staging_dir / REQUIRED_STAGING_FILE).write_bytes(b"data")

        # Should succeed without timeout
        await_staging_files(
            staging_root=tmp_path,
            execution_run_ids=execution_run_ids,
            timeout_seconds=5.0,
        )


class TestWorkerResultsHelpers:
    """Tests for extract_execution_run_ids helpers in worker_results.py."""

    def test_extract_execution_run_ids_from_results(self):
        """Extract execution_run_ids from UnitResult instances."""
        from artisan.orchestration.engine.results import (
            extract_execution_run_ids,
        )
        from artisan.schemas.execution.unit_result import UnitResult

        results = [
            UnitResult(
                success=True, error=None, item_count=1, execution_run_ids=["id1"]
            ),
            UnitResult(
                success=True, error=None, item_count=1, execution_run_ids=["id2", "id3"]
            ),
            UnitResult(
                success=False, error="fail", item_count=1, execution_run_ids=[None]
            ),
            UnitResult(success=True, error=None, item_count=1, execution_run_ids=[]),
        ]

        ids = extract_execution_run_ids(results)

        assert ids == ["id1", "id2", "id3"]

    def test_extract_execution_run_ids_empty_list(self):
        """Empty list returns empty list."""
        from artisan.orchestration.engine.results import (
            extract_execution_run_ids,
        )

        ids = extract_execution_run_ids([])

        assert ids == []
