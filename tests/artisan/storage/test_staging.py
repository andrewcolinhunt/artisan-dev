"""Test exact orchestrator staging on local and S3 backends."""

from __future__ import annotations

import polars as pl
import pytest

from artisan.storage.io.commit_plan import PlannedFile
from artisan.storage.io.staging import StagingManager
from artisan.utils.hashing import compute_content_digest


class TestStagingManager:
    def test_cleanup_hashing_disables_remote_readahead(self, tmp_path, monkeypatch):
        from fsspec.implementations.local import LocalFileSystem

        fs = LocalFileSystem()
        path = tmp_path / "payload.parquet"
        payload = b"payload"
        path.write_bytes(payload)
        original_open = fs.open

        def uncached_open(source, mode="rb", **kwargs):
            assert kwargs.get("cache_type") == "none"
            return original_open(source, mode, **kwargs)

        monkeypatch.setattr(fs, "open", uncached_open)
        evidence = PlannedFile(
            relative_path=path.name,
            size_bytes=len(payload),
            digest=compute_content_digest(payload),
            row_count=0,
            schema_signature=(),
        )

        StagingManager(str(tmp_path), fs).cleanup_plan([evidence])

        assert not path.exists()

    def test_stage_cache_reuse_is_exact_sorted_and_retry_idempotent(self, backend_fs):
        fs, _, root = backend_fs
        manager = StagingManager(f"{root}/staging", fs)
        current = "a" * 32

        uri = manager.stage_cache_reuse(
            current,
            ["c" * 32, "b" * 32, "c" * 32],
            step_number=3,
            operation_name="cached",
        )
        manager.stage_cache_reuse(
            current,
            ["b" * 32],
            step_number=3,
            operation_name="cached",
        )

        assert uri is not None
        with fs.open(uri, "rb") as stream:
            staged = pl.read_parquet(stream)
        assert staged.columns == [
            "current_step_run_id",
            "cached_execution_run_id",
        ]
        assert staged["cached_execution_run_id"].to_list() == ["b" * 32, "c" * 32]

    def test_stage_cache_reuse_rejects_noncanonical_ids(self, backend_fs):
        fs, _, root = backend_fs
        manager = StagingManager(f"{root}/staging", fs)

        with pytest.raises(ValueError, match="current_step_run_id"):
            manager.stage_cache_reuse(
                "not-an-id",
                ["b" * 32],
                step_number=0,
                operation_name="cached",
            )
