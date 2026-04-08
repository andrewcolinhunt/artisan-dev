"""Tests for PipelineConfig.files_root field."""

from __future__ import annotations

from pathlib import Path

import pytest

from artisan.schemas.execution.storage_config import StorageConfig
from artisan.schemas.orchestration.pipeline_config import PipelineConfig
from artisan.utils.path import uri_join, uri_parent


class TestPipelineConfigFilesRoot:
    """Tests for the files_root field on PipelineConfig."""

    def test_files_root_defaults_from_delta_root(self, tmp_path: Path) -> None:
        """When files_root is not set, it derives from delta_root.parent / 'files'."""
        delta_root = str(tmp_path / "pipeline" / "delta")
        config = PipelineConfig(
            name="test",
            delta_root=delta_root,
            staging_root=str(tmp_path / "staging"),
        )
        assert config.files_root == uri_join(uri_parent(delta_root), "files")

    def test_files_root_explicit_override(self, tmp_path: Path) -> None:
        """When files_root is explicitly set, the override is used."""
        delta_root = str(tmp_path / "pipeline" / "delta")
        custom_files = str(tmp_path / "bulk_nfs" / "files")
        config = PipelineConfig(
            name="test",
            delta_root=delta_root,
            staging_root=str(tmp_path / "staging"),
            files_root=custom_files,
        )
        assert config.files_root == custom_files

    def test_files_root_frozen(self, tmp_path: Path) -> None:
        """files_root cannot be mutated after creation (frozen model)."""
        config = PipelineConfig(
            name="test",
            delta_root=str(tmp_path / "delta"),
            staging_root=str(tmp_path / "staging"),
        )
        with pytest.raises(Exception):  # noqa: B017 (ValidationError on frozen model)
            config.files_root = str(tmp_path / "other")  # type: ignore[misc]

    def test_files_root_none_input_gets_default(self, tmp_path: Path) -> None:
        """Passing files_root=None explicitly still triggers the default."""
        delta_root = str(tmp_path / "pipeline" / "delta")
        config = PipelineConfig(
            name="test",
            delta_root=delta_root,
            staging_root=str(tmp_path / "staging"),
            files_root=None,
        )
        assert config.files_root == uri_join(uri_parent(delta_root), "files")

    def test_files_root_not_derived_for_cloud(self) -> None:
        """files_root remains None when storage is non-local (e.g. S3)."""
        config = PipelineConfig(
            name="test",
            delta_root="s3://bucket/pipeline/delta",
            staging_root="s3://bucket/staging",
            storage=StorageConfig(protocol="s3"),
        )
        assert config.files_root is None
