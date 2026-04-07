"""Tests for PipelineConfig.files_root field."""

from __future__ import annotations

from pathlib import Path

import pytest

from artisan.schemas.orchestration.pipeline_config import PipelineConfig


class TestPipelineConfigFilesRoot:
    """Tests for the files_root field on PipelineConfig."""

    def test_files_root_defaults_from_delta_root(self, tmp_path: Path) -> None:
        """When files_root is not set, it derives from delta_root.parent / 'files'."""
        delta_root = tmp_path / "pipeline" / "delta"
        config = PipelineConfig(
            name="test",
            delta_root=delta_root,
            staging_root=tmp_path / "staging",
        )
        assert config.files_root == delta_root.parent / "files"

    def test_files_root_explicit_override(self, tmp_path: Path) -> None:
        """When files_root is explicitly set, the override is used."""
        delta_root = tmp_path / "pipeline" / "delta"
        custom_files = tmp_path / "bulk_nfs" / "files"
        config = PipelineConfig(
            name="test",
            delta_root=delta_root,
            staging_root=tmp_path / "staging",
            files_root=custom_files,
        )
        assert config.files_root == custom_files

    def test_files_root_frozen(self, tmp_path: Path) -> None:
        """files_root cannot be mutated after creation (frozen model)."""
        config = PipelineConfig(
            name="test",
            delta_root=tmp_path / "delta",
            staging_root=tmp_path / "staging",
        )
        with pytest.raises(Exception):  # noqa: B017 (ValidationError on frozen model)
            config.files_root = tmp_path / "other"  # type: ignore[misc]

    def test_files_root_none_input_gets_default(self, tmp_path: Path) -> None:
        """Passing files_root=None explicitly still triggers the default."""
        delta_root = tmp_path / "pipeline" / "delta"
        config = PipelineConfig(
            name="test",
            delta_root=delta_root,
            staging_root=tmp_path / "staging",
            files_root=None,
        )
        assert config.files_root == delta_root.parent / "files"
