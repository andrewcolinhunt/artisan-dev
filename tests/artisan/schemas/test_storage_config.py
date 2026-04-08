"""Tests for StorageConfig model."""

from __future__ import annotations

import pytest
from fsspec.implementations.local import LocalFileSystem
from pydantic import ValidationError

from artisan.schemas.execution.storage_config import StorageConfig


class TestStorageConfigDefaults:
    """Default StorageConfig targets local filesystem."""

    def test_protocol_defaults_to_file(self):
        config = StorageConfig()
        assert config.protocol == "file"

    def test_options_defaults_to_empty(self):
        config = StorageConfig()
        assert config.options == {}

    def test_is_local_true(self):
        config = StorageConfig()
        assert config.is_local is True

    def test_filesystem_returns_local(self):
        config = StorageConfig()
        fs = config.filesystem()
        assert isinstance(fs, LocalFileSystem)

    def test_delta_storage_options_returns_none(self):
        config = StorageConfig()
        assert config.delta_storage_options() is None


class TestStorageConfigS3:
    """S3 StorageConfig behavior."""

    def test_is_local_false(self):
        config = StorageConfig(protocol="s3")
        assert config.is_local is False

    def test_delta_storage_options_returns_none(self):
        config = StorageConfig(protocol="s3")
        assert config.delta_storage_options() is None

    def test_options_passed_through(self):
        config = StorageConfig(protocol="s3", options={"region_name": "us-east-1"})
        assert config.options == {"region_name": "us-east-1"}


class TestStorageConfigFrozen:
    """StorageConfig is immutable."""

    def test_cannot_set_protocol(self):
        config = StorageConfig()
        with pytest.raises(ValidationError):
            config.protocol = "s3"

    def test_cannot_set_options(self):
        config = StorageConfig()
        with pytest.raises(ValidationError):
            config.options = {"key": "val"}


class TestStorageConfigSerialization:
    """Pydantic serialization round-trip."""

    def test_round_trip_default(self):
        config = StorageConfig()
        data = config.model_dump()
        restored = StorageConfig.model_validate(data)
        assert restored == config

    def test_round_trip_with_options(self):
        config = StorageConfig(protocol="gcs", options={"project": "my-project"})
        data = config.model_dump()
        restored = StorageConfig.model_validate(data)
        assert restored == config
        assert restored.protocol == "gcs"
        assert restored.options == {"project": "my-project"}
