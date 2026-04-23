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

    def test_delta_options_defaults_to_empty(self):
        config = StorageConfig()
        assert config.delta_options == {}

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


class TestStorageConfigDeltaOptions:
    """delta_options field and delta_storage_options() method."""

    def test_populated_returns_copy(self):
        opts = {"AWS_ENDPOINT_URL": "http://minio:9000", "AWS_REGION": "us-east-1"}
        config = StorageConfig(protocol="s3", delta_options=opts)
        result = config.delta_storage_options()
        assert result == opts

    def test_returns_dict_copy_not_underlying_field(self):
        config = StorageConfig(protocol="s3", delta_options={"AWS_REGION": "us-east-1"})
        result = config.delta_storage_options()
        assert result is not None
        result["AWS_REGION"] = "us-west-2"  # mutate the copy
        # Underlying field unchanged — frozen-model contract preserved.
        assert config.delta_options == {"AWS_REGION": "us-east-1"}

    def test_empty_returns_none(self):
        config = StorageConfig(protocol="s3", delta_options={})
        assert config.delta_storage_options() is None


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

    def test_cannot_set_delta_options(self):
        config = StorageConfig()
        with pytest.raises(ValidationError):
            config.delta_options = {"AWS_REGION": "us-east-1"}


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
