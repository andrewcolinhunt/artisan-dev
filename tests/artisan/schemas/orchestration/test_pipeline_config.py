"""Tests for pipeline configuration defaults and validation."""

from __future__ import annotations

import tempfile

import pytest
from pydantic import ValidationError

from artisan.schemas.enums import CachePolicy, FailurePolicy
from artisan.schemas.orchestration.pipeline_config import PipelineConfig


class TestPipelineConfig:
    """Tests for the PipelineConfig model."""

    def test_create_minimal(self):
        """Test minimal PipelineConfig creation."""
        config = PipelineConfig(
            name="test",
            delta_root="/data/delta",
            staging_root="/data/staging",
        )
        assert config.name == "test"
        assert config.delta_root == "/data/delta"
        assert config.staging_root == "/data/staging"
        assert config.working_root == tempfile.gettempdir()
        assert config.failure_policy == FailurePolicy.CONTINUE
        assert config.cache_policy == CachePolicy.ALL_SUCCEEDED
        assert config.default_step_runner == "local"

    def test_create_full(self):
        """Test PipelineConfig with all fields."""
        config = PipelineConfig(
            name="full_test",
            delta_root="/custom/delta",
            staging_root="/custom/staging",
            working_root="/tmp/work",
            failure_policy="fail_fast",
            default_step_runner="slurm",
        )
        assert config.working_root == "/tmp/work"
        assert config.failure_policy == FailurePolicy.FAIL_FAST
        assert config.default_step_runner == "slurm"

    def test_pipeline_config_has_pipeline_run_id(self):
        """Test PipelineConfig has pipeline_run_id with empty default."""
        config = PipelineConfig(
            name="test",
            delta_root="/data/delta",
            staging_root="/data/staging",
        )
        assert config.pipeline_run_id == ""

    def test_pipeline_config_custom_pipeline_run_id(self):
        """Test PipelineConfig accepts custom pipeline_run_id."""
        config = PipelineConfig(
            name="test",
            delta_root="/data/delta",
            staging_root="/data/staging",
            pipeline_run_id="my_run_20260215_120000_abcd1234",
        )
        assert config.pipeline_run_id == "my_run_20260215_120000_abcd1234"

    def test_pipeline_config_rejects_removed_prefect_server(self):
        """Removed configuration must fail instead of being silently ignored."""
        with pytest.raises(ValidationError, match="prefect_server"):
            PipelineConfig(
                name="test",
                delta_root="/data/delta",
                staging_root="/data/staging",
                prefect_server=False,  # type: ignore[call-arg]
            )

    def test_default_compute_provider_removed(self):
        """The inert pipeline compute default is absent and rejected."""
        assert "default_compute_provider" not in PipelineConfig.model_fields
        assert "default_step_runner" in PipelineConfig.model_fields

        with pytest.raises(ValidationError, match="default_compute_provider"):
            PipelineConfig(
                name="test",
                delta_root="/data/delta",
                staging_root="/data/staging",
                default_compute_provider="modal",  # type: ignore[call-arg]
            )

    def test_string_coercion_via_pydantic(self):
        """Test that Pydantic coerces strings to enum members."""
        config = PipelineConfig(
            name="test",
            delta_root="/data/delta",
            staging_root="/data/staging",
            failure_policy="continue",
        )
        assert config.failure_policy == FailurePolicy.CONTINUE


@pytest.mark.parametrize("preserve", [False, True])
@pytest.mark.parametrize("recover", [False, True])
def test_staging_controls_are_independent(preserve: bool, recover: bool) -> None:
    config = PipelineConfig(
        name="staging",
        delta_root="/data/delta",
        staging_root="/data/staging",
        preserve_staging=preserve,
        recover_staging=recover,
    )
    assert config.preserve_staging is preserve
    assert config.recover_staging is recover
    assert config.skip_cache is False


def test_staging_defaults_recover_without_preserving_committed_files() -> None:
    config = PipelineConfig(
        name="staging", delta_root="/data/delta", staging_root="/data/staging"
    )
    assert config.recover_staging is True
    assert config.preserve_staging is False
