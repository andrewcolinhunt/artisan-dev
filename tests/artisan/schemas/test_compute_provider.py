"""Tests for Compute configuration model."""

from __future__ import annotations

import pytest

from artisan.schemas.operation_config.compute import (
    ComputeConfig,
    ComputeProvider,
    LocalComputeConfig,
    ModalComputeConfig,
)


class TestCompute:
    def test_defaults(self):
        compute_provider = ComputeProvider()
        assert compute_provider.active == "local"
        assert isinstance(compute_provider.local, LocalComputeConfig)

    def test_current_returns_active(self):
        compute_provider = ComputeProvider()
        current = compute_provider.current()
        assert isinstance(current, LocalComputeConfig)

    def test_current_unconfigured_raises(self):
        compute_provider = ComputeProvider(active="modal")
        with pytest.raises(ValueError, match="not configured"):
            compute_provider.current()

    def test_available_default(self):
        compute_provider = ComputeProvider()
        assert compute_provider.available() == ["local"]

    def test_model_copy_switch_active(self):
        compute_provider = ComputeProvider()
        updated = compute_provider.model_copy(update={"active": "modal"})
        assert updated.active == "modal"
        assert compute_provider.active == "local"

    def test_unknown_active_raises(self):
        with pytest.raises(ValueError, match="Unknown compute provider"):
            ComputeProvider(active="slurm")

    def test_round_trip(self):
        compute_provider = ComputeProvider()
        data = compute_provider.model_dump()
        restored = ComputeProvider.model_validate(data)
        assert restored == compute_provider

    def test_current_returns_correct_base_type(self):
        compute_provider = ComputeProvider()
        current = compute_provider.current()
        assert isinstance(current, ComputeConfig)
        assert isinstance(current, LocalComputeConfig)


class TestModalComputeConfig:
    """ModalComputeConfig now carries Modal-specific non-hardware fields only.

    Hardware fields (gpu, memory_gb, timeout) live on ComputeResources.
    """

    def test_required_image(self):
        config = ModalComputeConfig(image="my-registry/my-image:latest")
        assert config.image == "my-registry/my-image:latest"

    def test_defaults(self):
        config = ModalComputeConfig(image="img")
        assert config.retries == 3
        assert config.min_containers == 0
        assert config.max_containers is None
        assert config.scaledown_window is None
        assert config.image_registry_secret is None
        assert config.secrets == []
        assert config.volumes == {}
        assert config.env == {}
        # baked-by-default: no source overlay unless explicitly configured
        assert config.local_python_sources == []
        assert config.endpoint_url is None
        assert config.auth_secret is None
        assert config.poll_interval == 2.0
        assert config.output_store is None

    def test_endpoint_client_fields(self):
        config = ModalComputeConfig(
            image="img",
            endpoint_url="https://my-org--tool.modal.run",
            auth_secret="MY_PROXY_AUTH",
            poll_interval=0.5,
            output_store="s3://bucket/prefix",
        )
        assert config.endpoint_url == "https://my-org--tool.modal.run"
        assert config.auth_secret == "MY_PROXY_AUTH"
        assert config.poll_interval == 0.5
        assert config.output_store == "s3://bucket/prefix"

    def test_output_store_round_trips(self):
        config = ModalComputeConfig(image="img", output_store="s3://b/p")
        assert ModalComputeConfig.model_validate(config.model_dump()) == config

    @pytest.mark.parametrize("scheme", ["http", "https"])
    def test_output_store_rejects_presigned_put_urls(self, scheme):
        with pytest.raises(ValueError, match="per-request wire data"):
            ModalComputeConfig(
                image="img", output_store=f"{scheme}://bucket.s3/key?sig=x"
            )

    def test_poll_interval_must_be_positive(self):
        with pytest.raises(ValueError, match="poll_interval"):
            ModalComputeConfig(image="img", poll_interval=0)

    def test_custom_fields(self):
        config = ModalComputeConfig(
            image="img",
            retries=1,
            min_containers=2,
            max_containers=50,
            scaledown_window=120,
            image_registry_secret="my-secret",
            secrets=["hf-read", "aws-s3"],
            volumes={"/weights": "foundry-weights"},
            env={"HF_XET_HIGH_PERFORMANCE": "1"},
            local_python_sources=["artisan", "pipelines"],
        )
        assert config.retries == 1
        assert config.min_containers == 2
        assert config.max_containers == 50
        assert config.scaledown_window == 120
        assert config.image_registry_secret == "my-secret"
        assert config.secrets == ["hf-read", "aws-s3"]
        assert config.volumes == {"/weights": "foundry-weights"}
        assert config.env == {"HF_XET_HIGH_PERFORMANCE": "1"}
        assert config.local_python_sources == ["artisan", "pipelines"]

    def test_round_trip(self):
        config = ModalComputeConfig(
            image="img",
            retries=5,
            max_containers=10,
            secrets=["hf-read"],
            volumes={"/v": "vol"},
            env={"K": "V"},
        )
        data = config.model_dump()
        restored = ModalComputeConfig.model_validate(data)
        assert restored == config


class TestComputeWithModal:
    def test_modal_none_by_default(self):
        compute_provider = ComputeProvider()
        assert compute_provider.modal is None

    def test_available_includes_modal_when_set(self):
        compute_provider = ComputeProvider(
            modal=ModalComputeConfig(image="img"),
        )
        assert "modal" in compute_provider.available()
        assert "local" in compute_provider.available()

    def test_available_excludes_modal_when_none(self):
        compute_provider = ComputeProvider()
        assert "modal" not in compute_provider.available()

    def test_current_returns_modal_config(self):
        modal_config = ModalComputeConfig(image="img", retries=5)
        compute_provider = ComputeProvider(active="modal", modal=modal_config)
        current = compute_provider.current()
        assert isinstance(current, ModalComputeConfig)
        assert current.retries == 5

    def test_round_trip_with_modal(self):
        compute_provider = ComputeProvider(
            active="modal",
            modal=ModalComputeConfig(image="img", retries=2),
        )
        data = compute_provider.model_dump()
        restored = ComputeProvider.model_validate(data)
        assert restored == compute_provider
        assert isinstance(restored.modal, ModalComputeConfig)
