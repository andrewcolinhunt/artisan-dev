"""Tests for Compute configuration model."""

from __future__ import annotations

import pytest

from artisan.schemas.operation_config.compute import (
    Compute,
    ComputeConfig,
    LocalComputeConfig,
    ModalComputeConfig,
)


class TestCompute:
    def test_defaults(self):
        compute = Compute()
        assert compute.active == "local"
        assert isinstance(compute.local, LocalComputeConfig)

    def test_current_returns_active(self):
        compute = Compute()
        current = compute.current()
        assert isinstance(current, LocalComputeConfig)

    def test_current_unconfigured_raises(self):
        compute = Compute(active="modal")
        with pytest.raises(ValueError, match="not configured"):
            compute.current()

    def test_available_default(self):
        compute = Compute()
        assert compute.available() == ["local"]

    def test_model_copy_switch_active(self):
        compute = Compute()
        updated = compute.model_copy(update={"active": "modal"})
        assert updated.active == "modal"
        assert compute.active == "local"

    def test_round_trip(self):
        compute = Compute()
        data = compute.model_dump()
        restored = Compute.model_validate(data)
        assert restored == compute

    def test_current_returns_correct_base_type(self):
        compute = Compute()
        current = compute.current()
        assert isinstance(current, ComputeConfig)
        assert isinstance(current, LocalComputeConfig)


class TestModalComputeConfig:
    def test_required_image(self):
        config = ModalComputeConfig(image="my-registry/my-image:latest")
        assert config.image == "my-registry/my-image:latest"

    def test_defaults(self):
        config = ModalComputeConfig(image="img")
        assert config.gpu is None
        assert config.memory_gb == 8
        assert config.timeout == 3600
        assert config.retries == 3

    def test_custom_fields(self):
        config = ModalComputeConfig(
            image="img", gpu="A100", memory_gb=32, timeout=7200, retries=1
        )
        assert config.gpu == "A100"
        assert config.memory_gb == 32
        assert config.timeout == 7200
        assert config.retries == 1

    def test_round_trip(self):
        config = ModalComputeConfig(image="img", gpu="H100")
        data = config.model_dump()
        restored = ModalComputeConfig.model_validate(data)
        assert restored == config


class TestComputeWithModal:
    def test_modal_none_by_default(self):
        compute = Compute()
        assert compute.modal is None

    def test_available_includes_modal_when_set(self):
        compute = Compute(
            modal=ModalComputeConfig(image="img"),
        )
        assert "modal" in compute.available()
        assert "local" in compute.available()

    def test_available_excludes_modal_when_none(self):
        compute = Compute()
        assert "modal" not in compute.available()

    def test_current_returns_modal_config(self):
        modal_config = ModalComputeConfig(image="img", gpu="A10G")
        compute = Compute(active="modal", modal=modal_config)
        current = compute.current()
        assert isinstance(current, ModalComputeConfig)
        assert current.gpu == "A10G"

    def test_round_trip_with_modal(self):
        compute = Compute(
            active="modal",
            modal=ModalComputeConfig(image="img", gpu="A100"),
        )
        data = compute.model_dump()
        restored = Compute.model_validate(data)
        assert restored == compute
        assert isinstance(restored.modal, ModalComputeConfig)
