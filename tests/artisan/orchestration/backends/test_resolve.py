"""Tests for backend resolution and registry."""

from __future__ import annotations

import pytest

from artisan.orchestration.backends import (
    Backend,
    BackendBase,
    LocalBackend,
    SlurmBackend,
    resolve_backend,
)


class TestBackendNamespace:
    def test_local_is_local_backend(self) -> None:
        assert isinstance(Backend.LOCAL, LocalBackend)

    def test_slurm_is_slurm_backend(self) -> None:
        assert isinstance(Backend.SLURM, SlurmBackend)


class TestResolveBackend:
    def test_resolve_string_local(self) -> None:
        result = resolve_backend("local")
        assert isinstance(result, LocalBackend)

    def test_resolve_string_slurm(self) -> None:
        result = resolve_backend("slurm")
        assert isinstance(result, SlurmBackend)

    def test_passthrough_instance(self) -> None:
        backend = LocalBackend(default_max_workers=8)
        result = resolve_backend(backend)
        assert result is backend

    def test_unknown_string_raises(self) -> None:
        with pytest.raises(ValueError, match="Unknown backend: 'kubernetes'"):
            resolve_backend("kubernetes")

    def test_passthrough_preserves_custom_config(self) -> None:
        backend = LocalBackend(default_max_workers=16)
        result = resolve_backend(backend)
        assert result._default_max_workers == 16

    def test_all_backends_are_backend_base(self) -> None:
        for name in ("local", "slurm"):
            assert isinstance(resolve_backend(name), BackendBase)
