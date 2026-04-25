"""Tests for step_runner resolution and registry."""

from __future__ import annotations

import pytest

from artisan.orchestration.runners import (
    LocalRunner,
    Runner,
    RunnerBase,
    SlurmIntraRunner,
    SlurmRunner,
    resolve_runner,
)


class TestBackendNamespace:
    def test_local_is_local_backend(self) -> None:
        assert isinstance(Runner.LOCAL, LocalRunner)

    def test_slurm_is_slurm_backend(self) -> None:
        assert isinstance(Runner.SLURM, SlurmRunner)

    def test_slurm_intra_is_slurm_intra_backend(self) -> None:
        assert isinstance(Runner.SLURM_INTRA, SlurmIntraRunner)


class TestResolveBackend:
    def test_resolve_string_local(self) -> None:
        result = resolve_runner("local")
        assert isinstance(result, LocalRunner)

    def test_resolve_string_slurm(self) -> None:
        result = resolve_runner("slurm")
        assert isinstance(result, SlurmRunner)

    def test_resolve_string_slurm_intra(self) -> None:
        result = resolve_runner("slurm_intra")
        assert isinstance(result, SlurmIntraRunner)

    def test_passthrough_instance(self) -> None:
        step_runner = LocalRunner(default_max_workers=8)
        result = resolve_runner(step_runner)
        assert result is step_runner

    def test_unknown_string_raises(self) -> None:
        with pytest.raises(ValueError, match="Unknown step_runner: 'kubernetes'"):
            resolve_runner("kubernetes")

    def test_passthrough_preserves_custom_config(self) -> None:
        step_runner = LocalRunner(default_max_workers=16)
        result = resolve_runner(step_runner)
        assert result._default_max_workers == 16

    def test_all_backends_are_backend_base(self) -> None:
        for name in ("local", "slurm", "slurm_intra"):
            assert isinstance(resolve_runner(name), RunnerBase)
