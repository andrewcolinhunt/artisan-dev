"""Tests for step_runner resolution and registry."""

from __future__ import annotations

import pytest

from artisan.orchestration.runners import (
    LocalRunner,
    Runner,
    RunnerBase,
    resolve_runner,
)


class TestRunnerNamespace:
    def test_local_is_local_runner(self) -> None:
        assert isinstance(Runner.LOCAL, LocalRunner)


class TestResolveRunner:
    def test_resolve_string_local(self) -> None:
        result = resolve_runner("local")
        assert isinstance(result, LocalRunner)

    def test_passthrough_instance(self) -> None:
        step_runner = LocalRunner(default_max_workers=8)
        result = resolve_runner(step_runner)
        assert result is step_runner

    def test_unknown_string_raises(self) -> None:
        with pytest.raises(ValueError, match="Unknown step_runner: 'kubernetes'"):
            resolve_runner("kubernetes")

    @pytest.mark.parametrize("name", ["slurm", "slurm_intra"])
    def test_removed_builtin_string_raises(self, name: str) -> None:
        with pytest.raises(ValueError, match=f"Unknown step_runner: {name!r}"):
            resolve_runner(name)

    def test_passthrough_preserves_custom_config(self) -> None:
        step_runner = LocalRunner(default_max_workers=16)
        result = resolve_runner(step_runner)
        assert result._default_max_workers == 16

    def test_all_runners_are_runner_base(self) -> None:
        assert isinstance(resolve_runner("local"), RunnerBase)
