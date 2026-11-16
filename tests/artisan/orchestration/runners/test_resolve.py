"""Built-in placement is explicit; provider instances carry their own settings."""

from __future__ import annotations

from pathlib import Path

import pytest

from artisan.orchestration.runners import LocalRunner
from artisan.schemas import Environment


def test_local_runner_preserves_explicit_worker_and_tool_declarations(
    tmp_path: Path,
) -> None:
    environment = Environment.from_pixi(tmp_path / "pixi.toml", name="worker")
    runner = LocalRunner(environment=environment, cache_root=tmp_path / "cache")
    assert runner.environment is environment
    assert runner.cache_root == tmp_path / "cache"
    assert runner.tool_backend == "pixi"
    assert not runner.cache_root.exists()


@pytest.mark.parametrize("field", ["cache_root", "pixi", "system_env"])
def test_local_runner_rejects_ambiguous_launcher_paths(field: str) -> None:
    with pytest.raises(ValueError, match="absolute paths"):
        LocalRunner(**{field: Path("relative")})
