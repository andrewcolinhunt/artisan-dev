"""Explicit preparation reuses content-keyed managed installations."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from artisan.execution.environments.native import (
    lookup_native_environment,
    prepare_native_environment,
)
from artisan.execution.environments.native_target import verify_native_environment
from artisan.schemas.operation_config.environment import Environment


@pytest.fixture
def declaration(tmp_path: Path) -> Environment:
    source = tmp_path / "source"
    source.mkdir()
    (source / "pixi.toml").write_text(
        '[workspace]\nname="example"\nplatforms=["linux-64"]\n'
    )
    (source / "pixi.lock").write_text(
        json.dumps(
            {
                "version": 7,
                "environments": {"default": {"packages": {"linux-64": []}}},
                "packages": [],
            }
        )
    )
    return Environment.from_pixi(source / "pixi.toml")


def test_prepare_once_reuses_target_across_declarations_and_preserves_old_cache(
    declaration: Environment, tmp_path: Path
) -> None:
    cache = tmp_path / "cache"
    old = cache / "native/old/.artisan-native.json"
    old.parent.mkdir(parents=True)
    old.write_text('{"schema_version": 1, "files": []}')

    def install(manifest: Path, name: str, *, skip: tuple[str, ...] = ()) -> None:
        assert not skip
        prefix = manifest.parent / ".pixi/envs" / name
        prefix.mkdir(parents=True)

    with (
        patch("artisan.execution.environments.native.pixi_version"),
        patch("artisan.execution.environments.native._validate_manifest"),
        patch(
            "artisan.execution.environments.native._install_locked", side_effect=install
        ) as installer,
    ):
        first = prepare_native_environment(declaration, cache_root=cache)
        second = prepare_native_environment(declaration, cache_root=cache)
        copied = tmp_path / "copied"
        copied.mkdir()
        for name in ("pixi.toml", "pixi.lock"):
            (copied / name).write_bytes(
                (Path(declaration.manifest).parent / name).read_bytes()
            )
        third = prepare_native_environment(
            Environment.from_pixi(copied / "pixi.toml"), cache_root=cache
        )
        assert first == second == third
        assert installer.call_count == 1
    assert first.workspace.is_relative_to(cache / "native-v2")
    assert old.read_text() == '{"schema_version": 1, "files": []}'


def test_bound_target_survives_authored_change_but_next_lookup_requires_preparation(
    declaration: Environment, tmp_path: Path
) -> None:
    def install(manifest: Path, name: str, *, skip: tuple[str, ...] = ()) -> None:
        (manifest.parent / ".pixi/envs" / name).mkdir(parents=True)

    with (
        patch("artisan.execution.environments.native.pixi_version"),
        patch("artisan.execution.environments.native._validate_manifest"),
        patch(
            "artisan.execution.environments.native._install_locked", side_effect=install
        ),
    ):
        target = prepare_native_environment(declaration, cache_root=tmp_path / "cache")
    manifest = Path(declaration.manifest)
    manifest.write_text(manifest.read_text() + "# changed declared source\n")
    verify_native_environment(target)
    with pytest.raises(ValueError, match="prepare explicitly"):
        lookup_native_environment(declaration, cache_root=tmp_path / "cache")
    manifest.unlink()
    verify_native_environment(target)
