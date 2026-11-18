"""Canonical environment capture includes declared files and rejects escapes."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch
from zipfile import Path as ZipPath
from zipfile import ZipFile

import pytest

from artisan.execution.environments.source import (
    capture_environment_source,
    read_environment_inputs,
)
from artisan.schemas.operation_config.environment import Environment


@pytest.fixture
def source(tmp_path: Path) -> Path:
    (tmp_path / "pixi.toml").write_text(
        '[workspace]\nname="example"\nplatforms=["linux-64"]\n'
        '[activation]\nscripts=["activate.sh"]\n'
        '[tool.dexterity-artisan.environment]\nassets=["data/config.json"]\n'
    )
    (tmp_path / "activate.sh").write_text("export EXAMPLE=literal\n")
    (tmp_path / "activate.sh").chmod(0o755)
    (tmp_path / "data").mkdir()
    (tmp_path / "data/config.json").write_text('{"scale":3}')
    (tmp_path / ".env").write_text("UNRELATED=excluded")
    (tmp_path / "pixi.lock").write_text(
        json.dumps(
            {
                "version": 7,
                "environments": {"default": {"packages": {"linux-64": []}}},
                "packages": [],
            }
        )
    )
    return tmp_path


def test_declaration_requires_explicit_source_and_does_not_read_or_provision() -> None:
    with patch.object(Path, "read_bytes", side_effect=AssertionError("read")):
        declaration = Environment.from_pixi(Path("/synthetic/pixi.toml"), name="tool-a")
    assert declaration.name == "tool-a"
    with pytest.raises(ValueError, match="absolute"):
        Environment.from_pixi("pixi.toml")
    with pytest.raises(ValueError, match="pixi.toml or pyproject.toml"):
        Environment.from_pixi(Path("/synthetic/requirements.txt"))


def test_source_captures_exact_declared_closure_and_preserves_original_bytes(
    source: Path, tmp_path: Path
) -> None:
    declaration = Environment.from_pixi(source / "pixi.toml")
    inputs = read_environment_inputs(declaration, platform="linux-64")
    captured = capture_environment_source(inputs)
    assert [item.path for item in captured.source.files] == [
        "activate.sh",
        "data/config.json",
        "pixi.lock",
        "pixi.toml",
    ]
    original = (source / "pixi.toml").read_bytes()
    (source / "pixi.toml").write_text("changed")
    destination = tmp_path / "captured"
    captured.write(destination)
    assert (destination / "pixi.toml").read_bytes() == original
    assert not (destination / ".env").exists()
    assert (destination / "activate.sh").stat().st_mode & 0o111 == 0o111


@pytest.mark.parametrize(
    "asset",
    [
        "../outside",
        "/outside",
        "C:/outside",
        "data/*.json",
        "data",
        "data//config.json",
    ],
)
def test_source_rejects_nonfile_or_noncanonical_asset_inputs(
    source: Path, asset: str
) -> None:
    manifest = source / "pixi.toml"
    manifest.write_text(manifest.read_text().replace("data/config.json", asset))
    with pytest.raises(ValueError, match="relative paths|regular file"):
        inputs = read_environment_inputs(
            Environment.from_pixi(manifest), platform="linux-64"
        )
        capture_environment_source(inputs)


def test_source_rejects_symlink_input(source: Path) -> None:
    asset = source / "data/config.json"
    asset.unlink()
    asset.symlink_to(source / ".env")
    inputs = read_environment_inputs(
        Environment.from_pixi(source / "pixi.toml"), platform="linux-64"
    )
    with pytest.raises(ValueError, match="symbolic links"):
        capture_environment_source(inputs)


@pytest.mark.parametrize(
    "config",
    [
        'assets=["activate.sh", "activate.sh"]',
        'asset=["activate.sh"]',
        'assets="activate.sh"',
    ],
)
def test_source_strictly_validates_extension(source: Path, config: str) -> None:
    manifest = source / "pixi.toml"
    manifest.write_text(
        manifest.read_text().replace('assets=["data/config.json"]', config)
    )
    with pytest.raises(ValueError, match="duplicates|only the assets|list of strings"):
        read_environment_inputs(Environment.from_pixi(manifest), platform="linux-64")


def test_source_uses_same_extension_in_pyproject(source: Path) -> None:
    original = source / "pixi.toml"
    manifest = source / "pyproject.toml"
    manifest.write_text(
        original.read_text()
        .replace("[workspace]", "[tool.pixi.workspace]")
        .replace("[activation]", "[tool.pixi.activation]")
    )
    inputs = read_environment_inputs(
        Environment.from_pixi(manifest), platform="linux-64"
    )
    assert "data/config.json" in inputs.paths
    assert "pyproject.toml" in inputs.paths


def test_source_captures_feature_target_and_inline_environment_scripts(
    source: Path,
) -> None:
    manifest = source / "pixi.toml"
    manifest.write_text(
        manifest.read_text()
        + """
[target.unix.activation]
scripts=["unix.sh"]
[feature.tool.target.linux.activation]
scripts=["linux.sh"]
[feature.tool.target.win-64.activation]
scripts=["windows.bat"]
[environments.default]
features=["tool"]
activation={scripts=["inline.sh"]}
"""
    )
    for name in ("unix.sh", "linux.sh", "windows.bat", "inline.sh"):
        (source / name).write_text("synthetic activation")
    inputs = read_environment_inputs(
        Environment.from_pixi(manifest), platform="linux-64"
    )
    assert {"unix.sh", "linux.sh", "windows.bat", "inline.sh", "activate.sh"} <= set(
        inputs.paths
    )


def test_source_package_resource_snapshot_survives_closed_resource(
    source: Path,
) -> None:
    archive = source / "resources.zip"
    with ZipFile(archive, "w") as stream:
        for name in ("pixi.toml", "pixi.lock", "activate.sh", "data/config.json"):
            stream.write(source / name, f"package/{name}")
    with ZipFile(archive) as stream:
        resource = ZipPath(stream, "package/pixi.toml")
        inputs = read_environment_inputs(
            Environment.from_pixi(resource), platform="linux-64"
        )
        captured = capture_environment_source(inputs)
    captured.write(source / "prepared")
    assert (source / "prepared/data/config.json").read_text() == '{"scale":3}'


def test_source_infers_locked_local_wheel_and_requires_source_package_selection(
    source: Path,
) -> None:
    wheel = source / "example.whl"
    wheel.write_bytes(b"synthetic wheel")
    lock = json.loads((source / "pixi.lock").read_text())
    lock["environments"]["default"]["packages"]["linux-64"] = [
        {"pypi": "./example.whl"},
        {"pypi": "./"},
    ]
    lock["packages"] = [
        {"pypi": "./example.whl", "name": "external"},
        {"pypi": "./", "name": "example", "requires_dist": ["packaging==25"]},
    ]
    (source / "pixi.lock").write_text(json.dumps(lock))
    inputs = read_environment_inputs(
        Environment.from_pixi(source / "pixi.toml"), platform="linux-64"
    )
    assert "example.whl" in inputs.paths
    assert inputs.packages[0].root == "."
    with pytest.raises(ValueError, match="Every locked local package"):
        capture_environment_source(inputs)


@pytest.mark.parametrize("scheme", ["http", "https", "direct+http", "direct+https"])
@pytest.mark.parametrize("selected", [False, True])
def test_source_keeps_remote_wheels_in_the_exact_lock_without_local_capture(
    source: Path, scheme: str, selected: bool
) -> None:
    path = f"{scheme}://example.invalid/example-1.0-py3-none-any.whl#sha256={'a' * 64}"
    lock_path = source / "pixi.lock"
    lock = json.loads(lock_path.read_text())
    lock["environments"]["default"]["packages"]["linux-64"] = (
        [{"pypi": path}] if selected else []
    )
    lock["packages"] = [{"pypi": path, "name": "example", "sha256": "a" * 64}]
    original = json.dumps(lock).encode()
    lock_path.write_bytes(original)
    declaration = Environment.from_pixi(source / "pixi.toml")
    inputs = read_environment_inputs(declaration, platform="linux-64")
    assert inputs.packages == ()
    assert set(inputs.paths) == {
        "pixi.toml",
        "pixi.lock",
        "activate.sh",
        "data/config.json",
    }
    captured = capture_environment_source(inputs)
    contents = dict(
        zip(
            (item.path for item in captured.source.files),
            captured.contents,
            strict=True,
        )
    )
    assert contents["pixi.lock"] == original
    assert lock_path.read_bytes() == original
    changed_path = path.replace("a" * 64, "b" * 64)
    if selected:
        lock["environments"]["default"]["packages"]["linux-64"][0]["pypi"] = (
            changed_path
        )
    lock["packages"][0]["pypi"] = changed_path
    lock["packages"][0]["sha256"] = "b" * 64
    lock_path.write_text(json.dumps(lock))
    assert capture_environment_source(inputs).source.digest != captured.source.digest


@pytest.mark.parametrize("scheme", ["git+https", "git+http", "git+ssh"])
@pytest.mark.parametrize("selected", [False, True])
def test_source_captures_locked_git_revision_without_local_files(
    source: Path, scheme: str, selected: bool
) -> None:
    revision = "a" * 40
    path = f"{scheme}://example.invalid/project.git?rev={revision}#{revision}"
    lock_path = source / "pixi.lock"
    lock = json.loads(lock_path.read_text())
    lock["environments"]["default"]["packages"]["linux-64"] = (
        [{"pypi": path}] if selected else []
    )
    lock["packages"] = [{"pypi": path, "name": "example", "version": "1.0"}]
    original = json.dumps(lock).encode()
    lock_path.write_bytes(original)
    inputs = read_environment_inputs(
        Environment.from_pixi(source / "pixi.toml"), platform="linux-64"
    )
    assert inputs.packages == ()
    assert set(inputs.paths) == {
        "pixi.toml",
        "pixi.lock",
        "activate.sh",
        "data/config.json",
    }
    captured = capture_environment_source(inputs)
    records = dict(
        zip(
            (item.path for item in captured.source.files),
            captured.contents,
            strict=True,
        )
    )
    assert records["pixi.lock"] == original
    assert lock_path.read_bytes() == original
    lock_path.write_bytes(original.replace(revision.encode(), b"b" * 40))
    assert capture_environment_source(inputs).source.digest != captured.source.digest


@pytest.mark.parametrize(
    "path",
    [
        "direct+file:///outside.whl",
        "direct+ftp://example.invalid/example.whl",
        "direct+https:/example.invalid/example.whl",
        "file:///outside.whl",
        "git+file:///outside/project",
        "git+https:/example.invalid/project.git",
        "git+ftp://example.invalid/project.git",
        "./../outside.whl",
    ],
)
def test_source_rejects_unsupported_lock_urls_and_escaping_local_wheels(
    source: Path, path: str
) -> None:
    lock = json.loads((source / "pixi.lock").read_text())
    lock["environments"]["default"]["packages"]["linux-64"] = [{"pypi": path}]
    lock["packages"] = [{"pypi": path, "name": "example"}]
    (source / "pixi.lock").write_text(json.dumps(lock))
    with pytest.raises(ValueError, match="canonical relative paths"):
        read_environment_inputs(
            Environment.from_pixi(source / "pixi.toml"), platform="linux-64"
        )


def test_source_captures_only_static_metadata_of_unselected_local_dependencies(
    source: Path,
) -> None:
    package = source / "other"
    package.mkdir()
    (package / "pyproject.toml").write_text(
        '[project]\nname="other"\nversion="1.0"\n'
        '[build-system]\nrequires=["hatchling"]\nbuild-backend="hatchling.build"\n'
    )
    (package / "private.py").write_text("unselected source")
    lock = json.loads((source / "pixi.lock").read_text())
    lock["packages"] = [{"pypi": "./other", "name": "other"}]
    (source / "pixi.lock").write_text(json.dumps(lock))
    declaration = Environment.from_pixi(source / "pixi.toml")
    inputs = read_environment_inputs(declaration, platform="linux-64")
    assert inputs.packages == ()
    captured = capture_environment_source(inputs)
    assert "other/pyproject.toml" in {item.path for item in captured.source.files}
    assert "other/private.py" not in {item.path for item in captured.source.files}
    metadata = package / "pyproject.toml"
    metadata.write_text(
        metadata.read_text().replace('version="1.0"', 'dynamic=["version"]')
    )
    with pytest.raises(ValueError, match="static Hatch metadata"):
        read_environment_inputs(declaration, platform="linux-64")
