"""Static source selection detects membership drift without rebuilding at lookup."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from artisan.execution.environments.local_packages import (
    LocalPackageSelection,
    select_local_package,
    validate_source_profile,
)
from artisan.execution.environments.native import (
    BINDING_FILENAME,
    _workspace,
    lookup_native_environment,
    prepare_native_environment,
    wheel_builder_digest,
)
from artisan.execution.environments.native_target import verify_native_environment
from artisan.execution.environments.source import (
    LocalPackageInput,
    capture_environment_source,
    read_environment_inputs,
)
from artisan.execution.environments.source_membership import source_membership
from artisan.schemas.execution.prepared_environment import (
    DerivedWheel,
    PreparedNativeEnvironment,
)
from artisan.schemas.operation_config.environment import Environment


@pytest.fixture
def local_source(tmp_path: Path) -> Path:
    root = tmp_path / "source"
    root.mkdir()
    (root / "pyproject.toml").write_text(
        '[project]\nname="probe"\nversion="0.1.0"\nrequires-python=">=3.12"\n'
        '[build-system]\nrequires=["hatchling==1.31.0"]\nbuild-backend="hatchling.build"\n'
        '[tool.hatch.build.targets.wheel]\npackages=["src/probe"]\n'
        '[tool.pixi.workspace]\nname="probe"\nplatforms=["linux-64"]\n'
    )
    (root / "src/probe").mkdir(parents=True)
    (root / "src/probe/__init__.py").write_text("value = 1\n")
    (root / "src/probe/ignored.txt").write_text("not part of the wheel")
    (root / ".gitignore").write_text("ignored*.txt\n")
    (root / "pixi.lock").write_text(
        json.dumps(
            {
                "version": 7,
                "environments": {
                    "default": {"packages": {"linux-64": [{"pypi": "./"}]}}
                },
                "packages": [
                    {"pypi": "./", "name": "probe", "requires_python": ">=3.12"}
                ],
            }
        )
    )
    return root


@pytest.fixture
def locked() -> LocalPackageInput:
    return LocalPackageInput(
        root=".", name="probe", version=None, requirements=(), requires_python=">=3.12"
    )


@pytest.fixture
def selected(local_source: Path, locked: LocalPackageInput) -> LocalPackageSelection:
    with patch(
        "artisan.execution.environments.local_packages.backend_command",
        return_value={
            "files": [
                {"source": "src/probe/__init__.py", "target": "probe/__init__.py"}
            ],
            "license_files": [],
        },
    ):
        return select_local_package(
            local_source, locked, builder_python=Path("/unused/python")
        )


@pytest.mark.parametrize(
    ("original", "replacement"),
    [
        ('version="0.1.0"', 'version="0.1.0"\nlicense-files=["licenses/**/*.txt"]'),
        ("hatchling.build", "custom.build"),
        ("hatchling==1.31.0", "hatchling<1.31"),
        ('version="0.1.0"', 'dynamic=["version"]'),
        ('packages=["src/probe"]', 'packages=["."]'),
        ('packages=["src/probe"]', 'packages=["src/probe"]\nsources=["src"]'),
        ('packages=["src/probe"]', 'packages=["src/probe"]\nhooks={custom={}}'),
    ],
)
def test_local_profile_rejects_unqualified_sources(
    local_source: Path, locked: LocalPackageInput, original: str, replacement: str
) -> None:
    manifest = local_source / "pyproject.toml"
    manifest.write_text(manifest.read_text().replace(original, replacement))
    with pytest.raises(ValueError, match="Unsupported|canonical relative"):
        validate_source_profile(local_source, locked)


def test_local_selection_maps_exact_payload_and_excludes_unselected_contents(
    local_source: Path, selected: LocalPackageSelection
) -> None:
    assert selected.editable_files(local_source) == {
        "probe/__init__.py": local_source / "src/probe/__init__.py"
    }
    assert "src/probe/ignored.txt" not in selected.source.files
    before = selected.source.membership_sha256
    (local_source / "src/probe/ignored.txt").write_text("different excluded bytes")
    assert source_membership(local_source, selected.source.scan_roots) == before
    (local_source / "src/probe/ignored-added.txt").write_text("new excluded filename")
    assert source_membership(local_source, selected.source.scan_roots) != before


def test_local_membership_ignores_python_cache_but_rejects_symlinks(
    local_source: Path, selected: LocalPackageSelection
) -> None:
    cache = local_source / "src/probe/__pycache__"
    cache.mkdir()
    (cache / "generated.pyc").write_bytes(b"generated")
    assert (
        source_membership(local_source, selected.source.scan_roots)
        == selected.source.membership_sha256
    )
    (local_source / "src/probe/linked.py").symlink_to("__init__.py")
    with pytest.raises(ValueError, match="symbolic links"):
        source_membership(local_source, selected.source.scan_roots)


def test_local_membership_detects_ignore_presence_and_type_changes(
    local_source: Path, selected: LocalPackageSelection
) -> None:
    (local_source / ".hgignore").write_text("new metadata")
    assert (
        source_membership(local_source, selected.source.scan_roots)
        != selected.source.membership_sha256
    )
    (local_source / ".hgignore").unlink()
    entry = local_source / "src/probe/ignored.txt"
    entry.unlink()
    entry.mkdir()
    assert (
        source_membership(local_source, selected.source.scan_roots)
        != selected.source.membership_sha256
    )


@pytest.fixture
def prepared_local_target(
    local_source: Path,
    selected: LocalPackageSelection,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> PreparedNativeEnvironment:
    monkeypatch.setattr(
        "artisan.execution.environments.native.native_platform", lambda: "linux-64"
    )
    declaration = Environment.from_pixi(local_source / "pyproject.toml")
    inputs = read_environment_inputs(declaration, platform="linux-64")
    captured = capture_environment_source(inputs, (selected.source,))
    cache_root = tmp_path / "cache"
    workspace = _workspace(cache_root, captured.source, "linux-64")
    captured.write(workspace)
    prefix = workspace / ".pixi/envs/default"
    (prefix / "bin").mkdir(parents=True)
    (prefix / "bin/tool").write_text("qualified payload")
    wheel_content = b"synthetic derived wheel evidence"
    wheel_directory = workspace / ".artisan-wheels"
    wheel_directory.mkdir()
    (wheel_directory / "probe.whl").write_bytes(wheel_content)
    target = PreparedNativeEnvironment(
        source=captured.source,
        platform="linux-64",
        workspace=workspace,
        prefix=prefix,
        wheels=(
            DerivedWheel(
                name="probe",
                version="0.1.0",
                source_root=".",
                source_digest="a" * 64,
                builder_digest=wheel_builder_digest("linux-64"),
                filename="probe.whl",
                sha256=hashlib.sha256(wheel_content).hexdigest(),
            ),
        ),
    )
    (workspace / BINDING_FILENAME).write_text(target.model_dump_json())
    return target


def test_lookup_source_package_never_executes_backend_or_installer(
    local_source: Path,
    prepared_local_target: PreparedNativeEnvironment,
    tmp_path: Path,
) -> None:
    target = prepared_local_target
    declaration = Environment.from_pixi(local_source / "pyproject.toml")
    cache_root = tmp_path / "cache"
    incomplete = target.model_dump()
    incomplete["wheels"] = ()
    with pytest.raises(ValueError, match="uniquely bind captured"):
        PreparedNativeEnvironment.model_validate(incomplete)
    with (
        patch("subprocess.run", side_effect=AssertionError("subprocess")),
        patch("subprocess.Popen", side_effect=AssertionError("subprocess")),
    ):
        assert lookup_native_environment(declaration, cache_root=cache_root) == target
        cache = local_source / "src/probe/__pycache__"
        cache.mkdir()
        (cache / "generated.pyc").write_bytes(b"generated")
        assert lookup_native_environment(declaration, cache_root=cache_root) == target
        added = local_source / "src/probe/new.py"
        added.write_text("new source")
        with pytest.raises(ValueError, match="prepare explicitly"):
            lookup_native_environment(declaration, cache_root=cache_root)
        added.unlink()
        (local_source / "src/probe/__init__.py").write_text("changed source")
        with pytest.raises(ValueError, match="prepare explicitly"):
            lookup_native_environment(declaration, cache_root=cache_root)


@pytest.mark.parametrize("change", ["backend-bytes", "hatch-version"])
def test_lookup_rejects_changed_builder_without_invalidating_admitted_target(
    local_source: Path,
    prepared_local_target: PreparedNativeEnvironment,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    change: str,
) -> None:
    target = prepared_local_target
    declaration = Environment.from_pixi(local_source / "pyproject.toml")
    cache_root = tmp_path / "cache"
    record = target.workspace / BINDING_FILENAME
    completed = record.read_bytes()
    if change == "backend-bytes":
        backend = tmp_path / "changed_backend.py"
        backend.write_text("# changed wheel builder\n")
        monkeypatch.setattr(
            "artisan.execution.environments.native.BACKEND_SCRIPT", backend
        )
    else:
        monkeypatch.setattr(
            "artisan.execution.environments.native.HATCH_VERSION", "1.31.1"
        )
    assert _workspace(cache_root, target.source, target.platform) != target.workspace
    with (
        patch("subprocess.run", side_effect=AssertionError("subprocess")),
        patch("subprocess.Popen", side_effect=AssertionError("subprocess")),
        pytest.raises(ValueError, match="prepare explicitly"),
    ):
        lookup_native_environment(declaration, cache_root=cache_root)
    verify_native_environment(target)
    assert record.read_bytes() == completed


def test_explicit_prepare_reuses_local_source_then_prepares_changed_builder(
    local_source: Path,
    selected: LocalPackageSelection,
    prepared_local_target: PreparedNativeEnvironment,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = prepared_local_target
    declaration = Environment.from_pixi(local_source / "pyproject.toml")
    cache_root = tmp_path / "cache"
    wheel = target.wheels[0]

    def install(manifest: Path, name: str, *, skip: tuple[str, ...] = ()) -> None:
        assert skip == ("probe",)
        (manifest.parent / ".pixi/envs" / name).mkdir(parents=True)

    with (
        patch("artisan.execution.environments.native.pixi_version"),
        patch("artisan.execution.environments.native._validate_manifest"),
        patch(
            "artisan.execution.environments.native._prepare_builder",
            return_value=Mock(),
        ),
        patch(
            "artisan.execution.environments.native.native_executable",
            return_value=Path("/unused/python"),
        ),
        patch(
            "artisan.execution.environments.native.select_local_package",
            return_value=selected,
        ),
        patch("artisan.execution.environments.native._build_wheels") as build,
        patch(
            "artisan.execution.environments.native._install_locked", side_effect=install
        ) as installer,
        patch(
            "artisan.execution.environments.native._install_wheels"
        ) as wheel_installer,
        patch("artisan.execution.environments.native.verify_wheel_installation"),
    ):
        assert prepare_native_environment(declaration, cache_root=cache_root) == target
        assert prepare_native_environment(declaration, cache_root=cache_root) == target
        build.assert_not_called()
        installer.assert_not_called()
        backend = tmp_path / "changed_backend.py"
        backend.write_text("# changed wheel builder\n")
        monkeypatch.setattr(
            "artisan.execution.environments.native.BACKEND_SCRIPT", backend
        )
        changed_wheel = wheel.model_copy(
            update={"builder_digest": wheel_builder_digest("linux-64")}
        )
        build.return_value = ((changed_wheel,), (Path("/unused/probe.whl"),))
        changed = prepare_native_environment(declaration, cache_root=cache_root)
        assert changed.workspace != target.workspace
        assert changed.digest != target.digest
        assert changed.source == target.source
        assert prepare_native_environment(declaration, cache_root=cache_root) == changed
        assert (
            build.call_count == installer.call_count == wheel_installer.call_count == 1
        )
    verify_native_environment(target)
    verify_native_environment(changed)


def test_membership_detects_new_root_license_without_reading_unrelated_bytes(
    local_source: Path,
    selected: LocalPackageSelection,
) -> None:
    before = selected.source.membership_sha256
    with patch.object(Path, "read_bytes", side_effect=AssertionError("content read")):
        assert source_membership(local_source, selected.source.scan_roots) == before
        (local_source / "LICENSE").write_text("new license")
        assert source_membership(local_source, selected.source.scan_roots) != before
