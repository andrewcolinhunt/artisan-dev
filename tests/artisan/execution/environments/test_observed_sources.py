"""Bound observed editable payloads to actual installed literal import roots."""

from __future__ import annotations

import importlib.metadata
import json
from pathlib import Path, PurePosixPath
from types import SimpleNamespace

import pytest

from artisan.execution.environments.observed_sources import observed_editable_sources
from artisan.execution.runtime_compatibility import capture_runtime_compatibility
from artisan.execution.worker_bootstrap import (
    execute_worker_call,
    serialize_worker_call,
)
from artisan.schemas.execution.worker_transport import TRANSPORT_PACKAGES


@pytest.fixture
def installed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Path, Path]:
    project, site = tmp_path / "project", tmp_path / "site"
    package = project / "src/application"
    package.mkdir(parents=True)
    site.mkdir()
    (package / "__init__.py").write_text("VALUE = 1\n")
    (project / ".env").write_text("PRIVATE_SENTINEL=outside_import_payload\n")
    pointer = site / "editable.pth"
    pointer.write_text(str(project / "src") + "\n")
    distribution = SimpleNamespace(
        version="1.0",
        files=(PurePosixPath("editable.pth"),),
        locate_file=lambda path: site / path,
        read_text=lambda name: json.dumps(
            {"url": project.as_uri(), "dir_info": {"editable": True}}
        )
        if name == "direct_url.json"
        else None,
    )
    monkeypatch.setattr(importlib.metadata, "distribution", lambda name: distribution)
    monkeypatch.setattr(
        importlib.metadata,
        "packages_distributions",
        lambda: {"application": ["demo-app"]},
    )
    return package, pointer


def test_observation_reads_only_import_payload_and_detects_additions(
    installed: tuple[Path, Path],
) -> None:
    package, _ = installed
    initial = observed_editable_sources(("demo-app",))
    assert initial == {"demo-app": {"application/__init__.py": package / "__init__.py"}}
    cache = package / "__pycache__"
    cache.mkdir()
    (cache / "generated.pyc").write_bytes(b"interpreter-cache")
    assert observed_editable_sources(("demo-app",)) == initial
    (package / "new.py").write_text("VALUE = 2\n")
    assert "application/new.py" in observed_editable_sources(("demo-app",))["demo-app"]


def test_literal_editable_discovers_imports_without_top_level_metadata(
    installed: tuple[Path, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    package, _ = installed
    monkeypatch.setattr(importlib.metadata, "packages_distributions", dict)
    assert observed_editable_sources(("demo-app",)) == {
        "demo-app": {"application/__init__.py": package / "__init__.py"}
    }


@pytest.mark.parametrize("hook", ["import arbitrary_hook", "relative/path", "/tmp"])
def test_observation_rejects_executable_or_unowned_mappings(
    installed: tuple[Path, Path], hook: str
) -> None:
    _, pointer = installed
    pointer.write_text(hook + "\n")
    with pytest.raises(ValueError, match="hooks"):
        observed_editable_sources(("demo-app",))


def test_observation_rejects_symlink_payload(installed: tuple[Path, Path]) -> None:
    package, _ = installed
    (package / "indirect.py").symlink_to(package / "__init__.py")
    with pytest.raises(ValueError, match="symbolic"):
        observed_editable_sources(("demo-app",))


@pytest.mark.parametrize("change", ["bytes", "added-source", "executable-hook"])
def test_worker_compatibility_does_not_inspect_editable_payloads(
    installed: tuple[Path, Path], monkeypatch: pytest.MonkeyPatch, change: str
) -> None:
    package, pointer = installed
    names = (*TRANSPORT_PACKAGES, "demo-app")
    monkeypatch.setattr(
        importlib.metadata,
        "packages_distributions",
        lambda: {"application": list(names)},
    )
    compatibility = capture_runtime_compatibility(names)
    call = serialize_worker_call(lambda: None, compatibility=compatibility)
    if change == "bytes":
        (package / "__init__.py").write_text("CHANGED = True\n")
    elif change == "added-source":
        (package / "extra.py").write_text("ADDED = True\n")
    else:
        pointer.write_text("import unqualified_hook\n")
    monkeypatch.setattr(
        Path, "read_bytes", lambda *_: pytest.fail("Compatibility read payload bytes")
    )
    monkeypatch.setattr(
        Path, "rglob", lambda *_: pytest.fail("Compatibility traversed package files")
    )
    assert execute_worker_call(call) is None


def test_worker_package_version_change_rejects_before_deserialization(
    installed: tuple[Path, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    names = (*TRANSPORT_PACKAGES, "demo-app")
    compatibility = capture_runtime_compatibility(names)
    call = serialize_worker_call(lambda: None, compatibility=compatibility)
    distribution = importlib.metadata.distribution("demo-app")
    monkeypatch.setattr(distribution, "version", "2.0")
    monkeypatch.setattr(
        "cloudpickle.loads",
        lambda *_: pytest.fail("Incompatible package reached deserialization"),
    )
    with pytest.raises(ValueError, match="prepared metadata"):
        execute_worker_call(call)
