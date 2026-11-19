"""Derived wheel metadata and installed content must match their frozen source."""

from __future__ import annotations

import base64
import csv
import hashlib
import io
import json
import sysconfig
import venv
from pathlib import Path
from unittest.mock import patch
from zipfile import ZipFile

import pytest

from artisan.execution.environments.source import LocalPackageInput
from artisan.execution.environments.wheels import (
    verify_wheel_installation,
    verify_wheel_metadata,
    wheel_contents,
)


def _wheel(
    path: Path,
    *,
    name: str = "probe",
    version: str = "0.1.0",
    dependency: str = "packaging>=24",
    python: str = ">=3.12",
) -> Path:
    contents = {
        "probe/__init__.py": b"def main():\n    return 0\n",
        "probe-0.1.0.dist-info/METADATA": (
            f"Metadata-Version: 2.4\nName: {name}\nVersion: {version}\nRequires-Dist: {dependency}\nRequires-Python: {python}\n"
        ).encode(),
        "probe-0.1.0.dist-info/entry_points.txt": b"[console_scripts]\nProbeCLI = probe:main\n",
        "probe-0.1.0.dist-info/WHEEL": b"Wheel-Version: 1.0\nRoot-Is-Purelib: true\nTag: py3-none-any\n",
    }
    record = "probe-0.1.0.dist-info/RECORD"
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    for relative, content in contents.items():
        checksum = (
            base64.urlsafe_b64encode(hashlib.sha256(content).digest())
            .rstrip(b"=")
            .decode()
        )
        writer.writerow((relative, f"sha256={checksum}", len(content)))
    writer.writerow((record, "", ""))
    contents[record] = buffer.getvalue().encode()
    with ZipFile(path, "w") as archive:
        for relative, content in contents.items():
            archive.writestr(relative, content)
    return path


@pytest.fixture
def locked() -> LocalPackageInput:
    return LocalPackageInput(
        root=".",
        name="probe",
        version=None,
        requirements=("packaging>=24",),
        requires_python=">=3.12",
    )


def test_wheel_metadata_agrees_with_static_source_and_canonical_lock(
    tmp_path: Path, locked: LocalPackageInput
) -> None:
    wheel = _wheel(tmp_path / "probe.whl")
    verify_wheel_metadata(wheel, locked, source_version="0.1.0")
    assert "probe/__init__.py" in wheel_contents(wheel)


@pytest.mark.parametrize(
    ("field", "changed"),
    [
        ("name", "different"),
        ("version", "0.2.0"),
        ("dependency", "packaging>=25"),
        ("python", ">=3.13"),
    ],
)
def test_wheel_metadata_rejects_divergence(
    tmp_path: Path,
    locked: LocalPackageInput,
    field: str,
    changed: str,
) -> None:
    wheel = _wheel(tmp_path / "probe.whl", **{field: changed})
    with pytest.raises(ValueError, match="differs|differ"):
        verify_wheel_metadata(wheel, locked, source_version="0.1.0")


def test_wheel_requirements_compare_normalized_names_specifiers_and_markers(
    tmp_path: Path,
) -> None:
    wheel = _wheel(
        tmp_path / "probe.whl",
        dependency="Example_Name[extra] (>=1,<3); python_version >= '3.12'",
    )
    locked = LocalPackageInput(
        root=".",
        name="probe",
        version="0.1",
        requirements=('example-name[extra]<3,>=1; python_version >= "3.12"',),
        requires_python=">=3.12",
    )
    verify_wheel_metadata(wheel, locked, source_version="0.1.0")


def test_wheel_rejects_payload_not_covered_by_record(tmp_path: Path) -> None:
    wheel = _wheel(tmp_path / "probe.whl")
    with ZipFile(wheel, "a") as archive:
        archive.writestr("probe/unregistered.py", "unexpected")
    with pytest.raises(ValueError, match="exact file set"):
        wheel_contents(wheel)


@pytest.mark.parametrize("change", [None, "payload", "added", "wrapper", "editable"])
def test_installed_wheel_checks_actual_payload_membership_wrapper_and_editability(
    tmp_path: Path, change: str | None
) -> None:
    wheel = _wheel(tmp_path / "probe.whl")
    prefix = tmp_path / "prefix"
    site = prefix / "lib/python3.12/site-packages"
    for name, content in wheel_contents(wheel).items():
        path = site / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
    direct = site / "probe-0.1.0.dist-info/direct_url.json"
    direct.write_text(json.dumps({"dir_info": {"editable": change == "editable"}}))
    interpreter = prefix / "bin/python"
    interpreter.parent.mkdir()
    wrapper = interpreter.parent / "ProbeCLI"
    wrapper.write_text(f"#!{interpreter}\nfrom probe import main\nmain()\n")
    wrapper.chmod(0o755)
    if change == "payload":
        (site / "probe/__init__.py").write_text("changed")
    elif change == "added":
        (site / "probe/new.py").write_text("added")
    elif change == "wrapper":
        wrapper.write_text("#!/other/python\nfrom other import main\n")
    with patch("subprocess.check_output", return_value=str(site)):
        if change is None:
            verify_wheel_installation((wheel,), interpreter=interpreter)
        else:
            with pytest.raises(ValueError, match="differs|does not bind|noneditably"):
                verify_wheel_installation((wheel,), interpreter=interpreter)


@pytest.mark.parametrize("change", [None, "changed", "missing"])
def test_installed_wheel_verification_preserves_cold_prefix(
    tmp_path: Path, change: str | None
) -> None:
    prefix = tmp_path / "prefix"
    venv.EnvBuilder(with_pip=False, symlinks=True).create(prefix)
    interpreter = prefix / "bin/python"
    site = Path(
        sysconfig.get_path(
            "purelib", vars={"base": str(prefix), "platbase": str(prefix)}
        )
    )
    wheel = _wheel(tmp_path / "probe.whl")
    for name, content in wheel_contents(wheel).items():
        path = site / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
    (site / "probe-0.1.0.dist-info/direct_url.json").write_text("{}")
    wrapper = interpreter.parent / "ProbeCLI"
    wrapper.write_text(f"#!{interpreter}\nfrom probe import main\nmain()\n")
    wrapper.chmod(0o755)
    marker = tmp_path / "startup-ran"
    (site / "_startup_probe.py").write_text(
        f"with open({str(marker)!r}, 'w') as stream:\n    stream.write('started')\n"
    )
    (site / "startup.pth").write_text("import _startup_probe\n")
    payload = site / "probe/__init__.py"
    if change == "changed":
        payload.write_text("changed")
    elif change == "missing":
        payload.unlink()
    assert not list(prefix.rglob("__pycache__"))
    before = _prefix_entries(prefix)

    if change is None:
        verify_wheel_installation((wheel,), interpreter=interpreter)
    else:
        with pytest.raises(ValueError, match="Installed wheel payload differs"):
            verify_wheel_installation((wheel,), interpreter=interpreter)

    assert marker.read_text() == "started"
    assert _prefix_entries(prefix) == before
    assert not list(prefix.rglob("__pycache__"))


def _prefix_entries(prefix: Path) -> dict[str, tuple[int, int, int]]:
    return {
        path.relative_to(prefix).as_posix(): (
            entry.st_mode,
            entry.st_size,
            entry.st_mtime_ns,
        )
        for path in prefix.rglob("*")
        for entry in (path.lstat(),)
    }
