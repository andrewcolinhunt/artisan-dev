"""Derived wheel metadata and installed content must match their frozen source."""

from __future__ import annotations

import base64
import csv
import hashlib
import io
import json
import os
import sysconfig
import venv
from pathlib import Path
from unittest.mock import patch
from zipfile import ZipFile

import pytest

from artisan.execution.environments.source import LocalPackageInput
from artisan.execution.environments.wheels import (
    _installed_script_bytes,
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
    extra_files: dict[str, bytes] | None = None,
) -> Path:
    contents = {
        "probe/__init__.py": b"def main():\n    return 0\n",
        "probe-0.1.0.dist-info/METADATA": (
            f"Metadata-Version: 2.4\nName: {name}\nVersion: {version}\nRequires-Dist: {dependency}\nRequires-Python: {python}\n"
        ).encode(),
        "probe-0.1.0.dist-info/entry_points.txt": b"[console_scripts]\nProbeCLI = probe:main\n",
        "probe-0.1.0.dist-info/WHEEL": b"Wheel-Version: 1.0\nRoot-Is-Purelib: true\nTag: py3-none-any\n",
    }
    contents.update(extra_files or {})
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
@pytest.mark.parametrize("platform", ["linux", "darwin", "win32"])
def test_installed_wheel_checks_actual_payload_membership_wrapper_and_editability(
    tmp_path: Path, change: str | None, platform: str
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
    with (
        patch("subprocess.check_output", return_value=str(site)),
        patch("artisan.execution.environments.wheels.sys.platform", platform),
    ):
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


def _install_script_wheel(
    root: Path,
    *,
    script_name: str = "run-probe",
    source: bytes = b"#!python\nprint('probe')\n",
    prefix_name: str = "prefix",
) -> tuple[Path, Path, Path, Path]:
    member = f"probe-0.1.0.data/scripts/{script_name}"
    wheel = _wheel(root / "probe.whl", extra_files={member: source})
    with ZipFile(wheel) as archive:
        contents = {name: archive.read(name) for name in archive.namelist()}
    prefix = root / prefix_name
    interpreter = prefix / "bin/python"
    interpreter.parent.mkdir(parents=True)
    site = prefix / "lib/python3.12/site-packages"
    for name, content in contents.items():
        if name != member:
            path = site / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(content)
    (site / "probe-0.1.0.dist-info/direct_url.json").write_text("{}")
    (interpreter.parent / "ProbeCLI").write_text(
        f"#!{interpreter}\nfrom probe import main\nmain()\n"
    )
    (interpreter.parent / "ProbeCLI").chmod(0o755)
    script = interpreter.parent / script_name
    if source.startswith(b"#!python"):
        executable = str(interpreter)
        if len(executable.encode()) + 3 > 127 or " " in executable:
            header = f"#!/bin/sh\n'''exec' '{executable}' \"$0\" \"$@\"\n' '''\n"
        else:
            header = f"#!{executable}\n"
        installed = header.encode() + b"print('probe')\n"
    else:
        installed = source
    script.write_bytes(installed)
    script.chmod(0o755)
    record = "probe-0.1.0.dist-info/RECORD"
    rows = list(csv.reader(io.StringIO(contents[record].decode())))
    checksum = (
        base64.urlsafe_b64encode(hashlib.sha256(installed).digest())
        .rstrip(b"=")
        .decode()
    )
    rows = [
        [
            Path(os.path.relpath(script, site)).as_posix(),
            f"sha256={checksum}",
            str(len(installed)),
        ]
        if row[0] == member
        else row
        for row in rows
    ]
    with (site / record).open("w", newline="") as stream:
        csv.writer(stream).writerows(rows)
    return wheel, site, interpreter, script


def test_source_wheel_capture_still_rejects_script_data(
    tmp_path: Path, locked: LocalPackageInput
) -> None:
    wheel = _wheel(
        tmp_path / "probe.whl",
        extra_files={"probe-0.1.0.data/scripts/probe": b"#!python\npass\n"},
    )
    with pytest.raises(ValueError, match="data schemes are unsupported"):
        wheel_contents(wheel)
    with pytest.raises(ValueError, match="data schemes are unsupported"):
        verify_wheel_metadata(wheel, locked, source_version="0.1.0")


@pytest.mark.parametrize("prefix_name", ["prefix", "with spaces", "long-" + "a" * 140])
@pytest.mark.parametrize(
    "source", [b"#!python\nprint('probe')\n", b"#!/bin/sh\necho probe\n"]
)
def test_installed_data_script_checks_relocated_or_unchanged_bytes(
    tmp_path: Path, prefix_name: str, source: bytes
) -> None:
    wheel, site, interpreter, _ = _install_script_wheel(
        tmp_path, prefix_name=prefix_name, source=source
    )
    before = _prefix_entries(interpreter.parent.parent)
    with (
        patch("subprocess.check_output", return_value=str(site)),
        patch("artisan.execution.environments.wheels.sys.platform", "linux"),
    ):
        verify_wheel_installation((wheel,), interpreter=interpreter)
    assert _prefix_entries(interpreter.parent.parent) == before


@pytest.mark.parametrize(
    "change",
    [
        "body",
        "interpreter",
        "mode",
        "missing",
        "symlink",
        "record",
        "duplicate-record",
        "record-symlink",
    ],
)
def test_installed_data_script_rejects_byte_identity_and_record_changes(
    tmp_path: Path, change: str
) -> None:
    wheel, site, interpreter, script = _install_script_wheel(tmp_path)
    record = site / "probe-0.1.0.dist-info/RECORD"
    if change == "body":
        script.write_bytes(script.read_bytes().replace(b"probe", b"other"))
    elif change == "interpreter":
        script.write_bytes(
            script.read_bytes().replace(str(interpreter).encode(), b"/other/python")
        )
    elif change == "mode":
        script.chmod(0o700)
    elif change == "missing":
        script.unlink()
    elif change == "symlink":
        elsewhere = tmp_path / "elsewhere"
        script.rename(elsewhere)
        script.symlink_to(elsewhere)
    elif change == "record-symlink":
        elsewhere = tmp_path / "RECORD"
        record.rename(elsewhere)
        record.symlink_to(elsewhere)
    else:
        rows = list(csv.reader(io.StringIO(record.read_text())))
        selected = next(row for row in rows if row[0].endswith("/bin/run-probe"))
        if change == "record":
            selected[1] = "sha256=wrong"
        else:
            rows.append(selected)
        with record.open("w", newline="") as stream:
            csv.writer(stream).writerows(rows)
    with (
        patch("subprocess.check_output", return_value=str(site)),
        patch("artisan.execution.environments.wheels.sys.platform", "linux"),
        pytest.raises(ValueError, match="Installed wheel script"),
    ):
        verify_wheel_installation((wheel,), interpreter=interpreter)


@pytest.mark.parametrize(
    "name",
    [
        "probe-0.1.0.data/purelib/data.py",
        "probe-0.1.0.data/headers/header.h",
        "different-0.1.0.data/scripts/probe",
        "probe-0.1.0.data/scripts/nested/probe",
    ],
)
def test_installed_wheel_rejects_unsupported_data_layouts(
    tmp_path: Path, name: str
) -> None:
    wheel = _wheel(tmp_path / "probe.whl", extra_files={name: b"payload"})
    prefix = tmp_path / "prefix"
    with (
        patch("subprocess.check_output", return_value=str(prefix / "site-packages")),
        pytest.raises(ValueError, match="only flat .data/scripts"),
    ):
        verify_wheel_installation((wheel,), interpreter=prefix / "bin/python")


@pytest.mark.parametrize(
    "name", ["ProbeCLI", "ProbeCLI.exe", "ProbeCLI-script.py", "ProbeCLI.pya"]
)
def test_installed_data_script_rejects_entrypoint_overlap(
    tmp_path: Path, name: str
) -> None:
    wheel, site, interpreter, _ = _install_script_wheel(tmp_path, script_name=name)
    with (
        patch("subprocess.check_output", return_value=str(site)),
        pytest.raises(ValueError, match="overlaps a declared entrypoint"),
    ):
        verify_wheel_installation((wheel,), interpreter=interpreter)


@pytest.mark.parametrize(
    ("source", "body"),
    [
        (b"#!python\nbody\n", b"body\n"),
        (b"#!pythonw ignored\r\nbody\r\n", b"body\r\n"),
        (b"#!python suffix\rbody", b"body"),
        (b"#!python suffix", b""),
    ],
)
def test_script_relocation_replaces_the_complete_first_line(
    source: bytes, body: bytes
) -> None:
    with patch("artisan.execution.environments.wheels.sys.platform", "linux"):
        assert (
            _installed_script_bytes(source, Path("/env/bin/python"))
            == b"#!/env/bin/python\n" + body
        )


@pytest.mark.parametrize(("length", "wrapped"), [(124, False), (125, True)])
def test_script_relocation_counts_the_newline_in_linux_shebang_limit(
    length: int, wrapped: bool
) -> None:
    executable = "/" + "x" * (length - 1)
    expected = (
        f"#!/bin/sh\n'''exec' '{executable}' \"$0\" \"$@\"\n' '''\nbody"
        if wrapped
        else f"#!{executable}\nbody"
    ).encode()
    with patch("artisan.execution.environments.wheels.sys.platform", "linux"):
        assert _installed_script_bytes(b"#!python\nbody", Path(executable)) == expected


@pytest.mark.parametrize(
    "executable",
    ["/env'quote/bin/python", "/env\nline/bin/python", "/env\rline/bin/python"],
)
def test_script_relocation_rejects_unsupported_interpreter_quoting(
    executable: str,
) -> None:
    with (
        patch("artisan.execution.environments.wheels.sys.platform", "linux"),
        pytest.raises(ValueError, match="Unsupported quoting"),
    ):
        _installed_script_bytes(b"#!python\nbody", Path(executable))


@pytest.mark.parametrize("platform", ["darwin", "win32"])
def test_script_relocation_rejects_unqualified_platform(platform: str) -> None:
    with (
        patch("artisan.execution.environments.wheels.sys.platform", platform),
        pytest.raises(ValueError, match="only on Linux"),
    ):
        _installed_script_bytes(b"#!python\nbody", Path("/env/bin/python"))
