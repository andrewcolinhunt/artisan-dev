"""Validate derived wheel metadata and actual noneditable installed contents."""

from __future__ import annotations

import base64
import configparser
import csv
import hashlib
import io
import json
import os
import stat
import subprocess
import sys
from email.parser import BytesParser
from pathlib import Path, PurePosixPath
from zipfile import ZipFile

from packaging.requirements import Requirement
from packaging.specifiers import SpecifierSet
from packaging.utils import canonicalize_name
from packaging.version import Version

from artisan.execution.environments.source import LocalPackageInput, relative_path


class _EntryPoints(configparser.ConfigParser):
    def optionxform(self, optionstr: str) -> str:
        return optionstr


def verify_wheel_metadata(
    wheel: Path, locked: LocalPackageInput, *, source_version: str
) -> None:
    """Require the derived package to match canonical locked runtime requirements."""
    contents = wheel_contents(wheel)
    metadata_names = [name for name in contents if name.endswith(".dist-info/METADATA")]
    if len(metadata_names) != 1:
        msg = "Derived wheel must contain exactly one distribution metadata file"
        raise ValueError(msg)
    metadata = BytesParser().parsebytes(contents[metadata_names[0]])
    if canonicalize_name(metadata["Name"]) != locked.name:
        msg = "Derived wheel name differs from the canonical lock"
        raise ValueError(msg)
    version = Version(metadata["Version"])
    if version != Version(source_version) or (
        locked.version is not None and version != Version(locked.version)
    ):
        msg = "Derived wheel version differs from captured source or lock"
        raise ValueError(msg)
    if _requirements(metadata.get_all("Requires-Dist", [])) != _requirements(
        locked.requirements
    ):
        msg = "Derived wheel dependencies differ from the canonical lock"
        raise ValueError(msg)
    if SpecifierSet(metadata.get("Requires-Python", "")) != SpecifierSet(
        locked.requires_python or ""
    ):
        msg = "Derived wheel Python requirement differs from the canonical lock"
        raise ValueError(msg)


def wheel_contents(wheel: Path) -> dict[str, bytes]:
    """Verify a pure wheel's complete RECORD before using any installed payload."""
    return _wheel_contents(wheel, scripts=False)


def _wheel_contents(wheel: Path, *, scripts: bool) -> dict[str, bytes]:
    with ZipFile(wheel) as archive:
        contents = {}
        for entry in archive.infolist():
            if entry.is_dir():
                continue
            name = relative_path(entry.filename)
            if name in contents or stat.S_ISLNK(entry.external_attr >> 16):
                msg = "Derived wheel contains duplicate or symbolic-link payloads"
                raise ValueError(msg)
            if not scripts and any(
                part.endswith(".data") for part in PurePosixPath(name).parts
            ):
                msg = "Local source wheel data schemes are unsupported; supply an ordinary wheel"
                raise ValueError(msg)
            contents[name] = archive.read(entry)
    records = [name for name in contents if name.endswith(".dist-info/RECORD")]
    if len(records) != 1:
        msg = "Derived wheel requires exactly one RECORD"
        raise ValueError(msg)
    record_name = records[0]
    if scripts:
        for name in contents:
            _wheel_script_name(name, record_name)
    rows = list(csv.reader(io.StringIO(contents[record_name].decode())))
    if len(rows) != len(contents) or {row[0] for row in rows} != contents.keys():
        msg = "Derived wheel RECORD does not cover its exact file set"
        raise ValueError(msg)
    for name, checksum, size in rows:
        if name == record_name and not checksum and not size:
            continue
        content = contents[name]
        expected = (
            "sha256="
            + base64.urlsafe_b64encode(hashlib.sha256(content).digest())
            .rstrip(b"=")
            .decode()
        )
        if checksum != expected or size != str(len(content)):
            msg = "Derived wheel payload differs from RECORD"
            raise ValueError(msg)
    return contents


def verify_wheel_installation(wheels: tuple[Path, ...], *, interpreter: Path) -> None:
    """Check wheel payload, wrappers and installed application file sets.

    Ordinary wheels may include flat ``.data/scripts`` entries installed by
    Pixi on Linux. Other data schemes remain unsupported. Static source-wheel
    capture continues to require pure wheels. Script names must not overlap
    declared entrypoints, and Python script interpreter paths must not contain
    apostrophes or line breaks.
    """
    site = Path(
        subprocess.check_output(
            [
                str(interpreter),
                "-I",
                # Isolated mode ignores PYTHONDONTWRITEBYTECODE, including at startup.
                "-B",
                "-c",
                "import sysconfig; print(sysconfig.get_path('purelib'))",
            ],
            text=True,
            timeout=30,
        ).strip()
    )
    prefix = interpreter.parent.parent
    if not site.resolve().is_relative_to(prefix):
        msg = "Target Python site-packages escapes the prepared prefix"
        raise ValueError(msg)
    expected_payload: set[str] = set()
    for wheel in wheels:
        contents = _wheel_contents(wheel, scripts=True)
        record_name = next(
            name for name in contents if name.endswith(".dist-info/RECORD")
        )
        for name, content in contents.items():
            if name.endswith(".dist-info/RECORD"):
                continue
            script = _wheel_script_name(name, record_name)
            if script is not None:
                _verify_wheel_script(script, content, contents, site, interpreter)
                continue
            path = site / name
            if path.is_symlink() or not path.is_file() or path.read_bytes() != content:
                msg = f"Installed wheel payload differs from the derived wheel: {name}"
                raise ValueError(msg)
            if not any(
                part.endswith(".dist-info") for part in PurePosixPath(name).parts
            ):
                expected_payload.add(name)
        _verify_entrypoints(contents, interpreter)
        _verify_noneditable(contents, site)
    _verify_application_membership(site, expected_payload)


def _wheel_script_name(name: str, record_name: str) -> str | None:
    parts = PurePosixPath(name).parts
    if not any(part.endswith(".data") for part in parts):
        return None
    record_parts = PurePosixPath(record_name).parts
    if (
        len(record_parts) != 2
        or len(parts) != 3
        or parts[0] != record_parts[0].removesuffix(".dist-info") + ".data"
        or parts[1] != "scripts"
    ):
        msg = "Wheel installation verification supports only flat .data/scripts entries"
        raise ValueError(msg)
    return parts[2]


def _script_entrypoint_names(contents: dict[str, bytes]) -> set[str]:
    names: set[str] = set()
    for name, content in contents.items():
        if name.endswith(".dist-info/entry_points.txt"):
            parser = _EntryPoints(interpolation=None)
            parser.read_string(content.decode())
            for section in ("console_scripts", "gui_scripts"):
                if parser.has_section(section):
                    names.update(parser.options(section))
    return names


def _installed_script_bytes(content: bytes, interpreter: Path) -> bytes:
    if sys.platform != "linux":
        msg = "Wheel .data/scripts verification is supported only on Linux"
        raise ValueError(msg)
    if not content.startswith(b"#!python"):
        return content
    executable = str(interpreter).encode()
    if any(character in executable for character in (b"'", b"\n", b"\r")):
        msg = "Unsupported quoting in target Python script path"
        raise ValueError(msg)
    shebang = b"#!" + executable
    if len(executable) + 3 > 127 or b" " in executable:
        shebang = b"#!/bin/sh\n'''exec' '" + executable + b"' \"$0\" \"$@\"\n' '''"
    # Pixi's installer replaces the entire first line, including any suffix.
    first_end = next(
        (i for i, byte in enumerate(content) if byte in (10, 13)), len(content)
    )
    body = content[first_end:]
    if body.startswith(b"\r\n"):
        body = body[2:]
    elif body:
        body = body[1:]
    return shebang + b"\n" + body


def _verify_wheel_script(
    name: str,
    content: bytes,
    contents: dict[str, bytes],
    site: Path,
    interpreter: Path,
) -> None:
    entrypoint_name = name
    for suffix in (".exe", "-script.py", ".pya"):
        if name.endswith(suffix):
            entrypoint_name = name.removesuffix(suffix)
            break
    if entrypoint_name in _script_entrypoint_names(contents):
        msg = "Wheel script overlaps a declared entrypoint"
        raise ValueError(msg)
    path = interpreter.parent / name
    expected = _installed_script_bytes(content, interpreter)
    if (
        path.is_symlink()
        or not path.is_file()
        or not path.resolve().is_relative_to(interpreter.parent.parent.resolve())
        or path.stat().st_mode & 0o111 != 0o111
        or path.read_bytes() != expected
    ):
        msg = f"Installed wheel script differs from the archive: {name}"
        raise ValueError(msg)
    _verify_script_record(contents, site, path, expected)


def _verify_script_record(
    contents: dict[str, bytes], site: Path, script: Path, content: bytes
) -> None:
    record_name = next(name for name in contents if name.endswith(".dist-info/RECORD"))
    record_path = site / record_name
    if (
        record_path.is_symlink()
        or not record_path.is_file()
        or not record_path.resolve().is_relative_to(site.resolve())
    ):
        msg = "Installed wheel script requires an ordinary RECORD file"
        raise ValueError(msg)
    rows = list(csv.reader(io.StringIO(record_path.read_text())))
    relative = Path(os.path.relpath(script, site)).as_posix()
    checksum = (
        "sha256="
        + base64.urlsafe_b64encode(hashlib.sha256(content).digest())
        .rstrip(b"=")
        .decode()
    )
    if any(len(row) != 3 for row in rows) or [
        row for row in rows if row[0] == relative
    ] != [[relative, checksum, str(len(content))]]:
        msg = "Installed wheel script differs from its RECORD"
        raise ValueError(msg)


def _verify_application_membership(site: Path, expected: set[str]) -> None:
    actual = set()
    for root in {PurePosixPath(name).parts[0] for name in expected}:
        entry = site / root
        if entry.is_file():
            actual.add(root)
        else:
            actual.update(
                path.relative_to(site).as_posix()
                for path in entry.rglob("*")
                if path.is_file() and "__pycache__" not in path.parts
            )
    if actual != expected:
        msg = "Installed application file set differs from derived wheels"
        raise ValueError(msg)


def _verify_noneditable(contents: dict[str, bytes], site: Path) -> None:
    metadata = next(name for name in contents if name.endswith(".dist-info/METADATA"))
    direct_url = site / PurePosixPath(metadata).parent / "direct_url.json"
    info = json.loads(direct_url.read_bytes())
    if info.get("dir_info", {}).get("editable", False):
        msg = "Derived packages must be installed noneditably"
        raise ValueError(msg)


def _verify_entrypoints(contents: dict[str, bytes], interpreter: Path) -> None:
    for name, content in contents.items():
        if not name.endswith(".dist-info/entry_points.txt"):
            continue
        parser = _EntryPoints(interpolation=None)
        parser.read_string(content.decode())
        for script, target in (
            parser.items("console_scripts")
            if parser.has_section("console_scripts")
            else ()
        ):
            relative_path(script)
            path = interpreter.parent / script
            if not path.is_file() or not path.stat().st_mode & 0o111:
                msg = "Derived wheel console entrypoint was not installed"
                raise ValueError(msg)
            module, _, function = target.partition(":")
            wrapper = path.read_text()
            if (
                str(interpreter) not in wrapper
                or f"from {module} import {function.split('.')[0]}" not in wrapper
            ):
                msg = "Installed console wrapper does not bind the expected Python and entrypoint"
                raise ValueError(msg)


def _requirements(values: tuple[str, ...] | list[str]) -> set[Requirement]:
    return {Requirement(value) for value in values}
