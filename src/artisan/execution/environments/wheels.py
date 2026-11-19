"""Validate derived wheel metadata and actual noneditable installed contents."""

from __future__ import annotations

import base64
import configparser
import csv
import hashlib
import io
import json
import stat
import subprocess
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
    with ZipFile(wheel) as archive:
        contents = {}
        for entry in archive.infolist():
            if entry.is_dir():
                continue
            name = relative_path(entry.filename)
            if name in contents or stat.S_ISLNK(entry.external_attr >> 16):
                msg = "Derived wheel contains duplicate or symbolic-link payloads"
                raise ValueError(msg)
            if any(part.endswith(".data") for part in PurePosixPath(name).parts):
                msg = "Local source wheel data schemes are unsupported; supply an ordinary wheel"
                raise ValueError(msg)
            contents[name] = archive.read(entry)
    records = [name for name in contents if name.endswith(".dist-info/RECORD")]
    if len(records) != 1:
        msg = "Derived wheel requires exactly one RECORD"
        raise ValueError(msg)
    record_name = records[0]
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
    """Check wheel payload, entrypoint wrappers and installed application file sets."""
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
        contents = wheel_contents(wheel)
        for name, content in contents.items():
            if name.endswith(".dist-info/RECORD"):
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
