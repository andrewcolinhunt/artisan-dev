"""Measure the selected configuration and legacy GPU binds of Apptainer 1.5.4."""

from __future__ import annotations

import glob
import hashlib
import json
import os
import re
import shutil
import subprocess
from pathlib import Path

from artisan.execution.environments.apptainer_elf import elf_machine as _elf_machine
from artisan.schemas.execution.worker_transport import PayloadFile

_LAUNCH_PATH = os.defpath
_INHERITED_LIBRARIES = Path("/.singularity.d/libs")
_DEFAULT_BINARY_PATH = (
    "$PATH:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
)


def capture_apptainer_payload(
    executable: Path, *, nvidia: bool
) -> tuple[PayloadFile, ...]:
    """Measure selected files without starting a container or querying GPU placement."""
    build = _build_configuration(executable)
    config_bytes = Path(build["APPTAINER_CONF_FILE"]).read_bytes()
    config = _read_config(config_bytes)
    _validate_configuration(config, nvidia=nvidia)
    _validate_plugins(Path(build["PLUGIN_ROOTDIR"]))
    helper_path = _binary_path(
        config.get("binary path", _DEFAULT_BINARY_PATH), build["LIBEXECDIR"]
    )
    records = [
        _bytes_record("config/apptainer.conf", config_bytes),
        *_file_records(_launcher_helpers(helper_path, build["LIBEXECDIR"])),
    ]
    if not nvidia:
        return tuple(sorted(records, key=lambda record: record.path))
    if _INHERITED_LIBRARIES.exists() and any(_INHERITED_LIBRARIES.iterdir()):
        msg = "Prepared Apptainer GPU execution does not support inherited container libraries"
        raise ValueError(msg)
    library_bytes = (Path(build["APPTAINER_CONFDIR"]) / "nvliblist.conf").read_bytes()
    records.append(_bytes_record("gpu/config/nvliblist.conf", library_bytes))
    ldconfig = shutil.which("ldconfig.real", path=helper_path) or shutil.which(
        "ldconfig", path=helper_path
    )
    if ldconfig is None:
        msg = "Apptainer GPU resolution requires the selected ldconfig executable"
        raise ValueError(msg)
    sources = {"gpu/resolver/ldconfig": Path(ldconfig)}
    machine = _elf_machine(executable)
    if machine is None:
        msg = "Apptainer launcher is not a valid ELF executable"
        raise ValueError(msg)
    sources.update(
        _resolve_payload(
            _library_entries(library_bytes), _library_cache(ldconfig), machine
        )
    )
    records.extend(_file_records(sources))
    return tuple(sorted(records, key=lambda record: record.path))


def _launcher_helpers(search_path: str, libexec: str) -> dict[str, Path]:
    sources = {"starter": Path(libexec) / "apptainer/bin/starter"}
    for name in ("mksquashfs", "unsquashfs"):
        selected = shutil.which(name, path=search_path)
        if selected is None:
            msg = f"Apptainer requires the selected {name} executable"
            raise ValueError(msg)
        sources[name] = Path(selected)
    if selected := shutil.which("fuse-overlayfs", path=search_path):
        sources["fuse-overlayfs"] = Path(selected)
    return sources


def _file_records(sources: dict[str, Path]) -> tuple[PayloadFile, ...]:
    records = []
    digests: dict[Path, str] = {}
    for name, source in sources.items():
        if not source.is_file():
            msg = f"Apptainer selected payload must be a regular file: {source}"
            raise ValueError(msg)
        resolved = source.resolve(strict=True)
        if resolved not in digests:
            with resolved.open("rb") as stream:
                digests[resolved] = hashlib.file_digest(stream, "sha256").hexdigest()
        records.append(PayloadFile(path=name, sha256=digests[resolved]))
    return tuple(sorted(records, key=lambda record: record.path))


def _build_configuration(executable: Path) -> dict[str, str]:
    output = _query([str(executable), "buildcfg"])
    values = dict(line.split("=", 1) for line in output.splitlines() if "=" in line)
    names = ("APPTAINER_CONF_FILE", "APPTAINER_CONFDIR", "LIBEXECDIR", "PLUGIN_ROOTDIR")
    if values.get("APPTAINER_SUID_INSTALL") != "0" or any(
        name not in values or not Path(values[name]).is_absolute() for name in names
    ):
        msg = (
            "Prepared Apptainer execution requires an explicit non-setuid installation"
        )
        raise ValueError(msg)
    return values


def _query(arguments: list[str]) -> str:
    return subprocess.check_output(
        arguments,
        text=True,
        env={"PATH": _LAUNCH_PATH, "LANG": "C.UTF-8"},
        timeout=10,
    )


def _bytes_record(name: str, value: bytes) -> PayloadFile:
    return PayloadFile(path=name, sha256=hashlib.sha256(value).hexdigest())


def _read_config(content: bytes) -> dict[str, str]:
    directives: dict[str, str] = {}
    pattern = r"^\s*([a-zA-Z _-]+)[ \t]*=[ \t]*(.*)$"
    for match in re.finditer(pattern, content.decode(), flags=re.MULTILINE | re.ASCII):
        value = match[2].strip()
        if value:
            directives.setdefault(match[1].strip(), value.split(",", 1)[0])
    return directives


def _validate_configuration(config: dict[str, str], *, nvidia: bool) -> None:
    required = {
        "always use nv": "no",
        "always use rocm": "no",
        "user bind control": "yes",
    }
    if nvidia:
        required["use nvidia-container-cli"] = "no"
    for name, expected in required.items():
        if config.get(name, expected) != expected:
            msg = f"Prepared Apptainer execution requires {name} = {expected}"
            raise ValueError(msg)


def _validate_plugins(root: Path) -> None:
    # Enabled hooks can replace the engine configuration after GPU resolution.
    for path in root.glob("*.meta"):
        value = json.loads(path.read_bytes())
        if not isinstance(value, dict):
            msg = "Apptainer plugin metadata must be a JSON object"
            raise ValueError(msg)
        fields = {key.lower(): item for key, item in value.items()}
        if fields.get("enabled", False) is not False:
            msg = (
                "Prepared Apptainer execution does not support enabled runtime plugins"
            )
            raise ValueError(msg)


def _binary_path(value: str, libexec: str) -> str:
    internal = str(Path(libexec) / "apptainer/bin") + os.pathsep
    if "$PATH:" not in value:
        return internal + value
    return value.replace("$PATH:", internal + _LAUNCH_PATH + os.pathsep, 1)


def _library_entries(content: bytes) -> tuple[str, ...]:
    lines = content.split(b"\n")
    if any(len(line) >= 65536 for line in lines):
        msg = "Apptainer GPU configuration exceeds its line length limit"
        raise ValueError(msg)
    entries = (line.decode().strip() for line in lines)
    return tuple(line for line in entries if line and not line.startswith("#"))


def _library_cache(ldconfig: str) -> dict[str, Path]:
    cache: dict[str, Path] = {}
    output = _query([ldconfig, "-p"])
    for match in re.finditer(
        r"^(.*)\s*\(.*\)\s*=>\s*(.*)$", output, flags=re.MULTILINE
    ):
        cache.setdefault(match[1].strip(), Path(match[2].strip()))
    return cache


def _resolve_payload(
    entries: tuple[str, ...], cache: dict[str, Path], machine: int
) -> dict[str, Path]:
    sources: dict[str, Path] = {}
    libraries: set[str] = set()
    for entry in entries:
        path = Path(entry)
        if ".so" in entry:
            candidates: list[Path] = []
            if path.is_absolute():
                candidates.extend(_absolute_library(path, machine))
            else:
                for name, source in cache.items():
                    if (
                        name.startswith(entry)
                        and name not in libraries
                        and _elf_machine(source) == machine
                    ):
                        libraries.add(name)
                        candidates.append(source)
            for source in candidates:
                _add_source(sources, ".singularity.d/libs/" + source.name, source)
        elif path.is_absolute():
            if path.exists():
                _add_source(sources, str(path).lstrip("/"), path)
        elif resolved := shutil.which(entry, path=_LAUNCH_PATH):
            source = Path(resolved)
            _add_source(sources, "usr/bin/" + source.name, source)
    return sources


def _absolute_library(path: Path, machine: int) -> tuple[Path, ...]:
    if _elf_machine(path) != machine:
        return ()
    sources = [path]
    for name in sorted(glob.glob(str(path).split(".so", 1)[0] + ".so*")):
        candidate = Path(name)
        if candidate.is_symlink():
            try:
                if candidate.resolve(strict=True) == path:
                    sources.append(candidate)
            except (OSError, RuntimeError):
                continue
    return tuple(sources)


def _add_source(sources: dict[str, Path], destination: str, source: Path) -> None:
    key = "gpu/rootfs/" + destination
    previous = sources.get(key)
    if previous is not None and previous.resolve(strict=True) != source.resolve(
        strict=True
    ):
        msg = (
            f"Apptainer GPU sources compete for the same container path: {destination}"
        )
        raise ValueError(msg)
    sources[key] = source
