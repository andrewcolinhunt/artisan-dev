"""Activate a captured Pixi worker before the SDK or application imports.

This file is executed directly with isolated stdlib Python, not imported through
artisan_modal. It contains no authored environment recipe or provider credentials.
"""

from __future__ import annotations

import ctypes
import hashlib
import json
import os
import shutil
import subprocess
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Any

PIXI = Path("/usr/local/bin/artisan-pixi")
ENTRYPOINT = Path("/opt/artisan/image-inputs/worker-entrypoint.py")
CONFIG = Path("/opt/artisan/image-inputs/worker-activation.json")
ACTIVATION_MARKER = "_ARTISAN_MODAL_ACTIVATION"
CONTROL_NAMES = frozenset(
    {
        "_ARTISAN_COMMAND_SOCKET",
        "_ARTISAN_COMMAND_SECRET",
        "_ARTISAN_COMMAND_SCOPE",
        "_ARTISAN_PARENT_COMMAND",
        "CUDA_VISIBLE_DEVICES",
        "CUDA_DEVICE_ORDER",
        "NVIDIA_VISIBLE_DEVICES",
        "NVIDIA_DRIVER_CAPABILITIES",
    }
)


def _configuration(encoded: bytes) -> dict[str, Any]:
    config = json.loads(encoded)
    expected = {
        "schema_version",
        "workspace",
        "environment",
        "manifest_name",
    }
    if (
        not isinstance(config, dict)
        or set(config) != expected
        or type(config["schema_version"]) is not int
        or config["schema_version"] != 2
    ):
        msg = "Invalid worker activation configuration"
        raise ValueError(msg)
    workspace = Path(config["workspace"])
    if not workspace.is_absolute() or ".." in workspace.parts:
        msg = "Worker activation requires an absolute workspace"
        raise ValueError(msg)
    if (
        config["manifest_name"] not in {"pixi.toml", "pyproject.toml"}
        or not isinstance(config["environment"], str)
        or not config["environment"]
        or any(
            character
            not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-"
            for character in config["environment"]
        )
    ):
        msg = "Invalid canonical worker selection"
        raise ValueError(msg)
    manifest = workspace / config["manifest_name"]
    if manifest.is_symlink() or not manifest.is_file():
        msg = "Prepared worker activation manifest is unavailable"
        raise ValueError(msg)
    if not PIXI.is_file() or not ENTRYPOINT.is_file():
        msg = "Prepared worker activation launcher is unavailable"
        raise ValueError(msg)
    return config


def _activation_command(config: Mapping[str, Any], descriptor: int) -> list[str]:
    return [
        str(PIXI),
        "run",
        "--as-is",
        "--offline",
        "--no-config",
        "--executable",
        "--manifest-path",
        str(Path(config["workspace"]) / config["manifest_name"]),
        "-e",
        config["environment"],
        sys.executable,
        "-I",
        "-B",
        "-S",
        str(ENTRYPOINT),
        "--capture-environment",
        str(descriptor),
    ]


def _anonymous_descriptor() -> int:
    # Some qualified Python builds omit os.memfd_create; the pinned Linux libc
    # exposes the same API. No disk-backed or named-file fallback is permitted.
    create = ctypes.CDLL(None, use_errno=True).memfd_create
    create.argtypes = (ctypes.c_char_p, ctypes.c_uint)
    create.restype = ctypes.c_int
    descriptor = create(b"artisan-worker-activation", 0x0001)  # MFD_CLOEXEC
    if descriptor < 0:
        message = "Anonymous worker activation capture failed"
        raise OSError(ctypes.get_errno(), message)
    return int(descriptor)


def _activated_bytes(config: Mapping[str, Any], original: Mapping[str, str]) -> bytes:
    descriptor = _anonymous_descriptor()
    with os.fdopen(descriptor, "w+b") as capture:
        result = subprocess.run(
            _activation_command(config, descriptor),
            env=dict(original),
            pass_fds=(descriptor,),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
            timeout=30,
        )
        if result.returncode:
            message = "Canonical worker activation failed"
            raise RuntimeError(message)
        if os.fstat(descriptor).st_size > 8 * 1024 * 1024:
            message = "Worker activation environment exceeds its startup bound"
            raise ValueError(message)
        capture.seek(0)
        return capture.read()


def _environment(
    config: Mapping[str, Any], original: Mapping[str, str]
) -> dict[str, str]:
    try:
        payload = json.loads(_activated_bytes(config, original))
    except (UnicodeDecodeError, json.JSONDecodeError):
        message = "Invalid Pixi activation response"
        raise ValueError(message) from None
    if not isinstance(payload, dict):
        message = "Invalid Pixi activation response"
        raise ValueError(message)
    environment: dict[str, str] = {}
    for key, value in payload.items():
        if (
            not isinstance(key, str)
            or not key
            or "=" in key
            or "\0" in key
            or not isinstance(value, str)
            or "\0" in value
        ):
            msg = "Invalid Pixi activation environment"
            raise ValueError(msg)
        environment[key] = value
    for key in set(original) | set(environment):
        if key.startswith("MODAL_") or key in CONTROL_NAMES:
            if key in original:
                environment[key] = original[key]
            else:
                environment.pop(key, None)
    return environment


def verify_activation(expected: bytes) -> str:
    """Require the measured startup wrapper before accepting this image."""
    encoded = CONFIG.read_bytes()
    digest = hashlib.sha256(encoded).hexdigest()
    if encoded != expected or os.environ.get(ACTIVATION_MARKER) != digest:
        msg = "Canonical worker startup activation was not established"
        raise ValueError(msg)
    _configuration(encoded)
    return digest


def main(arguments: list[str]) -> None:
    """Apply canonical activation and exec the unchanged SDK Python argv."""
    if not arguments or arguments[0] != str(CONFIG) or len(arguments) < 2:
        msg = "Worker startup requires its fixed configuration and SDK argv"
        raise ValueError(msg)
    encoded = CONFIG.read_bytes()
    config = _configuration(encoded)
    environment = _environment(config, os.environ)
    command = arguments[1:]
    executable = shutil.which(command[0], path=environment.get("PATH", os.defpath))
    interpreter = (
        Path(config["workspace"]) / ".pixi/envs" / config["environment"] / "bin/python"
    )
    if executable is None or Path(executable).resolve() != interpreter.resolve():
        msg = "SDK command must use the captured worker interpreter"
        raise ValueError(msg)
    environment[ACTIVATION_MARKER] = hashlib.sha256(encoded).hexdigest()
    os.execve(executable, command, environment)


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "--capture-environment":
        with os.fdopen(int(sys.argv[2]), "wb") as capture:
            capture.write(json.dumps(dict(os.environ)).encode())
    else:
        main(sys.argv[1:])
