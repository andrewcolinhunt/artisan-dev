"""Freeze native GPU visibility and expose only measured driver libraries."""

from __future__ import annotations

import ctypes
import json
import os
import platform
import subprocess
import sys
from collections.abc import Mapping
from pathlib import Path
from uuid import UUID

from artisan.execution.environments.container_runtime import (
    capture_gpu_driver,
    file_digest,
    validate_gpu_devices,
)
from artisan.schemas.execution.gpu import (
    GPUDriverLibrary,
    GPUDriverLibraryName,
    NativeGPUAllocation,
)
from artisan.utils.command_supervisor import strip_internal_attachment

_DRIVER_LIBRARIES: tuple[GPUDriverLibraryName, ...] = (
    "libcuda.so.1",
    "libnvidia-ptxjitcompiler.so.1",
)
_DRIVER_ROOT = Path("/lib/x86_64-linux-gnu")
_SHELL = Path("/bin/sh")
_DRIVER_LAUNCH = (
    'LD_LIBRARY_PATH="$1${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"; '
    'export LD_LIBRARY_PATH; shift; exec "$@"'
)


def resolve_container_gpu_devices(devices: tuple[str, ...] | None) -> tuple[str, ...]:
    """Resolve explicit container indices using the container launcher's NVML order."""
    validate_gpu_devices(devices)
    output = subprocess.check_output(
        [
            "/usr/bin/nvidia-smi",
            "--query-gpu=index,uuid",
            "--format=csv,noheader",
        ],
        text=True,
        timeout=10,
    )
    available = tuple(
        tuple(part.strip() for part in line.split(",")) for line in output.splitlines()
    )
    if any(len(record) != 2 or not all(record) for record in available):
        msg = "GPU inventory is malformed"
        raise ValueError(msg)
    if devices is None:
        return tuple(record[1] for record in available)
    selected = []
    for device in devices:
        matches = [
            record[1]
            for record in available
            if device == record[0]
            or (device.startswith("GPU-") and record[1].startswith(device))
        ]
        if len(matches) != 1 or matches[0] in selected:
            msg = "GPU visibility requires unique available indices or UUIDs"
            raise ValueError(msg)
        selected.append(matches[0])
    return tuple(selected)


def capture_native_gpu(count: int) -> NativeGPUAllocation | None:
    """Bind a requested subset of current visibility without claiming reservation."""
    if count == 0:
        return None
    if count < 0:
        msg = "GPU count cannot be negative"
        raise ValueError(msg)
    if platform.system() != "Linux" or platform.machine() != "x86_64":
        msg = "Native GPU driver exposure currently requires Linux x86_64"
        raise ValueError(msg)
    environment = strip_internal_attachment(os.environ)
    visibility = environment.get("CUDA_VISIBLE_DEVICES")
    devices = (
        None
        if visibility is None
        else tuple(visibility.split(","))
        if visibility
        else ()
    )
    if devices == ():
        msg = "Requested GPUs are hidden by CUDA_VISIBLE_DEVICES"
        raise ValueError(msg)
    validate_gpu_devices(devices)
    allowed = _probe_cuda_visibility(environment)
    if len(set(allowed)) != len(allowed):
        msg = "CUDA visibility names a physical GPU more than once"
        raise ValueError(msg)
    if len(allowed) < count:
        msg = "Requested GPU count exceeds current visible devices"
        raise ValueError(msg)
    selected = allowed[:count]
    driver = capture_gpu_driver(selected)
    assert driver is not None
    libraries = tuple(
        GPUDriverLibrary(
            name=name,
            path=(_DRIVER_ROOT / name).resolve(strict=True),
            sha256=file_digest(_DRIVER_ROOT / name),
        )
        for name in _DRIVER_LIBRARIES
    )
    return NativeGPUAllocation(
        devices=selected,
        driver=driver,
        libraries=libraries,
        shell_sha256=file_digest(_SHELL),
    )


def verify_native_gpu(allocation: NativeGPUAllocation) -> None:
    """Reject changed visibility, driver payload, or launcher before science."""
    if capture_native_gpu(len(allocation.devices)) != allocation:
        msg = "Native GPU allocation or measured driver changed"
        raise ValueError(msg)


def _probe_cuda_visibility(environment: Mapping[str, str]) -> tuple[str, ...]:
    """Use a fresh CUDA initialization so controller ordering state cannot leak in."""
    output = subprocess.check_output(
        [sys.executable, "-I", "-m", __name__],
        env=dict(environment),
        text=True,
        timeout=15,
    )
    devices = json.loads(output)
    if not isinstance(devices, list) or any(
        not isinstance(item, str) for item in devices
    ):
        msg = "Native CUDA visibility probe returned malformed inventory"
        raise ValueError(msg)
    return tuple(devices)


def verify_native_gpu_loader(allocation: NativeGPUAllocation) -> None:
    """Check actual worker libcuda resolution before entering authored science."""
    driver = ctypes.CDLL("libcuda.so.1")
    paths = {
        Path(fields[5])
        for line in Path("/proc/self/maps").read_text().splitlines()
        if len(fields := line.split(maxsplit=5)) == 6
        and Path(fields[5]).name.startswith("libcuda.so")
    }
    if not driver._handle or paths != {allocation.libraries[0].path}:
        msg = "Worker libcuda resolution differs from its admitted driver payload"
        raise ValueError(msg)
    if _cuda_devices(driver) != allocation.devices:
        msg = "Worker CUDA visibility differs from its admitted GPU allocation"
        raise ValueError(msg)


def _cuda_devices(driver: ctypes.CDLL) -> tuple[str, ...]:
    """Enumerate CUDA's actual ordinal mapping without creating a context."""
    _cuda_call(driver, "cuInit", 0)
    count = ctypes.c_int()
    _cuda_call(driver, "cuDeviceGetCount", ctypes.byref(count))
    visible = []
    for index in range(count.value):
        device = ctypes.c_int()
        identifier = (ctypes.c_ubyte * 16)()
        _cuda_call(driver, "cuDeviceGet", ctypes.byref(device), index)
        _cuda_call(driver, "cuDeviceGetUuid", ctypes.byref(identifier), device)
        visible.append("GPU-" + str(UUID(bytes=bytes(identifier))))
    return tuple(visible)


def _cuda_call(driver: ctypes.CDLL, name: str, *arguments: object) -> None:
    if code := getattr(driver, name)(*arguments):
        msg = f"Native GPU startup {name} failed with CUDA error {code}"
        raise ValueError(msg)


def native_gpu_environment(
    allocation: NativeGPUAllocation | None, environment: Mapping[str, str]
) -> dict[str, str]:
    """Reject injection overrides and return protected exact device visibility."""
    if allocation is None:
        return {}
    visible = ",".join(allocation.devices)
    values = {"CUDA_VISIBLE_DEVICES": visible, "NVIDIA_VISIBLE_DEVICES": visible}
    if any(
        key in environment and environment[key] != value
        for key, value in values.items()
    ):
        msg = "Runtime injections cannot replace the admitted GPU visibility"
        raise ValueError(msg)
    return values


def native_gpu_command(
    allocation: NativeGPUAllocation | None, command: list[str], scratch: Path
) -> list[str]:
    """Prepend only measured driver names to the selected activation's library path."""
    if allocation is None:
        return command
    directory = scratch / "gpu-driver"
    if (
        not directory.is_absolute()
        or directory.resolve() != directory
        or any(character in str(directory) for character in ":\n\0")
    ):
        msg = "Native GPU driver scratch requires an absolute loader-safe path"
        raise ValueError(msg)
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    if {path.name for path in directory.iterdir()} - {
        item.name for item in allocation.libraries
    }:
        msg = "Native GPU driver scratch contains unowned entries"
        raise ValueError(msg)
    for library in allocation.libraries:
        path = directory / library.name
        if path.is_symlink():
            if path.readlink() != library.path:
                msg = "Native GPU driver link changed"
                raise ValueError(msg)
        else:
            path.symlink_to(library.path)
    return [str(_SHELL), "-c", _DRIVER_LAUNCH, "artisan-gpu", str(directory), *command]


if __name__ == "__main__":
    sys.stdout.write(
        json.dumps(_cuda_devices(ctypes.CDLL(str(_DRIVER_ROOT / "libcuda.so.1"))))
        + "\n"
    )
