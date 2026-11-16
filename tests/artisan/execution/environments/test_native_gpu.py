"""Native GPU placement cannot expand visibility or substitute host libraries."""

from __future__ import annotations

import ctypes
import json
import os
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID

import pytest

from artisan.execution.compute.prepared import PreparedCommandEnvironment
from artisan.execution.environments import native_gpu
from artisan.execution.environments.container_runtime import file_digest
from artisan.execution.environments.observed_worker import ObservedCommandEnvironment
from artisan.schemas.execution.container_environment import PreparedOCIEnvironment
from artisan.schemas.execution.gpu import GPUDriverIdentity

UUID_A = "GPU-00000000-0000-0000-0000-000000000001"
UUID_B = "GPU-00000000-0000-0000-0000-000000000002"


@pytest.fixture
def hardware(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    libraries = tmp_path / "host-driver"
    libraries.mkdir()
    for name in ("libcuda.so.1", "libnvidia-ptxjitcompiler.so.1"):
        (libraries / name).write_text(name)
    monkeypatch.setattr(native_gpu, "_DRIVER_ROOT", libraries)
    monkeypatch.delenv("CUDA_VISIBLE_DEVICES", raising=False)

    def inventory(command: list[str], **kwargs: object) -> str:
        if command[0] == "/usr/bin/nvidia-smi":
            return f"0, {UUID_A}\n1, {UUID_B}\n"
        environment = kwargs["env"]
        visibility = environment.get("CUDA_VISIBLE_DEVICES")
        if visibility is None:
            return json.dumps([UUID_A, UUID_B])
        selected = []
        for token in visibility.split(","):
            matches = [
                identifier
                for index, identifier in enumerate((UUID_A, UUID_B))
                if token == str(index) or identifier.startswith(token)
            ]
            if len(matches) != 1:
                return "[]"
            selected.append(matches[0])
        return json.dumps(selected)

    monkeypatch.setattr(native_gpu.subprocess, "check_output", inventory)
    monkeypatch.setattr(
        native_gpu,
        "capture_gpu_driver",
        lambda devices: GPUDriverIdentity(
            driver_version="synthetic-driver",
            device_models=("synthetic-device",),
            libcuda_sha256=file_digest(libraries / "libcuda.so.1"),
            nvidia_smi_sha256="a" * 64,
        ),
    )
    return libraries


def test_zero_request_does_not_inspect_or_claim_hardware(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        native_gpu,
        "_probe_cuda_visibility",
        lambda *a: pytest.fail("CPU work inspected GPU hardware"),
    )
    assert native_gpu.capture_native_gpu(0) is None
    assert native_gpu.native_gpu_environment(None, {"CUDA_VISIBLE_DEVICES": ""}) == {}


def test_native_numeric_visibility_uses_cuda_order_not_nvml_index(
    hardware: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("CUDA_VISIBLE_DEVICES", "0")
    monkeypatch.setenv("CUDA_DEVICE_ORDER", "FASTEST_FIRST")

    def inventory(command: list[str], **kwargs: object) -> str:
        if command[0] == "/usr/bin/nvidia-smi":
            return f"0, {UUID_A}\n1, {UUID_B}\n"
        assert command[1:3] == ["-I", "-m"]
        assert kwargs["env"]["CUDA_VISIBLE_DEVICES"] == "0"
        assert kwargs["env"]["CUDA_DEVICE_ORDER"] == "FASTEST_FIRST"
        return json.dumps([UUID_B])

    monkeypatch.setattr(native_gpu.subprocess, "check_output", inventory)
    allocation = native_gpu.capture_native_gpu(1)
    assert allocation is not None
    assert allocation.devices == (UUID_B,)
    assert native_gpu.resolve_container_gpu_devices(("0",)) == (UUID_A,)


def test_requested_subset_preserves_visibility_order_and_exact_uuid(
    hardware: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("CUDA_VISIBLE_DEVICES", "1,0")
    allocation = native_gpu.capture_native_gpu(1)
    assert allocation is not None
    assert allocation.devices == (UUID_B,)
    monkeypatch.setenv("CUDA_VISIBLE_DEVICES", UUID_B)
    native_gpu.verify_native_gpu(allocation)
    monkeypatch.setenv("CUDA_VISIBLE_DEVICES", UUID_A)
    with pytest.raises(ValueError, match="allocation or measured driver changed"):
        native_gpu.verify_native_gpu(allocation)


@pytest.mark.parametrize(
    "visibility", ["", "-1", "all", "2", f"0,{UUID_A}", "GPU-0000"]
)
def test_hidden_unavailable_duplicate_or_ambiguous_visibility_rejects(
    hardware: Path, monkeypatch: pytest.MonkeyPatch, visibility: str
) -> None:
    monkeypatch.setenv("CUDA_VISIBLE_DEVICES", visibility)
    with pytest.raises(ValueError, match="GPU"):
        native_gpu.capture_native_gpu(1)


def test_request_count_and_driver_changes_are_checked(hardware: Path) -> None:
    with pytest.raises(ValueError, match="count exceeds"):
        native_gpu.capture_native_gpu(3)
    allocation = native_gpu.capture_native_gpu(1)
    assert allocation is not None
    (hardware / "libnvidia-ptxjitcompiler.so.1").write_text("changed")
    with pytest.raises(ValueError, match="driver changed"):
        native_gpu.verify_native_gpu(allocation)


def test_software_identity_excludes_uuid_and_library_locator(hardware: Path) -> None:
    allocation = native_gpu.capture_native_gpu(1)
    assert allocation is not None
    relocated = allocation.model_copy(
        update={
            "devices": (UUID_B,),
            "libraries": tuple(
                item.model_copy(update={"path": Path("/other") / item.name})
                for item in allocation.libraries
            ),
        }
    )
    assert allocation != relocated
    assert allocation.software_descriptor() == relocated.software_descriptor()


def test_only_owned_driver_names_precede_existing_tool_library_lookup(
    hardware: Path, tmp_path: Path
) -> None:
    allocation = native_gpu.capture_native_gpu(1)
    assert allocation is not None
    scratch = tmp_path / "invocation"
    command = native_gpu.native_gpu_command(
        allocation,
        [
            "/bin/sh",
            "-c",
            'printf "%s|%s" "$LD_LIBRARY_PATH" "$1"',
            "probe",
            "literal $VALUE",
        ],
        scratch,
    )
    result = subprocess.run(
        command,
        env={**os.environ, "LD_LIBRARY_PATH": "/tool/own-libraries"},
        capture_output=True,
        text=True,
        check=True,
    )
    assert result.stdout == f"{scratch}/gpu-driver:/tool/own-libraries|literal $VALUE"
    links = tuple((scratch / "gpu-driver").iterdir())
    assert {path.name for path in links} == {item.name for item in allocation.libraries}
    assert all(
        path.is_symlink() and path.resolve().parent == hardware for path in links
    )
    assert str(hardware) not in result.stdout
    (scratch / "gpu-driver" / "unowned.so").write_text("unexpected")
    with pytest.raises(ValueError, match="unowned"):
        native_gpu.native_gpu_command(allocation, ["/bin/true"], scratch)


def test_runtime_cannot_expand_or_replace_allocated_visibility(hardware: Path) -> None:
    allocation = native_gpu.capture_native_gpu(1)
    assert allocation is not None
    expected = {"CUDA_VISIBLE_DEVICES": UUID_A, "NVIDIA_VISIBLE_DEVICES": UUID_A}
    assert native_gpu.native_gpu_environment(allocation, {}) == expected
    assert native_gpu.native_gpu_environment(allocation, expected) == expected
    for name in expected:
        with pytest.raises(ValueError, match="cannot replace"):
            native_gpu.native_gpu_environment(allocation, {name: "all"})


def test_driver_scratch_cannot_follow_an_external_directory_link(
    hardware: Path, tmp_path: Path
) -> None:
    allocation = native_gpu.capture_native_gpu(1)
    assert allocation is not None
    scratch, outside = tmp_path / "invocation", tmp_path / "outside"
    scratch.mkdir()
    outside.mkdir()
    (scratch / "gpu-driver").symlink_to(outside, target_is_directory=True)
    with pytest.raises(ValueError, match="absolute loader-safe"):
        native_gpu.native_gpu_command(allocation, ["/bin/true"], scratch)
    assert not any(outside.iterdir())


def test_worker_preflight_rejects_wrong_loaded_driver(
    hardware: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    allocation = native_gpu.capture_native_gpu(1)
    assert allocation is not None
    driver = SimpleNamespace(
        _handle=1,
        cuInit=lambda *_: 0,
        cuDeviceGetCount=lambda pointer: setattr(pointer._obj, "value", 1) or 0,
        cuDeviceGet=lambda *_: 0,
        cuDeviceGetUuid=lambda pointer, _: ctypes.memmove(
            pointer, UUID(UUID_A[4:]).bytes, 16
        )
        and 0,
    )
    monkeypatch.setattr(native_gpu.ctypes, "CDLL", lambda *a: driver)
    original = Path.read_text
    loaded = allocation.libraries[0].path

    def maps(path: Path, *args: object, **kwargs: object) -> str:
        if str(path) == "/proc/self/maps":
            return f"1-2 r-xp 00000000 00:00 0 {loaded}\n"
        return original(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", maps)
    native_gpu.verify_native_gpu_loader(allocation)
    with pytest.raises(ValueError, match="CUDA visibility"):
        native_gpu.verify_native_gpu_loader(
            allocation.model_copy(update={"devices": (UUID_B,)})
        )
    loaded = Path("/unqualified/libcuda.so.1")
    with pytest.raises(ValueError, match="libcuda resolution"):
        native_gpu.verify_native_gpu_loader(allocation)


def test_process_pack_cannot_mix_concrete_gpu_allocations(
    hardware: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from artisan.execution import local_packing

    allocation = native_gpu.capture_native_gpu(1)
    assert allocation is not None
    requests = tuple(
        SimpleNamespace(
            bindings=SimpleNamespace(
                worker=SimpleNamespace(digest="same-worker"),
                compatibility="same-compatibility",
                launcher="same-launcher",
                native_gpu=allocation.model_copy(update={"devices": (device,)}),
            )
        )
        for device in (UUID_A, UUID_B)
    )
    monkeypatch.setattr(
        local_packing,
        "_LocalPack",
        lambda *a: pytest.fail("Mismatched GPU pack started"),
    )
    with pytest.raises(ValueError, match="GPU allocation"):
        local_packing.launch_local_assignments(
            requests, working_root=tmp_path, units_per_worker=2
        )


def test_observed_and_prepared_activation_cannot_expand_gpu_visibility(
    hardware: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    oci: PreparedOCIEnvironment,
) -> None:
    allocation = native_gpu.capture_native_gpu(1)
    assert allocation is not None
    monkeypatch.setenv("CUDA_VISIBLE_DEVICES", "all")
    target = SimpleNamespace(
        executable=Path(sys.executable), environment={"CUDA_VISIBLE_DEVICES": "all"}
    )
    observed = ObservedCommandEnvironment(target, tmp_path / "observed", gpu=allocation)
    assert observed.prepare_env()["CUDA_VISIBLE_DEVICES"] == UUID_A
    monkeypatch.setattr(
        "artisan.execution.compute.prepared.native_executable",
        lambda *a: oci.native.prefix / "bin/python",
    )
    prepared = PreparedCommandEnvironment(
        oci.native,
        Path("/qualified/pixi"),
        Path("/usr/bin/env"),
        tmp_path / "prepared",
        gpu=allocation,
    )
    argv = prepared.wrap_command(["python", "-c", "pass"])
    assert f"CUDA_VISIBLE_DEVICES={UUID_A}" in argv
    assert "CUDA_VISIBLE_DEVICES=all" not in argv
    assert prepared.prepare_env()["CUDA_VISIBLE_DEVICES"] == UUID_A
    for factory in (
        lambda: ObservedCommandEnvironment(
            target, tmp_path, {"CUDA_VISIBLE_DEVICES": "all"}, gpu=allocation
        ),
        lambda: PreparedCommandEnvironment(
            oci.native,
            Path("/pixi"),
            Path("/env"),
            tmp_path,
            {"CUDA_VISIBLE_DEVICES": "all"},
            gpu=allocation,
        ),
    ):
        with pytest.raises(ValueError, match="cannot replace"):
            factory()


def test_closed_software_profile_preserves_existing_descriptor_bytes(
    hardware: Path,
) -> None:
    from artisan.schemas.execution.gpu import NativeGPUSoftware

    allocation = native_gpu.capture_native_gpu(1)
    assert allocation is not None
    expected = {
        "device_count": len(allocation.devices),
        "driver": allocation.driver.model_dump(mode="json"),
        "libraries": [
            {"name": item.name, "sha256": item.sha256} for item in allocation.libraries
        ],
        "shell_sha256": allocation.shell_sha256,
    }
    assert allocation.software_descriptor() == expected
    profile = allocation.software_profile()
    assert NativeGPUSoftware.model_validate_json(profile.model_dump_json()) == profile
    assert UUID_A not in profile.model_dump_json()
    assert str(hardware) not in profile.model_dump_json()
    for update in (
        {"device_count": True},
        {"device_count": 0},
        {"libraries": []},
        {"devices": [UUID_A]},
        {
            "libraries": (
                profile.libraries[0],
                profile.libraries[1].model_copy(update={"sha256": "invalid"}),
            )
        },
    ):
        with pytest.raises(ValueError):
            NativeGPUSoftware.model_validate({**profile.model_dump(), **update})
