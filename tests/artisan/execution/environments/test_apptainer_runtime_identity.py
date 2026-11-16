"""Apptainer identity binds the same conditional payload used by its launcher."""

from __future__ import annotations

from pathlib import Path

import pytest

from artisan.execution.environments import container_runtime
from artisan.schemas.execution.worker_transport import PayloadFile


@pytest.mark.parametrize("devices", [None, (), ("GPU-synthetic",)])
def test_apptainer_identity_captures_payload_for_concrete_allocation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, devices: tuple[str, ...] | None
) -> None:
    executable = tmp_path / "apptainer"
    executable.write_bytes(b"launcher")
    executable.chmod(0o755)
    payload = PayloadFile(path="config/apptainer.conf", sha256="a" * 64)
    calls: list[tuple[Path, bool]] = []

    def capture(path: Path, *, nvidia: bool) -> tuple[PayloadFile, ...]:
        calls.append((path, nvidia))
        return (payload,)

    monkeypatch.setattr(container_runtime, "capture_apptainer_payload", capture)
    monkeypatch.setattr(container_runtime, "file_digest", lambda _path: "b" * 64)
    monkeypatch.setattr(
        container_runtime.subprocess,
        "check_output",
        lambda *_args, **_kwargs: "apptainer version 1.5.4",
    )
    runtime = container_runtime.ApptainerRuntime(executable, gpu_devices=devices)
    identity = runtime.identity()
    assert calls == [(executable, bool(devices))]
    assert identity.helpers[-1] == payload
    assert "GPU-synthetic" not in identity.model_dump_json()


def test_apptainer_identity_changes_when_selected_payload_changes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    executable = tmp_path / "apptainer"
    executable.write_bytes(b"launcher")
    executable.chmod(0o755)
    digest = "a" * 64

    def capture(path: Path, *, nvidia: bool) -> tuple[PayloadFile, ...]:
        assert path == executable
        assert nvidia
        return (
            PayloadFile(
                path="gpu/rootfs/.singularity.d/libs/libcuda.so.1", sha256=digest
            ),
        )

    monkeypatch.setattr(container_runtime, "capture_apptainer_payload", capture)
    monkeypatch.setattr(container_runtime, "file_digest", lambda _path: "b" * 64)
    monkeypatch.setattr(
        container_runtime.subprocess,
        "check_output",
        lambda *_args, **_kwargs: "apptainer version 1.5.4",
    )
    runtime = container_runtime.ApptainerRuntime(
        executable, gpu_devices=("GPU-synthetic",)
    )
    original = runtime.identity()
    digest = "c" * 64
    assert runtime.identity() != original
