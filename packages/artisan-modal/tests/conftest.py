"""Synthetic canonical image bindings shared by provider unit checks."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from types import ModuleType
from typing import BinaryIO

import pytest
from artisan_modal.bindings import (
    ImageEnvironment,
    ImageLayout,
    ImageStartup,
    ModalRuntimePayload,
    PreparedModalImage,
)
from artisan_modal.pack_control import (
    ModalPack,
)
from artisan_modal.preparation import BASE_IMAGE
from artisan_modal.transport import ModalCall
from fsspec import AbstractFileSystem

from artisan.orchestration.runner_api import (
    Environment,
    EnvironmentSource,
    LocalLauncherIdentity,
    PreparedNativeEnvironment,
    RuntimeCompatibility,
)


@pytest.fixture
def prepared(tmp_path: Path) -> tuple[PreparedModalImage, Environment, Environment]:
    manifest = tmp_path / "pixi.toml"
    manifest.write_text(
        '[workspace]\nname="example"\nplatforms=["linux-64"]\n[environments]\nworker=[]\ntool=[]\nother=[]\n'
    )
    lock = tmp_path / "pixi.lock"
    lock.write_text(
        json.dumps(
            {
                "version": 7,
                "environments": {
                    name: {"packages": {"linux-64": []}}
                    for name in ("worker", "tool", "other")
                },
                "packages": [],
            }
        )
    )
    environments = tuple(
        sorted(
            (
                ImageEnvironment(
                    source=EnvironmentSource.model_validate(
                        {
                            "manifest_name": "pixi.toml",
                            "name": name,
                            "files": [
                                {
                                    "path": path.name,
                                    "sha256": hashlib.sha256(
                                        path.read_bytes()
                                    ).hexdigest(),
                                }
                                for path in (lock, manifest)
                            ],
                        }
                    )
                )
                for name in ("worker", "tool")
            ),
            key=lambda item: item.source.digest,
        )
    )
    worker = next(item for item in environments if item.source.name == "worker")
    layout = ImageLayout(
        worker_source=worker.source.digest,
        environments=environments,
        pixi_sha256="a" * 64,
        worker_entrypoint_sha256="f" * 64,
    )
    compatibility = RuntimeCompatibility.model_validate(
        {
            "python_implementation": "CPython",
            "python_version": "3.12.12",
            "cache_tag": "cpython-312",
            "packages": [
                {
                    "name": name,
                    "version": "1",
                }
                for name in (
                    "cloudpickle",
                    "dexterity-artisan",
                    "pydantic",
                    "pydantic-core",
                )
            ],
        }
    )
    targets = tuple(
        PreparedNativeEnvironment.model_validate(
            {
                "source": item.source,
                "platform": "linux-64",
                "workspace": item.workspace,
                "prefix": item.prefix,
                "compatibility": compatibility if item == worker else None,
            }
        )
        for item in environments
    )
    startup = ImageStartup(
        layout=layout,
        activation_sha256=hashlib.sha256(layout.activation_config()).hexdigest(),
        targets=targets,
        launcher=LocalLauncherIdentity(
            "pixi 0.83.0", "a" * 64, "c" * 64, ("glibc", "2.36")
        ),
        runtime=ModalRuntimePayload(sdk_version="1.4.1"),
    )
    binding = PreparedModalImage(
        image_id="im-example",
        base_image=BASE_IMAGE,
        recipe_sha256="e" * 64,
        startup=startup,
    )
    return (
        binding,
        Environment.from_pixi(manifest, name="worker"),
        Environment.from_pixi(manifest, name="tool"),
    )


@pytest.fixture
def pack_request(prepared):
    from uuid import uuid4

    from artisan_modal.bindings import ModalFunctionRuntime

    from artisan.orchestration.runner_api import (
        CuratorAdmissionReference,
        CuratorReplyContext,
        StorageConfig,
        WorkerTransportHeader,
    )

    image, _, _ = prepared
    calls = []
    for index in range(3):
        payload = f"opaque call {index}".encode()
        header = WorkerTransportHeader(
            compatibility=image.startup.worker.compatibility,
            payload_sha256=hashlib.sha256(payload).hexdigest(),
        )
        context = CuratorReplyContext(
            admission=CuratorAdmissionReference(
                step_run_id=f"{index + 1:032x}", admission_digest="b" * 32
            ),
            payload_sha256=header.payload_sha256,
            sensitive_values=(),
        )
        calls.append(
            ModalCall(
                function=ModalFunctionRuntime(
                    image_id="im-qualified",
                    milli_cpu=1000,
                    memory_mb=1024,
                    gpu_count=0,
                    gpu_type="",
                    max_inputs=1,
                    timeout_seconds=60,
                ),
                runtime=image.startup.runtime,
                header=header,
                payload=payload,
                reply_context=context.model_dump_json().encode(),
            )
        )
    return ModalPack(
        pack_id=uuid4(),
        staging_root="s3://example/staging",
        storage=StorageConfig(
            protocol="s3", options={"key": "selected-key", "secret": "selected-secret"}
        ),
        calls=tuple(calls),
    )


@pytest.fixture
def control_fs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> AbstractFileSystem:
    import os
    import tempfile

    from fsspec.implementations.local import LocalFileSystem

    from artisan.orchestration.runner_api import StorageConfig

    class ControlFS(LocalFileSystem):
        def _path(self, path: str) -> str:
            relative = path.removeprefix("s3://")
            assert not relative.startswith("/")
            assert ".." not in relative.split("/")
            return str(tmp_path / "channel" / relative)

        def pipe_file(
            self, path: str, value: bytes, mode: str = "overwrite", **_kwargs: object
        ) -> None:
            target = Path(self._path(path))
            target.parent.mkdir(parents=True, exist_ok=True)
            fd, temporary = tempfile.mkstemp(dir=target.parent)
            try:
                with os.fdopen(fd, "wb") as stream:
                    stream.write(value)
                if mode == "create":
                    os.link(temporary, target)
                else:
                    os.replace(temporary, target)
            finally:
                Path(temporary).unlink(missing_ok=True)

        def open(self, path: str, mode: str = "rb", **kwargs: object) -> BinaryIO:
            return super().open(self._path(path), mode, **kwargs)

        def rm(self, path: str, **kwargs: object) -> None:
            return super().rm(self._path(path), **kwargs)

    fs = ControlFS(skip_instance_cache=True)
    monkeypatch.setattr(StorageConfig, "filesystem", lambda _self: fs)
    return fs


@pytest.fixture
def pack_runtime(monkeypatch: pytest.MonkeyPatch) -> ModuleType:
    import threading

    from artisan_modal import pack_owner, pack_worker

    monkeypatch.setattr(pack_worker, "_lease", threading.Lock())
    monkeypatch.setattr(pack_worker, "_retained", [])
    monkeypatch.setattr(pack_worker, "_POLL_SECONDS", 0.01)
    monkeypatch.setattr(pack_owner, "_POLL_SECONDS", 0.01)
    return pack_worker
