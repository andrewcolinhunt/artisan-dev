"""Owned named deployment and authenticated disposable store for final proof."""

from __future__ import annotations

import asyncio
import contextlib
import secrets
import time
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

import modal
from artisan_modal import ModalRunner
from artisan_modal.bindings import PreparedModalImage
from artisan_modal.image_store import publish_image
from artisan_modal.pack_control import write_once
from artisan_modal.resolution import function_profile
from artisan_modal.runner import _deployment_name, _function
from assignment import _stop_store
from final_apps import app_control, recorded_apps
from progress import Progress
from shared_store import MINIO_IMAGE, _check_tunnel_closed

from artisan.orchestration.runner_api import Environment, RunnerResources, StorageConfig


def stop_application(app_id: str) -> dict[str, object]:
    """Stop only the recorded application and independently observe zero tasks."""
    registry = recorded_apps()
    asyncio.run(app_control(registry, "stop", app_id))
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        records = asyncio.run(app_control(registry, "list", app_id))
        if (
            len(records) == 1
            and records[0]["State"] == "stopped"
            and int(records[0]["Tasks"]) == 0
        ):
            return {"app_id": app_id, "state": "stopped", "tasks": 0}
        time.sleep(0.2)
    msg = "Exact application did not report stopped with zero tasks"
    raise RuntimeError(msg)


@contextlib.contextmanager
def named_runner(
    binding: PreparedModalImage,
    manifest: Path,
    cache: Path,
    resources: RunnerResources,
    report: dict[str, object],
    progress: Progress,
) -> Iterator[ModalRunner]:
    """Prepare one exact named function; invocation uses normal provider lookup."""
    name = "qualification-" + uuid4().hex[:16]
    profile = function_profile(binding.image_id, resources)
    deployment = _deployment_name(name, binding, profile)
    app = modal.App()
    _function(app, profile)
    publish_image(binding, cache_root=cache)
    progress.update(phase="deploying", deployment=deployment)
    report["deployment"] = deployment
    failed = False
    try:
        app.deploy(name=deployment)
        assert app.app_id is not None
        report["worker_app_id"] = app.app_id
        progress.update(phase="deployed", worker_app_id=app.app_id)
        yield ModalRunner(
            Environment.from_pixi(manifest, name="worker"), name, cache_root=cache
        )
    except BaseException:
        failed = True
        raise
    finally:
        try:
            if app.app_id is None:
                msg = "Deployment ownership unresolved before an app ID was returned"
                raise RuntimeError(msg)
            report["worker_app_cleanup"] = stop_application(app.app_id)
            progress.update(worker_app_cleanup=report["worker_app_cleanup"])
        except Exception as error:
            report["worker_app_cleanup_error"] = type(error).__name__
            progress.failure("worker_app_cleanup_error", error)
            if not failed:
                raise


@dataclass(frozen=True)
class SharedStore:
    storage: StorageConfig
    bucket: str

    def root(self, part: str) -> str:
        return f"s3://{self.bucket}/{part}"


@contextlib.contextmanager
def shared_store(
    report: dict[str, object], progress: Progress
) -> Iterator[SharedStore]:
    """Use one bounded authenticated server; no public bucket or mirror semantics."""
    credentials = {
        "MINIO_ROOT_USER": "artisan" + secrets.token_hex(12),
        "MINIO_ROOT_PASSWORD": secrets.token_urlsafe(36),
    }
    progress.learn(tuple(credentials.values()))
    secret = modal.Secret.from_dict(dict(credentials))
    app = modal.App()
    image = modal.Image.from_registry(MINIO_IMAGE, add_python="3.12").entrypoint([])
    endpoint = None
    outer_failed = False
    try:
        with modal.Queue.ephemeral() as control:

            @app.function(
                image=image,
                region="us-east",
                cpu=1,
                memory=1024,
                timeout=1200,
                startup_timeout=300,
                max_containers=1,
                retries=0,
                secrets=[secret],
                serialized=True,
                include_source=False,
            )
            def serve() -> dict[str, bool]:
                import os
                import queue
                import socket
                import subprocess
                import time

                import modal

                process = subprocess.Popen(
                    ["/usr/bin/minio", "server", "--address", ":9000", "/tmp/store"],
                    env={**os.environ, "MINIO_BROWSER": "off", "MINIO_UPDATE": "off"},
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
                try:
                    deadline = time.monotonic() + 40
                    while True:
                        if process.poll() is not None or time.monotonic() >= deadline:
                            msg = "Disposable store readiness failed"
                            raise RuntimeError(msg)
                        try:
                            with socket.create_connection(
                                ("127.0.0.1", 9000), timeout=1
                            ):
                                break
                        except OSError:
                            time.sleep(0.1)
                    with modal.forward(9000) as tunnel:
                        control.put(
                            {
                                "url": tunnel.url,
                                "region": os.environ.get("MODAL_REGION"),
                                "cloud": os.environ.get("MODAL_CLOUD_PROVIDER"),
                            },
                            partition="ready",
                        )
                        deadline = time.monotonic() + 1080
                        while time.monotonic() < deadline:
                            try:
                                if control.get(timeout=1, partition="stop") == "stop":
                                    break
                            except queue.Empty:
                                pass
                        else:
                            msg = "Disposable store lifetime expired"
                            raise TimeoutError(msg)
                finally:
                    process.terminate()
                    try:
                        process.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait(timeout=5)
                return {
                    "server_closed": process.poll() is not None,
                    "tunnel_context_closed": True,
                }

            with app.run():
                report["store_app_id"] = app.app_id
                progress.update(phase="store_submit", store_app_id=app.app_id)
                call = serve.spawn()
                progress.update(phase="store_submitted", store_call_id=call.object_id)
                failed = False
                try:
                    ready = control.get(timeout=300, partition="ready")
                    assert isinstance(ready, dict)
                    assert set(ready) == {"url", "region", "cloud"}
                    endpoint = ready["url"]
                    assert isinstance(endpoint, str)
                    assert endpoint.startswith("https://")
                    progress.learn((endpoint,))
                    for field in ("region", "cloud"):
                        assert ready[field] is None or (
                            isinstance(ready[field], str) and len(ready[field]) <= 80
                        )
                    report["store_placement"] = {
                        field: ready[field] for field in ("region", "cloud")
                    }
                    progress.update(
                        phase="store_ready",
                        requested_region="us-east",
                        store_placement=report["store_placement"],
                        store_app_id=app.app_id,
                    )
                    assert ready["region"] == "us-east"
                    assert isinstance(ready["cloud"], str)
                    assert ready["cloud"]
                    key, password = (
                        credentials["MINIO_ROOT_USER"],
                        credentials["MINIO_ROOT_PASSWORD"],
                    )
                    storage = StorageConfig(
                        protocol="s3",
                        options={
                            "key": key,
                            "secret": password,
                            "client_kwargs": {
                                "endpoint_url": endpoint,
                                "region_name": "us-east-1",
                            },
                            "config_kwargs": {
                                "s3": {"addressing_style": "path"},
                                "retries": {"max_attempts": 2},
                                "connect_timeout": 10,
                                "read_timeout": 30,
                            },
                        },
                        delta_options={
                            "AWS_ENDPOINT_URL": endpoint,
                            "AWS_ACCESS_KEY_ID": key,
                            "AWS_SECRET_ACCESS_KEY": password,
                            "AWS_REGION": "us-east-1",
                            "AWS_EC2_METADATA_DISABLED": "true",
                        },
                    )
                    bucket = "artisan-final-" + uuid4().hex
                    fs = storage.filesystem()
                    fs.mkdir(bucket)
                    fs.pipe_file(bucket + "/signed-probe", b"verified")
                    assert fs.cat_file(bucket + "/signed-probe") == b"verified"
                    write_once(fs, bucket + "/conditional-probe", b"first")
                    try:
                        write_once(fs, bucket + "/conditional-probe", b"second")
                    except FileExistsError:
                        pass
                    else:
                        message = "Shared store replaced a conditional-create claim"
                        raise AssertionError(message)
                    assert fs.cat_file(bucket + "/conditional-probe") == b"first"
                    report["conditional_create_verified"] = True
                    import pyarrow as pa
                    from deltalake import DeltaTable, write_deltalake

                    delta_root = f"s3://{bucket}/signed-delta-probe"
                    write_deltalake(
                        delta_root,
                        pa.table({"value": [42]}),
                        storage_options=storage.delta_storage_options(),
                    )
                    assert DeltaTable(
                        delta_root, storage_options=storage.delta_storage_options()
                    ).to_pyarrow_table().to_pydict() == {"value": [42]}
                    report["signed_s3_delta_verified"] = True
                    report["bucket"] = bucket
                    yield SharedStore(storage, bucket)
                except BaseException:
                    failed = True
                    raise
                finally:
                    cleanup = _stop_store(control, call, report, progress)
                    if cleanup is None and report.get("server_cleanup") != {
                        "server_closed": True,
                        "tunnel_context_closed": True,
                    }:
                        message = "Store did not confirm server and tunnel closure"
                        cleanup = RuntimeError(message)
                        report["server_cleanup_validation_error"] = type(
                            cleanup
                        ).__name__
                        progress.failure("server_cleanup_validation_error", cleanup)
                    if cleanup is not None and not failed:
                        raise cleanup
        report["queue_context_closed"] = True
    except BaseException:
        outer_failed = True
        raise
    finally:
        cleanup_errors = []
        if endpoint is not None:
            try:
                _check_tunnel_closed(endpoint)
                report["tunnel_closed"] = True
            except Exception as error:
                cleanup_errors.append(error)
                progress.failure("tunnel_cleanup_error", error)
        if app.app_id is not None:
            try:
                report["store_app_cleanup"] = stop_application(app.app_id)
            except Exception as error:
                cleanup_errors.append(error)
                progress.failure("store_app_cleanup_error", error)
        else:
            report["store_app_cleanup"] = {"state": "unknown"}
        report["store_outer_cleanup_errors"] = [
            type(error).__name__ for error in cleanup_errors
        ]
        if cleanup_errors and not outer_failed:
            raise cleanup_errors[0]
