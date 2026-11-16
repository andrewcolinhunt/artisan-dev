"""Qualify one disposable authenticated S3 store shared with a Modal worker."""

from __future__ import annotations

import argparse
import hashlib
import json
import secrets
import sys
import time
import uuid
from http.client import RemoteDisconnected
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

import modal

MINIO_IMAGE = (
    "ghcr.io/coollabsio/minio@"
    "sha256:a4938f37f1be1841b8e7b627ad0207b265345fd0d063e42d7410c78af0e63e68"
)


def install_storage() -> None:
    """Build only the disposable store consumer, never a production worker image."""
    import subprocess

    subprocess.run(
        [
            "/usr/local/bin/python",
            "-m",
            "pip",
            "--python",
            "/opt/provider-probe/.pixi/envs/worker/bin/python",
            "install",
            "--no-compile",
            "s3fs==2026.9.0",
            "deltalake==1.6.6",
            "pyarrow==26.0.1",
        ],
        check=True,
        timeout=280,
    )


def qualify(consumer_image: str, report: dict[str, object]) -> None:
    """Close all test-owned resources after checking signed objects and Delta."""
    import os

    os.environ["MODAL_IMAGE_BUILDER_VERSION"] = "2025.06"
    credentials = {
        "MINIO_ROOT_USER": "artisan" + secrets.token_hex(12),
        "MINIO_ROOT_PASSWORD": secrets.token_urlsafe(36),
    }
    secret = modal.Secret.from_dict(credentials)
    server_image = modal.Image.from_registry(MINIO_IMAGE, add_python="3.12").entrypoint(
        []
    )
    consumer = modal.Image.from_id(consumer_image).run_function(
        install_storage, cpu=1, memory=1024, timeout=300, include_source=True
    )
    app = modal.App("artisan-shared-store-qualification-" + uuid.uuid4().hex[:12])
    endpoint: str | None = None
    with modal.Queue.ephemeral() as control:

        @app.function(
            image=server_image,
            cpu=1,
            memory=1024,
            timeout=900,
            startup_timeout=300,
            max_containers=1,
            retries=0,
            secrets=[secret],
            serialized=True,
            include_source=False,
        )
        def serve() -> dict[str, object]:
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
                        message = "Disposable store did not become ready"
                        raise RuntimeError(message)
                    try:
                        with socket.create_connection(("127.0.0.1", 9000), timeout=1):
                            break
                    except OSError:
                        time.sleep(0.1)
                with modal.forward(9000) as tunnel:
                    control.put(tunnel.url, partition="ready")
                    deadline = time.monotonic() + 780
                    while time.monotonic() < deadline:
                        if process.poll() is not None:
                            message = "Disposable store exited early"
                            raise RuntimeError(message)
                        try:
                            if control.get(timeout=1, partition="stop") == "stop":
                                break
                        except queue.Empty:
                            pass
                    else:
                        message = "Disposable store lifetime expired"
                        raise TimeoutError(message)
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

        @app.function(
            image=consumer,
            cpu=1,
            memory=1024,
            timeout=120,
            startup_timeout=300,
            max_containers=1,
            retries=0,
            secrets=[secret],
            serialized=True,
            include_source=False,
        )
        def consume(endpoint: str, bucket: str) -> dict[str, object]:
            import os
            import sys

            import modal
            import pyarrow as pa
            import s3fs
            from deltalake import DeltaTable, write_deltalake

            key, password = (
                os.environ["MINIO_ROOT_USER"],
                os.environ["MINIO_ROOT_PASSWORD"],
            )
            fs = s3fs.S3FileSystem(
                key=key,
                secret=password,
                client_kwargs={"endpoint_url": endpoint, "region_name": "us-east-1"},
                config_kwargs={
                    "s3": {"addressing_style": "path"},
                    "retries": {"max_attempts": 2},
                },
            )
            assert fs.cat(f"{bucket}/controller") == b"controller"
            fs.pipe(f"{bucket}/worker", b"worker")
            options = {
                "AWS_ENDPOINT_URL": endpoint,
                "AWS_ACCESS_KEY_ID": key,
                "AWS_SECRET_ACCESS_KEY": password,
                "AWS_REGION": "us-east-1",
                "AWS_EC2_METADATA_DISABLED": "true",
            }
            uri = f"s3://{bucket}/delta"
            before = DeltaTable(uri, storage_options=options)
            assert before.to_pyarrow_table().column("value").to_pylist() == [1]
            write_deltalake(
                uri, pa.table({"value": [2]}), mode="append", storage_options=options
            )
            return {
                "read_controller_object": True,
                "wrote_worker_object": True,
                "delta_version_before": before.version(),
                "delta_version_after": DeltaTable(
                    uri, storage_options=options
                ).version(),
                "sdk_version": modal.__version__,
                "sdk_origin": modal.__file__,
                "python": sys.executable,
            }

        report["phase"] = "app_startup"
        with app.run():
            report["app_id"] = app.app_id
            report["server_image_id"] = server_image.object_id
            report["consumer_derived_image_id"] = consumer.object_id
            call = serve.spawn()
            report["phase"] = "server_readiness"
            try:
                endpoint = control.get(timeout=300, partition="ready")
                if not isinstance(endpoint, str) or not endpoint.startswith("https://"):
                    message = "Store did not publish a TLS endpoint"
                    raise ValueError(message)
                report["endpoint_sha256"] = hashlib.sha256(
                    endpoint.encode()
                ).hexdigest()
                _check_storage(endpoint, credentials, consume, report)
            finally:
                control.put("stop", partition="stop")
                try:
                    report["server_cleanup"] = call.get(timeout=30)
                except Exception:
                    call.cancel(terminate_containers=True)
                    report["server_cleanup"] = {
                        "server_closed": False,
                        "ownership": "unknown",
                    }
                    raise
    report["app_context_closed"] = True
    report["queue_context_closed"] = True
    if endpoint is not None:
        _check_tunnel_closed(endpoint)
        report["tunnel_unavailable_after_close"] = True


def _check_storage(
    endpoint: str,
    credentials: dict[str, str],
    consume: object,
    report: dict[str, object],
) -> None:
    import pyarrow as pa
    import s3fs
    from deltalake import CommitProperties, DeltaTable, write_deltalake
    from deltalake.exceptions import CommitFailedError

    key, password = credentials["MINIO_ROOT_USER"], credentials["MINIO_ROOT_PASSWORD"]
    fs = s3fs.S3FileSystem(
        key=key,
        secret=password,
        client_kwargs={"endpoint_url": endpoint, "region_name": "us-east-1"},
        config_kwargs={
            "s3": {"addressing_style": "path"},
            "retries": {"max_attempts": 2},
        },
    )
    bucket = "artisan-modal-" + uuid.uuid4().hex
    report["phase"] = "signed_s3"
    fs.mkdir(bucket)
    fs.pipe(f"{bucket}/controller", b"controller")
    assert fs.cat(f"{bucket}/controller") == b"controller"
    try:
        with urlopen(f"{endpoint}/{bucket}/controller", timeout=10):
            message = "Unsigned object access unexpectedly succeeded"
            raise AssertionError(message)
    except HTTPError as error:
        response_code = error.code
    assert response_code == 403
    wrong = s3fs.S3FileSystem(
        key=key, secret=password + "wrong", client_kwargs={"endpoint_url": endpoint}
    )
    try:
        wrong.cat(f"{bucket}/controller")
    except PermissionError:
        pass
    else:
        message = "Wrong-key object access unexpectedly succeeded"
        raise AssertionError(message)
    report["unsigned_and_wrong_key_rejected"] = True
    options = {
        "AWS_ENDPOINT_URL": endpoint,
        "AWS_ACCESS_KEY_ID": key,
        "AWS_SECRET_ACCESS_KEY": password,
        "AWS_REGION": "us-east-1",
        "AWS_EC2_METADATA_DISABLED": "true",
    }
    uri = f"s3://{bucket}/delta"
    report["phase"] = "controller_delta"
    write_deltalake(uri, pa.table({"value": [1]}), storage_options=options)
    stale = DeltaTable(uri, storage_options=options)
    report["phase"] = "worker_s3_delta"
    report["worker"] = consume.remote(endpoint, bucket)  # type: ignore[attr-defined]
    assert fs.cat(f"{bucket}/worker") == b"worker"
    latest = DeltaTable(uri, storage_options=options)
    assert sorted(latest.to_pyarrow_table().column("value").to_pylist()) == [1, 2]
    try:
        write_deltalake(
            stale,
            pa.table({"value": [99]}),
            mode="overwrite",
            commit_properties=CommitProperties(max_commit_retries=0),
        )
    except CommitFailedError:
        report["stale_delta_overwrite_rejected"] = True
    else:
        message = "Stale Delta overwrite unexpectedly succeeded"
        raise AssertionError(message)
    assert DeltaTable(uri, storage_options=options).version() == 1
    assert (
        sum(item["size"] for item in fs.find(bucket, detail=True).values())
        < 32 * 1024**2
    )
    report["controller_worker_same_store"] = True
    fs.rm(bucket, recursive=True)


def _check_tunnel_closed(endpoint: str) -> None:
    deadline = time.monotonic() + 15
    while True:
        try:
            with urlopen(endpoint + "/minio/health/live", timeout=3) as response:
                if response.status != 200:
                    return
        except (HTTPError, URLError, TimeoutError, RemoteDisconnected):
            return
        if time.monotonic() >= deadline:
            message = "Tunnel still serves the test store after closure"
            raise AssertionError(message)
        time.sleep(0.2)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--consumer-image", required=True)
    parser.add_argument("--report", required=True, type=Path)
    args = parser.parse_args()
    if args.report.exists():
        parser.error("Report already exists; preserve previous evidence")
    report: dict[str, object] = {
        "sdk": modal.__version__,
        "builder": "2025.06",
        "minio_base": MINIO_IMAGE,
        "consumer_image": args.consumer_image,
        "passed": False,
        "harness_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
    }
    try:
        qualify(args.consumer_image, report)
        report["passed"] = True
    except Exception as error:
        # Store SDK/HTTP exceptions may contain signed locators or credentials.
        report["error_type"] = type(error).__name__
        if isinstance(error, ModuleNotFoundError):
            report["missing_module"] = error.name
    finally:
        args.report.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    sys.stdout.write(
        json.dumps({"passed": report["passed"], "report": str(args.report)}) + "\n"
    )
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
