"""Qualify admitted native creator/curator calls against one ephemeral shared S3 store.

The controller uses the normal provider runner and SDK spawn/get path. Only named
function discovery is substituted with a function in this owned ephemeral App.
This does not qualify persistent deployment, multi-unit packs, or GPU execution.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import os
import secrets
import shutil
import signal
import sys
import tempfile
import time
import uuid
from pathlib import Path
from unittest.mock import patch

import modal
from artisan_modal import ModalRunner
from artisan_modal.bindings import PreparedModalImage
from artisan_modal.diagnostics import redacted_message
from artisan_modal.image_store import publish_image
from artisan_modal.resolution import function_profile
from artisan_modal.runner import _function
from artisan_modal.submission import sensitive_values
from prepare_image import inventory
from progress import Progress
from shared_store import MINIO_IMAGE, _check_tunnel_closed

from artisan.operations.curator.ingest_csv import IngestCSV
from artisan.operations.curator.merge import Merge
from artisan.operations.examples.data_transformer import DataTransformer
from artisan.orchestration import PipelineManager
from artisan.orchestration.engine.current_outputs import read_current_step_outputs
from artisan.orchestration.runner_api import (
    Environment,
    OwnershipState,
    RunnerResources,
)
from artisan.schemas.enums import CachePolicy
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.schemas.orchestration.step_lifecycle import StepDisposition, StepStatus


def _expire(_signal: int, _frame: object) -> None:
    msg = "Bounded assignment workflow expired"
    raise TimeoutError(msg)


def _safe_error(error: BaseException, values: tuple[str, ...]) -> dict[str, str]:
    try:
        return {
            "type": type(error).__name__,
            "message": redacted_message(str(error), values),
        }
    except Exception:
        return {"type": "diagnostic_failure", "message": "Diagnostic formatting failed"}


def _stop_store(
    control: modal.Queue,
    call: modal.FunctionCall,
    report: dict[str, object],
    progress: Progress,
) -> Exception | None:
    """Retain store cleanup errors without replacing a prior scientific failure."""
    failures: list[Exception] = []
    progress.update(phase="store_stop")
    try:
        control.put("stop", partition="stop")
    except Exception as error:
        failures.append(error)
        progress.failure("server_stop_error", error)
    try:
        report["server_cleanup"] = call.get(timeout=30)
        progress.update(server_cleanup=report["server_cleanup"])
    except Exception as error:
        failures.append(error)
        report["server_cleanup"] = {"ownership": "unknown"}
        progress.failure("server_result_error", error)
        try:
            call.cancel(terminate_containers=True)
        except Exception as cancel_error:
            failures.append(cancel_error)
            progress.failure("server_cancel_error", cancel_error)
    report["server_cleanup_errors"] = [type(error).__name__ for error in failures]
    return failures[0] if failures else None


def _pipeline(
    endpoint: str,
    credentials: dict[str, str],
    runner: ModalRunner,
    scratch: Path,
    report: dict[str, object],
    progress: Progress,
) -> None:
    key, password = credentials["MINIO_ROOT_USER"], credentials["MINIO_ROOT_PASSWORD"]
    storage = StorageConfig(
        protocol="s3",
        options={
            "key": key,
            "secret": password,
            "client_kwargs": {"endpoint_url": endpoint, "region_name": "us-east-1"},
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
    progress.learn((key, password, endpoint))
    bucket = "artisan-assignments-" + uuid.uuid4().hex
    fs = storage.filesystem()
    fs.mkdir(bucket)
    delta_root, files_root, staging_root = (
        f"s3://{bucket}/{part}" for part in ("delta", "files", "staging")
    )
    pipeline: PipelineManager | None = None
    body_failed = False
    try:
        report["phase"] = "store_initialization"
        progress.update(phase="store_initialization")
        pipeline = PipelineManager.create(
            name="modal-assignment-qualification",
            delta_root=delta_root,
            files_root=files_root,
            staging_root=staging_root,
            storage=storage,
            working_root=str(scratch / "working"),
            default_step_runner=runner,
            recover_staging=False,
        )
        committer = pipeline.committer
        progress.learn(sensitive_values(pipeline.dispatch))
        report["pipeline_run_id"] = pipeline.config.pipeline_run_id
        progress.update(pipeline_run_id=pipeline.config.pipeline_run_id)
        resources = RunnerResources(cpus=1, memory_gb=1, gpus=0, time_limit="00:03:00")
        source = scratch / "input.csv"
        original = b"id,x,y,z,score\nsample,1,2,3,4\n"
        source.write_bytes(original)
        params = {"scale_factor": 2.0, "noise_amplitude": 0.0, "seed": 1}
        report["phase"] = "remote_ingest"
        progress.update(phase="remote_ingest")
        ingested = pipeline.run(
            IngestCSV, inputs=[str(source)], runner_resources=resources
        )
        assert ingested.status == StepStatus.SUCCEEDED, ingested.error
        progress.update(phase="ingest_accepted", ingest_status=ingested.status.value)
        report["ingest_status"] = ingested.status.value
        source.unlink()
        report["phase"] = "remote_transform_and_merge"
        progress.update(phase="remote_transform_and_merge")
        future = pipeline.submit(
            DataTransformer,
            inputs={"dataset": ingested.output("csv")},
            params=params,
            runner_resources=resources,
        )
        progress.update(
            phase="transform_submitted", transform_step_number=future.step_number
        )
        merge_future = pipeline.submit(
            Merge,
            inputs={
                "original": ingested.output("csv"),
                "transformed": future.output("dataset"),
            },
            runner_resources=resources,
        )
        progress.update(
            phase="merge_submitted", merge_step_number=merge_future.step_number
        )
        transformed = future.result(timeout=240)
        progress.update(
            phase="transform_result", transform_status=transformed.status.value
        )
        merged = merge_future.result(timeout=240)
        progress.update(phase="merge_result", merge_status=merged.status.value)
        assert transformed.status == merged.status == StepStatus.SUCCEEDED, (
            transformed.error,
            merged.error,
        )
        assert transformed.step_run_id
        assert merged.step_run_id
        transformed_outputs = read_current_step_outputs(
            committer, transformed.step_run_id
        )
        artifact = next(
            role.occurrences[0].artifact
            for role in transformed_outputs
            if role.role == "dataset"
        )
        row = next(csv.DictReader(io.StringIO(artifact.content.decode())))
        assert [float(row[name]) for name in ("x", "y", "z", "score")] == [
            2,
            4,
            6,
            8,
        ]
        merged_outputs = read_current_step_outputs(committer, merged.step_run_id)
        assert (
            len(
                next(
                    role.occurrences for role in merged_outputs if role.role == "merged"
                )
            )
            == 2
        )
        report["phase"] = "controller_cache_and_fresh_delivery"
        progress.update(phase="controller_cache_and_fresh_delivery")
        cached = pipeline.run(
            DataTransformer,
            inputs={"dataset": ingested.output("csv")},
            params=params,
            runner_resources=resources,
            cache_policy=CachePolicy.ALL_SUCCEEDED,
        )
        assert cached.status == StepStatus.SUCCEEDED, cached.error
        assert cached.disposition == StepDisposition.CACHE_HIT
        repeated = pipeline.run(
            DataTransformer,
            inputs={"dataset": ingested.output("csv")},
            params=params,
            runner_resources=resources,
            skip_cache=True,
        )
        assert repeated.status == StepStatus.SUCCEEDED, repeated.error
        assert repeated.disposition == StepDisposition.EXECUTED
        executions = pipeline.executions
        handles = [handle for execution in executions for handle in execution.handles]
        assert len(handles) == 4
        assert all(
            handle.done() and handle.ownership_state() == OwnershipState.CLOSED
            for handle in handles
        )
        owners = {id(handle.worker_owner): handle.worker_owner for handle in handles}
        assert all(
            owner.ownership_state() == OwnershipState.CLOSED
            for owner in owners.values()
        )
        progress_records = [
            handle.worker_owner.outcome(handle.index) for handle in handles
        ]
        assert all(
            record is not None and not record.skipped for record in progress_records
        )
        outcomes = [record.outcome for record in progress_records]
        assert all(
            outcome is not None
            and outcome.ownership == OwnershipState.CLOSED
            and not outcome.retained_working
            for outcome in outcomes
        )
        assert len({outcome.entry_id for outcome in outcomes}) == 4
        receipts = [handle.result() for handle in handles if hasattr(handle, "attempt")]
        assert len(receipts) == 2
        assert len({receipt.delivery.delivery_id for receipt in receipts}) == 2
        assert all(
            handle.attempt.software_cacheable
            for handle in handles
            if hasattr(handle, "attempt")
        )
        report["steps"] = [
            {
                "status": result.status.value,
                "disposition": result.disposition.value,
                "step_run_id": result.step_run_id,
            }
            for result in (ingested, transformed, merged, cached, repeated)
        ]
        report["entry_ids"] = [str(outcome.entry_id) for outcome in outcomes]
        report["delivery_ids"] = [receipt.delivery.delivery_id for receipt in receipts]
        report["exact_outputs"] = [2, 4, 6, 8]
        report["worker_scratch_removed"] = True
        report["shared_owner_count"] = len(owners)
        report["controller_cache_skipped_sdk_entry"] = True
        report["source_removed_after_ingestion"] = not source.exists()
        assert pipeline.ownership_state() == OwnershipState.CLOSED
        size = sum(item["size"] for item in fs.find(bucket, detail=True).values())
        assert size < 32 * 1024**2
        report["store_bytes"] = size
        report["owners_closed"] = True
    except BaseException as error:
        body_failed = True
        report["pipeline_error"] = _safe_error(
            error,
            sensitive_values(pipeline.dispatch)
            if pipeline is not None
            else (key, password, endpoint),
        )
        progress.failure("pipeline_error", error)
        raise
    finally:
        cleanup_error = None
        if pipeline is not None:
            if body_failed:
                try:
                    pipeline.cancel()
                except Exception as error:
                    cleanup_error = error
                    report["pipeline_cancel_error"] = {"type": type(error).__name__}
                    progress.failure("pipeline_cancel_error", error)
            try:
                pipeline.close()
            except Exception as error:
                cleanup_error = error
                report["pipeline_cleanup_error"] = {"type": type(error).__name__}
                progress.failure("pipeline_cleanup_error", error)
            handles = [
                handle
                for execution in pipeline.executions
                for handle in execution.handles
            ]
            report["handle_outcomes"] = [
                {
                    "handle_type": type(handle).__name__,
                    "done": handle.done(),
                    "ownership": handle.ownership_state().value,
                    "observations": [
                        event.model_dump(mode="json")
                        for event in handle.worker_owner.observations
                    ],
                    "diagnostic_write_error": handle.worker_owner.diagnostic_write_error,
                }
                for handle in handles
            ]
            progress.update(handle_outcomes=report["handle_outcomes"])
        if pipeline is None or pipeline.ownership_state() == OwnershipState.CLOSED:
            try:
                fs.rm(bucket, recursive=True)
                report["bucket_removed"] = not fs.exists(bucket)
                shutil.rmtree(scratch)
            except Exception as error:
                cleanup_error = error
                report["storage_cleanup_error"] = {"type": type(error).__name__}
                progress.failure("storage_cleanup_error", error)
        else:
            report["unresolved_scratch_retained"] = True
        if cleanup_error is not None and not body_failed:
            raise cleanup_error


def qualify(
    binding: PreparedModalImage,
    manifest: Path,
    cache_root: Path,
    report: dict[str, object],
    progress: Progress,
) -> None:
    os.environ["MODAL_IMAGE_BUILDER_VERSION"] = "2025.06"
    credentials = {
        "MINIO_ROOT_USER": "artisan" + secrets.token_hex(12),
        "MINIO_ROOT_PASSWORD": secrets.token_urlsafe(36),
    }
    secret = modal.Secret.from_dict(credentials)
    server_image = modal.Image.from_registry(MINIO_IMAGE, add_python="3.12").entrypoint(
        []
    )
    app = modal.App()
    resources = RunnerResources(cpus=1, memory_gb=1, gpus=0, time_limit="00:03:00")
    invoke = _function(app, function_profile(binding.image_id, resources))
    runner = ModalRunner(
        Environment.from_pixi(manifest.resolve(), name="worker"),
        "qualification",
        cache_root=cache_root,
    )
    publish_image(binding, cache_root=cache_root)
    endpoint: str | None = None
    with modal.Queue.ephemeral() as control:

        @app.function(
            image=server_image,
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
                        with socket.create_connection(("127.0.0.1", 9000), timeout=1):
                            break
                    except OSError:
                        time.sleep(0.1)
                with modal.forward(9000) as tunnel:
                    control.put(tunnel.url, partition="ready")
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
            report["app_id"] = app.app_id
            progress.update(phase="app_running", app_id=app.app_id)
            progress.update(phase="store_submit")
            call = serve.spawn()
            progress.update(phase="store_submitted", store_call_id=call.object_id)
            report["phase"] = "store_readiness"
            progress.update(phase="store_readiness")
            body_failed = False
            try:
                endpoint = control.get(timeout=300, partition="ready")
                assert isinstance(endpoint, str)
                assert endpoint.startswith("https://")
                report["endpoint_sha256"] = hashlib.sha256(
                    endpoint.encode()
                ).hexdigest()
                scratch = Path(tempfile.mkdtemp(prefix="modal-assignment-"))
                previous = signal.signal(signal.SIGALRM, _expire)
                signal.setitimer(signal.ITIMER_REAL, 900)
                try:
                    with patch.object(
                        ModalRunner, "_lookup_function", return_value=invoke
                    ):
                        _pipeline(
                            endpoint, credentials, runner, scratch, report, progress
                        )
                finally:
                    signal.setitimer(signal.ITIMER_REAL, 0)
                    signal.signal(signal.SIGALRM, previous)
            except BaseException as error:
                body_failed = True
                report["workflow_error"] = _safe_error(
                    error, tuple(credentials.values())
                )
                progress.failure("workflow_error", error)
                raise
            finally:
                cleanup_error = _stop_store(control, call, report, progress)
                if cleanup_error is not None and not body_failed:
                    raise cleanup_error
    report.update(app_context_closed=True, queue_context_closed=True)
    progress.update(app_context_closed=True, queue_context_closed=True)
    if endpoint is not None:
        _check_tunnel_closed(endpoint)
        report["tunnel_unavailable_after_close"] = True


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binding", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--cache-root", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    if args.report.exists():
        parser.error("Preserve earlier qualification receipts")
    binding = PreparedModalImage.model_validate_json(args.binding.read_bytes())
    before = inventory()
    report: dict[str, object] = {
        "passed": False,
        "scope": "ephemeral-sdk-native-assignment",
        "named_discovery_substituted": True,
        "persistent_deployment_qualified": False,
        "image_id": binding.image_id,
        "image_digest": binding.digest,
        "core_provider_inventory_before": before,
        "harness_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
    }
    started = time.monotonic()
    progress = Progress(args.report.with_suffix(".progress.json"))
    try:
        qualify(binding, args.manifest, args.cache_root.resolve(), report, progress)
        after = inventory()
        assert before == after
        report.update(
            passed=True, source_unchanged=True, core_provider_inventory_after=after
        )
    except Exception as error:
        report["error_type"] = type(error).__name__
        if isinstance(error, AssertionError):
            report["assertion_failed"] = True
    finally:
        progress.update(phase="finished", passed=report["passed"])
        report["diagnostic_write_error"] = progress.write_error
        progress.close()
        report["elapsed_seconds"] = time.monotonic() - started
        args.report.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    sys.stdout.write(
        json.dumps({"passed": report["passed"], "report": str(args.report)}) + "\n"
    )
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
