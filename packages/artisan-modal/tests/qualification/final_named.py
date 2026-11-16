"""Bounded installed named-pack, sibling cancellation and GPU qualification.

Run one selected group per invocation. Preparation is a separate command. Every
body and cleanup outcome is retained; expected UNKNOWN cases preserve evidence.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib
import importlib.metadata
import json
import os
import shutil
import signal
import tempfile
import time
from pathlib import Path
from typing import Any

from artisan_modal.bindings import PreparedModalImage
from assignment import _expire
from final_cases import (
    APPLICATIONS,
    cancellation,
    curator_gpu,
    nominal,
    nonzero,
    stale_gpu,
)
from final_environment import SharedStore, named_runner, shared_store
from final_redelivery import redelivery
from progress import Progress

from artisan.orchestration import PipelineManager
from artisan.orchestration.runner_api import (
    CommandOwnershipError,
    OwnershipState,
    RunnerResources,
)


def installed_inventory() -> dict[str, object]:
    """Require ordinary installed wheels, including the application definitions."""
    result: dict[str, object] = {}
    for module, distribution in (
        ("artisan", "dexterity-artisan"),
        ("artisan_modal", "artisan-modal"),
        ("modal_qualification_app", "artisan-modal-qualification"),
    ):
        package = importlib.import_module(module)
        owner = importlib.metadata.distribution(distribution)
        direct = json.loads(owner.read_text("direct_url.json") or "{}")
        assert not direct.get("dir_info", {}).get("editable", False)
        assert importlib.metadata.packages_distributions().get(module) == [distribution]
        assert package.__file__ is not None
        root = Path(package.__file__).parent
        record = owner.read_text("RECORD")
        assert record
        result[module] = {
            "version": owner.version,
            "record_sha256": hashlib.sha256(record.encode()).hexdigest(),
            "files": {
                str(path.relative_to(root)): hashlib.sha256(
                    path.read_bytes()
                ).hexdigest()
                for path in sorted(root.rglob("*"))
                if path.is_file() and "__pycache__" not in path.parts
            },
        }
    return result


def _quarantine(store: SharedStore, scratch: Path, report: dict[str, Any]) -> None:
    """Retain bounded diagnostic copies after UNKNOWN; never claim a store snapshot."""
    fs = store.storage.filesystem()
    files = fs.find(store.bucket, detail=True)
    size = sum(item["size"] for item in files.values())
    assert size <= 32 * 1024**2
    copied: dict[str, dict[str, Any]] = {}
    destination = scratch / "unresolved-store"
    destination.mkdir()
    for source in sorted(files):
        relative = source.removeprefix(store.bucket + "/")
        assert relative != source
        assert not Path(relative).is_absolute()
        assert ".." not in Path(relative).parts
        remaining = 32 * 1024**2 - sum(item["bytes"] for item in copied.values())
        with fs.open(source, "rb") as stream:
            data = stream.read(remaining + 1)
        assert len(data) <= remaining
        path = destination / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        copied[relative] = {
            "sha256": hashlib.sha256(data).hexdigest(),
            "bytes": len(data),
        }
    report["unresolved_copy"] = {
        "scope": "diagnostic copy; no consistent-snapshot or recovery authority",
        "files": copied,
        "bytes": size,
    }


def _exercise(
    group: str,
    pipeline: PipelineManager,
    runner: Any,
    manifest: Path,
    resources: RunnerResources,
    report: dict[str, Any],
    progress: Progress,
) -> None:
    results: dict[str, Any] = {}
    report["cases"] = results
    progress.update(
        phase="science", group=group, pipeline_run_id=pipeline.config.pipeline_run_id
    )
    if group == "cpu":
        results["named_pack"] = nominal(pipeline, manifest, resources)
        progress.update(phase="nonzero")
        results["nonzero_continue"] = nonzero(pipeline, manifest, resources)
        progress.update(phase="sibling_cancel")
        assert (
            results["named_pack"]["entries"]["pack_entry"]
            != results["nonzero_continue"]["entries"]["pack_entry"]
        )
        results["sibling_cancel"] = cancellation(
            pipeline, runner, manifest, resources, unknown=False
        )
    elif group == "gpu":
        results["native_and_commands"] = nominal(pipeline, manifest, resources)
        progress.update(phase="gpu_curator")
        results["curator"] = curator_gpu(pipeline, resources)
        progress.update(phase="gpu_stale_negative")
        results["stale_profile"] = stale_gpu(pipeline, runner, resources)
    elif group == "redelivery":
        results["redelivery"] = redelivery(pipeline, runner, manifest, resources)
    else:
        results["native_unknown"] = cancellation(
            pipeline, runner, manifest, resources, unknown=True
        )
    report["pipeline_ownership"] = pipeline.ownership_state().value
    if group in {"cpu", "redelivery"}:
        assert pipeline.ownership_state() == OwnershipState.CLOSED
    else:
        assert pipeline.ownership_state() == OwnershipState.UNKNOWN
        report["expected_unknown_retained"] = True


def _close_pipeline(
    pipeline: PipelineManager,
    report: dict[str, Any],
    progress: Progress,
    *,
    failed: bool,
) -> None:
    errors: list[BaseException] = []
    if failed:
        try:
            pipeline.cancel()
        except Exception as error:
            errors.append(error)
            progress.failure("pipeline_cancel_error", error)
    try:
        pipeline.close()
    except Exception as error:
        progress.failure("pipeline_close_error", error)
        expected = (
            isinstance(error, CommandOwnershipError)
            and report.get("expected_unknown_retained")
            and pipeline.ownership_state() == OwnershipState.UNKNOWN
        )
        if not expected:
            errors.append(error)
        report["pipeline_close_error"] = type(error).__name__
    report["pipeline_ownership"] = pipeline.ownership_state().value
    report["pipeline_cleanup_errors"] = [type(error).__name__ for error in errors]
    if errors and not failed:
        raise errors[0]


def _finish_store(
    store: SharedStore,
    scratch: Path,
    report: dict[str, Any],
    progress: Progress,
    *,
    failed: bool,
) -> None:
    try:
        if report.get("pipeline_ownership") == "closed":
            fs = store.storage.filesystem()
            assert (
                sum(
                    item["size"] for item in fs.find(store.bucket, detail=True).values()
                )
                <= 32 * 1024**2
            )
            fs.rm(store.bucket, recursive=True)
            report["bucket_removed"] = not fs.exists(store.bucket)
            assert report["bucket_removed"]
        else:
            report["unresolved_scratch_retained"] = str(scratch)
            _quarantine(store, scratch, report)
    except Exception as error:
        report["store_retention_error"] = type(error).__name__
        progress.failure("store_retention_error", error)
        if not failed:
            raise


def run_group(
    group: str,
    binding: PreparedModalImage,
    manifest: Path,
    cache: Path,
    scratch: Path,
    report: dict[str, Any],
    progress: Progress,
) -> None:
    resources = RunnerResources(
        cpus=1,
        memory_gb=1,
        gpus=1 if group == "gpu" else 0,
        extra={"gpu_type": "A10G"} if group == "gpu" else {},
        time_limit="00:03:00",
    )
    failed = False
    with shared_store(report, progress) as store:
        try:
            with named_runner(
                binding, manifest, cache, resources, report, progress
            ) as runner:
                pipeline = PipelineManager.create(
                    name="modal-final-" + group,
                    delta_root=store.root("delta"),
                    files_root=store.root("files"),
                    staging_root=store.root("staging"),
                    storage=store.storage,
                    working_root=str(scratch / "working"),
                    default_step_runner=runner,
                    recover_staging=False,
                    application_modules=APPLICATIONS,
                )
                report["pipeline_run_id"] = pipeline.config.pipeline_run_id
                try:
                    _exercise(
                        group, pipeline, runner, manifest, resources, report, progress
                    )
                except BaseException:
                    failed = True
                    raise
                finally:
                    # Request unit closure before stopping the SDK interpreter.
                    _close_pipeline(pipeline, report, progress, failed=failed)
        except BaseException:
            failed = True
            raise
        finally:
            _finish_store(store, scratch, report, progress, failed=failed)


def finish_report(
    scratch: Path,
    before: dict[str, object],
    report: dict[str, Any],
    progress: Progress,
) -> None:
    """Qualification observation failures cannot bypass owned cleanup or hide errors."""
    try:
        after = installed_inventory()
        report["inventory_after"] = after
        report["source_unchanged"] = before == after
        assert before == after
    except Exception as error:
        report["passed"] = False
        report["inventory_error"] = type(error).__name__
        progress.failure("inventory_error", error)
    if (
        report.get("pipeline_ownership") == "closed"
        and report.get("worker_app_cleanup", {}).get("tasks") == 0
        and report.get("store_app_cleanup", {}).get("tasks") == 0
        and report.get("bucket_removed")
        and report.get("tunnel_closed")
        and report.get("queue_context_closed")
        and report.get("server_cleanup")
        == {"server_closed": True, "tunnel_context_closed": True}
    ):
        try:
            shutil.rmtree(scratch)
            report["scratch_removed"] = True
        except Exception as error:
            report["passed"] = False
            report["scratch_removed"] = False
            report["scratch_cleanup_error"] = type(error).__name__
            progress.failure("scratch_cleanup_error", error)
    else:
        report["scratch_removed"] = False


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--case", required=True, choices=("cpu", "gpu", "redelivery", "unknown")
    )
    parser.add_argument("--binding", required=True, type=Path)
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--cache-root", required=True, type=Path)
    parser.add_argument("--working-root", required=True, type=Path)
    parser.add_argument("--report", required=True, type=Path)
    args = parser.parse_args()
    if args.report.exists() or args.report.with_suffix(".progress.json").exists():
        parser.error("Preserve existing qualification evidence")
    assert args.working_root.is_absolute()
    assert args.working_root.is_dir()
    before = installed_inventory()
    binding = PreparedModalImage.model_validate_json(args.binding.read_bytes())
    scratch = Path(tempfile.mkdtemp(prefix="modal-final-", dir=args.working_root))
    report: dict[str, Any] = {
        "passed": False,
        "case": args.case,
        "named_discovery_substituted": False,
        "image_id": binding.image_id,
        "image_digest": binding.digest,
        "inventory_before": before,
        "scratch": str(scratch),
        "harness_files": {
            path.name: hashlib.sha256(path.read_bytes()).hexdigest()
            for path in sorted(Path(__file__).parent.glob("*.py"))
        },
    }
    progress = Progress(args.report.with_suffix(".progress.json"))
    previous = signal.signal(signal.SIGALRM, _expire)
    started = time.monotonic()
    signal.setitimer(signal.ITIMER_REAL, 900)
    os.environ["MODAL_IMAGE_BUILDER_VERSION"] = "2025.06"
    try:
        run_group(
            args.case,
            binding,
            args.manifest.resolve(),
            args.cache_root.resolve(),
            scratch,
            report,
            progress,
        )
        report["passed"] = True
    except Exception as error:
        report["body_error"] = type(error).__name__
        progress.failure("body_error", error)
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)
        finish_report(scratch, before, report, progress)
        report["elapsed_seconds"] = time.monotonic() - started
        progress.update(phase="finished", passed=report["passed"])
        report["diagnostic_write_error"] = progress.write_error
        progress.close()
        args.report.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
