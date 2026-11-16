"""Build and qualify canonical worker/tool prefixes in a bounded Modal image."""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import sys
from pathlib import Path

import modal
from artisan_modal.bindings import ImageLayout
from artisan_modal.preparation import prepare_image

from artisan.operations.examples.data_transformer import DataTransformer
from artisan.orchestration.runner_api import (
    Environment,
    compile_definition,
    controller_worker_sources,
    observed_controller_sources,
    select_runtime_compatibility,
    verify_runtime_compatibility,
)


def inventory() -> dict[str, str]:
    import artisan_modal

    import artisan

    files = {}
    for name, package in (("core", artisan), ("provider", artisan_modal)):
        root = Path(package.__file__).parent
        for path in sorted(root.rglob("*")):
            if path.is_file() and "__pycache__" not in path.parts:
                files[name + "/" + path.relative_to(root).as_posix()] = hashlib.sha256(
                    path.read_bytes()
                ).hexdigest()
    return files


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--cache-root", type=Path, required=True)
    parser.add_argument("--binding", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    if args.binding.exists() or args.report.exists():
        parser.error("Evidence path exists; preserve earlier receipts")
    before = inventory()
    worker = Environment.from_pixi(args.manifest.resolve(), name="worker")
    tools = tuple(
        Environment.from_pixi(args.manifest.resolve(), name=name)
        for name in ("tool-a", "tool-b")
    )
    report = {
        "passed": False,
        "scope": "canonical-image-startup",
        "source_inventory": before,
    }
    try:
        distribution = importlib.metadata.distribution("artisan-modal")
        report["provider_installation"] = {
            "owners": importlib.metadata.packages_distributions().get("artisan_modal"),
            "version": distribution.version,
            "direct_url": json.loads(distribution.read_text("direct_url.json") or "{}"),
            "record_sha256": hashlib.sha256(
                (distribution.read_text("RECORD") or "").encode()
            ).hexdigest(),
        }
        plan = compile_definition(DataTransformer)
        roots = (ImageLayout, modal.Function)
        names, _ = observed_controller_sources(plan, roots)
        with modal.enable_output():
            binding = prepare_image(
                worker, tools, package_names=names, cache_root=args.cache_root.resolve()
            )
        actual_names, _sources = controller_worker_sources(
            plan, roots, worker, binding.startup.worker
        )
        assert actual_names == names
        assert binding.startup.worker.compatibility is not None
        verify_runtime_compatibility(
            select_runtime_compatibility(binding.startup.worker.compatibility, names)
        )
        assert inventory() == before
        args.binding.write_text(binding.model_dump_json(indent=2) + "\n")
        report.update(
            passed=True,
            source_unchanged=True,
            image_id=binding.image_id,
            image_digest=binding.digest,
            target_count=len(binding.startup.targets),
            sdk_payload=binding.startup.runtime.model_dump(mode="json"),
            packages=list(names),
            app_context_closed=True,
        )
    except Exception as error:
        report["error_type"] = type(error).__name__
        report["error"] = str(error)
    finally:
        args.report.write_text(json.dumps(report, sort_keys=True, indent=2) + "\n")
    sys.stdout.write(
        json.dumps({"passed": report["passed"], "report": str(args.report)}) + "\n"
    )
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
