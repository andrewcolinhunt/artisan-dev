"""Prepare the exact installed application coverage for final Modal qualification."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import modal
from artisan_modal import ModalRunner
from artisan_modal.preparation import prepare_image
from artisan_modal.resolution import controller_compatibility, requirements
from modal_qualification_app import TwoTools

from artisan.orchestration.runner_api import Environment, compile_definition


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--cache-root", required=True, type=Path)
    parser.add_argument("--binding", required=True, type=Path)
    parser.add_argument("--report", required=True, type=Path)
    args = parser.parse_args()
    if args.binding.exists() or args.report.exists():
        parser.error("Preserve previous binding and qualification evidence")
    from final_named import installed_inventory

    before = installed_inventory()
    report: dict[str, object] = {
        "scope": "final-installed-image-preparation",
        "passed": False,
        "inventory_before": before,
    }
    try:
        manifest = args.manifest.resolve()
        worker = Environment.from_pixi(manifest, name="worker")
        plan = compile_definition(TwoTools, params={"manifest": str(manifest)})
        applications = ("modal_qualification_app",)
        providers = (ModalRunner, modal.Function)
        names, declarations = requirements(plan, providers, applications)
        with modal.enable_output():
            binding = prepare_image(
                worker,
                declarations,
                package_names=names,
                cache_root=args.cache_root.resolve(),
            )
        controller_compatibility(plan, providers, worker, binding, applications)
        args.binding.write_text(binding.model_dump_json(indent=2) + "\n")
        report.update(
            image_id=binding.image_id, image_digest=binding.digest, passed=True
        )
    except Exception as error:
        report["body_error"] = type(error).__name__
    finally:
        try:
            after = installed_inventory()
            report.update(inventory_after=after, source_unchanged=before == after)
            assert before == after
        except Exception as error:
            report.update(passed=False, inventory_error=type(error).__name__)
        args.report.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
