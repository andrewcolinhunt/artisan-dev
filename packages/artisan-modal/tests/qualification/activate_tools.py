"""Qualify separate tool activation in an already-built canonical Modal image."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

import modal
from artisan_modal.bindings import PreparedModalImage


def probe(encoded: str) -> dict[str, object]:
    """Run tiny bounded children through the installed shared activation adapter."""
    import json
    import os
    import shutil
    import subprocess
    import tempfile
    from pathlib import Path

    from artisan_modal.bindings import PIXI, PreparedModalImage
    from artisan_modal.runtime import verify_modal_runtime
    from modal._runtime.container_io_manager import _ContainerIOManager

    from artisan.orchestration.runner_api import LocalWorkerProfile

    binding = PreparedModalImage.model_validate_json(encoded)
    verify_modal_runtime(binding.startup.runtime)
    manager = _ContainerIOManager._singleton
    assert manager is not None
    definition = manager.function_def
    runtime = {
        "image_id": definition.image_id,
        "milli_cpu": definition.resources.milli_cpu,
        "memory_mb": definition.resources.memory_mb,
        "max_inputs": definition.max_concurrent_inputs or 1,
        "timeout_seconds": definition.timeout_secs,
    }
    assert runtime["image_id"] == binding.image_id
    assert runtime["milli_cpu"] == 1000
    assert runtime["memory_mb"] == 1024
    assert runtime["max_inputs"] == 1
    assert runtime["timeout_seconds"] == 90
    observed_runtime = runtime
    scratch = Path(tempfile.mkdtemp(prefix="artisan-modal-activation-"))
    closed = False
    original = dict(os.environ)
    results = {}
    code = (
        "import json,os,sys,packaging; "
        "print(json.dumps({'python':sys.executable,'packaging':packaging.__version__,"
        "'activation':os.environ.get('ARTISAN_MODAL_TOOL'),"
        "'literal':os.environ.get('QUALIFICATION_LITERAL'),"
        "'credential':os.environ.get('QUALIFICATION_CREDENTIAL'),"
        "'pythonpath':os.environ.get('PYTHONPATH'),"
        "'loader':os.environ.get('LD_LIBRARY_PATH'),"
        "'home':os.environ.get('HOME'),'tmp':os.environ.get('TMPDIR')}))"
    )
    try:
        os.environ.update(
            PYTHONPATH="/unqualified-python-path",
            LD_LIBRARY_PATH="/unqualified-loader-path",
            ARTISAN_MODAL_TOOL="worker-ambient",
        )
        for target in binding.startup.targets:
            if target.source.name not in {"tool-a", "tool-b"}:
                continue
            profile = LocalWorkerProfile(
                target, PIXI, Path("/usr/bin/env"), binding.startup.launcher
            )
            profile.verify()
            runtime = {
                "QUALIFICATION_LITERAL": "$(false) spaces ' quotes \\",
                "QUALIFICATION_CREDENTIAL": "synthetic-selected-value",
            }
            environment = profile.environment(scratch / target.source.name, runtime)
            completed = subprocess.run(
                environment.wrap_command(["python", "-c", code]),
                env=environment.prepare_env(),
                cwd=scratch,
                check=True,
                capture_output=True,
                text=True,
                timeout=20,
            )
            result = json.loads(completed.stdout)
            assert Path(result["python"]).is_relative_to(target.prefix)
            assert result["packaging"] == (
                "24.2" if target.source.name == "tool-a" else "25.0"
            )
            assert result["activation"] == (
                "alpha" if target.source.name == "tool-a" else "beta"
            )
            assert result["literal"] == runtime["QUALIFICATION_LITERAL"]
            assert result["credential"] == runtime["QUALIFICATION_CREDENTIAL"]
            assert result["pythonpath"] is None
            assert result["loader"] is None
            assert Path(result["home"]).is_relative_to(scratch)
            assert Path(result["tmp"]).is_relative_to(scratch)
            profile.verify()
            results[target.source.name] = result
        assert set(results) == {"tool-a", "tool-b"}
        verify_modal_runtime(binding.startup.runtime)
        closed = True
    finally:
        os.environ.clear()
        os.environ.update(original)
        if closed:
            shutil.rmtree(scratch)
    return {
        "targets": results,
        "scratch_removed": not scratch.exists(),
        "actual_runtime": observed_runtime,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binding", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    if args.report.exists():
        parser.error("Evidence path exists; preserve earlier receipts")
    encoded = args.binding.read_text()
    binding = PreparedModalImage.model_validate_json(encoded)
    report: dict[str, object] = {
        "passed": False,
        "scope": "installed-image-tool-activation",
        "image_id": binding.image_id,
        "image_digest": binding.digest,
        "harness_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
    }
    app = modal.App()
    invoke = app.function(
        image=modal.Image.from_id(binding.image_id),
        cpu=1,
        memory=1024,
        max_containers=1,
        retries=0,
        timeout=90,
        startup_timeout=300,
        serialized=True,
        include_source=False,
    )(probe)
    try:
        with modal.enable_output(), app.run():
            report["result"] = invoke.remote(encoded)
        report.update(passed=True, app_context_closed=True)
    except Exception as error:
        report.update(error_type=type(error).__name__, error=str(error))
    finally:
        args.report.write_text(json.dumps(report, sort_keys=True, indent=2) + "\n")
    sys.stdout.write(
        json.dumps({"passed": report["passed"], "report": str(args.report)}) + "\n"
    )
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
