"""Explicit deployment entry point; never invoked by the qualification tests."""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import subprocess
import sys
from pathlib import Path

import modal
from managed_cancellation_probe import APP_NAME, ManagedCancellationProbe, source_digest
from modal._utils.async_utils import synchronizer

from artisan.execution.tool_endpoint.deploy import build_app
from artisan.execution.tool_endpoint.spec import endpoint_spec


def main() -> None:
    """Create only a new, explicitly authorized temporary qualification app."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--deploy-reviewed-probe", action="store_true", required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    args = parser.parse_args()
    if args.manifest.exists():
        msg = "refusing to replace an existing deployment manifest"
        raise RuntimeError(msg)
    try:
        modal.App.lookup(APP_NAME, create_if_missing=False)
    except modal.exception.NotFoundError:
        pass
    else:
        msg = "dedicated app already exists; refusing to change it"
        raise RuntimeError(msg)
    spec = endpoint_spec(ManagedCancellationProbe)
    assert spec.secrets == []
    assert spec.volumes == {}
    report = {
        "app_name": APP_NAME,
        "app_id": None,
        "git_head": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], text=True
        ).strip(),
        "source_digest": source_digest(),
        "probe_digest": hashlib.sha256(
            Path(__file__).with_name("managed_cancellation_probe.py").read_bytes()
        ).hexdigest(),
        "modal_sdk": importlib.metadata.version("modal"),
        "image": spec.image,
        "resources": {"cpu": 1, "memory_gb": 1, "max_containers": 3, "timeout": 180},
        "max_owned_calls": 18,
    }
    args.manifest.write_text(json.dumps(report, indent=2) + "\n")
    app = build_app(ManagedCancellationProbe)
    try:
        app.deploy()
    except BaseException:
        # Modal sets this exact newly created ID before building functions;
        # App.app_id itself is populated only after deployment succeeds.
        owned_id = (
            app.app_id or synchronizer._translate_in(app)._root_load_context.app_id
        )
        report["app_id"] = owned_id
        if owned_id:
            stopped = subprocess.run(
                [sys.executable, "-m", "modal", "app", "stop", owned_id],
                timeout=45,
                capture_output=True,
                check=False,
            )
            report["failed_deployment_cleanup_returncode"] = stopped.returncode
        raise
    finally:
        report["app_id"] = app.app_id or report["app_id"]
        args.manifest.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
