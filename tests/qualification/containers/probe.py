"""Run the shared sequence with explicit input/output mounts and a literal env."""

from __future__ import annotations

import argparse
import errno
import hashlib
import json
import os
import sys
from pathlib import Path

LITERAL = "literal $value $(touch /outputs/expanded) with spaces"


def harness_digest(root: Path) -> str:
    """Bind portable harness source independently of its physical host path."""
    digest = hashlib.sha256()
    for path in sorted(root.rglob("*.py")):
        if "__pycache__" not in path.parts:
            digest.update(str(path.relative_to(root)).encode())
            digest.update(path.read_bytes())
    return digest.hexdigest()


def qualify(
    prepared: Path,
    source: Path,
    output: Path,
    cwd: Path,
    hold: float,
    target_kind: str,
    target_id: str,
    launcher_version: str,
    expected_harness_digest: str,
    source_image_id: str,
) -> None:
    """Record actual runtimes, mount behavior, sealed bytes, and scratch cleanup."""
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from qualification.providers import bootstrap
    from qualification.providers.sequence import execute_sequence

    from artisan.schemas.artifact.metric import MetricArtifact

    assert Path.cwd() == cwd
    assert os.environ["QUALIFICATION_RUNTIME_LITERAL"] == LITERAL
    assert (
        harness_digest(Path(__file__).resolve().parents[1]) == expected_harness_digest
    )
    assert not (output.parent / "expanded").exists()
    original = source.read_bytes()
    try:
        source.write_bytes(original)
    except OSError as error:
        assert error.errno in {errno.EACCES, errno.EROFS}
    else:
        msg = "Input bind was writable"
        raise AssertionError(msg)
    header = {
        "runtime": bootstrap.runtime_identity(
            [
                "cloudpickle",
                "pydantic",
                "dexterity-artisan",
                "polars",
                "deltalake",
                "pyarrow",
            ]
        ),
        "framework_digest": bootstrap.framework_digest(),
        "application_digest": bootstrap.application_digest(),
        "lock_digest": hashlib.sha256(
            (prepared / "pixi.lock").read_bytes()
        ).hexdigest(),
        "selected_tools": ["a", "b"],
    }
    identity = (
        target_id
        if target_kind == "native"
        else {
            "artifact_id": target_id,
            "launcher_version": launcher_version,
            "harness_digest": expected_harness_digest,
            "source_image_id": source_image_id,
        }
    )
    descriptor = bootstrap.software_descriptor(
        header, bootstrap.target_identity(target_kind, identity)
    )
    evidence = execute_sequence(
        delivery_root=str(output),
        tool_script=str(Path(__file__).resolve().parents[1] / "providers/tool.py"),
        environments=json.loads((prepared / "tool-environments.json").read_text()),
        input_artifact=MetricArtifact.draft(json.loads(original), "input.json", 0),
        tool_hold_seconds=hold,
        software_descriptor=descriptor,
        envelope={
            "unit_id": "container-unit",
            "attempt": 0,
            "generation": 0,
            "delivery_id": output.name,
            "recipe_digest": "two-tool-sequence",
            "software_digest": bootstrap.software_digest(descriptor),
        },
    )
    evidence.update(
        {
            "cwd": str(Path.cwd()),
            "input_read_only": True,
            "runtime_literal": os.environ["QUALIFICATION_RUNTIME_LITERAL"],
            "manifest_digest": hashlib.sha256(
                (prepared / "pixi.toml").read_bytes()
            ).hexdigest(),
            "lock_digest": hashlib.sha256(
                (prepared / "pixi.lock").read_bytes()
            ).hexdigest(),
            "scratch_removed": not (output / "scratch").exists(),
            "launcher_version": launcher_version,
            "harness_digest": expected_harness_digest,
        }
    )
    (output / "container-report.json").write_text(json.dumps(evidence, indent=2) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--prepared", type=Path, required=True)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--cwd", type=Path, required=True)
    parser.add_argument("--hold", type=float, default=0)
    parser.add_argument("--target-kind", required=True)
    parser.add_argument("--target-id", required=True)
    parser.add_argument("--launcher-version", required=True)
    parser.add_argument("--harness-digest", required=True)
    parser.add_argument("--source-image-id", default="")
    arguments = parser.parse_args()
    qualify(
        arguments.prepared,
        arguments.input,
        arguments.output,
        arguments.cwd,
        arguments.hold,
        arguments.target_kind,
        arguments.target_id,
        arguments.launcher_version,
        arguments.harness_digest,
        arguments.source_image_id,
    )
