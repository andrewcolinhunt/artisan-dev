"""Portable, hard-coded worker lifecycle used only for provider qualification."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path


def command_environment(
    environment: dict[str, str], scratch: Path, owner: str, *, hold_seconds: float = 0
) -> dict[str, str]:
    """Construct an explicit command environment with selected runtime injections."""
    selected = {
        **environment,
        "TMPDIR": str(scratch),
        "QUALIFICATION_HOLD_SECONDS": str(hold_seconds),
        "QUALIFICATION_SELECTED_CREDENTIAL": "synthetic-selected",
        "QUALIFICATION_OWNER": owner,
    }
    selected.update(
        {
            key: os.environ[key]
            for key in (
                "CUDA_VISIBLE_DEVICES",
                "NVIDIA_VISIBLE_DEVICES",
                "NVIDIA_DRIVER_CAPABILITIES",
            )
            if key in os.environ
        }
    )
    return selected


def execute_sequence(
    *,
    delivery_root: str,
    tool_script: str,
    environments: list[dict[str, str]],
    value: int = 5,
    delay_seconds: float = 0,
    tool_hold_seconds: float = 0,
    input_artifact: object = None,
    input_schema: object = None,
    envelope: dict[str, object] | None = None,
    scratch_root: str | None = None,
    software_descriptor: dict[str, object] | None = None,
) -> dict[str, object]:
    """Run native, pre, two command, and post phases in one worker process."""
    from artisan.schemas.artifact.file_ref import FileRefArtifact
    from artisan.schemas.artifact.metric import MetricArtifact
    from artisan.utils.hashing import compute_content_digest

    root = Path(delivery_root)
    delivery_id = os.environ.get("QUALIFICATION_DELIVERY_ID")
    if delivery_id is not None:
        root = root.with_name(delivery_id)
        if envelope is not None:
            envelope = {**envelope, "delivery_id": delivery_id}
    root.mkdir(parents=True, exist_ok=False)
    scratch = Path(scratch_root) / root.name if scratch_root else root / "scratch"
    scratch.mkdir(parents=True, exist_ok=False)
    phases = [{"phase": "native", "pid": os.getpid()}]
    source = scratch / "input.json"
    if input_schema is not None:
        from application_schema import ProbeInput

        assert isinstance(input_schema, ProbeInput)
        input_artifact = input_schema.metric
    if input_artifact is not None:
        assert isinstance(input_artifact, MetricArtifact)
        value = json.loads(input_artifact.content)["value"]
    native = MetricArtifact.draft({"value": value}, "native.json", 1)
    source.write_bytes(native.content)
    phases.append({"phase": "pre", "pid": os.getpid()})
    (root / "worker-started.json").write_text(json.dumps({"pid": os.getpid()}))
    time.sleep(delay_seconds)
    for role, environment in zip(("a", "b"), environments, strict=True):
        destination = scratch / f"{role}.json"
        child_environment = command_environment(
            environment, scratch, root.name, hold_seconds=tool_hold_seconds
        )
        subprocess.run(
            [environment["PYTHON"], tool_script, str(source), str(destination), role],
            cwd=scratch,
            env=child_environment,
            check=True,
            timeout=30,
        )
        phases.append({"phase": role, "pid": os.getpid()})
        source = destination
    staged = root / "staged"
    staged.mkdir()
    for path in scratch.glob("*.json"):
        shutil.copyfile(path, staged / path.name)
    output = json.loads(source.read_text())
    result_artifact = MetricArtifact.draft({"value": output["value"]}, "result.json", 1)
    result_artifact.finalize()
    retained_file = root / "retained-output.json"
    retained_bytes = json.dumps({"value": output["value"]}, sort_keys=True).encode()
    retained_file.write_bytes(retained_bytes)
    file_artifact = FileRefArtifact.draft(
        str(retained_file),
        compute_content_digest(retained_bytes),
        len(retained_bytes),
        1,
        original_name="final",
        extension=".json",
    )
    file_artifact.finalize()
    phases.append({"phase": "post", "pid": os.getpid()})
    evidence = {
        "worker_pid": os.getpid(),
        "delivery_id": root.name,
        "slurm_job_id": os.environ.get("SLURM_JOB_ID"),
        "slurm_step_id": os.environ.get("SLURM_STEP_ID"),
        "slurm_restart_count": os.environ.get("SLURM_RESTART_COUNT", "0"),
        "scratch_root": str(scratch),
        "worker_executable": sys.executable,
        "phases": phases,
        "value": output["value"],
        "artifact": result_artifact.model_dump(mode="json"),
        "software_descriptor": software_descriptor,
        "staged": {
            path.name: hashlib.sha256(path.read_bytes()).hexdigest()
            for path in staged.iterdir()
        },
        "tools": [json.loads((staged / f"{role}.json").read_text()) for role in "ab"],
    }
    (root / "evidence.json").write_text(json.dumps(evidence, indent=2) + "\n")
    shutil.rmtree(scratch)
    if envelope is not None:
        import fsspec
        from qualification.lineage.projection import (
            BoundaryProjection,
            Occurrence,
            Relation,
        )
        from qualification.persistence.prototype import Envelope, stage_delivery

        projection = BoundaryProjection(
            boundary_inputs={},
            occurrences=(
                Occurrence(
                    key="final",
                    artifact_id=file_artifact.artifact_id,
                    artifact_type=file_artifact.artifact_type,
                ),
                Occurrence(
                    key="result",
                    artifact_id=result_artifact.artifact_id,
                    artifact_type="metric",
                ),
            ),
            exports={"files": ("final",), "metrics": ("result",)},
            parents=(Relation(source="final", target="result"),),
            associations=(Relation(source="final", target="result"),),
        )
        evidence["seal_digest"] = stage_delivery(
            str(root / "candidate"),
            Envelope.model_validate(envelope),
            projection,
            {
                result_artifact.artifact_id: result_artifact.model_dump_json().encode(),
                file_artifact.artifact_id: file_artifact.model_dump_json().encode(),
            },
            fs=fsspec.filesystem("file"),
        )
        evidence["projection"] = projection.model_dump(mode="json")
        (root / "evidence.json").write_text(json.dumps(evidence, indent=2) + "\n")
    retained_file.unlink()
    return evidence


def execute_scheduler_sequence(
    *, wait_for_requeue: bool = False, **arguments: object
) -> dict[str, object]:
    """Assign a fresh delivery on each observed scheduler entry."""
    import uuid

    restart = int(os.environ.get("SLURM_RESTART_COUNT", "0"))
    os.environ["QUALIFICATION_DELIVERY_ID"] = f"restart-{restart}-{uuid.uuid4().hex}"
    try:
        return execute_sequence(
            **arguments, delay_seconds=30 if wait_for_requeue and restart == 0 else 0
        )
    finally:
        os.environ.pop("QUALIFICATION_DELIVERY_ID", None)
