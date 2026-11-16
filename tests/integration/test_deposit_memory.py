"""Bounded publication/retry/audit of streamed inline payloads larger than RAM cap."""

from __future__ import annotations

import base64
import json
import os
import random
import subprocess
import sys
import time
from collections.abc import Iterator, Mapping
from enum import StrEnum
from pathlib import Path
from typing import ClassVar

import polars as pl
import pytest
from fixtures.memory import peak_rss_bytes
from fixtures.retained_acceptance import open_committer
from fixtures.retained_curators import admit_curator, stage_curator
from pydantic import BaseModel, Field

from artisan.operations.base import OperationDefinition
from artisan.orchestration.engine.retained_curator import CuratorResults
from artisan.orchestration.engine.retained_step_tracker import RetainedStepTracker
from artisan.schemas import (
    ArtifactEmission,
    ArtifactStreamResult,
    CSVArtifact,
    MetricArtifact,
    OutputSpec,
)
from artisan.schemas.enums import TablePath
from artisan.schemas.execution.curator_admission import CuratorAdmissionReference
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.schemas.orchestration.step_lifecycle import StepStatus
from artisan.schemas.orchestration.step_result import StepResult
from artisan.storage.core.artifact_store import ArtifactStore
from artisan.storage.core.curator_view import CuratorView, capture_curator_view
from artisan.storage.io.repair import repair_store
from artisan.storage.io.retained_commit import RetainedCommitter

pytestmark = pytest.mark.integration
_MIB = 1024 * 1024
_RSS_LIMIT = 768 * _MIB
_ROWS_PER_TYPE = 192


class StreamPayloads(OperationDefinition):
    """Generate bounded drafts; publication never retains the entire stream."""

    name = "memory_payloads"

    class OutputRole(StrEnum):
        csv = "csv"
        metric = "metric"

    inputs: ClassVar[dict] = {}
    outputs: ClassVar[dict[str, OutputSpec]] = {
        kind: OutputSpec(artifact_type=kind, derives_from={"inputs": []})
        for kind in ("csv", "metric")
    }

    class Params(BaseModel):
        """Synthetic payload count and size, not execution software configuration."""

        count: int = Field(description="Rows per type.")
        size: int = Field(description="Decoded payload bytes per row.")
        offset: int = Field(default=0, description="Distinct synthetic content seeds.")

    params: Params

    def execute_curator(
        self,
        inputs: Mapping[str, pl.DataFrame],
        step_number: int,
        artifact_store: ArtifactStore,
    ) -> ArtifactStreamResult:
        def emit() -> Iterator[ArtifactEmission]:
            for kind in ("csv", "metric"):
                for index in range(self.params.count):
                    seed = self.params.offset + index
                    value = base64.b64encode(
                        random.Random(seed).randbytes(self.params.size * 3 // 4)
                    )
                    artifact = (
                        CSVArtifact.draft(
                            b"value\n"
                            + b"\n".join(
                                value[offset : offset + 64 * 1024]
                                for offset in range(0, len(value), 64 * 1024)
                            )
                            + b"\n",
                            f"csv-{seed}.csv",
                            step_number,
                        )
                        if kind == "csv"
                        else MetricArtifact.draft(
                            {"value": value.decode()},
                            f"metric-{seed}.json",
                            step_number,
                        )
                    )
                    yield ArtifactEmission(role=kind, artifact=artifact, sources={})

        return ArtifactStreamResult(items=emit())


def _committer(root: Path) -> RetainedCommitter:
    storage = StorageConfig()
    return open_committer(
        str(root / "delta"), storage.filesystem(), storage, initialize=True
    )


def _coordinator(committer: RetainedCommitter) -> CuratorResults:
    return CuratorResults(
        RetainedStepTracker(committer, "synthetic-pipeline"),
        f"{committer.delta_root}/curator-staging",
    )


def _fixtures(root: Path, count: int = _ROWS_PER_TYPE, size: int = 3 * _MIB) -> None:
    root.mkdir(parents=True)
    committer = _committer(root)
    requests = {}
    staged_bytes = {}
    for number, (phase, rows, offset) in enumerate(
        (("bulk", count, 0), ("small", 2, count - 1), ("recovery", count, 0))
    ):
        operation = StreamPayloads(
            params={"count": rows, "size": size, "offset": offset}
        )
        coordinator, admission, reference = admit_curator(
            committer,
            operation,
            {},
            step_run_id=f"{number + 1:032x}",
            step_number=number,
        )
        staged, result = stage_curator(coordinator, admission, operation)
        assert staged.success, staged.error
        requests[phase] = {
            "reference": reference.model_dump(mode="json"),
            "execution_run_id": staged.execution_run_id,
            "result": result.model_dump(mode="json"),
        }
        staged_bytes[phase] = sum(
            Path(path).stat().st_size
            for path in committer.fs.find(coordinator.staging.staging_dir)
            if str(staged.execution_run_id) in path
        )
    (root / "requests.json").write_text(json.dumps(requests))
    (root / "fixture.json").write_text(
        json.dumps(
            {
                "rows_per_type": count,
                "payload_bytes": 2 * count * size,
                "staged_bytes": staged_bytes,
            }
        )
    )


def _finish(coordinator: CuratorResults, request: dict) -> StepResult:
    return coordinator.finish(
        CuratorAdmissionReference.model_validate(request["reference"]),
        request["execution_run_id"],
        StepResult.model_validate(request["result"]),
    )


def _measure(root: Path, phase: str) -> None:
    started = time.monotonic()
    committer = _committer(root)
    coordinator = _coordinator(committer)
    requests = json.loads((root / "requests.json").read_text())
    if phase in {"bulk", "small", "recovery"}:
        if phase != "small":
            boundary = "table:artifacts/csvs" if phase == "bulk" else "plan"

            def interrupt(event: str) -> None:
                if event == boundary:
                    raise InterruptedError(boundary)

            coordinator._checkpoint = interrupt
            with pytest.raises(InterruptedError, match=boundary):
                _finish(coordinator, requests[phase])
            coordinator._checkpoint = lambda _event: None
        if phase == "recovery":
            report = repair_store(
                delta_root=committer.delta_root,
                staging_root=coordinator.staging.staging_dir,
                files_root=committer.files_root,
                fs=committer.fs,
                apply=True,
            )
            assert not report.blocking, report
        else:
            assert _finish(coordinator, requests[phase]).status == StepStatus.SUCCEEDED
        assert _finish(coordinator, requests[phase]).status == StepStatus.SUCCEEDED
    else:
        report = repair_store(
            delta_root=committer.delta_root,
            staging_root=coordinator.staging.staging_dir,
            files_root=committer.files_root,
            fs=committer.fs,
        )
        assert not report.blocking, report
    count = json.loads((root / "fixture.json").read_text())["rows_per_type"]
    expected = 2 * count + (0 if phase == "bulk" else 2)
    frame = (
        CuratorView(committer, capture_curator_view(committer))
        .scan(TablePath.ARTIFACT_INDEX.value)
        .select("artifact_id", "origin_step_number")
        .collect()
    )
    assert frame.height == frame["artifact_id"].n_unique() == expected
    assert frame.filter(pl.col("origin_step_number") == 0).height == 2 * count
    (root / f"{phase}.json").write_text(
        json.dumps(
            {
                "peak_bytes": peak_rss_bytes(),
                "visible_artifacts": expected,
                "elapsed_seconds": time.monotonic() - started,
            }
        )
    )


def _rss(pid):
    if sys.platform.startswith("linux"):
        try:
            for line in Path(f"/proc/{pid}/status").read_text().splitlines():
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) * 1024
        except FileNotFoundError:
            return 0
        return 0
    output = subprocess.run(
        ["ps", "-o", "rss=", "-p", str(pid)],
        capture_output=True,
        text=True,
        check=False,
    )
    return int(output.stdout.strip() or 0) * 1024


def _run_phase(root, phase, *, monitor):
    log = root.parent / f"{phase}.log"
    with log.open("w") as output:
        process = subprocess.Popen(
            [sys.executable, str(Path(__file__).resolve()), phase, str(root)],
            stdout=output,
            stderr=subprocess.STDOUT,
            env={
                **os.environ,
                "POLARS_MAX_THREADS": "2",
                "RAYON_NUM_THREADS": "2",
                "PYTHONPATH": str(Path(__file__).resolve().parents[1])
                + os.pathsep
                + os.environ.get("PYTHONPATH", ""),
            },
        )
        deadline = time.monotonic() + 600
        sampled_peak = 0
        try:
            while process.poll() is None:
                if monitor:
                    sampled_peak = max(sampled_peak, _rss(process.pid))
                    assert sampled_peak < _RSS_LIMIT, (
                        f"{phase} exceeded {_RSS_LIMIT // _MIB}MiB RSS: {sampled_peak / _MIB:.1f}MiB"
                    )
                assert time.monotonic() < deadline, (
                    f"{phase} exceeded ten-minute timeout"
                )
                time.sleep(0.05)
        finally:
            if process.poll() is None:
                process.kill()
            process.wait()
    assert process.returncode == 0, log.read_text()[-6000:]
    if monitor:
        result = json.loads((root / f"{phase}.json").read_text())
        result["sampled_peak_bytes"] = sampled_peak
        assert result["peak_bytes"] < _RSS_LIMIT, {"phase": phase, **result}
        return result
    return None


@pytest.mark.skipif(
    not sys.platform.startswith("linux"),
    reason="RSS qualification uses Linux process accounting",
)
def test_deposits_larger_than_rss_ceiling_survive_retry_audit_and_recovery(
    tmp_path: Path, request: pytest.FixtureRequest
) -> None:
    root = tmp_path / "store"
    _run_phase(root, "fixtures", monitor=False)
    fixture = json.loads((root / "fixture.json").read_text())
    assert fixture["payload_bytes"] > 1024 * _MIB
    assert fixture["staged_bytes"]["bulk"] > _RSS_LIMIT
    assert fixture["staged_bytes"]["recovery"] > _RSS_LIMIT
    request.node.user_properties.append(
        ("deposit_fixture_json", json.dumps(fixture, sort_keys=True))
    )
    measurements = {}
    for phase in ("bulk", "small", "audit", "recovery"):
        measurements[phase] = _run_phase(root, phase, monitor=True)
        request.node.user_properties.append(
            (f"deposit_{phase}_json", json.dumps(measurements[phase], sort_keys=True))
        )
    (tmp_path / "rss-measurements.json").write_text(
        json.dumps({"fixture": fixture, "phases": measurements}, indent=2)
    )


def test_small_fixture_uses_selected_publication_and_keeps_original_origin(
    tmp_path: Path,
) -> None:
    root = tmp_path / "small-proof"
    _fixtures(root, count=3, size=1024)
    for phase in ("bulk", "small", "audit", "recovery"):
        _measure(root, phase)


if __name__ == "__main__":
    command, directory = sys.argv[1:]
    if command == "fixtures":
        _fixtures(Path(directory))
    else:
        _measure(Path(directory), command)
