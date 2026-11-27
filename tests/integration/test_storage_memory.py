"""Fresh-process recording/controller RSS diagnostics for incremental ingestion.

The explicit in-process runner exercises real admission and publication. Its
phase readings share a process; installed-worker memory is a separate gate.
"""

from __future__ import annotations

import json
import os
import random
import subprocess
import sys
from pathlib import Path

import pytest
from fixtures.memory import peak_rss_bytes
from pydantic import BaseModel, Field

from artisan.operations.curator import IngestCSV
from artisan.orchestration import StepStatus
from artisan.schemas.execution.storage_config import StorageConfig

pytestmark = pytest.mark.integration


class MeasuredIngestCSV(IngestCSV):
    """Observe the in-process admitted curator at the end of stream recording."""

    name = "measured_ingest_csv_memory_test"

    class Params(BaseModel):
        peak_path: str = Field(description="Local file receiving the worker RSS peak.")

    params: Params

    def execute_curator(self, inputs, step_number, artifact_store):
        from artisan.execution.recording.stream_recorder import StreamRecorder

        cleanup = StreamRecorder.cleanup
        peak_path = self.params.peak_path
        Path(peak_path + ".baseline").write_text(str(peak_rss_bytes()))

        def measured_cleanup(session):
            cleanup(session)
            Path(peak_path).write_text(str(peak_rss_bytes()))

        StreamRecorder.cleanup = measured_cleanup
        return super().execute_curator(inputs, step_number, artifact_store)


def _prepare_inputs(configuration):
    root = Path(configuration["root"])
    root.mkdir(parents=True)
    count = configuration["count"]
    storage = StorageConfig.model_validate(configuration["storage"])
    fs = storage.filesystem()
    prefix = configuration.get("prefix", str(root))
    files = []
    for index in range(count):
        path = root / f"source_{index:03}.csv"
        random_source = random.Random(index)
        with path.open("wb") as stream:
            stream.write(b"value\n")
            for _ in range(64):
                stream.write(random_source.randbytes(32768).hex().encode() + b"\n")
        if storage.protocol == "s3":
            target = f"{prefix}/inputs/{path.name}"
            fs.put_file(str(path), target)
            files.append(target)
        else:
            files.append(str(path))
    (root / "inputs.json").write_text(json.dumps(files))


def _probe(configuration):
    root = Path(configuration["root"])
    count = configuration["count"]
    storage = StorageConfig.model_validate(configuration["storage"])
    prefix = configuration.get("prefix", str(root))
    files = json.loads((root / "inputs.json").read_text())
    driver_start = peak_rss_bytes()
    worker_peak = root / "worker-peak"
    from integration.conftest import create_native_pipeline

    with create_native_pipeline(
        name="memory_qualification",
        delta_root=f"{prefix}/delta",
        staging_root=f"{prefix}/staging",
        working_root=str(root / "work"),
        storage=storage,
        files_root=f"{prefix}/files",
    ) as pipeline:
        result = pipeline.run(
            MeasuredIngestCSV,
            inputs=files,
            params={"peak_path": str(worker_peak)},
        )
    if result.status is not StepStatus.SUCCEEDED or result.succeeded_count != 1:
        msg = f"Memory qualification ingestion failed: {result.status}"
        raise AssertionError(msg)
    from artisan.orchestration.engine.current_outputs import read_current_step_outputs

    assert (
        sum(
            len(group.occurrences)
            for group in read_current_step_outputs(
                pipeline.committer, result.step_run_id
            )
        )
        == count
    )
    (root / "measurement.json").write_text(
        json.dumps(
            {
                "recording_peak_bytes": int(worker_peak.read_text()),
                "recording_start_bytes": int(
                    Path(str(worker_peak) + ".baseline").read_text()
                ),
                "controller_peak_bytes": peak_rss_bytes(),
                "controller_start_bytes": driver_start,
                "payload_bytes": count * (6 + 64 * 65537),
            }
        )
    )


@pytest.mark.parametrize("backend", ["local", pytest.param("s3", marks=pytest.mark.s3)])
def test_ingestion_records_worker_and_driver_rss(tmp_path, request, backend):
    storage = StorageConfig()
    prefix = None
    if backend == "s3":
        _fs, storage, prefix = request.getfixturevalue("s3_fs")
    for count in (4, 64):
        root = tmp_path / f"count-{count}"
        config = tmp_path / f"probe-{count}.json"
        values = {"root": str(root), "count": count, "storage": storage.model_dump()}
        if prefix:
            values["prefix"] = f"{prefix}/count-{count}"
        config.write_text(json.dumps(values))
        config.chmod(0o600)
        for phase in ("prepare", "measure"):
            completed = subprocess.run(
                [sys.executable, str(Path(__file__).resolve()), str(config), phase],
                capture_output=True,
                text=True,
                timeout=300,
                check=False,
                env={
                    **os.environ,
                    "POLARS_MAX_THREADS": "2",
                    "PYTHONPATH": os.pathsep.join(
                        (
                            str(Path(__file__).resolve().parents[1]),
                            os.environ.get("PYTHONPATH", ""),
                        )
                    ),
                },
            )
            assert completed.returncode == 0, (
                completed.stdout[-3000:] + completed.stderr[-3000:]
            )
        measurement = json.loads((root / "measurement.json").read_text())
        request.node.user_properties.append(
            (
                f"{backend}_rss_{count}_files_json",
                json.dumps(measurement, sort_keys=True),
            )
        )


if __name__ == "__main__":
    configuration = json.loads(Path(sys.argv[1]).read_text())
    if sys.argv[2] == "prepare":
        _prepare_inputs(configuration)
    else:
        _probe(configuration)
