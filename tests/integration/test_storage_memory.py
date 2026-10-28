"""Fresh-process RSS qualification of incremental producer and Delta deposition."""

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
from artisan.orchestration import PipelineManager, StepStatus
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.schemas.orchestration.pipeline_config import PipelineConfig

pytestmark = pytest.mark.integration


class MeasuredIngestCSV(IngestCSV):
    """Observe the worker peak after all stream recording and publication."""

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
        # Generate on disk, one 64KiB CSV field at a time; never retain the fixture set.
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
    config = PipelineConfig(
        name="memory_qualification",
        delta_root=f"{prefix}/delta",
        staging_root=f"{prefix}/staging",
        working_root=str(root / "work"),
        storage=storage,
    )
    with PipelineManager(config, configure_logging=False) as pipeline:
        result = pipeline.run(
            MeasuredIngestCSV,
            inputs=files,
            params={"peak_path": str(worker_peak)},
            compact=False,
        )
    if result.status is not StepStatus.SUCCEEDED or result.succeeded_count != count:
        msg = f"Memory qualification ingestion failed: {result.status}"
        raise AssertionError(msg)
    (root / "measurement.json").write_text(
        json.dumps(
            {
                "worker_bytes": int(worker_peak.read_text()),
                "worker_start_bytes": int(
                    Path(str(worker_peak) + ".baseline").read_text()
                ),
                "driver_bytes": peak_rss_bytes(),
                "driver_start_bytes": driver_start,
                "payload_bytes": count * (6 + 64 * 65537),
            }
        )
    )


@pytest.mark.parametrize("backend", ["local", pytest.param("s3", marks=pytest.mark.s3)])
def test_ingestion_worker_and_driver_rss_do_not_scale_with_total_payload(
    tmp_path, request, backend
):
    storage = StorageConfig()
    prefix = None
    if backend == "s3":
        _fs, storage, prefix = request.getfixturevalue("s3_fs")
    measurements = []
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
        measurements.append(measurement)
        request.node.user_properties.append(
            (
                f"{backend}_rss_{count}_files_json",
                json.dumps(measurement, sort_keys=True),
            )
        )
    small, large = measurements
    # 240MiB more source payload must fit within a 96MiB native allocator envelope.
    # Both fresh processes include parsing, staging, verification and final commit.
    limit = 96 * 1024 * 1024
    assert large["payload_bytes"] - small["payload_bytes"] > 2 * limit
    for process in ("worker_bytes", "driver_bytes"):
        assert large[process] - small[process] < limit, {
            "process": process,
            "small": small,
            "large": large,
        }


if __name__ == "__main__":
    configuration = json.loads(Path(sys.argv[1]).read_text())
    if sys.argv[2] == "prepare":
        _prepare_inputs(configuration)
    else:
        _probe(configuration)
