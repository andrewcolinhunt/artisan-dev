"""Fresh processes interpret domain artifacts using only trusted configuration."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from textwrap import dedent

import pytest

pytestmark = pytest.mark.integration
APPLICATION = "integration_registration_application"


def _run(source: str, root: Path, *, ambient: str = "") -> str:
    repo = Path(__file__).resolve().parents[2]
    site = root / "application-site"
    package = site / APPLICATION
    package.mkdir(parents=True, exist_ok=True)
    (package / "__init__.py").write_bytes(
        (repo / "tests/fixtures/registration_application.py").read_bytes()
    )
    metadata = site / f"{APPLICATION}-1.0.dist-info"
    metadata.mkdir(exist_ok=True)
    (metadata / "METADATA").write_text(f"Name: {APPLICATION}\nVersion: 1.0\n")
    (metadata / "RECORD").write_text(f"{APPLICATION}/__init__.py,,\n")
    environment = {
        **os.environ,
        "PYTHONPATH": os.pathsep.join(
            [str(site), str(repo / "src"), str(repo / "tests")]
        ),
        "ARTISAN_LOAD_MODULES": ambient,
        "POLARS_MAX_THREADS": "2",
        "RAYON_NUM_THREADS": "2",
    }
    result = subprocess.run(
        [sys.executable, "-c", dedent(source), str(root)],
        check=False,
        capture_output=True,
        text=True,
        env=environment,
        cwd=repo,
        timeout=90,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    return result.stdout


def test_generic_curator_ingest_resume_and_standalone_repair(tmp_path: Path) -> None:
    assert APPLICATION not in sys.modules
    source = _run(
        f"""
        import json, sys
        from pathlib import Path
        from integration.conftest import create_native_pipeline
        from artisan.registry import bootstrap_applications
        from artisan.schemas.orchestration.step_lifecycle import StepStatus
        root = Path(sys.argv[1]) / "source"
        bootstrap_applications(({APPLICATION!r},))
        from integration_registration_application import ApplicationNoteGenerator
        with create_native_pipeline(name="source", delta_root=str(root / "delta"), staging_root=str(root / "staging"), working_root=str(root / "work"), application_modules=({APPLICATION!r},)) as manager:
            __import__("os").environ["ARTISAN_LOAD_MODULES"] = "malicious_child_environment"
            step = manager.run(ApplicationNoteGenerator, params={{"count": 1, "seed": 7}})
            assert "malicious_child_environment" not in sys.modules
            assert step.status == StepStatus.SUCCEEDED, step
            print(json.dumps({{"run": manager.config.pipeline_run_id}}))
    """,
        tmp_path,
    )
    source_run = json.loads(
        next(line for line in source.splitlines() if line.startswith("{"))
    )["run"]
    _run(
        f"""
        import sys
        from pathlib import Path
        from artisan.operations.curator import IngestPipelineStep
        from artisan.orchestration import PipelineManager
        from artisan.storage.io.retained_store import open_configured_store
        from integration.conftest import create_native_pipeline, _SerialNativePipelineRunner
        from artisan.schemas.orchestration.step_lifecycle import StepStatus
        assert {APPLICATION!r} not in sys.modules
        root = Path(sys.argv[1])
        with create_native_pipeline(name="destination", delta_root=str(root / "destination" / "delta"), staging_root=str(root / "destination" / "staging"), working_root=str(root / "destination" / "work"), application_modules=({APPLICATION!r},)) as manager:
            step = manager.run(IngestPipelineStep, params={{"source_delta_root": str(root / "source" / "delta"), "source_run_id": {source_run!r}, "source_step": 0}})
            assert step.status == StepStatus.SUCCEEDED, step
        source = open_configured_store(str(root / "source" / "delta"))
        with PipelineManager.resume(delta_root=str(root / "source" / "delta"), staging_root=str(root / "source" / "staging"), default_step_runner=_SerialNativePipelineRunner(source), application_modules=({APPLICATION!r},)) as manager:
            assert manager.config.application_modules == ({APPLICATION!r},)
    """,
        tmp_path,
    )
    _run(
        f"""
        import sys
        from pathlib import Path
        from artisan.cli import main
        root = Path(sys.argv[1]) / "source"
        assert {APPLICATION!r} not in sys.modules
        assert main(["store", "repair", "--delta-root", str(root / "delta"), "--staging-root", str(root / "staging"), "--load-module", {APPLICATION!r}, "--json"]) == 0
    """,
        tmp_path,
    )


def test_cached_attempt_records_current_application_selection(tmp_path: Path) -> None:
    _run(
        """
        import json, sys
        from pathlib import Path
        import polars as pl
        from integration.conftest import create_native_pipeline
        from artisan.orchestration import StepDisposition
        from artisan.operations.examples import DataGenerator
        root = Path(sys.argv[1])
        kwargs = dict(delta_root=str(root / "delta"), staging_root=str(root / "staging"), working_root=str(root / "work"))
        with create_native_pipeline(name="source", **kwargs) as manager:
            first = manager.run(DataGenerator, params={"count": 1, "seed": 9})
            assert first.disposition == StepDisposition.EXECUTED, first
        with create_native_pipeline(name="changed-apps", **kwargs, application_modules=("json",)) as manager:
            changed = manager.run(DataGenerator, params={"count": 1, "seed": 9})
            assert changed.disposition == StepDisposition.EXECUTED, changed
        with create_native_pipeline(name="cached", **kwargs, application_modules=("json",)) as manager:
            second = manager.run(DataGenerator, params={"count": 1, "seed": 9})
            assert second.disposition == StepDisposition.CACHE_HIT, second
            with manager.committer._read_snapshot() as read:
                rows, states = manager.tracker._read_lifecycle(read)
            selected = rows.filter(pl.col("step_run_id") == second.step_run_id)
            assert all(json.loads(value)["application_modules"] == ["json"] for value in selected["compute_options_json"])
            original = rows.filter(pl.col("step_run_id") == first.step_run_id)
            assert all(json.loads(value)["application_modules"] == [] for value in original["compute_options_json"])
            assert first.step_run_id != changed.step_run_id != second.step_run_id
            assert manager.executions[0].handles == ()
    """,
        tmp_path,
    )
