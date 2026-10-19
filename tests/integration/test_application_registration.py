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
APPLICATION = "fixtures.registration_application"


def _run(source: str, root: Path, *, ambient: str = "") -> str:
    repo = Path(__file__).resolve().parents[2]
    environment = {
        **os.environ,
        "PYTHONPATH": os.pathsep.join([str(repo / "src"), str(repo / "tests")]),
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


@pytest.mark.parametrize("kind", ["creator", "curator"])
def test_lifecycle_bootstraps_before_recording_in_fresh_process(
    tmp_path: Path, kind: str
) -> None:
    assert APPLICATION not in sys.modules
    _run(
        f"""
        import importlib
        import sys
        from artisan.operations.examples import DataGenerator
        from artisan.execution.models.execution_unit import ExecutionUnit
        from artisan.schemas.execution.runtime_environment import RuntimeEnvironment
        from artisan.schemas.artifact.registry import ArtifactTypeDef
        from artisan.utils.process_call import execute_process_call, serialize_process_call
        assert {APPLICATION!r} not in sys.modules
        lifecycle = importlib.import_module("artisan.execution.executors.{kind}")
        def check(*args):
            assert ArtifactTypeDef.get("application_note").key == "application_note"
            assert "malicious_child_environment" not in sys.modules
            return "loaded"
        lifecycle._run_{kind}_flow = check
        unit = ExecutionUnit(operation=DataGenerator(), inputs={{}}, step_number=0, execution_spec_id="x")
        runtime = RuntimeEnvironment(delta_root="/unused", staging_root="/unused", application_modules=({APPLICATION!r},))
        call = serialize_process_call(lifecycle.run_{kind}_flow, unit, runtime)
        assert execute_process_call(call) == "loaded"
    """,
        tmp_path,
        ambient="malicious_child_environment",
    )


def test_generic_curator_ingest_resume_and_standalone_repair(tmp_path: Path) -> None:
    assert APPLICATION not in sys.modules
    source = _run(
        f"""
        import json, sys
        from pathlib import Path
        from artisan.orchestration import PipelineManager
        from artisan.registry import bootstrap_applications
        from artisan.schemas.orchestration.step_lifecycle import StepStatus
        root = Path(sys.argv[1]) / "source"
        bootstrap_applications(({APPLICATION!r},))
        from fixtures.registration_application import ApplicationNoteGenerator
        with PipelineManager.create("source", delta_root=str(root / "delta"), staging_root=str(root / "staging"), working_root=str(root / "work"), application_modules=({APPLICATION!r},)) as manager:
            step = manager.run(ApplicationNoteGenerator, params={{"count": 1, "seed": 7}}, compact=False)
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
        from artisan.schemas.orchestration.step_lifecycle import StepStatus
        assert {APPLICATION!r} not in sys.modules
        root = Path(sys.argv[1])
        with PipelineManager.create("destination", delta_root=str(root / "destination" / "delta"), staging_root=str(root / "destination" / "staging"), working_root=str(root / "destination" / "work"), application_modules=({APPLICATION!r},)) as manager:
            step = manager.run(IngestPipelineStep, params={{"source_delta_root": str(root / "source" / "delta"), "source_run_id": {source_run!r}, "source_step": 0}}, compact=False)
            assert step.status == StepStatus.SUCCEEDED, step
        with PipelineManager.resume(delta_root=str(root / "source" / "delta"), staging_root=str(root / "source" / "staging"), application_modules=({APPLICATION!r},)) as manager:
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
        from artisan.orchestration import PipelineManager, StepDisposition
        from artisan.operations.examples import DataGenerator
        from artisan.schemas.enums import TablePath
        from artisan.storage.core.committed_scan import read_committed
        root = Path(sys.argv[1])
        kwargs = dict(delta_root=str(root / "delta"), staging_root=str(root / "staging"), working_root=str(root / "work"))
        with PipelineManager.create("source", **kwargs) as manager:
            first = manager.run(DataGenerator, params={"count": 1, "seed": 9}, compact=False)
        with PipelineManager.create("cached", **kwargs, application_modules=("json",)) as manager:
            second = manager.run(DataGenerator, params={"count": 1, "seed": 9}, compact=False)
            assert second.disposition == StepDisposition.CACHE_HIT, second
            rows = read_committed(manager.config.delta_root, TablePath.STEPS, fs=manager.config.storage.filesystem())
            selected = rows.filter(__import__("polars").col("step_run_id") == second.step_run_id)
            assert all(json.loads(value)["application_modules"] == ["json"] for value in selected["compute_options_json"])
            original = rows.filter(__import__("polars").col("step_run_id") == first.step_run_id)
            assert original["step_spec_id"].drop_nulls().unique().to_list() == selected["step_spec_id"].drop_nulls().unique().to_list()
    """,
        tmp_path,
    )
