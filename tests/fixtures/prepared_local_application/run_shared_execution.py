"""Compare one creator through direct, session, callback and spawned pipeline calls.

Run in the worker environment made by create_profile.py after locking it.
Pass --profile, --cache and --output as absolute paths. The output must be new.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from unittest.mock import patch

from prepared_local_application.shared_execution import (
    ScaleValue,
    SeedValue,
    SelectedScale,
)

from artisan.composites.base.compiler import compile_definition
from artisan.execution import CalledOperation, Executor, run_operation
from artisan.execution.configured_calls import configured_calls
from artisan.execution.local_assignment import PreparedLocalBindings
from artisan.orchestration import PipelineManager, StepStatus
from artisan.orchestration.engine.current_outputs import read_current_step_outputs
from artisan.orchestration.runners import LocalRunner
from artisan.schemas import ArtifactResult, Environment, RunnerResources
from artisan.storage import ArtifactStore


def check_result(result: ArtifactResult, parent_id: str) -> str:
    output = result.artifacts["metrics"][0]
    assert output.values == {"value": 6}
    assert len(result.lineage["metrics"]) == 1
    assert result.lineage["metrics"][0].source_artifact_id == parent_id
    assert output.artifact_id is not None
    return output.artifact_id


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", type=Path, required=True)
    parser.add_argument("--cache", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    root = args.output.resolve()
    root.mkdir(parents=True, exist_ok=False)
    declaration = Environment.from_pixi(
        args.profile.resolve() / "source/pixi.toml", name="worker"
    )
    modules = ("prepared_local_application.shared_execution",)
    runner = LocalRunner(environment=declaration, cache_root=args.cache.resolve())
    selections = {"scale": CalledOperation(ScaleValue, {"factor": 2})}
    runner.prepare(
        SelectedScale, called_operations=selections, application_modules=modules
    )
    plan = compile_definition(SelectedScale, called_operations=selections)
    callback_runner = LocalRunner(cache_root=args.cache.resolve())
    bindings = callback_runner.resolve(
        plan, RunnerResources(), application_modules=modules
    )
    assert isinstance(bindings, PreparedLocalBindings)
    source = run_operation(SeedValue).artifacts["metrics"][0]
    assert source.artifact_id is not None
    inputs = {"source": [source]}
    direct = run_operation(ScaleValue, inputs=inputs)
    with Executor(working_root=root / "session") as executor:
        session = executor.run(ScaleValue, inputs=inputs)
    with (
        configured_calls(bindings._call_scope(bindings.worker, {})),
        patch(
            "artisan.execution.local_execution.select_local_software",
            side_effect=AssertionError("callback lookup"),
        ),
    ):
        callback = run_operation(ScaleValue, selection_key="scale", inputs=inputs)
    ids = {
        name: check_result(result, source.artifact_id)
        for name, result in (
            ("direct", direct),
            ("session", session),
            ("callback", callback),
        )
    }
    assert not (root / "delta").exists()
    assert executor.cleanup_complete
    with PipelineManager.create(
        "shared-execution-proof",
        delta_root=str(root / "delta"),
        staging_root=str(root / "staging"),
        working_root=str(root / "work"),
        default_step_runner=runner,
        application_modules=modules,
    ) as pipeline:
        seed = pipeline.run(SeedValue)
        assert seed.status == StepStatus.SUCCEEDED, seed.error
        store = ArtifactStore(str(root / "delta"))
        for edge_count, (name, definition, selection) in enumerate(
            (
                ("pipeline", ScaleValue, None),
                ("pipeline_callback", SelectedScale, selections),
            ),
            start=1,
        ):
            result = pipeline.run(
                definition,
                inputs={"source": seed.output("metrics")},
                called_operations=selection,
            )
            assert result.status == StepStatus.SUCCEEDED, result.error
            outputs = read_current_step_outputs(pipeline.committer, result.step_run_id)
            assert len(outputs) == len(outputs[0].occurrences) == 1
            artifact_id = outputs[0].occurrences[0].artifact.artifact_id
            output = store.get_artifact(artifact_id)
            assert output is not None
            assert output.values == {"value": 6}
            assert (
                store.provenance.get_direct_ancestors(artifact_id)
                == [source.artifact_id] * edge_count
            )
            ids[name] = artifact_id
        assert len(pipeline) == 3
    assert len(set(ids.values())) == 1, ids
    report = {
        "input": 3,
        "factor": 2,
        "outputs": dict.fromkeys(ids, 6),
        "same_artifact_id": True,
        "input_lineage_verified": True,
        "standalone_created_store": False,
        "pipeline_steps": 3,
        "callback_steps": 0,
    }
    (root / "proof.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2), flush=True)


if __name__ == "__main__":
    main()
