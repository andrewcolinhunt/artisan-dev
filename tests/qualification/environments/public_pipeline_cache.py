"""Qualify one preparation reused by independent LocalRunner pipelines.

Run this driver from the matched installed core environment. Its supplied locked
manifest must include that exact core distribution. Preparation is explicit and
may provision the caller-owned cache; submission itself must only look up targets.
The retained store and JSON report remain under the supplied evidence directory.
"""

from __future__ import annotations

import argparse
import json
from collections.abc import Iterator
from contextlib import ExitStack, contextmanager
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from artisan.execution.environments import native, worker_qualification
from artisan.execution.local_worker import LocalAssignmentHandle
from artisan.operations.examples import DataGenerator, DataTransformer
from artisan.orchestration import PipelineManager
from artisan.orchestration.engine.current_outputs import read_current_step_outputs
from artisan.orchestration.runners import LocalRunner, local
from artisan.schemas.execution.prepared_environment import PreparedNativeEnvironment
from artisan.schemas.execution.worker_ownership import OwnershipState
from artisan.schemas.operation_config.environment import Environment
from artisan.schemas.orchestration.step_lifecycle import StepDisposition, StepStatus


def qualify(root: Path, runner: LocalRunner) -> dict[str, object]:
    """Prepare once, run another operation, and reuse across independent pipelines."""
    runner.prepare(DataGenerator)
    runs, workers, prefixes = [], [], set()
    original_source, original_outputs = (), ()
    with _without_preparation():
        for index, current_runner in enumerate((runner, replace(runner))):
            with PipelineManager.create(
                "prepared-cache",
                str(root / "delta"),
                str(root / "staging"),
                files_root=str(root / "files"),
                working_root=str(root / "working"),
                default_step_runner=current_runner,
            ) as pipeline:
                source = pipeline.run(DataGenerator, params={"count": 2, "seed": 42})
                assert source.status == StepStatus.SUCCEEDED, source.error
                assert source.disposition == (
                    StepDisposition.CACHE_HIT if index else StepDisposition.EXECUTED
                )
                transformed = pipeline.run(
                    DataTransformer,
                    inputs={"dataset": source.output("datasets")},
                    params={"scale_factor": 2.0 + index, "variants": 1},
                )
                assert transformed.status == StepStatus.SUCCEEDED, transformed.error
                assert transformed.disposition == StepDisposition.EXECUTED
                results = [source, transformed]
                selected_source = read_current_step_outputs(
                    pipeline.committer, source.step_run_id
                )
                if index == 0:
                    original_source = selected_source
                    original_outputs = read_current_step_outputs(
                        pipeline.committer, transformed.step_run_id
                    )
                else:
                    reused = pipeline.run(
                        DataTransformer,
                        inputs={"dataset": source.output("datasets")},
                        params={"scale_factor": 2.0, "variants": 1},
                    )
                    assert reused.status == StepStatus.SUCCEEDED, reused.error
                    assert reused.disposition == StepDisposition.CACHE_HIT
                    assert selected_source == original_source
                    selected = read_current_step_outputs(
                        pipeline.committer, reused.step_run_id
                    )
                    assert selected == original_outputs
                    assert sum(len(role.occurrences) for role in selected) == 2
                    assert pipeline.executions[-1].handles == ()
                    results.append(reused)
                handles = [
                    handle
                    for execution in pipeline.executions
                    for handle in execution.handles
                ]
                assert handles
                for handle in handles:
                    assert isinstance(handle, LocalAssignmentHandle)
                    assert handle.attempt.software_cacheable
                    assert handle.ownership_state() == OwnershipState.CLOSED
                    worker = handle._profile.worker
                    assert isinstance(worker, PreparedNativeEnvironment)
                    prefixes.add(worker.prefix)
                    workers.append(
                        {
                            "assignment": handle.attempt.assignment.model_dump(
                                mode="json"
                            ),
                            "software_digest": handle.attempt.software_digest,
                            "worker": worker.model_dump(mode="json"),
                        }
                    )
                runs.append(
                    {
                        "pipeline_run_id": pipeline.config.pipeline_run_id,
                        "steps": [result.model_dump(mode="json") for result in results],
                    }
                )
            assert pipeline.ownership_state() == OwnershipState.CLOSED
    assert len(prefixes) == 1
    assert runs[0]["pipeline_run_id"] != runs[1]["pipeline_run_id"]
    return {
        "runs": runs,
        "workers": workers,
        "same_original_sources": True,
        "same_prepared_prefix": True,
        "prepare_calls": 1,
    }


@contextmanager
def _without_preparation() -> Iterator[None]:
    """Fail if submission reaches preparation, installation or wheel building."""
    with ExitStack() as guards:
        for owner, names in (
            (LocalRunner, ("prepare", "prepare_curator", "_prepare_tool")),
            (local, ("prepare_native_environment", "prepare_worker_qualification")),
            (
                native,
                (
                    "prepare_native_environment",
                    "_prepare_builder",
                    "_build_wheels",
                    "_install_locked",
                    "_install_wheels",
                ),
            ),
            (worker_qualification, ("qualify_local_worker",)),
        ):
            for name in names:
                guards.enter_context(
                    patch.object(
                        owner,
                        name,
                        side_effect=AssertionError(f"Submission called {name}"),
                    )
                )
        yield


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--environment", default="default")
    parser.add_argument("--cache-root", required=True, type=Path)
    arguments = parser.parse_args()
    arguments.root.mkdir(parents=True, exist_ok=False)
    runner = LocalRunner(
        environment=Environment(
            manifest=arguments.manifest.resolve(), name=arguments.environment
        ),
        cache_root=arguments.cache_root.resolve(),
    )
    report = qualify(arguments.root.resolve(), runner)
    (arguments.root / "report.json").write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
