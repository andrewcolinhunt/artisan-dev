"""Exercise batch filtering through the local curator worker."""

from __future__ import annotations

import pytest

from artisan.execution.assignment_transport import AssignmentDispatch
from artisan.execution.curator_worker import AdmittedCurator
from artisan.operations.curator import Filter
from artisan.operations.examples import DataGeneratorWithMetrics
from artisan.orchestration.runners.assignment import AssignmentSoftware
from artisan.orchestration.runners.curator import CuratorHandle
from artisan.orchestration.runners.local import LocalRunner
from artisan.schemas.execution.worker_ownership import OwnershipState
from artisan.schemas.orchestration.step_lifecycle import StepStatus

from .conftest import create_native_pipeline, get_execution_outputs

pytestmark = pytest.mark.integration


@pytest.mark.parametrize(("threshold", "expected_count"), [(0.0, 1), (1.0, 0)])
def test_filter_closes_local_worker_owner(
    pipeline_env: dict[str, str],
    monkeypatch: pytest.MonkeyPatch,
    threshold: float,
    expected_count: int,
) -> None:
    handles: list[CuratorHandle] = []
    launch = LocalRunner.launch_curator

    def tracked_launch(
        runner: LocalRunner,
        curator: AdmittedCurator,
        *,
        software: AssignmentSoftware,
        dispatch: AssignmentDispatch,
    ) -> CuratorHandle:
        handle = launch(runner, curator, software=software, dispatch=dispatch)
        handles.append(handle)
        return handle

    monkeypatch.setattr(LocalRunner, "launch_curator", tracked_launch)
    with create_native_pipeline(name="filter_worker", **pipeline_env) as pipeline:
        source = pipeline.run(DataGeneratorWithMetrics, params={"seed": 42})
        result = pipeline.run(
            Filter,
            step_runner=LocalRunner(),
            inputs={"passthrough": source.output("datasets")},
            params={
                "criteria": [
                    {"metric": "mean_score", "operator": "gt", "value": threshold}
                ]
            },
        )

    assert result.status is StepStatus.SUCCEEDED
    assert result.total_count == result.succeeded_count == 1
    assert result.metadata["diagnostics"]["total_passed"] == expected_count
    delta_root = pipeline_env["delta_root"]
    source_ids = get_execution_outputs(delta_root, 0, "datasets")
    assert len(source_ids) == 1
    assert (
        get_execution_outputs(delta_root, 1, "passthrough")
        == source_ids[:expected_count]
    )
    assert len(handles) == 1
    assert handles[0].ownership_state() is OwnershipState.CLOSED
