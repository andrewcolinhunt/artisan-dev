"""Interactive selection uses the ordinary local curator worker lifecycle."""

from __future__ import annotations

from pathlib import Path

import pytest
from fixtures.interactive_store import commit_interactive_case

from artisan.execution.assignment_transport import AssignmentDispatch
from artisan.execution.curator_worker import AdmittedCurator
from artisan.operations.curator.interactive_filter import InteractiveFilter
from artisan.orchestration.engine.current_outputs import read_current_step_outputs
from artisan.orchestration.runners.assignment import AssignmentSoftware
from artisan.orchestration.runners.curator import CuratorHandle
from artisan.orchestration.runners.local import LocalRunner
from artisan.schemas import CSVArtifact, MetricArtifact
from artisan.schemas.execution.worker_ownership import OwnershipState
from artisan.schemas.orchestration.step_lifecycle import StepStatus
from artisan.storage.io.retained_store import open_configured_store


def test_interactive_selection_uses_actual_local_worker_and_closes_owner(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "delta"
    data = CSVArtifact.draft(b"value\n1\n", "sample.csv", 0).finalize()
    metric = MetricArtifact.draft({"score": 0.9}, "score.json", 1).finalize()
    commit_interactive_case(root, [data], {}, run="interactive", number=0, name="data")
    commit_interactive_case(
        root,
        [metric],
        {metric.artifact_id: [data]},
        run="interactive",
        number=1,
        name="metric",
    )
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
    selected = InteractiveFilter(str(root))
    selected.load()
    selected.set_criteria([{"metric": "score", "operator": "gt", "value": 0.5}])
    result = selected.commit()
    assert result.status == StepStatus.SUCCEEDED
    assert result.total_count == result.succeeded_count == 1
    assert result.metadata["diagnostics"]["total_passed"] == 1
    assert result.step_run_id is not None
    outputs = read_current_step_outputs(
        open_configured_store(str(root)), result.step_run_id
    )
    assert [
        item.artifact.artifact_id for group in outputs for item in group.occurrences
    ] == [data.artifact_id]
    assert len(handles) == 1
    assert handles[0].ownership_state() == OwnershipState.CLOSED
    if handles[0].worker_owner is not None:
        assert handles[0].worker_owner.ownership_state() == OwnershipState.CLOSED
