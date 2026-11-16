"""Small completed candidate steps for output selection and cross-store imports."""

from __future__ import annotations

import json
from pathlib import Path

from artisan.execution.lineage.graph import OccurrenceGraph
from artisan.orchestration.engine.retained_step_results import RetainedStepResults
from artisan.orchestration.engine.retained_step_tracker import RetainedStepTracker
from artisan.schemas.artifact.appendable import AppendableArtifact
from artisan.schemas.artifact.file_ref import FileRefArtifact
from artisan.schemas.execution.assignment import SealedDelivery
from artisan.schemas.execution.step_admission import StepAdmission
from artisan.schemas.orchestration.step_start_record import StepStartRecord
from artisan.schemas.specs.output_spec import OutputSpec
from artisan.storage.io.retained_commit import RetainedCommitter
from artisan.utils.hashing import compute_content_digest
from fixtures.retained_readers import admit_read_case
from fixtures.retained_results import RetainedCase, _derive, boundary_artifact


def external_case(scratch: Path, *, appendable: bool = False) -> RetainedCase:
    graph = OccurrenceGraph()
    source = graph.add_input("data", boundary_artifact())
    scratch.mkdir(parents=True, exist_ok=True)
    path = scratch / "content.bin"
    if appendable:
        payload = b'{"record_id": "selected", "value": 1}'
        path.write_bytes(payload + b'\n{"record_id": "remainder", "value": 2}\n')
        artifact = AppendableArtifact.draft(
            "selected", compute_content_digest(payload), len(payload), 7, str(path)
        ).finalize()
    else:
        payload = b"independent complete source bytes\n"
        path.write_bytes(payload)
        artifact = FileRefArtifact.draft(
            str(path), compute_content_digest(payload), len(payload), 7
        ).finalize()
    output = _derive(graph, "produce", artifact, [source])
    return RetainedCase(
        graph.project({"data": [output, output]}, {"data": OutputSpec()}),
        {source: graph.nodes[source].artifact},
        {output: graph.nodes[output].artifact},
    )


def prepare_scientific(
    committer: RetainedCommitter,
    case: RetainedCase,
    *,
    step_run_id: str = "a" * 32,
    count: int = 1,
    admit: bool = True,
) -> tuple[RetainedStepResults, StepAdmission, tuple[SealedDelivery, ...]]:
    recipe = case.recipe
    tracker = RetainedStepTracker(committer, "synthetic-pipeline")
    tracker.create_attempt(
        StepStartRecord(
            step_run_id=step_run_id,
            step_number=7,
            step_name="produce",
            operation_class=f"{recipe.definition.module}.{recipe.definition.qualname}",
            params_json=recipe.params_json,
            input_refs_json="{}",
            compute_backend="local",
            compute_options_json="{}",
            output_roles_json=json.dumps([item.role for item in recipe.outputs]),
            output_types_json=json.dumps(
                {item.role: item.artifact_type for item in recipe.outputs}
            ),
        )
    )
    admitted = tuple(
        admit_read_case(committer, case, step_run_id=step_run_id, unit_index=index)
        for index in range(count)
    )
    admission = StepAdmission(
        step_run_id=step_run_id,
        step_number=7,
        recipe=recipe,
        software_digest="c" * 64 if admitted else None,
        assignments=tuple(item[0] for item in admitted),
    )
    if admit:
        tracker.admit(admission)
    return RetainedStepResults(tracker), admission, tuple(item[1] for item in admitted)
