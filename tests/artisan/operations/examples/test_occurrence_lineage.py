"""Package examples retain exact parents when equal content appears repeatedly."""

from __future__ import annotations

from pathlib import Path

import pytest

from artisan.composites.base.compiler import compile_definition
from artisan.execution.composition import execute_compiled_plan
from artisan.execution.compute.local import LocalExecuteRouter
from artisan.execution.session import ExecutionSession
from artisan.operations.base import OperationDefinition
from artisan.operations.examples import (
    DataTransformer,
    DataTransformerConfig,
    MetricCalculator,
)
from artisan.operations.examples.data_transformer import SequentialDataTransformer
from artisan.operations.examples.slow_transformer import (
    SequentialSlowTransformer,
    SlowTransformer,
)
from artisan.schemas.artifact.csv import CSVArtifact


@pytest.mark.parametrize(
    ("definition", "role", "params"),
    [
        (DataTransformer, "dataset", {}),
        (SequentialDataTransformer, "dataset", {}),
        (MetricCalculator, "metrics", {}),
        (
            DataTransformerConfig,
            "config",
            {"scale_factors": [2.0], "noise_amplitudes": [0.0]},
        ),
        (SlowTransformer, "dataset", {"duration": 0}),
        (SequentialSlowTransformer, "dataset", {"duration": 0}),
    ],
)
def test_repeated_inputs_keep_order_and_distinct_lineage(
    tmp_path: Path,
    definition: type[OperationDefinition],
    role: str,
    params: dict[str, object],
) -> None:
    first = CSVArtifact.draft(b"x,score\n1,2\n", "first.csv", step_number=0).finalize()
    second = CSVArtifact.draft(
        b"x,score\n3,4\n", "second.csv", step_number=0
    ).finalize()
    with ExecutionSession(working_root=tmp_path / "session") as session:
        execution = execute_compiled_plan(
            compile_definition(definition, params=params),
            {"dataset": [second, first, second]},
            session,
            invocation_id="repeated-inputs",
            execute_adapter=lambda _operation, _path: LocalExecuteRouter(),
        )
    artifacts = execution.result.artifacts[role]
    assert len(artifacts) == 3
    assert [edge.source_input_index for edge in execution.result.lineage[role]] == [
        0,
        1,
        2,
    ]
    assert [edge.source_artifact_id for edge in execution.result.lineage[role]] == [
        second.artifact_id,
        first.artifact_id,
        second.artifact_id,
    ]
    exports = execution.projection.export_roles()[role]
    assert len(set(exports)) == 3
