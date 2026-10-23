"""Check that example authors preserve exact parents through their own records."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest
from fixtures.csv import make_csv

from artisan.operations.examples.csv_head import CsvHead
from artisan.operations.examples.data_transformer import (
    DataTransformer,
    SequentialDataTransformer,
)
from artisan.operations.examples.metric_calculator import MetricCalculator
from artisan.operations.examples.slow_transformer import (
    SequentialSlowTransformer,
    SlowTransformer,
)
from artisan.operations.examples.wait_tool import WaitTool
from artisan.operations.lineage import match_outputs_to_inputs_by_stem
from artisan.schemas import (
    DataArtifact,
    ExecuteInput,
    PostprocessInput,
    PreprocessInput,
)


def _datasets(directory: Path) -> list[DataArtifact]:
    directory.mkdir()
    artifacts = [
        DataArtifact.draft(
            content=make_csv(rows=3, seed=seed),
            original_name="shared.csv",
            step_number=0,
        ).finalize()
        for seed in (1, 2)
    ]
    for artifact in artifacts:
        artifact.materialize_to(str(directory))
    return artifacts


@pytest.mark.parametrize(
    ("operation", "role", "variants"),
    [
        (DataTransformer(params={"variants": 2}), "dataset", 2),
        (SequentialDataTransformer(params={"variants": 2}), "dataset", 2),
        (MetricCalculator(), "metrics", 1),
        (SlowTransformer(params={"duration": 0}), "dataset", 1),
        (SequentialSlowTransformer(params={"duration": 0}), "dataset", 1),
    ],
)
def test_function_records_preserve_parents_with_duplicate_names(
    tmp_path: Path, operation, role: str, variants: int
) -> None:
    sources = _datasets(tmp_path / "inputs")
    execute_dir = tmp_path / "execute"
    execute_dir.mkdir()
    prepared = operation.preprocess(
        PreprocessInput(
            input_artifacts={"dataset": sources}, preprocess_dir=str(tmp_path / "pre")
        )
    )
    raw = operation.execute_function(
        ExecuteInput(inputs=prepared, execute_dir=str(execute_dir))
    )
    result = operation.postprocess(
        PostprocessInput(
            memory_outputs=json.loads(json.dumps(raw)),
            file_outputs=[str(path) for path in reversed(list(execute_dir.iterdir()))],
            input_artifacts={"dataset": sources},
            step_number=1,
            postprocess_dir=str(tmp_path / "post"),
        )
    )
    assert len(result.artifacts[role]) == len(sources) * variants
    expected = [source.artifact_id for source in sources for _ in range(variants)]
    assert [
        (mapping.draft_index, mapping.source_role, mapping.source_artifact_id)
        for mapping in result.lineage[role]
    ] == [(index, "dataset", artifact_id) for index, artifact_id in enumerate(expected)]
    assert all(
        artifact.original_name.startswith("shared_")
        for artifact in result.artifacts[role]
    )


@pytest.mark.parametrize(
    ("operation", "role", "suffix", "module"),
    [
        (CsvHead(), "dataset", "head", "csv_head"),
        (WaitTool(), "output", "waited", "wait_tool"),
    ],
)
def test_command_postprocess_calls_matcher_and_declares_exact_parents(
    tmp_path: Path, operation, role: str, suffix: str, module: str
) -> None:
    sources = _datasets(tmp_path / "inputs")
    outputs = []
    for source in reversed(sources):
        output = tmp_path / f"{source.artifact_id}_{suffix}.csv"
        output.write_text("value\n1\n")
        outputs.append(str(output))
    with patch(
        f"artisan.operations.examples.{module}.match_outputs_to_inputs_by_stem",
        wraps=match_outputs_to_inputs_by_stem,
    ) as matcher:
        result = operation.postprocess(
            PostprocessInput(
                memory_outputs=None,
                file_outputs=outputs,
                input_artifacts={"dataset": sources},
                step_number=1,
                postprocess_dir=str(tmp_path / "post"),
            )
        )
    matcher.assert_called_once()
    assert [mapping.source_artifact_id for mapping in result.lineage[role]] == [
        source.artifact_id for source in reversed(sources)
    ]
    assert [artifact.original_name for artifact in result.artifacts[role]] == [
        f"shared_{suffix}",
        f"shared_{suffix}",
    ]
