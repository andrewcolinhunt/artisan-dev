"""Check that example authors preserve exact parents through their own records."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from fixtures.csv import make_csv

from artisan.operations.base import OperationDefinition
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
from artisan.schemas import (
    CSVArtifact,
    ExecuteInput,
    FrozenInputContext,
    PostprocessInput,
    PreprocessInput,
)


def _datasets(directory: Path) -> list[CSVArtifact]:
    directory.mkdir()
    artifacts = [
        CSVArtifact.draft(
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
@pytest.mark.parametrize("repeated", [False, True], ids=["distinct", "repeated"])
def test_function_records_preserve_parents_with_duplicate_names(
    tmp_path: Path, operation, role: str, variants: int, repeated: bool
) -> None:
    sources = _datasets(tmp_path / "inputs")
    if repeated:
        sources.append(sources[0])
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
    if "created_files" in raw:
        assert len(set(raw["created_files"])) == len(sources) * variants
    result = operation.postprocess(
        PostprocessInput(
            memory_outputs=json.loads(json.dumps(raw)),
            file_outputs=[str(path) for path in reversed(list(execute_dir.iterdir()))],
            input_artifacts={"dataset": sources},
            step_number=1,
            postprocess_dir=str(tmp_path / "post"),
            _context=FrozenInputContext("example-lineage", {"dataset": sources}),
        )
    )
    assert len(result.artifacts[role]) == len(sources) * variants
    expected = [source.artifact_id for source in sources for _ in range(variants)]
    assert [
        (mapping.draft_index, mapping.source_role, mapping.source_artifact_id)
        for mapping in result.lineage[role]
    ] == [(index, "dataset", artifact_id) for index, artifact_id in enumerate(expected)]
    assert [mapping.source_input_index for mapping in result.lineage[role]] == [
        index for index in range(len(sources)) for _ in range(variants)
    ]
    assert all(
        artifact.original_name.startswith("shared_")
        for artifact in result.artifacts[role]
    )


@pytest.mark.parametrize(
    ("operation", "role", "suffix"),
    [
        (CsvHead(), "dataset", "head"),
        (WaitTool(), "output", "waited"),
    ],
)
def test_command_postprocess_preserves_repeated_occurrences_and_input_order(
    tmp_path: Path, operation: OperationDefinition, role: str, suffix: str
) -> None:
    sources = _datasets(tmp_path / "inputs") * 6
    outputs = []
    for index in reversed(range(len(sources))):
        output = tmp_path / f"{index}_{suffix}.csv"
        output.write_text(f"value\n{index}\n")
        outputs.append(str(output))
    result = operation.postprocess(
        PostprocessInput(
            memory_outputs=None,
            file_outputs=outputs,
            input_artifacts={"dataset": sources},
            step_number=1,
            postprocess_dir=str(tmp_path / "post"),
            _context=FrozenInputContext("example-lineage", {"dataset": sources}),
        )
    )
    assert [mapping.source_artifact_id for mapping in result.lineage[role]] == [
        source.artifact_id for source in sources
    ]
    assert [mapping.source_input_index for mapping in result.lineage[role]] == list(
        range(len(sources))
    )
    assert [artifact.content for artifact in result.artifacts[role]] == [
        f"value\n{index}\n".encode() for index in range(len(sources))
    ]
    assert [artifact.original_name for artifact in result.artifacts[role]] == [
        f"shared_{suffix}" for _ in sources
    ]
