"""The installed pair example keeps positions when input contents repeat."""

from __future__ import annotations

from pathlib import Path

from artisan.operations.examples import CsvPair
from artisan.schemas import (
    CSVArtifact,
    ExecuteInput,
    FrozenInputContext,
    PostprocessInput,
    PreprocessInput,
)


def test_csv_pair_preserves_repeated_exact_parents(tmp_path: Path) -> None:
    left = CSVArtifact.draft(b"value\n1\n", "left.csv").finalize()
    right = CSVArtifact.draft(b"value\n2\n", "right.csv").finalize()
    left.materialize_to(str(tmp_path))
    right.materialize_to(str(tmp_path))
    context = FrozenInputContext(
        "pair-invocation", {"left": (left, left), "right": (right, right)}
    )
    operation = CsvPair()
    prepared = operation.preprocess(PreprocessInput(str(tmp_path), _context=context))
    memory = operation.execute_function(ExecuteInput(str(tmp_path), inputs=prepared))
    result = operation.postprocess(
        PostprocessInput(None, str(tmp_path), memory_outputs=memory, _context=context)
    )
    assert [artifact.content for artifact in result.artifacts["pair"]] == [
        b"value\n1\n\n---\nvalue\n2\n"
    ] * 2
    assert [edge.source_input_index for edge in result.lineage["pair"]] == [0, 0, 1, 1]
    assert [edge.source_role for edge in result.lineage["pair"]] == [
        "left",
        "right",
    ] * 2
    assert all(
        artifact.origin_step_number is None for artifact in result.artifacts["pair"]
    )
