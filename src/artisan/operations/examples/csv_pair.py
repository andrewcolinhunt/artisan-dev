"""Join aligned CSV payloads while preserving both exact input occurrences."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any, ClassVar, cast

from artisan.operations.base import OperationDefinition, PerArtifact
from artisan.schemas import (
    ArtifactResult,
    CSVArtifact,
    ExecuteInput,
    InputReference,
    InputSpec,
    OutputSpec,
    PostprocessInput,
    PreprocessInput,
)


@dataclass(frozen=True)
class _Pair:
    left: Path
    right: Path
    left_ref: InputReference
    right_ref: InputReference
    name: str


@dataclass(frozen=True)
class _Joined:
    pair: _Pair
    content: bytes


class CsvPair(OperationDefinition):
    """Concatenate each aligned pair, separating its two source payloads visibly.

    The caller selects NAME, ZIP or CROSS_PRODUCT grouping. This example keeps
    both original byte streams, including their headers, with a separator row.
    """

    name = "csv_pair"
    description = "Join paired CSV payloads with exact two-input lineage"

    class InputRole(StrEnum):
        left = "left"
        right = "right"

    class OutputRole(StrEnum):
        pair = "pair"

    inputs: ClassVar[dict[str, InputSpec]] = {
        InputRole.left: InputSpec(artifact_type="csv"),
        InputRole.right: InputSpec(artifact_type="csv"),
    }
    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.pair: OutputSpec(
            artifact_type="csv", derives_from={"inputs": ["left", "right"]}
        ),
    }

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        """Keep each aligned pair's invocation references beside its local paths."""
        pairs = []
        for index, group in enumerate(inputs.grouped()):
            left, right = (
                cast(CSVArtifact, group["left"]),
                cast(CSVArtifact, group["right"]),
            )
            assert left.materialized_path is not None
            assert right.materialized_path is not None
            pairs.append(
                _Pair(
                    Path(left.materialized_path),
                    Path(right.materialized_path),
                    inputs.ref("left", index=index),
                    inputs.ref("right", index=index),
                    f"{left.original_name}__{right.original_name}.csv",
                )
            )
        return {"pairs": PerArtifact(pairs)}

    def execute_function(self, inputs: ExecuteInput) -> dict[str, Any]:
        """Join each pair without deduplicating equal content or repeated inputs."""
        return {
            "joined": [
                _Joined(
                    pair, pair.left.read_bytes() + b"\n---\n" + pair.right.read_bytes()
                )
                for pair in inputs.inputs["pairs"]
            ]
        }

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        """Declare both exact parents for every joined output."""
        result = ArtifactResult()
        for joined in inputs.memory_outputs["joined"]:
            result.add_artifact(
                "pair",
                CSVArtifact.draft(
                    content=joined.content,
                    original_name=joined.pair.name,
                    step_number=inputs.step_number,
                ),
                sources={
                    "left": [joined.pair.left_ref],
                    "right": [joined.pair.right_ref],
                },
            )
        return result
