"""A tool-owned model preset expanded into ordinary artifact inputs.

Import this example explicitly: its external artifact types are optional.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any, ClassVar, Literal, cast

from artisan.operations.base import OperationDefinition, PerArtifact
from artisan.schemas import (
    Artifact,
    ArtifactResult,
    GroupByStrategy,
    InputSpec,
    MetricArtifact,
    OutputSpec,
    PostprocessInput,
    PreprocessInput,
    ToolSpec,
)
from artisan.schemas.external_artifacts import (
    ExternalFile,
    ExternalFilesDescriptor,
    ModelWeightsArtifact,
)
from artisan.utils.hashing import compute_content_digest

__all__ = [
    "ScaleSelection",
    "ScaleWithExternalWeights",
    "default_scale_weights",
    "scale_with_weights",
]


def default_scale_weights() -> ModelWeightsArtifact:
    """Describe the pinned synthetic scalar checkpoint, without filesystem I/O."""
    descriptor = ExternalFilesDescriptor(
        files=(
            ExternalFile(
                path="scale.txt",
                content_hash=compute_content_digest(b"2\n"),
                size_bytes=2,
            ),
        )
    )
    return cast(
        ModelWeightsArtifact,
        ModelWeightsArtifact.draft_weights(
            descriptor,
            model_family="example-scalar",
        ).finalize(),
    )


class ScaleWithExternalWeights(OperationDefinition):
    """Multiply each metric value by the scalar in an external checkpoint.

    The two-role cross product is explicit: one checkpoint shares across values;
    several supplied checkpoints create a deliberate all-combinations sweep.
    """

    name = "scale_with_external_weights"
    description = "Scale metric values with an explicitly selected external checkpoint"

    class InputRole(StrEnum):
        value = "value"
        weights = "weights"

    class OutputRole(StrEnum):
        result = "result"

    inputs: ClassVar[dict[str, InputSpec]] = {
        InputRole.value: InputSpec(artifact_type="metric", materialize=False),
        InputRole.weights: InputSpec(artifact_type="model_weights", materialize=False),
    }
    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.result: OutputSpec(
            artifact_type="metric",
            derives_from={"inputs": ["value", "weights"]},
        ),
    }
    group_by: GroupByStrategy | None = GroupByStrategy.CROSS_PRODUCT
    tool: ToolSpec = ToolSpec(executable="python", subcommand="-m")

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        """Pass each selected checkpoint path directly to the installed command."""
        items = []
        for index, group in enumerate(inputs.grouped()):
            value = cast(MetricArtifact, group["value"])
            weights = cast(ModelWeightsArtifact, group["weights"])
            _validate_weights(weights)
            items.append(
                {
                    "index": index,
                    "value": float(value.values["value"]),
                    "weights": str(weights.external_root / "scale.txt"),
                }
            )
        return {"items": PerArtifact(items)}

    def execute_command(self, inputs: dict[str, Any]) -> list[str]:
        """Read the selected installation in a separate Python process."""
        item = inputs["items"]
        return [
            *self.tool.parts(),
            "artisan.operations.examples.scripts.scale_external",
            "--value",
            str(item["value"]),
            "--weights",
            item["weights"],
            "--output",
            f"{item['index']}_scaled.json",
        ]

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        """Attach both exact input occurrences to each result."""
        result = ArtifactResult(artifacts={"result": []}, lineage={"result": []})
        paths = {Path(path).name: Path(path) for path in inputs.file_outputs}
        for index in range(len(inputs.input_artifacts["value"])):
            content = json.loads(paths[f"{index}_scaled.json"].read_text())
            result.add_artifact(
                "result",
                MetricArtifact.draft(
                    content=content,
                    original_name="scaled.json",
                    step_number=inputs.step_number,
                ),
                sources={
                    "value": [inputs.ref("value", index=index)],
                    "weights": [inputs.ref("weights", index=index)],
                },
            )
        return result


@dataclass(frozen=True)
class ScaleSelection:
    """Inspectable tool selection whose origin is separate from scientific inputs."""

    values: tuple[MetricArtifact, ...]
    weights: ModelWeightsArtifact
    origin: Literal["preset", "override"]
    operation: ClassVar[type[ScaleWithExternalWeights]] = ScaleWithExternalWeights

    @property
    def inputs(self) -> dict[str, tuple[Artifact, ...]]:
        """Return complete ordinary inputs, including exactly one checkpoint."""
        return {"value": self.values, "weights": (self.weights,)}


def scale_with_weights(
    values: Sequence[MetricArtifact],
    *,
    weights: ModelWeightsArtifact | None = None,
) -> ScaleSelection:
    """Select a pinned default or replace it with one explicit checkpoint.

    This function performs no I/O and no execution. Inspect ``weights`` and
    ``origin`` before passing ``operation`` and ``inputs`` to Artisan. Multiple
    checkpoints require an explicit operation input stream rather than a preset.
    """
    selected = _select_weights(weights)
    selected.finalize()
    _validate_weights(selected)
    return ScaleSelection(
        tuple(values), selected, "preset" if weights is None else "override"
    )


def _validate_weights(weights: ModelWeightsArtifact) -> None:
    if weights.model_family != "example-scalar" or tuple(
        item.path for item in weights.external_descriptor().files
    ) != ("scale.txt",):
        msg = "This example requires example-scalar weights with one scale.txt file"
        raise ValueError(msg)


def _select_weights(value: object) -> ModelWeightsArtifact:
    if value is None:
        return default_scale_weights()
    if not isinstance(value, ModelWeightsArtifact):
        msg = "A preset accepts one model-weights artifact; declare sweeps explicitly"
        raise TypeError(msg)
    return value.model_copy(deep=True)
