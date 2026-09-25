"""Operations carrying exact pair identities through memory and file transport."""

from __future__ import annotations

import json
from enum import StrEnum, auto
from pathlib import Path
from typing import Any, ClassVar

from pydantic import BaseModel, Field

from artisan.operations.base import OperationDefinition, PerArtifact
from artisan.schemas import ArtifactResult, InputSpec, MetricArtifact, OutputSpec
from artisan.schemas.enums import GroupByStrategy
from artisan.schemas.specs.input_models import (
    ExecuteInput,
    PostprocessInput,
    PreprocessInput,
)


class DeclaredPairs(OperationDefinition):
    """Keep repeated primary occurrences associated with their exact partners."""

    name = "test_declared_pairs"

    class InputRole(StrEnum):
        primary = auto()
        partner = auto()
        reference = auto()

    class OutputRole(StrEnum):
        prediction = auto()

    inputs: ClassVar[dict[str, InputSpec]] = {
        role: InputSpec(artifact_type="data", materialize=False) for role in InputRole
    }
    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.prediction: OutputSpec(
            artifact_type="metric", derives_from={"inputs": ["primary", "partner"]}
        )
    }
    group_by: GroupByStrategy | None = GroupByStrategy.CROSS_PRODUCT

    class Params(BaseModel):
        file_outputs: bool = Field(
            default=False,
            description="Transport pair records as files instead of Python results.",
        )

    params: Params = Params()

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        pairs = [
            {role: artifact.artifact_id for role, artifact in group.items()}
            for group in inputs.grouped()
        ]
        return {"pairs": PerArtifact(pairs)}

    def execute_function(self, inputs: ExecuteInput) -> dict[str, Any] | None:
        pairs = inputs.inputs["pairs"]
        if self.params.file_outputs:
            _write_manifest(pairs, Path(inputs.execute_dir))
            return None
        return {"pairs": pairs}

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        pairs = (
            _read_manifests(inputs.file_outputs)
            if self.params.file_outputs
            else inputs.memory_outputs["pairs"]
        )
        return _declare_pairs(pairs, inputs.step_number)


class MonolithicDeclaredPairs(DeclaredPairs):
    """Produce all pairs inside one execution call."""

    name = "test_monolithic_declared_pairs"
    per_artifact_dispatch: ClassVar[bool] = False


class DeclaredPairCommand(OperationDefinition):
    """Transport exact input IDs through an operation-owned relative-path manifest."""

    name = "test_declared_pair_command"
    execute_as_tool: ClassVar[bool] = True
    InputRole: ClassVar[type[StrEnum]] = DeclaredPairs.InputRole
    OutputRole: ClassVar[type[StrEnum]] = DeclaredPairs.OutputRole
    inputs: ClassVar[dict[str, InputSpec]] = DeclaredPairs.inputs
    outputs: ClassVar[dict[str, OutputSpec]] = DeclaredPairs.outputs
    group_by: GroupByStrategy | None = GroupByStrategy.CROSS_PRODUCT

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        manifests = []
        for index, group in enumerate(inputs.grouped()):
            path = Path(inputs.preprocess_dir) / f"pair-{index}.json"
            path.write_text(
                json.dumps(
                    {role: artifact.artifact_id for role, artifact in group.items()}
                )
            )
            manifests.append(str(path))
        return {"pair_file": PerArtifact(manifests)}

    def execute_function(self, inputs: ExecuteInput) -> None:
        paths = inputs.inputs["pair_file"]
        if isinstance(paths, str):
            paths = [paths]
        pairs = [json.loads(Path(path).read_text()) for path in paths]
        _write_manifest(pairs, Path(inputs.execute_dir))

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        return _declare_pairs(_read_manifests(inputs.file_outputs), inputs.step_number)


class MonolithicDeclaredPairCommand(DeclaredPairCommand):
    """Use one local command process to handle the whole paired batch."""

    name = "test_monolithic_declared_pair_command"
    per_artifact_dispatch: ClassVar[bool] = False


def _write_manifest(pairs: list[dict[str, str]], directory: Path) -> None:
    records = []
    for index, pair in enumerate(pairs):
        relative = f"predictions/output-{index}.json"
        path = directory / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        content = {role: pair[role] for role in ("primary", "partner")}
        path.write_text(json.dumps(content))
        records.append({"path": relative, "parents": content})
    (directory / "declared-outputs.json").write_text(json.dumps(records))


def _read_manifests(files: list[str]) -> list[dict[str, str]]:
    pairs = []
    for filename in files:
        manifest = Path(filename)
        if manifest.name != "declared-outputs.json":
            continue
        for record in json.loads(manifest.read_text()):
            relative = Path(record["path"])
            assert not relative.is_absolute()
            content = json.loads((manifest.parent / relative).read_text())
            assert content == record["parents"]
            pairs.append(content)
    return pairs


def _declare_pairs(pairs: list[dict[str, str]], step_number: int) -> ArtifactResult:
    result = ArtifactResult()
    for pair in pairs:
        parents = {role: [pair[role]] for role in ("primary", "partner")}
        result.add_artifact(
            "prediction",
            MetricArtifact.draft(
                content={role: values[0] for role, values in parents.items()},
                original_name="prediction.json",
                step_number=step_number,
            ),
            sources=parents,
        )
    return result
