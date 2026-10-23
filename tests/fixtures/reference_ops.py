"""Domain-agnostic consumers of complete configs for lifecycle and replay tests."""

from __future__ import annotations

import json
from enum import StrEnum, auto
from pathlib import Path
from typing import Any, ClassVar

from artisan.operations.base import OperationDefinition, PerArtifact
from artisan.schemas import (
    ArtifactResult,
    ExecuteInput,
    ExecutionConfigArtifact,
    InputSpec,
    MetricArtifact,
    OutputSpec,
    PostprocessInput,
    PreprocessInput,
)


class ReferenceConsumer(OperationDefinition):
    """Prepare portable values, then verify dependencies survive postprocessing."""

    name: ClassVar[str] = "test_reference_consumer"

    class InputRole(StrEnum):
        config = auto()

    class OutputRole(StrEnum):
        metrics = auto()

    inputs: ClassVar[dict[str, InputSpec]] = {
        "config": InputSpec(
            artifact_type="config", materialize=False, resolve_references=True
        )
    }
    outputs: ClassVar[dict[str, OutputSpec]] = {
        "metrics": OutputSpec(
            artifact_type="metric", derives_from={"inputs": ["config"]}
        )
    }

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        paths = []
        for config in inputs.input_artifacts["config"]:
            assert isinstance(config, ExecutionConfigArtifact)
            references = inputs.referenced_artifacts(config)
            payload = {
                "config_id": config.artifact_id,
                "contents": [
                    Path(references[ref_id].materialized_path).read_text()
                    for ref_id in config.get_artifact_references()
                ],
            }
            path = Path(inputs.preprocess_dir) / f"{config.artifact_id}.json"
            path.write_text(json.dumps(payload))
            paths.append(str(path))
        return {"config": PerArtifact(paths)}

    def execute_function(self, inputs: ExecuteInput) -> None:
        for path in inputs.inputs["config"]:
            payload = json.loads(Path(path).read_text())
            output = Path(inputs.execute_dir) / f"{payload['config_id']}.json"
            output.write_text(json.dumps(payload))

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        configs = {
            config.artifact_id: config for config in inputs.input_artifacts["config"]
        }
        result = ArtifactResult()
        for path in inputs.file_outputs:
            payload = json.loads(Path(path).read_text())
            config = configs[payload["config_id"]]
            assert isinstance(config, ExecutionConfigArtifact)
            references = inputs.referenced_artifacts(config)
            contents = [
                Path(references[ref_id].materialized_path).read_text()
                for ref_id in config.get_artifact_references()
            ]
            assert payload["contents"] == contents
            result.add_artifact(
                "metrics",
                MetricArtifact.draft(
                    {"bytes": sum(len(content) for content in contents)},
                    f"{config.artifact_id}.json",
                    inputs.step_number,
                ),
                sources={"config": [config.artifact_id]},
            )
        return result
