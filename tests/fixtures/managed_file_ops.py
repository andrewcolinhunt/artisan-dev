"""Command operations that retain selected outputs through managed file storage."""

from __future__ import annotations

import json
import shutil
from enum import StrEnum, auto
from pathlib import Path
from typing import Any, ClassVar

from artisan.operations.base import OperationDefinition, PerArtifact
from artisan.schemas import (
    ArtifactResult,
    ExecuteInput,
    FileRefArtifact,
    InputSpec,
    OutputSpec,
    PostprocessInput,
    PreprocessInput,
)
from artisan.schemas.operation_config.compute import ComputeProvider, ModalComputeConfig
from artisan.utils.hashing import compute_stream_digest


class ManagedFileCommand(OperationDefinition):
    """Copy selected command outputs while preserving exact manifest parents."""

    name = "test_managed_file_command"
    execute_as_tool: ClassVar[bool] = True

    class InputRole(StrEnum):
        source = auto()

    inputs: ClassVar[dict[str, InputSpec]] = {
        InputRole.source: InputSpec(artifact_type="metric", materialize=False),
    }

    class OutputRole(StrEnum):
        files = auto()

    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.files: OutputSpec(
            artifact_type="file_ref", derives_from={"inputs": ["source"]}
        ),
    }

    compute_provider: ComputeProvider = ComputeProvider(modal=ModalComputeConfig())

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        """Carry source IDs in portable files for local and endpoint commands."""
        manifests = []
        for index, artifact in enumerate(inputs.input_artifacts["source"]):
            path = Path(inputs.preprocess_dir) / f"source-{index}.json"
            path.write_text(json.dumps({"source_id": artifact.artifact_id}))
            manifests.append(str(path))
        return {"source_manifest": PerArtifact(manifests)}

    def execute_function(self, inputs: ExecuteInput) -> None:
        """Produce same-basename files and an explicit output manifest."""
        assert inputs.files_dir is None
        paths = inputs.inputs["source_manifest"]
        if isinstance(paths, str):
            paths = [paths]
        directory = Path(inputs.execute_dir)
        records = []
        for index, path in enumerate(paths):
            source_id = json.loads(Path(path).read_text())["source_id"]
            relative = f"selected-{index}/output.bin"
            output = directory / relative
            output.parent.mkdir(parents=True)
            output.write_bytes(source_id.encode())
            records.append({"path": relative, "source_id": source_id})
        (directory / "managed-outputs.json").write_text(json.dumps(records))
        (directory / "scratch.txt").write_text("unselected command evidence")

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        """Adopt only declared complete files and hash their staged bytes."""
        if inputs.files_dir is None:
            msg = "Managed file storage must be configured to retain command outputs"
            raise ValueError(msg)
        result = ArtifactResult()
        for filename in inputs.file_outputs:
            manifest = Path(filename)
            if manifest.name != "managed-outputs.json":
                continue
            for record in json.loads(manifest.read_text()):
                source = manifest.parent / record["path"]
                index = len(result.artifacts.get("files", []))
                target = Path(inputs.files_dir) / f"output-{index}" / source.name
                target.parent.mkdir(parents=True)
                shutil.copyfile(source, target)
                with target.open("rb") as stream:
                    digest, size = compute_stream_digest(stream)
                result.add_artifact(
                    "files",
                    FileRefArtifact.draft(
                        path=str(target),
                        content_hash=digest,
                        size_bytes=size,
                        step_number=inputs.step_number,
                        original_name=source.stem,
                        extension=source.suffix,
                    ),
                    sources={"source": [record["source_id"]]},
                )
        return result


class WholeBatchManagedFileCommand(ManagedFileCommand):
    """Produce all selected files in one command subprocess."""

    name = "test_whole_batch_managed_file_command"
    per_artifact_dispatch: ClassVar[bool] = False
