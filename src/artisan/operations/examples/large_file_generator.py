"""Generate large binary files stored externally.

Demonstrates the one-to-one external-content pattern: each artifact
maps to a single large file in files_root, too large for a Parquet
binary column. Delta stores only metadata (hash, size, name).
"""

from __future__ import annotations

import random
from enum import StrEnum, auto
from typing import Any, ClassVar

from pydantic import BaseModel, Field

from artisan.operations.base.operation_definition import OperationDefinition
from artisan.schemas.artifact.large_file import LargeFileArtifact
from artisan.schemas.execution.curator_result import ArtifactResult
from artisan.schemas.execution.execution_config import ExecutionConfig
from artisan.schemas.operation_config.resource_config import ResourceConfig
from artisan.schemas.specs.input_models import ExecuteInput, PostprocessInput
from artisan.schemas.specs.output_spec import OutputSpec
from artisan.utils.hashing import compute_content_hash


class LargeFileGenerator(OperationDefinition):
    """Generate large binary files stored externally.

    Produces deterministic pseudo-random binary files of a specified
    size. Each file becomes a separate LargeFileArtifact with
    external_path pointing to the file in files_root.

    Output Roles:
        files (large_file) -- Generated large binary files
    """

    name = "large_file_generator"
    description = "Generate large binary files stored externally"

    inputs: ClassVar[dict] = {}

    class OutputRole(StrEnum):
        files = auto()

    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.files: OutputSpec(
            artifact_type="large_file",
            description="Generated large binary files",
            infer_lineage_from={"inputs": []},
        ),
    }

    class Params(BaseModel):
        """Algorithm parameters for LargeFileGenerator."""

        count: int = Field(default=1, ge=1, description="Number of files to generate")
        file_size_bytes: int = Field(
            default=1_000_000,
            ge=1,
            description="Approximate size of each generated file",
        )
        seed: int | None = Field(
            default=None, description="Random seed for reproducibility"
        )

    params: Params = Params()
    resources: ResourceConfig = ResourceConfig(time_limit="00:30:00")
    execution: ExecutionConfig = ExecutionConfig(job_name="large_file_generator")

    def execute(self, inputs: ExecuteInput) -> dict[str, Any]:
        """Generate deterministic binary files in files_dir."""
        if inputs.files_dir is None:
            msg = "files_dir required for LargeFileGenerator"
            raise ValueError(msg)

        rng = random.Random(self.params.seed)
        files_meta: list[dict[str, Any]] = []

        for i in range(self.params.count):
            filename = f"output_{i:05d}.bin"
            output_path = inputs.files_dir / filename
            data = rng.randbytes(self.params.file_size_bytes)
            output_path.write_bytes(data)
            files_meta.append({
                "path": str(output_path),
                "content_hash": compute_content_hash(data),
                "size_bytes": len(data),
                "original_name": f"output_{i:05d}",
                "extension": ".bin",
            })

        return {"files": files_meta}

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        """Create LargeFileArtifact drafts from execute metadata."""
        files = inputs.memory_outputs["files"]

        drafts = [
            LargeFileArtifact.draft(
                content_hash=f["content_hash"],
                size_bytes=f["size_bytes"],
                step_number=inputs.step_number,
                external_path=f["path"],
                original_name=f["original_name"],
                extension=f["extension"],
            )
            for f in files
        ]

        return ArtifactResult(
            success=True,
            artifacts={"files": drafts},
        )
