"""Generate JSONL record bundles with random data.

Demonstrates the many-to-one external-content pattern: one JSONL file
contains many independently addressable records, each tracked as a
separate RecordBundleArtifact in Delta.
"""

from __future__ import annotations

import json
import random
from enum import StrEnum, auto
from typing import Any, ClassVar

from pydantic import BaseModel, Field

from artisan.operations.base.operation_definition import OperationDefinition
from artisan.schemas.artifact.record_bundle import RecordBundleArtifact
from artisan.schemas.execution.curator_result import ArtifactResult
from artisan.schemas.execution.execution_config import ExecutionConfig
from artisan.schemas.operation_config.resource_config import ResourceConfig
from artisan.schemas.specs.input_models import ExecuteInput, PostprocessInput
from artisan.schemas.specs.output_spec import OutputSpec
from artisan.utils.hashing import compute_content_hash


class RecordBundleGenerator(OperationDefinition):
    """Generate JSONL record bundles with random data.

    Produces a single JSONL file with N records, each containing a
    record_id and a dict of random float values. Each record becomes
    a separate RecordBundleArtifact sharing the same external_path.

    Output Roles:
        records (record_bundle) -- Generated JSONL record bundle
    """

    name = "record_bundle_generator"
    description = "Generate JSONL record bundles with random data"

    inputs: ClassVar[dict] = {}

    class OutputRole(StrEnum):
        records = auto()

    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.records: OutputSpec(
            artifact_type="record_bundle",
            description="Generated JSONL record bundle",
            infer_lineage_from={"inputs": []},
        ),
    }

    class Params(BaseModel):
        """Algorithm parameters for RecordBundleGenerator."""

        count: int = Field(default=10, ge=1, description="Number of records to generate")
        fields_per_record: int = Field(
            default=5, ge=1, description="Number of float fields per record"
        )
        seed: int | None = Field(
            default=None, description="Random seed for reproducibility"
        )

    params: Params = Params()
    resources: ResourceConfig = ResourceConfig(time_limit="00:30:00")
    execution: ExecutionConfig = ExecutionConfig(job_name="record_bundle_generator")

    def execute(self, inputs: ExecuteInput) -> dict[str, Any]:
        """Write a JSONL file with random records to files_dir."""
        if inputs.files_dir is None:
            msg = "files_dir required for RecordBundleGenerator"
            raise ValueError(msg)

        rng = random.Random(self.params.seed)
        output_path = inputs.files_dir / "records.jsonl"
        records_meta: list[dict[str, Any]] = []

        with output_path.open("w") as f:
            for i in range(self.params.count):
                record = {
                    "record_id": f"rec_{i:06d}",
                    "values": {
                        f"field_{j}": round(rng.gauss(0, 1), 6)
                        for j in range(self.params.fields_per_record)
                    },
                }
                line = json.dumps(record, sort_keys=True)
                f.write(line + "\n")
                records_meta.append({
                    "record_id": record["record_id"],
                    "content_hash": compute_content_hash(line.encode()),
                    "size_bytes": len(line.encode()),
                })

        return {
            "output_path": str(output_path),
            "records": records_meta,
        }

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        """Create RecordBundleArtifact drafts from execute metadata."""
        output_path = inputs.memory_outputs["output_path"]
        records = inputs.memory_outputs["records"]

        drafts = [
            RecordBundleArtifact.draft(
                record_id=rec["record_id"],
                content_hash=rec["content_hash"],
                size_bytes=rec["size_bytes"],
                step_number=inputs.step_number,
                external_path=output_path,
                original_name=rec["record_id"],
            )
            for rec in records
        ]

        return ArtifactResult(
            success=True,
            artifacts={"records": drafts},
        )
