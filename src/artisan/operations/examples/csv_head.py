"""Truncate CSV datasets to their first N rows — an execute_as_tool example."""

from __future__ import annotations

import csv
import os
import sys
from enum import StrEnum, auto
from itertools import islice
from typing import Any, ClassVar

from pydantic import BaseModel, Field

from artisan.operations.base.operation_definition import OperationDefinition
from artisan.operations.base.per_artifact import PerArtifact
from artisan.operations.lineage import match_outputs_to_inputs_by_stem
from artisan.schemas import ArtifactResult
from artisan.schemas.artifact.data import DataArtifact
from artisan.schemas.operation_config.compute import ComputeProvider, ModalComputeConfig
from artisan.schemas.specs.input_models import (
    ExecuteInput,
    PostprocessInput,
    PreprocessInput,
)
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec


class CsvHead(OperationDefinition):
    """Truncate CSV datasets to their first N rows.

    The reference ``execute_as_tool`` op: the body is plain Python — no
    external binary, no ``execute_command`` — yet the single flag makes
    it a command op everywhere. Locally it runs as a subprocess under
    the active environment, ``compute_provider='modal'`` routes it to a
    deployed tool endpoint, and a container runs it standalone via
    ``artisan op run artisan.operations.examples.csv_head:CsvHead``.

    The flag's file-shaped contract in action: preprocess delivers
    ``PerArtifact`` paths, the body reads input files and writes
    ``<stem>_head.csv`` to ``execute_dir``. Postprocess explicitly calls a
    matching helper and declares each parent. The body returns None and keeps all per-run
    config in the nested ``Params`` model.
    """

    name = "csv_head"
    description = "Truncate CSV datasets to their first N rows"
    execute_as_tool: ClassVar[bool] = True

    class InputRole(StrEnum):
        dataset = auto()

    inputs: ClassVar[dict[str, InputSpec]] = {
        InputRole.dataset: InputSpec(
            artifact_type="data",
            required=True,
            description="Input CSV dataset to truncate",
        ),
    }

    class OutputRole(StrEnum):
        dataset = auto()

    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.dataset: OutputSpec(
            artifact_type="data",
            description="Truncated CSV dataset, one per input",
            derives_from={"inputs": ["dataset"]},
        ),
    }

    class Params(BaseModel):
        """Parameters for CsvHead."""

        model_config = {"extra": "forbid"}  # the endpoint's typed schema

        rows: int = Field(
            default=5,
            ge=1,
            description="Number of data rows to keep from each input CSV.",
        )

    params: Params = Params()

    compute_provider: ComputeProvider = ComputeProvider(
        modal=ModalComputeConfig(
            # example ops exercise in-development artisan — overlay it live
            local_python_sources=["artisan"],
        ),
    )

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        """One materialized path per artifact, sliced per dispatch."""
        return {
            role: PerArtifact([a.materialized_path for a in artifacts])
            for role, artifacts in inputs.input_artifacts.items()
        }

    def execute_function(self, inputs: ExecuteInput) -> None:
        """Write the header plus first ``rows`` rows of each input CSV."""
        for input_path in inputs.inputs["dataset"]:
            with open(input_path, newline="") as f:
                kept = list(islice(csv.reader(f), self.params.rows + 1))
            stem = os.path.splitext(os.path.basename(input_path))[0]
            output_path = os.path.join(inputs.execute_dir, f"{stem}_head.csv")
            with open(output_path, "w", newline="") as f:
                csv.writer(f).writerows(kept)
            # stdout is the execute_as_tool logging channel — run_command captures
            # it to the unit log, surfaced in the executions table
            sys.stdout.write(
                f"csv_head: wrote {stem}_head.csv ({len(kept) - 1} rows)\n"
            )

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        """Match output basenames and explicitly declare each dataset parent."""
        outputs = [path for path in inputs.file_outputs if path.endswith("_head.csv")]
        sources = inputs.input_artifacts["dataset"]
        parent_ids = match_outputs_to_inputs_by_stem(
            outputs,
            [
                (artifact.materialized_path, artifact.artifact_id)
                for artifact in sources
            ],
        )
        source_by_id = {artifact.artifact_id: artifact for artifact in sources}
        result = ArtifactResult(artifacts={"dataset": []}, lineage={"dataset": []})
        for file_path, parent_id in zip(outputs, parent_ids, strict=True):
            with open(file_path, "rb") as f:
                draft = DataArtifact.draft(
                    content=f.read(),
                    original_name=f"{source_by_id[parent_id].original_name}_head.csv",
                    step_number=inputs.step_number,
                )
            result.add_artifact("dataset", draft, sources={"dataset": [parent_id]})
        return result
