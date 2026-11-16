"""Truncate CSV datasets to their first N rows — an execute_as_tool example."""

from __future__ import annotations

import csv
import os
import sys
from enum import StrEnum, auto
from itertools import islice
from pathlib import Path
from typing import Any, ClassVar, cast

from pydantic import BaseModel, Field

from artisan.operations.base.operation_definition import OperationDefinition
from artisan.operations.base.per_artifact import PerArtifact
from artisan.schemas import ArtifactResult
from artisan.schemas.artifact.csv import CSVArtifact
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
    the selected software. The same command can run standalone via
    ``artisan op run artisan.operations.examples.csv_head:CsvHead``.

    The flag's file-shaped contract in action: preprocess delivers
    ``PerArtifact`` paths, the body reads input files and writes
    ``<input-position>_head.csv`` to ``execute_dir``. Postprocess declares
    each exact parent occurrence. The body returns None and keeps all per-run
    config in the nested ``Params`` model.
    """

    name = "csv_head"
    version: ClassVar[str] = "2"
    description = "Truncate CSV datasets to their first N rows"
    execute_as_tool: ClassVar[bool] = True

    class InputRole(StrEnum):
        dataset = auto()

    inputs: ClassVar[dict[str, InputSpec]] = {
        InputRole.dataset: InputSpec(
            artifact_type="csv",
            required=True,
            description="Input CSV dataset to truncate",
        ),
    }

    class OutputRole(StrEnum):
        dataset = auto()

    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.dataset: OutputSpec(
            artifact_type="csv",
            description="Truncated CSV dataset, one per input",
            derives_from={"inputs": ["dataset"]},
        ),
    }

    class Params(BaseModel):
        """Parameters for CsvHead."""

        model_config = {"extra": "forbid"}

        rows: int = Field(
            default=5,
            ge=1,
            description="Number of data rows to keep from each input CSV.",
        )

    params: Params = Params()

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        """One materialized path per artifact, sliced per dispatch."""
        index_paths = []
        for index in range(len(inputs.input_artifacts["dataset"])):
            path = Path(inputs.preprocess_dir) / f"input-index-{index}.txt"
            path.write_text(str(index))
            index_paths.append(str(path))
        return {
            "dataset": PerArtifact(
                [a.materialized_path for a in inputs.input_artifacts["dataset"]]
            ),
            "input_index": PerArtifact(index_paths),
        }

    def execute_function(self, inputs: ExecuteInput) -> None:
        """Write the header plus first ``rows`` rows of each input CSV."""
        for index_path, input_path in zip(
            inputs.inputs["input_index"], inputs.inputs["dataset"], strict=True
        ):
            index = int(Path(index_path).read_text())
            with open(input_path, newline="") as f:
                kept = list(islice(csv.reader(f), self.params.rows + 1))
            output_path = os.path.join(inputs.execute_dir, f"{index}_head.csv")
            with open(output_path, "w", newline="") as f:
                csv.writer(f).writerows(kept)
            # stdout is the execute_as_tool logging channel — run_command captures
            # it to the unit log, surfaced in the executions table
            sys.stdout.write(
                f"csv_head: wrote {index}_head.csv ({len(kept) - 1} rows)\n"
            )

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        """Declare each output's exact input occurrence in its original order."""
        outputs = sorted(
            (path for path in inputs.file_outputs if path.endswith("_head.csv")),
            key=lambda path: int(Path(path).name.removesuffix("_head.csv")),
        )
        sources = cast(list[CSVArtifact], inputs.input_artifacts["dataset"])
        result = ArtifactResult(artifacts={"dataset": []}, lineage={"dataset": []})
        for file_path in outputs:
            index = int(Path(file_path).name.removesuffix("_head.csv"))
            with open(file_path, "rb") as f:
                draft = CSVArtifact.draft(
                    content=f.read(),
                    original_name=f"{sources[index].original_name}_head.csv",
                    step_number=inputs.step_number,
                )
            result.add_artifact(
                "dataset",
                draft,
                sources={"dataset": [inputs.ref("dataset", index=index)]},
            )
        return result
