"""Curator operation that unions multiple datastreams into one.

All input streams must share the same artifact type. No new artifacts are
created; the output contains passthrough references to the originals.
"""

from __future__ import annotations

from enum import StrEnum, auto
from typing import TYPE_CHECKING, ClassVar

import polars as pl

from artisan.operations.base.operation_definition import OperationDefinition
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.schemas.execution.curator_result import PassthroughResult
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec

if TYPE_CHECKING:
    from artisan.storage.core.artifact_store import ArtifactStore


class Merge(OperationDefinition):
    """Union multiple datastreams of the same artifact type into one.

    Accepts any number of named input streams via ``runtime_defined_inputs``
    and concatenates their artifact IDs into a single ``merged`` output.
    No new artifacts are created (passthrough semantics).
    """

    # ---------- Metadata ----------
    name = "merge"
    description = (
        "Union multiple datastreams of the same artifact type into a single stream"
    )

    # ---------- Inputs ----------
    # InputRole not defined: input roles are user-defined at pipeline construction time.
    inputs: ClassVar[dict[str, InputSpec]] = {}

    # ---------- Outputs ----------
    class OutputRole(StrEnum):
        merged = auto()

    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.merged: OutputSpec(
            artifact_type=ArtifactTypes.ANY,
            description="Unified stream containing all artifacts from all inputs",
        )
    }

    # ---------- Behavior ----------
    runtime_defined_inputs: ClassVar[bool] = True
    hydrate_inputs: ClassVar[bool] = False
    independent_input_streams: ClassVar[bool] = True

    # ---------- Lifecycle ----------
    def execute_curator(
        self,
        inputs: dict[str, pl.DataFrame],
        step_number: int,  # noqa: ARG002
        artifact_store: ArtifactStore,  # noqa: ARG002
    ) -> PassthroughResult:
        """Concatenate artifact IDs from all input streams into one output.

        Args:
            inputs: Role names to DataFrames with ``artifact_id`` column.
            step_number: Unused.
            artifact_store: Unused.

        Returns:
            PassthroughResult with all artifact IDs under the ``merged`` role.
        """
        merged = (
            pl.concat(inputs.values()).select("artifact_id")
            if inputs
            else pl.DataFrame({"artifact_id": []})
        )
        return PassthroughResult(
            success=True,
            passthrough={"merged": merged["artifact_id"].to_list()},
        )
