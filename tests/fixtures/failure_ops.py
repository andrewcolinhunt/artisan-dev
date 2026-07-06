"""Importable failing operations for the failure-persistence matrix.

Lives in the ``fixtures`` package (on ``sys.path`` via ``pythonpath = ["tests"]``,
which ``multiprocessing`` spawn inherits) so spawn workers can import each op by
its qualified name ``fixtures.failure_ops.<Op>`` — exercising the *real* worker
execute path. This is the whole point: ops loaded via
``importlib.spec_from_file_location`` in a parent process are NOT importable by
spawn workers, which is itself a failure-to-record trigger. Do not path-load
these.
"""

from __future__ import annotations

import csv
import os
from enum import StrEnum, auto
from typing import Any, ClassVar

import polars as pl
from pydantic import BaseModel

from artisan.composites.base.composite_context import CompositeContext
from artisan.composites.base.composite_definition import CompositeDefinition
from artisan.errors import ArtisanError
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.schemas import ArtifactResult
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.data import DataArtifact
from artisan.schemas.execution.batch_strategy import BatchStrategy
from artisan.schemas.execution.curator_result import (
    ArtifactResult as CuratorArtifactResult,
)
from artisan.schemas.operation_config.runner_resources import RunnerResources
from artisan.schemas.operation_config.tool_spec import ToolSpec
from artisan.schemas.specs.input_models import (
    ExecuteInput,
    PostprocessInput,
    PreprocessInput,
)
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec
from artisan.storage.core.artifact_store import ArtifactStore

# ---------------------------------------------------------------------------
# Generative creators: no inputs -> one unit -> full-step failure.
# ---------------------------------------------------------------------------


class _GenBase(OperationDefinition):
    """Generative creator base: writes one CSV artifact when it succeeds."""

    inputs: ClassVar[dict[str, Any]] = {}

    class OutputRole(StrEnum):
        datasets = auto()

    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.datasets: OutputSpec(
            artifact_type="data",
            description="generated dataset",
            infer_lineage_from={"inputs": []},
        ),
    }
    runner_resources: RunnerResources = RunnerResources(time_limit="00:10:00")
    batch_strategy: BatchStrategy = BatchStrategy(job_name="fp_gen")

    def _write_one(self, out_dir: str) -> None:
        os.makedirs(out_dir, exist_ok=True)
        with open(os.path.join(out_dir, "d_0.csv"), "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["id", "x"])
            writer.writerow([0, 1])

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        drafts: list[Artifact] = []
        for file_path in inputs.file_outputs:
            if file_path.endswith(".csv"):
                with open(file_path, "rb") as f:
                    drafts.append(
                        DataArtifact.draft(
                            content=f.read(),
                            original_name=os.path.basename(file_path),
                            step_number=inputs.step_number,
                        )
                    )
        return ArtifactResult(success=True, artifacts={"datasets": drafts})


class FailPreprocess(_GenBase):
    """Creator that raises in preprocess (plain ValueError -> null envelope)."""

    name = "fp_preprocess"

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        msg = "boom in preprocess"
        raise ValueError(msg)

    def execute_function(self, inputs: ExecuteInput) -> dict[str, Any]:
        self._write_one(inputs.execute_dir)
        return {}


class FailExecute(_GenBase):
    """Creator that raises a plain ValueError in execute (null envelope)."""

    name = "fp_execute"

    def execute_function(self, inputs: ExecuteInput) -> dict[str, Any]:
        msg = "boom in execute (plain ValueError)"
        raise ValueError(msg)


class FailExecuteArtisan(_GenBase):
    """Creator that raises an ArtisanError in execute (populated envelope)."""

    name = "fp_execute_artisan"

    def execute_function(self, inputs: ExecuteInput) -> dict[str, Any]:
        raise ArtisanError(
            code="op_execute_failed",
            message="boom in execute (ArtisanError)",
            error_type="runtime",
            recovery_hint="REPORT_TO_USER",
            field="my_field",
            suggestions=["do_x", "do_y"],
        )


class FailPostprocessRaise(_GenBase):
    """Creator that raises inside postprocess (null envelope)."""

    name = "fp_postprocess_raise"

    def execute_function(self, inputs: ExecuteInput) -> dict[str, Any]:
        self._write_one(inputs.execute_dir)
        return {}

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        msg = "boom in postprocess"
        raise ValueError(msg)


class FailPostprocessReturn(_GenBase):
    """Creator whose postprocess returns success=False (no raise)."""

    name = "fp_postprocess_return"

    def execute_function(self, inputs: ExecuteInput) -> dict[str, Any]:
        self._write_one(inputs.execute_dir)
        return {}

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        return ArtifactResult(success=False, error="postprocess returned success=False")


class FailCommand(_GenBase):
    """Command op whose subprocess exits nonzero (populated envelope)."""

    name = "fp_command"
    tool: ToolSpec | None = ToolSpec(executable="false")

    def execute_command(self, inputs: dict[str, Any]) -> list[str]:
        return ["false"]


class WorkerCrash(_GenBase):
    """Creator whose execute hard-kills the worker (models OOM/segfault).

    ``os._exit`` bypasses the executor's own failure recording, so the record
    must be synthesized by the orchestrator (Mechanism B).
    """

    name = "fp_worker_crash"

    def execute_function(self, inputs: ExecuteInput) -> dict[str, Any]:
        os._exit(1)  # deliberate hard kill of the worker


# ---------------------------------------------------------------------------
# Curator ops: run in a spawned subprocess.
# ---------------------------------------------------------------------------


class _CuratorBase(OperationDefinition):
    """Curator base with a single passthrough input role and no outputs."""

    class InputRole(StrEnum):
        passthrough = auto()

    inputs: ClassVar[dict[str, InputSpec]] = {
        InputRole.passthrough: InputSpec(
            artifact_type="data", required=True, description="in"
        ),
    }
    outputs: ClassVar[dict[str, OutputSpec]] = {}


class FailCuratorRaise(_CuratorBase):
    """Curator whose body raises a plain ValueError (null envelope)."""

    name = "fp_curator_raise"

    def execute_curator(
        self,
        inputs: dict[str, pl.DataFrame],
        step_number: int,
        artifact_store: ArtifactStore,
    ) -> Any:
        msg = "boom in curator body"
        raise ValueError(msg)


class FailCuratorArtisan(_CuratorBase):
    """Curator whose body raises an ArtisanError (populated envelope)."""

    name = "fp_curator_artisan"

    def execute_curator(
        self,
        inputs: dict[str, pl.DataFrame],
        step_number: int,
        artifact_store: ArtifactStore,
    ) -> Any:
        raise ArtisanError(
            code="op_execute_failed",
            message="boom in curator (ArtisanError)",
            error_type="runtime",
            recovery_hint="CHECK_INPUT",
        )


class FailCuratorReturn(_CuratorBase):
    """Curator that returns success=False (no raise, null envelope)."""

    name = "fp_curator_return"

    def execute_curator(
        self,
        inputs: dict[str, pl.DataFrame],
        step_number: int,
        artifact_store: ArtifactStore,
    ) -> Any:
        return CuratorArtifactResult(
            success=False, error="curator returned success=False", artifacts={}
        )


class CuratorWorkerCrash(_CuratorBase):
    """Curator whose subprocess hard-exits (BrokenProcessPool -> synthesized)."""

    name = "fp_curator_worker_crash"

    def execute_curator(
        self,
        inputs: dict[str, pl.DataFrame],
        step_number: int,
        artifact_store: ArtifactStore,
    ) -> Any:
        os._exit(1)  # deliberate hard kill of the subprocess


# ---------------------------------------------------------------------------
# Composite whose only internal step is a failing creator.
# ---------------------------------------------------------------------------


class FailingComposite(CompositeDefinition):
    """Composite whose only internal step is FailExecute (creator that raises)."""

    name = "fp_failing_composite"
    description = "composite with a failing internal creator step"

    class OutputRole(StrEnum):
        datasets = auto()

    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.datasets: OutputSpec(artifact_type="data", description="out"),
    }

    class Params(BaseModel):
        pass

    def compose(self, ctx: CompositeContext) -> None:
        step = ctx.run(FailExecute)
        ctx.output("datasets", step.output("datasets"))
