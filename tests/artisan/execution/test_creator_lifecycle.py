"""Scientific invocation preserves human names, split parity and failure causes."""

from __future__ import annotations

import json
import os
from enum import StrEnum, auto
from pathlib import Path
from typing import Any, ClassVar
from unittest.mock import Mock

import pytest
from pydantic import BaseModel, Field

from artisan.execution.compute.base import ExecuteRouter
from artisan.execution.compute.local import LocalExecuteRouter
from artisan.execution.executor import Executor
from artisan.execution.invocation import ExecuteFailure
from artisan.operations.base import OperationDefinition, PerArtifact
from artisan.operations.lineage import match_outputs_to_inputs_by_stem
from artisan.schemas.artifact.metric import MetricArtifact
from artisan.schemas.execution.curator_result import ArtifactResult
from artisan.schemas.specs.input_models import (
    ExecuteInput,
    FrozenInputContext,
    PostprocessInput,
    PreprocessInput,
)
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec


class _SuffixOp(OperationDefinition):
    """Test operation that reads inputs by materialized path and appends a suffix.

    The operation opts into the public matcher for its filename convention
    and chooses its own human-readable output names.
    """

    class InputRole(StrEnum):
        source = auto()

    class OutputRole(StrEnum):
        output = auto()

    name: ClassVar[str] = "suffix_test"
    inputs: ClassVar[dict[str, InputSpec]] = {
        InputRole.source: InputSpec(artifact_type="metric", required=True),
    }
    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.output: OutputSpec(
            artifact_type="metric",
            derives_from={"inputs": ["source"]},
        ),
    }

    class Params(BaseModel):
        suffix: str = Field(default="_scored", description="Output file suffix.")

    params: Params = Params()

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        return {
            role: PerArtifact([a.materialized_path for a in artifacts])
            for role, artifacts in inputs.input_artifacts.items()
        }

    def execute_function(self, inputs: ExecuteInput) -> dict:
        source_paths = inputs.inputs["source"]
        for path in source_paths:
            with open(path) as fh:
                content = json.loads(fh.read())
            content["scored"] = True
            stem = os.path.splitext(os.path.basename(path))[0]
            out = os.path.join(inputs.execute_dir, f"{stem}{self.params.suffix}.json")
            with open(out, "w") as fh:
                fh.write(json.dumps(content))
        return {}

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        drafts = []
        for fp in inputs.file_outputs:
            if fp.endswith(".json"):
                with open(fp) as fh:
                    content = json.loads(fh.read())
                drafts.append(
                    MetricArtifact.draft(
                        content=content,
                        original_name=os.path.basename(fp),
                        step_number=inputs.step_number,
                    )
                )
        source_ids = match_outputs_to_inputs_by_stem(
            [draft.original_name for draft in drafts],
            [
                (a.materialized_path, a.artifact_id)
                for a in inputs.input_artifacts["source"]
            ],
        )
        sources = {a.artifact_id: a for a in inputs.input_artifacts["source"]}
        result = ArtifactResult()
        for draft, source_id in zip(drafts, source_ids, strict=True):
            draft.original_name = sources[source_id].original_name + self.params.suffix
            result.add_artifact("output", draft, sources={"source": [source_id]})
        return result


class _MonolithicSuffixOp(_SuffixOp):
    """Same body, monolithic dispatch — for split-parity comparison."""

    name: ClassVar[str] = "suffix_test_monolithic"
    per_artifact_dispatch: ClassVar[bool] = False


class _StubRouter(ExecuteRouter):
    """Injected router returning canned per-artifact results."""

    def __init__(self, results: list[Any]) -> None:
        self._results = results

    def route_execute(
        self,
        operation: OperationDefinition,
        execute_inputs: list[ExecuteInput],
        sandbox_root: str | None,
    ) -> list[Any]:
        return self._results[: len(execute_inputs)]


def _inputs() -> list[MetricArtifact]:
    return [
        MetricArtifact.draft({"value": value}, "sample.json").finalize()
        for value in (1, 2)
    ]


@pytest.mark.parametrize("operation_class", [_SuffixOp, _MonolithicSuffixOp])
def test_human_names_and_equal_names_keep_explicit_parents(
    tmp_path: Path,
    operation_class: type[OperationDefinition],
) -> None:
    artifacts = _inputs()
    with Executor(working_root=tmp_path / "work") as executor:
        result = executor.invoke(
            operation_class(),
            FrozenInputContext("named", {"source": artifacts}),
            batch_size=2,
            execute_router=LocalExecuteRouter(),
            materialization_references={},
        )
    outputs = result.artifacts["output"]
    assert [output.original_name for output in outputs] == ["sample_scored"] * 2
    assert sorted(
        [output.values for output in outputs], key=lambda value: value["value"]
    ) == [
        {"value": 1, "scored": True},
        {"value": 2, "scored": True},
    ]
    assert len({output.artifact_id for output in outputs}) == 2
    assert {
        (outputs[edge.draft_index].values["value"], edge.source_artifact_id)
        for edge in result.lineage["output"]
    } == {(artifact.values["value"], artifact.artifact_id) for artifact in artifacts}


def test_split_and_whole_invocations_have_identical_outputs(tmp_path: Path) -> None:
    with Executor(working_root=tmp_path / "work") as executor:
        results = [
            executor.invoke(
                operation(),
                FrozenInputContext(str(index), {"source": _inputs()}),
                batch_size=2,
                execute_router=LocalExecuteRouter(),
                materialization_references={},
            )
            for index, operation in enumerate((_SuffixOp, _MonolithicSuffixOp))
        ]
    assert {artifact.artifact_id for artifact in results[0].artifacts["output"]} == {
        artifact.artifact_id for artifact in results[1].artifacts["output"]
    }


def test_returned_failure_keeps_cause_and_prevents_postprocess(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    postprocess = Mock()
    monkeypatch.setattr(_SuffixOp, "postprocess", postprocess)
    failure = ValueError("container died")
    with (
        Executor(working_root=tmp_path / "work") as executor,
        pytest.raises(ExecuteFailure, match="1/2 artifact executions failed") as caught,
    ):
        executor.invoke(
            _SuffixOp(),
            FrozenInputContext("failed", {"source": _inputs()}),
            batch_size=2,
            execute_router=_StubRouter([{}, failure]),
            materialization_references={},
        )
    assert caught.value.__cause__ is failure
    postprocess.assert_not_called()
