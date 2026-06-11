"""End-to-end tests for artifact-ID materialization through the creator lifecycle.

Verifies: materialization with artifact_id filenames -> filesystem match map
-> lineage via match map -> name derivation -> correct original_name in output.
"""

from __future__ import annotations

import json
import os
from enum import StrEnum, auto
from pathlib import Path
from typing import Any, ClassVar

import polars as pl
import pytest
import xxhash

from artisan.execution.executors.creator import (
    LifecycleResult,
    run_creator_lifecycle,
)
from artisan.execution.models.execution_unit import ExecutionUnit
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.operations.base.per_artifact import PerArtifact
from artisan.schemas.artifact.metric import MetricArtifact
from artisan.schemas.execution.curator_result import ArtifactResult
from artisan.schemas.execution.runtime_environment import RuntimeEnvironment
from artisan.schemas.specs.input_models import (
    ExecuteInput,
    PostprocessInput,
    PreprocessInput,
)
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec
from artisan.storage.core.table_schemas import ARTIFACT_INDEX_SCHEMA


def _compute_id(content: bytes) -> str:
    return xxhash.xxh3_128(content).hexdigest()


def _setup_delta(base_path: Path, metrics: list[dict], index: list[dict]) -> None:
    """Write Delta Lake tables for test input artifacts."""
    metrics_path = base_path / "artifacts/metrics"
    pl.DataFrame(metrics, schema=MetricArtifact.POLARS_SCHEMA).write_delta(
        str(metrics_path)
    )
    index_path = base_path / "artifacts/index"
    pl.DataFrame(index, schema=ARTIFACT_INDEX_SCHEMA).write_delta(str(index_path))


class _SuffixOp(OperationDefinition):
    """Test operation that reads inputs by materialized path and appends a suffix.

    The key behavior: the output filename preserves the input filename stem
    (which is now the artifact_id) plus a suffix. This allows the filesystem
    match map to connect outputs back to inputs.
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
            infer_lineage_from={"inputs": ["source"]},
        ),
    }

    suffix: str = "_scored"

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
            out = os.path.join(inputs.execute_dir, f"{stem}{self.suffix}.json")
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
        return ArtifactResult(success=True, artifacts={"output": drafts})


@pytest.fixture
def delta_with_named_input(tmp_path: Path):
    """Create a Delta root with a metric artifact that has a human name."""
    base = tmp_path / "delta"
    content = json.dumps({"value": 42}, sort_keys=True).encode("utf-8")
    aid = _compute_id(content)

    _setup_delta(
        base,
        metrics=[
            {
                "artifact_id": aid,
                "origin_step_number": 0,
                "content": content,
                "original_name": "sample_001",
                "extension": ".json",
                "metadata": "{}",
                "external_path": None,
            }
        ],
        index=[
            {
                "artifact_id": aid,
                "artifact_type": "metric",
                "origin_step_number": 0,
                "metadata": "{}",
            }
        ],
    )
    return base, aid


class TestCreatorLifecycleNameDerivation:
    """End-to-end: materialization -> match map -> lineage -> name derivation."""

    def test_output_gets_human_name_with_suffix(
        self, delta_with_named_input, tmp_path: Path
    ):
        """Output artifact's original_name is derived from input name + suffix."""
        delta_path, input_id = delta_with_named_input
        working = tmp_path / "working"
        working.mkdir()
        staging = tmp_path / "staging"
        staging.mkdir()

        config = RuntimeEnvironment(
            delta_root=str(delta_path),
            working_root=str(working),
            staging_root=str(staging),
        )
        unit = ExecutionUnit(
            operation=_SuffixOp(suffix="_scored"),
            inputs={"source": [input_id]},
            execution_spec_id="spec01" + "0" * 26,
            step_number=1,
        )

        result = run_creator_lifecycle(unit, config)

        assert isinstance(result, LifecycleResult)
        assert "output" in result.artifacts
        outputs = result.artifacts["output"]
        assert len(outputs) == 1

        # The output should have the human-readable derived name
        output_art = outputs[0]
        assert output_art.original_name == "sample_001_scored"
        assert output_art.artifact_id is not None

        # Lineage edge should exist
        assert len(result.edges) >= 1
        edge = result.edges[0]
        assert edge.source_artifact_id == input_id
        assert edge.target_artifact_id == output_art.artifact_id

    def test_no_collision_with_duplicate_names(self, tmp_path: Path):
        """Two inputs with the same original_name produce distinct outputs."""
        base = tmp_path / "delta"
        content_a = json.dumps({"v": 1}, sort_keys=True).encode("utf-8")
        content_b = json.dumps({"v": 2}, sort_keys=True).encode("utf-8")
        id_a = _compute_id(content_a)
        id_b = _compute_id(content_b)

        _setup_delta(
            base,
            metrics=[
                {
                    "artifact_id": id_a,
                    "origin_step_number": 0,
                    "content": content_a,
                    "original_name": "output",
                    "extension": ".json",
                    "metadata": "{}",
                    "external_path": None,
                },
                {
                    "artifact_id": id_b,
                    "origin_step_number": 0,
                    "content": content_b,
                    "original_name": "output",
                    "extension": ".json",
                    "metadata": "{}",
                    "external_path": None,
                },
            ],
            index=[
                {
                    "artifact_id": id_a,
                    "artifact_type": "metric",
                    "origin_step_number": 0,
                    "metadata": "{}",
                },
                {
                    "artifact_id": id_b,
                    "artifact_type": "metric",
                    "origin_step_number": 0,
                    "metadata": "{}",
                },
            ],
        )

        working = tmp_path / "working"
        working.mkdir()
        staging = tmp_path / "staging"
        staging.mkdir()

        config = RuntimeEnvironment(
            delta_root=str(base),
            working_root=str(working),
            staging_root=str(staging),
        )
        unit = ExecutionUnit(
            operation=_SuffixOp(suffix="_processed"),
            inputs={"source": [id_a, id_b]},
            execution_spec_id="spec02" + "0" * 26,
            step_number=1,
        )

        result = run_creator_lifecycle(unit, config)

        outputs = result.artifacts["output"]
        assert len(outputs) == 2
        names = {a.original_name for a in outputs}
        # Both get the same derived name because both inputs were "output"
        assert names == {"output_processed"}
        # But they have different artifact_ids
        ids = {a.artifact_id for a in outputs}
        assert len(ids) == 2


@pytest.fixture
def delta_with_two_inputs(tmp_path: Path):
    """Create a Delta root with two metric artifacts."""
    base = tmp_path / "delta"
    contents = [
        json.dumps({"value": i}, sort_keys=True).encode("utf-8") for i in (1, 2)
    ]
    aids = [_compute_id(c) for c in contents]

    _setup_delta(
        base,
        metrics=[
            {
                "artifact_id": aid,
                "origin_step_number": 0,
                "content": content,
                "original_name": f"sample_{i}",
                "extension": ".json",
                "metadata": "{}",
                "external_path": None,
            }
            for i, (aid, content) in enumerate(zip(aids, contents, strict=True))
        ],
        index=[
            {
                "artifact_id": aid,
                "artifact_type": "metric",
                "origin_step_number": 0,
                "metadata": "{}",
            }
            for aid in aids
        ],
    )

    working = tmp_path / "working"
    working.mkdir()
    staging = tmp_path / "staging"
    staging.mkdir()
    runtime_env = RuntimeEnvironment(
        delta_root=str(base),
        working_root=str(working),
        staging_root=str(staging),
    )
    return runtime_env, aids


class _MonolithicSuffixOp(_SuffixOp):
    """Same body, monolithic dispatch — for split-parity comparison."""

    name: ClassVar[str] = "suffix_test_monolithic"
    per_artifact_dispatch: ClassVar[bool] = False


class TestPerArtifactSplitParity:
    def test_split_matches_monolithic_results(self, delta_with_two_inputs):
        """per_artifact_dispatch True and False produce equivalent outputs."""
        runtime_env, aids = delta_with_two_inputs

        split_result = run_creator_lifecycle(
            ExecutionUnit(
                operation=_SuffixOp(),
                inputs={"source": aids},
                execution_spec_id="spec_sp" + "0" * 26,
                step_number=1,
            ),
            runtime_env,
        )
        mono_result = run_creator_lifecycle(
            ExecutionUnit(
                operation=_MonolithicSuffixOp(),
                inputs={"source": aids},
                execution_spec_id="spec_mo" + "0" * 26,
                step_number=1,
            ),
            runtime_env,
        )

        assert isinstance(split_result, LifecycleResult)
        assert len(split_result.artifacts["output"]) == 2
        assert len(mono_result.artifacts["output"]) == 2
        assert {a.artifact_id for a in split_result.artifacts["output"]} == {
            a.artifact_id for a in mono_result.artifacts["output"]
        }
        assert len(split_result.edges) == len(mono_result.edges)


class _StubRouter:
    """Injected router returning canned per-artifact results."""

    def __init__(self, results: list[Any]) -> None:
        self._results = results

    def route_execute(self, operation, execute_inputs, sandbox_root):
        return self._results[: len(execute_inputs)]


class TestPerArtifactFailureSurfacing:
    def test_exception_entries_raise_execute_failure(self, delta_with_two_inputs):
        """Exception entries surface as _ExecuteFailure with the N/M message."""
        from artisan.execution.executors.creator import _ExecuteFailure

        runtime_env, aids = delta_with_two_inputs
        unit = ExecutionUnit(
            operation=_SuffixOp(),
            inputs={"source": aids},
            execution_spec_id="spec_fa" + "0" * 26,
            step_number=1,
        )

        with pytest.raises(_ExecuteFailure, match="1/2 artifact executions failed"):
            run_creator_lifecycle(
                unit,
                runtime_env,
                execute_router=_StubRouter([{}, ValueError("container died")]),
            )


_seen_memory_outputs: list[Any] = []


class _ReturningOp(_SuffixOp):
    """Function op whose per-artifact returns must reassemble in order."""

    name: ClassVar[str] = "suffix_test_returning"

    def execute_function(self, inputs: ExecuteInput) -> dict:
        super().execute_function(inputs)
        return {"stems": [os.path.basename(p) for p in inputs.inputs["source"]]}

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        _seen_memory_outputs.append(inputs.memory_outputs)
        return super().postprocess(inputs)


class TestFunctionReturnsReassembled:
    def test_per_artifact_returns_merge_into_memory_outputs(
        self, delta_with_two_inputs
    ):
        """Per-artifact dict returns merge to the batch shape for postprocess."""
        runtime_env, aids = delta_with_two_inputs
        _seen_memory_outputs.clear()

        run_creator_lifecycle(
            ExecutionUnit(
                operation=_ReturningOp(),
                inputs={"source": aids},
                execution_spec_id="spec_me" + "0" * 26,
                step_number=1,
            ),
            runtime_env,
        )

        assert len(_seen_memory_outputs) == 1
        memory_outputs = _seen_memory_outputs[0]
        # Two single-artifact returns reassemble into one batch-shaped dict
        assert sorted(memory_outputs["stems"]) == sorted(f"{aid}.json" for aid in aids)


class TestCancelCheck:
    def test_none_step_run_id_disables_cancel_check(self, tmp_path: Path):
        """Composite-internal lifecycles (no step_run_id) get no probe."""
        from artisan.execution.executors.creator import _cancel_check

        runtime_env = RuntimeEnvironment(
            delta_root=str(tmp_path / "delta"),
            working_root=str(tmp_path / "working"),
            staging_root=str(tmp_path / "staging"),
        )
        assert _cancel_check(runtime_env, None) is None

    def test_probe_flips_when_sentinel_appears(self, tmp_path: Path):
        from artisan.execution.executors.creator import _cancel_check
        from artisan.utils.path import cancel_sentinel_path

        staging = tmp_path / "staging"
        staging.mkdir()
        runtime_env = RuntimeEnvironment(
            delta_root=str(tmp_path / "delta"),
            working_root=str(tmp_path / "working"),
            staging_root=str(staging),
        )
        probe = _cancel_check(runtime_env, "step-xyz")
        assert probe is not None
        assert probe() is False

        sentinel = Path(cancel_sentinel_path(str(staging), "step-xyz"))
        sentinel.parent.mkdir(parents=True, exist_ok=True)
        sentinel.touch()
        assert probe() is True
