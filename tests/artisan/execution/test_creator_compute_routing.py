"""Tests for compute routing through the creator lifecycle."""

from __future__ import annotations

import json
import os
from enum import StrEnum, auto
from pathlib import Path
from typing import Any, ClassVar

import polars as pl
import pytest
import xxhash

from artisan.execution.compute.local import LocalComputeRouter
from artisan.execution.executors.creator import (
    LifecycleResult,
    run_creator_lifecycle,
)
from artisan.execution.models.execution_unit import ExecutionUnit
from artisan.operations.base.operation_definition import OperationDefinition
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


class _SimpleOp(OperationDefinition):
    """Minimal test operation for compute routing validation."""

    class InputRole(StrEnum):
        source = auto()

    class OutputRole(StrEnum):
        output = auto()

    name: ClassVar[str] = "compute_routing_test"
    inputs: ClassVar[dict[str, InputSpec]] = {
        InputRole.source: InputSpec(artifact_type="metric", required=True),
    }
    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.output: OutputSpec(
            artifact_type="metric",
            infer_lineage_from={"inputs": ["source"]},
        ),
    }

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        return {
            role: [a.materialized_path for a in artifacts]
            for role, artifacts in inputs.input_artifacts.items()
        }

    def execute(self, inputs: ExecuteInput) -> dict:
        for path in inputs.inputs["source"]:
            with open(path) as fh:
                content = json.loads(fh.read())
            content["routed"] = True
            stem = os.path.splitext(os.path.basename(path))[0]
            out = os.path.join(inputs.execute_dir, f"{stem}_out.json")
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
def delta_env(tmp_path: Path):
    """Create Delta root, working dir, and staging dir with one input."""
    base = tmp_path / "delta"
    content = json.dumps({"value": 1}, sort_keys=True).encode("utf-8")
    aid = _compute_id(content)

    _setup_delta(
        base,
        metrics=[
            {
                "artifact_id": aid,
                "origin_step_number": 0,
                "content": content,
                "original_name": "test_metric",
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

    working = tmp_path / "working"
    working.mkdir()
    staging = tmp_path / "staging"
    staging.mkdir()

    runtime_env = RuntimeEnvironment(
        delta_root=str(base),
        working_root=str(working),
        staging_root=str(staging),
    )
    return runtime_env, aid


class TestCreatorComputeRouting:
    def test_explicit_local_router_matches_baseline(self, delta_env):
        """Passing an explicit LocalComputeRouter produces identical results."""
        runtime_env, input_id = delta_env

        unit = ExecutionUnit(
            operation=_SimpleOp(),
            inputs={"source": [input_id]},
            execution_spec_id="spec_cr" + "0" * 26,
            step_number=1,
        )

        result = run_creator_lifecycle(
            unit,
            runtime_env,
            compute_router=LocalComputeRouter(),
        )

        assert isinstance(result, LifecycleResult)
        assert "output" in result.artifacts
        assert len(result.artifacts["output"]) == 1
        assert result.artifacts["output"][0].artifact_id is not None
        assert len(result.edges) >= 1

    def test_default_router_from_operation_config(self, delta_env):
        """When compute_router is None, router is created from operation config."""
        runtime_env, input_id = delta_env

        unit = ExecutionUnit(
            operation=_SimpleOp(),
            inputs={"source": [input_id]},
            execution_spec_id="spec_df" + "0" * 26,
            step_number=1,
        )

        # No compute_router — should auto-create from operation.compute
        result = run_creator_lifecycle(unit, runtime_env)

        assert isinstance(result, LifecycleResult)
        assert "output" in result.artifacts
        assert len(result.artifacts["output"]) == 1
