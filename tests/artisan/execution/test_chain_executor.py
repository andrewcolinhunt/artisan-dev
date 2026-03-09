"""Tests for the chain executor."""

from __future__ import annotations

import json
import pickle
from enum import StrEnum, auto
from typing import Any, ClassVar

import pytest

from artisan.execution.executors.chain import (
    remap_output_roles,
    validate_required_roles,
)
from artisan.execution.models.artifact_source import ArtifactSource
from artisan.execution.models.execution_chain import (
    ChainIntermediates,
    ExecutionChain,
)
from artisan.execution.models.execution_unit import ExecutionUnit
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.schemas.artifact.metric import MetricArtifact
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.schemas.execution.curator_result import ArtifactResult
from artisan.schemas.execution.execution_config import ExecutionConfig
from artisan.schemas.execution.runtime_environment import RuntimeEnvironment
from artisan.schemas.operation_config.resource_config import ResourceConfig
from artisan.schemas.specs.input_models import (
    ExecuteInput,
    PostprocessInput,
    PreprocessInput,
)
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec


def _make_source(role: str) -> ArtifactSource:
    """Create an in-memory source with a dummy artifact."""
    art = MetricArtifact(
        artifact_id="a" * 32,
        artifact_type="metric",
        content=json.dumps({"value": 1}).encode(),
        original_name="test",
        extension=".json",
        origin_step_number=0,
    )
    return ArtifactSource.from_artifacts([art])


# =============================================================================
# Test operations for chain testing
# =============================================================================


class GeneratorOp(OperationDefinition):
    """Generates metric artifacts."""

    class OutputRole(StrEnum):
        data = auto()

    name: ClassVar[str] = "generator"
    inputs: ClassVar[dict[str, InputSpec]] = {}
    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.data: OutputSpec(
            artifact_type=ArtifactTypes.METRIC,
            infer_lineage_from={"inputs": []},
        ),
    }
    count: int = 1

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        return {}

    def execute(self, inputs: ExecuteInput) -> dict:
        for i in range(self.count):
            path = inputs.execute_dir / f"gen_{i:03d}.json"
            path.write_text(json.dumps({"value": i}))
        return {"generated": self.count}

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        drafts = []
        for f in inputs.file_outputs:
            if f.suffix == ".json":
                drafts.append(
                    MetricArtifact.draft(
                        content=json.loads(f.read_text()),
                        original_name=f.name,
                        step_number=inputs.step_number,
                    )
                )
        return ArtifactResult(success=True, artifacts={"data": drafts})


class TransformerOp(OperationDefinition):
    """Transforms input metric artifacts."""

    class InputRole(StrEnum):
        data = auto()

    class OutputRole(StrEnum):
        data = auto()

    name: ClassVar[str] = "transformer"
    inputs: ClassVar[dict[str, InputSpec]] = {
        InputRole.data: InputSpec(artifact_type=ArtifactTypes.METRIC, required=True),
    }
    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.data: OutputSpec(
            artifact_type=ArtifactTypes.METRIC,
            infer_lineage_from={"inputs": ["data"]},
        ),
    }
    suffix: str = "_transformed"

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        return {
            role: [a.materialized_path for a in artifacts]
            for role, artifacts in inputs.input_artifacts.items()
        }

    def execute(self, inputs: ExecuteInput) -> dict:
        for path in inputs.inputs.get("data", []):
            content = json.loads(path.read_text())
            content["transformed"] = True
            out = inputs.execute_dir / f"{path.stem}{self.suffix}.json"
            out.write_text(json.dumps(content))
        return {"transformed": True}

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        drafts = []
        for f in inputs.file_outputs:
            if f.suffix == ".json":
                drafts.append(
                    MetricArtifact.draft(
                        content=json.loads(f.read_text()),
                        original_name=f.name,
                        step_number=inputs.step_number,
                    )
                )
        return ArtifactResult(success=True, artifacts={"data": drafts})


# =============================================================================
# Tests: remap_output_roles
# =============================================================================


class TestRemapOutputRoles:
    def test_identity_mapping_when_none(self) -> None:
        prev = {"data": _make_source("data")}
        next_spec = {"data": InputSpec(artifact_type="metric")}
        result = remap_output_roles(prev, next_spec, mapping=None)
        assert "data" in result

    def test_explicit_mapping(self) -> None:
        prev = {"processed": _make_source("processed")}
        next_spec = {"data": InputSpec(artifact_type="metric")}
        result = remap_output_roles(prev, next_spec, mapping={"processed": "data"})
        assert "data" in result
        assert "processed" not in result

    def test_additive_mapping(self) -> None:
        """Explicit mapping plus identity matching for unmapped roles."""
        prev = {
            "processed": _make_source("processed"),
            "config": _make_source("config"),
        }
        next_spec = {
            "data": InputSpec(artifact_type="metric"),
            "config": InputSpec(artifact_type="metric"),
        }
        result = remap_output_roles(prev, next_spec, mapping={"processed": "data"})
        assert "data" in result
        assert "config" in result

    def test_extra_output_roles_dropped(self) -> None:
        prev = {
            "data": _make_source("data"),
            "extra": _make_source("extra"),
        }
        next_spec = {"data": InputSpec(artifact_type="metric")}
        result = remap_output_roles(prev, next_spec, mapping=None)
        assert "data" in result
        assert "extra" not in result

    def test_empty_mapping(self) -> None:
        prev = {"data": _make_source("data")}
        next_spec = {"data": InputSpec(artifact_type="metric")}
        result = remap_output_roles(prev, next_spec, mapping={})
        assert "data" in result


# =============================================================================
# Tests: validate_required_roles
# =============================================================================


class TestValidateRequiredRoles:
    def test_valid_when_all_required_present(self) -> None:
        sources = {"data": _make_source("data")}
        spec = {"data": InputSpec(artifact_type="metric", required=True)}
        validate_required_roles(sources, spec)

    def test_raises_when_required_missing(self) -> None:
        sources = {}
        spec = {"data": InputSpec(artifact_type="metric", required=True)}
        with pytest.raises(ValueError, match="Required input role 'data'"):
            validate_required_roles(sources, spec)

    def test_optional_role_not_required(self) -> None:
        sources = {}
        spec = {"data": InputSpec(artifact_type="metric", required=False)}
        validate_required_roles(sources, spec)


# =============================================================================
# Tests: ExecutionChain pickle serialization
# =============================================================================


class TestExecutionChainPickle:
    def test_pickle_round_trip(self) -> None:
        unit = ExecutionUnit(
            operation=GeneratorOp(count=1),
            inputs={},
            execution_spec_id="a" * 32,
            step_number=0,
        )
        chain = ExecutionChain(
            operations=[unit],
            role_mappings=[],
            resources=ResourceConfig(),
            execution=ExecutionConfig(),
            intermediates=ChainIntermediates.DISCARD,
        )
        data = pickle.dumps(chain)
        restored = pickle.loads(data)
        assert len(restored.operations) == 1
        assert restored.intermediates == ChainIntermediates.DISCARD


# =============================================================================
# Tests: Full chain execution
# =============================================================================


class TestRunCreatorChain:
    @pytest.fixture
    def delta_root(self, tmp_path):
        """Create minimal delta tables for generative ops."""
        import polars as pl

        from artisan.storage.core.table_schemas import ARTIFACT_INDEX_SCHEMA

        base = tmp_path / "delta"
        index_path = base / "artifacts/index"
        df = pl.DataFrame(schema=ARTIFACT_INDEX_SCHEMA)
        df.write_delta(str(index_path))
        return base

    @pytest.fixture
    def runtime_env(self, delta_root, tmp_path):
        return RuntimeEnvironment(
            delta_root_path=delta_root,
            working_root_path=tmp_path / "working",
            staging_root_path=tmp_path / "staging",
        )

    def test_two_op_chain(self, runtime_env, tmp_path) -> None:
        """Generator → Transformer chain produces final output."""
        from artisan.execution.executors.chain import run_creator_chain

        (tmp_path / "working").mkdir(exist_ok=True)
        (tmp_path / "staging").mkdir(exist_ok=True)

        gen_unit = ExecutionUnit(
            operation=GeneratorOp(count=2),
            inputs={},
            execution_spec_id="b" * 32,
            step_number=1,
        )
        trans_unit = ExecutionUnit(
            operation=TransformerOp(suffix="_t"),
            inputs={},
            execution_spec_id="c" * 32,
            step_number=1,
        )
        chain = ExecutionChain(
            operations=[gen_unit, trans_unit],
            role_mappings=[None],
        )

        result = run_creator_chain(chain, runtime_env)
        assert result.success is True
        assert len(result.artifact_ids) >= 2
