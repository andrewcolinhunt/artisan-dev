"""Tests for the split creator lifecycle phases (prep_unit / post_unit)."""

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
from artisan.execution.executors.creator import LifecycleResult, run_creator_lifecycle
from artisan.execution.executors.creator_phases import (
    PreppedUnit,
    _reassemble_results,
    _split_prepared_inputs,
    post_unit,
    prep_unit,
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

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _compute_id(content: bytes) -> str:
    return xxhash.xxh3_128(content).hexdigest()


def _setup_delta(base_path: Path, metrics: list[dict], index: list[dict]) -> None:
    metrics_path = base_path / "artifacts/metrics"
    pl.DataFrame(metrics, schema=MetricArtifact.POLARS_SCHEMA).write_delta(
        str(metrics_path)
    )
    index_path = base_path / "artifacts/index"
    pl.DataFrame(index, schema=ARTIFACT_INDEX_SCHEMA).write_delta(str(index_path))


class _SimpleOp(OperationDefinition):
    """Minimal operation for lifecycle phase testing."""

    class InputRole(StrEnum):
        source = auto()

    class OutputRole(StrEnum):
        output = auto()

    name: ClassVar[str] = "phases_test"
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
        # Handle both monolithic (list) and per-artifact (string) inputs
        source = inputs.inputs["source"]
        paths = source if isinstance(source, list) else [source]
        for path in paths:
            with open(path) as fh:
                content = json.loads(fh.read())
            content["processed"] = True
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


class _NoFanOutOp(_SimpleOp):
    """Operation that opts out of per-artifact dispatch."""

    name: ClassVar[str] = "no_fanout_test"
    per_artifact_dispatch: ClassVar[bool] = False


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def delta_env(tmp_path: Path):
    """Create Delta root with two input artifacts for batch testing."""
    base = tmp_path / "delta"
    working = tmp_path / "working"
    working.mkdir()
    staging = tmp_path / "staging"
    staging.mkdir()

    metrics = []
    index = []
    ids = []
    for i in range(2):
        content = json.dumps({"value": i}, sort_keys=True).encode("utf-8")
        aid = _compute_id(content)
        ids.append(aid)
        metrics.append(
            {
                "artifact_id": aid,
                "origin_step_number": 0,
                "content": content,
                "original_name": f"metric_{i}",
                "extension": ".json",
                "metadata": "{}",
                "external_path": None,
            }
        )
        index.append(
            {
                "artifact_id": aid,
                "artifact_type": "metric",
                "origin_step_number": 0,
                "metadata": "{}",
            }
        )

    _setup_delta(base, metrics, index)

    runtime_env = RuntimeEnvironment(
        delta_root=str(base),
        working_root=str(working),
        staging_root=str(staging),
    )
    return runtime_env, ids


# ---------------------------------------------------------------------------
# _split_prepared_inputs tests
# ---------------------------------------------------------------------------


class TestSplitPreparedInputs:
    def test_slices_lists_matching_batch_size(self):
        prepared = {"data": ["/a.csv", "/b.csv"], "config": "shared"}
        assert _split_prepared_inputs(prepared, 0, 2) == {
            "data": "/a.csv",
            "config": "shared",
        }
        assert _split_prepared_inputs(prepared, 1, 2) == {
            "data": "/b.csv",
            "config": "shared",
        }

    def test_passthrough_non_list(self):
        prepared = {"count": 5, "options": {"key": "val"}}
        result = _split_prepared_inputs(prepared, 0, 3)
        assert result == {"count": 5, "options": {"key": "val"}}

    def test_passthrough_different_length_list(self):
        prepared = {"stages": ["a", "b", "c"], "data": ["/x.csv"]}
        result = _split_prepared_inputs(prepared, 0, 2)
        assert result["stages"] == ["a", "b", "c"]
        assert result["data"] == ["/x.csv"]

    def test_nested_dict_list(self):
        """Pattern B: list of dicts, each representing per-artifact config."""
        prepared = {
            "items": [
                {"config_path": "/a.json", "name": "a"},
                {"config_path": "/b.json", "name": "b"},
            ]
        }
        result = _split_prepared_inputs(prepared, 0, 2)
        assert result["items"] == {"config_path": "/a.json", "name": "a"}

    def test_empty_dict(self):
        assert _split_prepared_inputs({}, 0, 1) == {}


# ---------------------------------------------------------------------------
# _reassemble_results tests
# ---------------------------------------------------------------------------


class TestReassembleResults:
    def test_all_none(self, tmp_path):
        d0 = tmp_path / "artifact_0"
        d0.mkdir()
        memory, files = _reassemble_results([None, None], [str(d0), str(d0)])
        assert memory is None

    def test_dict_concatenates_lists(self, tmp_path):
        d0 = tmp_path / "artifact_0"
        d0.mkdir()
        d1 = tmp_path / "artifact_1"
        d1.mkdir()

        results = [
            {"created_files": ["a.txt"]},
            {"created_files": ["b.txt"]},
        ]
        memory, _ = _reassemble_results(results, [str(d0), str(d1)])
        assert memory == {"created_files": ["a.txt", "b.txt"]}

    def test_filters_exceptions(self, tmp_path):
        d0 = tmp_path / "artifact_0"
        d0.mkdir()
        d1 = tmp_path / "artifact_1"
        d1.mkdir()

        results = [
            RuntimeError("boom"),
            {"results": ["ok"]},
        ]
        memory, _ = _reassemble_results(results, [str(d0), str(d1)])
        assert memory == {"results": ["ok"]}

    def test_all_exceptions_returns_none(self, tmp_path):
        d0 = tmp_path / "artifact_0"
        d0.mkdir()
        results = [RuntimeError("a"), ValueError("b")]
        memory, _ = _reassemble_results(results, [str(d0), str(d0)])
        assert memory is None

    def test_collects_files_from_all_dirs(self, tmp_path):
        d0 = tmp_path / "artifact_0"
        d0.mkdir()
        (d0 / "out_0.csv").write_text("data0")

        d1 = tmp_path / "artifact_1"
        d1.mkdir()
        (d1 / "out_1.csv").write_text("data1")

        _, files = _reassemble_results([None, None], [str(d0), str(d1)])
        basenames = sorted(os.path.basename(f) for f in files)
        assert basenames == ["out_0.csv", "out_1.csv"]

    def test_non_dict_results_returned_as_list(self, tmp_path):
        d0 = tmp_path / "artifact_0"
        d0.mkdir()
        results = ["result_a", "result_b"]
        memory, _ = _reassemble_results(results, [str(d0), str(d0)])
        assert memory == ["result_a", "result_b"]


# ---------------------------------------------------------------------------
# prep_unit tests
# ---------------------------------------------------------------------------


class TestPrepUnit:
    def test_returns_per_artifact_inputs(self, delta_env):
        runtime_env, ids = delta_env
        unit = ExecutionUnit(
            operation=_SimpleOp(),
            inputs={"source": ids},
            execution_spec_id="spec_pp" + "0" * 26,
            step_number=1,
        )

        prepped = prep_unit(unit, runtime_env)

        assert isinstance(prepped, PreppedUnit)
        assert len(prepped.artifact_execute_inputs) == 2
        assert len(prepped.artifact_execute_dirs) == 2
        for d in prepped.artifact_execute_dirs:
            assert os.path.isdir(d)
        # Each ExecuteInput has a single-element source (sliced from list of 2)
        for ei in prepped.artifact_execute_inputs:
            assert isinstance(ei.inputs["source"], str)

    def test_per_artifact_dispatch_false_single_input(self, delta_env):
        runtime_env, ids = delta_env
        unit = ExecutionUnit(
            operation=_NoFanOutOp(),
            inputs={"source": ids},
            execution_spec_id="spec_nf" + "0" * 26,
            step_number=1,
        )

        prepped = prep_unit(unit, runtime_env)

        assert len(prepped.artifact_execute_inputs) == 1
        assert len(prepped.artifact_execute_dirs) == 1
        # The single ExecuteInput has the full list
        assert isinstance(prepped.artifact_execute_inputs[0].inputs["source"], list)
        assert len(prepped.artifact_execute_inputs[0].inputs["source"]) == 2

    def test_split_batch_size_one_produces_single_input(self, delta_env):
        """A single-artifact unit with splitting produces 1 ExecuteInput."""
        runtime_env, ids = delta_env
        unit = ExecutionUnit(
            operation=_SimpleOp(),
            inputs={"source": [ids[0]]},
            execution_spec_id="spec_s1" + "0" * 26,
            step_number=1,
        )

        prepped = prep_unit(unit, runtime_env)

        assert len(prepped.artifact_execute_inputs) == 1
        assert len(prepped.artifact_execute_dirs) == 1


# ---------------------------------------------------------------------------
# Round-trip equivalence: prep → execute → post matches run_creator_lifecycle
# ---------------------------------------------------------------------------


class TestRoundTripEquivalence:
    def test_split_path_matches_monolithic(self, delta_env):
        """prep_unit → execute per-artifact → post_unit produces same results."""
        runtime_env, ids = delta_env

        # --- monolithic path ---
        unit_mono = ExecutionUnit(
            operation=_SimpleOp(),
            inputs={"source": ids},
            execution_spec_id="spec_mo" + "0" * 26,
            step_number=1,
        )
        mono_result = run_creator_lifecycle(unit_mono, runtime_env)

        # --- split path ---
        unit_split = ExecutionUnit(
            operation=_SimpleOp(),
            inputs={"source": ids},
            execution_spec_id="spec_sp" + "0" * 26,
            step_number=1,
        )
        prepped = prep_unit(unit_split, runtime_env)

        # Execute each artifact individually
        router = LocalComputeRouter()
        raw_results = []
        for ei in prepped.artifact_execute_inputs:
            result = router.route_execute(prepped.operation, ei, prepped.sandbox_path)
            raw_results.append(result)

        split_result = post_unit(prepped, raw_results, runtime_env)

        # Compare results
        assert isinstance(split_result, LifecycleResult)
        assert set(split_result.artifacts.keys()) == set(mono_result.artifacts.keys())
        for role in mono_result.artifacts:
            assert len(split_result.artifacts[role]) == len(mono_result.artifacts[role])
        assert len(split_result.edges) == len(mono_result.edges)
