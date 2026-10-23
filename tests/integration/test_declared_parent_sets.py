"""Persist repeated-primary pair declarations across execution and transport shapes."""

from __future__ import annotations

import json
import os
import shutil
from pathlib import Path

import polars as pl
import pytest
from fixtures.declared_pair_ops import (
    DeclaredPairCommand,
    DeclaredPairs,
    MonolithicDeclaredPairCommand,
    MonolithicDeclaredPairs,
)

from artisan.execution.tool_endpoint.transport import InlineTransport
from artisan.operations.examples import DataGenerator
from artisan.orchestration import PipelineManager
from artisan.schemas.enums import TablePath
from artisan.schemas.orchestration.step_lifecycle import StepDisposition
from artisan.schemas.specs.input_models import ExecuteInput, PostprocessInput

from .conftest import (
    get_execution_inputs,
    get_execution_outputs,
    load_artifact_edges,
    read_table,
)

pytestmark = pytest.mark.integration


def _assert_exact_parent_sets(
    delta_root: str, step: int, primary: str, partners: list[str]
) -> dict[str, str]:
    output_ids = get_execution_outputs(delta_root, step, "prediction")
    assert len(output_ids) == len(set(output_ids)) == 2
    metrics = read_table(delta_root, "artifacts/metrics").filter(
        pl.col("artifact_id").is_in(output_ids)
    )
    assert metrics["original_name"].to_list() == ["prediction", "prediction"]
    execution_ids = (
        read_table(delta_root, TablePath.EXECUTIONS.value)
        .filter(pl.col("origin_step_number") == step)["execution_run_id"]
        .to_list()
    )
    edges = load_artifact_edges(delta_root, output_ids).filter(
        pl.col("execution_run_id").is_in(execution_ids)
    )
    groups = {}
    actual_partners = set()
    for row in metrics.iter_rows(named=True):
        content = json.loads(row["content"])
        assert content["primary"] == primary
        actual_partners.add(content["partner"])
        declared = edges.filter(pl.col("target_artifact_id") == row["artifact_id"])
        assert set(
            zip(declared["source_role"], declared["source_artifact_id"], strict=True)
        ) == {("primary", primary), ("partner", content["partner"])}
        assert declared.height == 2
        assert declared["group_id"].null_count() == 0
        assert declared["group_id"].n_unique() == 1
        groups[content["partner"]] = declared["group_id"][0]
    assert actual_partners == set(partners)
    assert len(set(groups.values())) == 2
    return groups


@pytest.mark.parametrize("operation", [DeclaredPairs, MonolithicDeclaredPairs])
@pytest.mark.parametrize("file_outputs", [False, True])
def test_repeated_primary_pairs_and_reference_inputs_remain_distinct(
    pipeline_env, operation, file_outputs
):
    pipeline = PipelineManager.create(name="declared_pairs", **pipeline_env)
    pipeline.run(DataGenerator, params={"count": 5, "seed": 41})
    ids = get_execution_outputs(pipeline_env["delta_root"], 0, "datasets")
    first = pipeline.run(
        operation,
        inputs={"primary": ids[:1], "partner": ids[1:3], "reference": ids[3:4]},
        params={"file_outputs": file_outputs},
        batch_strategy={"artifacts_per_unit": 2},
    )
    changed_reference = pipeline.run(
        operation,
        inputs={"primary": ids[:1], "partner": ids[1:3], "reference": ids[4:5]},
        params={"file_outputs": file_outputs},
        batch_strategy={"artifacts_per_unit": 2},
    )
    assert pipeline.finalize()["overall_success"]
    assert (
        first.disposition == changed_reference.disposition == StepDisposition.EXECUTED
    )
    first_groups = _assert_exact_parent_sets(
        pipeline_env["delta_root"], 1, ids[0], ids[1:3]
    )
    assert (
        _assert_exact_parent_sets(pipeline_env["delta_root"], 2, ids[0], ids[1:3])
        == first_groups
    )
    assert set(get_execution_inputs(pipeline_env["delta_root"], 1, "primary")) == {
        ids[0]
    }
    assert set(get_execution_inputs(pipeline_env["delta_root"], 1, "reference")) == {
        ids[3]
    }
    assert set(get_execution_inputs(pipeline_env["delta_root"], 2, "reference")) == {
        ids[4]
    }
    executions = read_table(
        pipeline_env["delta_root"], TablePath.EXECUTIONS.value
    ).filter(pl.col("origin_step_number").is_in([1, 2]))
    assert executions.height == 2
    assert executions["execution_spec_id"].n_unique() == 2
    assert set(
        get_execution_outputs(pipeline_env["delta_root"], 1, "prediction")
    ) == set(get_execution_outputs(pipeline_env["delta_root"], 2, "prediction"))


@pytest.mark.parametrize(
    "operation", [DeclaredPairCommand, MonolithicDeclaredPairCommand]
)
def test_command_manifest_preserves_repeated_primary_pairs(
    pipeline_env, monkeypatch, operation
):
    tests_root = str(Path(__file__).parents[1])
    monkeypatch.setenv(
        "PYTHONPATH",
        os.pathsep.join(filter(None, [tests_root, os.environ.get("PYTHONPATH", "")])),
    )
    pipeline = PipelineManager.create(name="declared_pair_command", **pipeline_env)
    pipeline.run(DataGenerator, params={"count": 4, "seed": 42})
    ids = get_execution_outputs(pipeline_env["delta_root"], 0, "datasets")
    pipeline.run(
        operation,
        inputs={"primary": ids[:1], "partner": ids[1:3], "reference": ids[3:]},
        batch_strategy={"artifacts_per_unit": 2},
    )
    assert pipeline.finalize()["overall_success"]
    _assert_exact_parent_sets(pipeline_env["delta_root"], 1, ids[0], ids[1:3])


def test_command_manifest_survives_file_only_transport_and_remote_path_removal(
    tmp_path: Path,
):
    transport = InlineTransport()
    operation = DeclaredPairCommand()
    received = []
    for index, partner in enumerate(["b" * 32, "c" * 32]):
        remote_root = tmp_path / f"remote-{index}"
        local_input = tmp_path / f"prepared-{index}.json"
        local_input.write_text(
            json.dumps({"primary": "a" * 32, "partner": partner, "reference": "d" * 32})
        )
        remote_inputs = transport.unpack_inputs(
            transport.pack_inputs({"pair_file": str(local_input)}),
            str(remote_root / "inputs"),
        )
        remote_output = remote_root / "output"
        remote_output.mkdir()
        assert (
            operation.execute_function(
                ExecuteInput(inputs=remote_inputs, execute_dir=str(remote_output))
            )
            is None
        )
        names = [
            str(path.relative_to(remote_output))
            for path in remote_output.rglob("*")
            if path.is_file()
        ]
        payload = transport.pack_outputs(str(remote_output), names)
        local_output = tmp_path / f"received-{index}"
        transport.unpack_outputs(payload, str(local_output))
        received.extend(str(path) for path in local_output.rglob("*") if path.is_file())
        shutil.rmtree(remote_root)
    result = operation.postprocess(
        PostprocessInput(
            file_outputs=received,
            memory_outputs=None,
            step_number=1,
            postprocess_dir=str(tmp_path),
        )
    )
    assert len(result.artifacts["prediction"]) == 2
    declared = {
        index: {
            (mapping.source_role, mapping.source_artifact_id)
            for mapping in result.lineage["prediction"]
            if mapping.draft_index == index
        }
        for index in range(2)
    }
    assert declared == {
        0: {("primary", "a" * 32), ("partner", "b" * 32)},
        1: {("primary", "a" * 32), ("partner", "c" * 32)},
    }
