"""Public run identity and input snapshots retain exact logical references."""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest
from fixtures.composition_operations import Transform
from fixtures.output_references import SYNTHETIC_STORE, output_reference

from artisan.orchestration import PipelineManager
from artisan.orchestration.engine.pipeline_inputs import normalize_pipeline_inputs


def test_run_and_attempt_identities_are_unique_and_names_are_explicit(
    tmp_path: Path,
) -> None:
    identities = []
    attempts = []
    for index in range(2):
        with PipelineManager.create(
            "example_pipeline",
            str(tmp_path / f"store-{index}"),
            str(tmp_path / "staging"),
        ) as pipeline:
            identities.append(pipeline.config.pipeline_run_id)
            assert pipeline.config.name == "example_pipeline"
            assert re.fullmatch(
                r"example_pipeline_\d{8}_\d{6}_[0-9a-f]{8}", identities[-1]
            )
            result = pipeline.run(Transform, inputs={"structure": []})
            attempts.append(result.step_run_id)
            assert re.fullmatch(r"[0-9a-f]{32}", result.step_run_id)
            assert pipeline.executions[0].record.operation_class == (
                f"{Transform.__module__}.{Transform.__qualname__}"
            )
    assert len(set(identities)) == len(set(attempts)) == 2


def test_named_input_snapshots_bind_attempt_identity_and_preserve_duplicates() -> None:
    reference = output_reference("a" * 32, artifact_type="csv")
    source = {"data": [reference, reference]}
    inputs = normalize_pipeline_inputs(source, store_identity=SYNTHETIC_STORE)
    source["data"].clear()
    assert inputs.dependency_step_run_ids == ("a" * 32,)
    assert json.loads(json.dumps(inputs.diagnostic_data())) == {
        "data": [
            {
                "step_run_id": "a" * 32,
                "role": "data",
                "artifact_type": "csv",
            },
            {
                "step_run_id": "a" * 32,
                "role": "data",
                "artifact_type": "csv",
            },
        ]
    }


def test_unnamed_reference_streams_keep_source_order() -> None:
    inputs = normalize_pipeline_inputs(
        [
            output_reference("c" * 32),
            output_reference("b" * 32, role="score"),
        ],
        store_identity=SYNTHETIC_STORE,
    )
    assert inputs.dependency_step_run_ids == ("c" * 32, "b" * 32)
    assert [
        value["step_run_id"] for value in inputs.diagnostic_data()["_merged_streams"]
    ] == ["c" * 32, "b" * 32]


@pytest.mark.parametrize(
    "value", [None, {"data": ["a" * 32, "b" * 32]}, ["/data/a.csv", "/data/b.csv"]]
)
def test_literal_inputs_do_not_invent_step_dependencies(value: object) -> None:
    inputs = normalize_pipeline_inputs(value, store_identity=SYNTHETIC_STORE)
    assert inputs.dependency_step_run_ids == ()
    if value is None:
        assert inputs.diagnostic_data() == {}
    elif isinstance(value, list):
        assert inputs.diagnostic_data() == {"raw_paths": value}
    else:
        assert inputs.diagnostic_data() == {
            "data": [{"artifact_id": "a" * 32}, {"artifact_id": "b" * 32}]
        }


def test_arbitrary_objects_are_rejected_without_string_coercion() -> None:
    with pytest.raises(TypeError):
        normalize_pipeline_inputs(42, store_identity=SYNTHETIC_STORE)
