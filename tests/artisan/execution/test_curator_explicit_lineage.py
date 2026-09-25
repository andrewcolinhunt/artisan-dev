"""Creator and curator boundaries enforce the same explicit lineage contract."""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum, auto
from typing import ClassVar
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from artisan.execution.exceptions import (
    ArtifactValidationError,
    LineageCompletenessError,
    LineageIntegrityError,
)
from artisan.execution.executors.creator_phases import PreppedUnit, post_unit
from artisan.execution.executors.curator import _handle_artifact_result
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.data import DataArtifact
from artisan.schemas.artifact.execution_config import ExecutionConfigArtifact
from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.schemas.execution.command_record import CommandRecording
from artisan.schemas.execution.curator_result import ArtifactResult
from artisan.schemas.execution.execution_context import ExecutionContext
from artisan.schemas.execution.replay import ReplaySnapshot
from artisan.schemas.execution.runtime_environment import RuntimeEnvironment
from artisan.schemas.provenance.lineage_mapping import LineageMapping
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec


class _TestOp(OperationDefinition):
    """Minimal operation with matching input and output role names."""

    name: ClassVar[str] = "test_declared_lineage"

    class InputRole(StrEnum):
        data = auto()

    class OutputRole(StrEnum):
        data = auto()

    inputs: ClassVar[dict[str, InputSpec]] = {"data": InputSpec(artifact_type="data")}
    outputs: ClassVar[dict[str, OutputSpec]] = {
        "data": OutputSpec(artifact_type="data", derives_from={"inputs": ["data"]})
    }

    def execute_curator(self, inputs, step_number, artifact_store):
        return ArtifactResult()


class _NamelessArtifact(Artifact):
    """A custom artifact whose semantic content needs no filename."""

    artifact_type: str = "nameless"
    value: str
    POLARS_SCHEMA: ClassVar[dict] = {
        "artifact_id": pl.String,
        "artifact_type": pl.String,
        "origin_step_number": pl.Int32,
        "metadata": pl.String,
        "value": pl.String,
    }

    def _identity_payload(self) -> bytes:
        return self.value.encode()


def _draft(name: str = "unrelated.csv") -> DataArtifact:
    return DataArtifact.draft(b"value\n1\n", name, step_number=1)


def _mapping(source: str = "a" * 32, **kwargs) -> LineageMapping:
    return LineageMapping(
        draft_index=kwargs.pop("draft_index", 0),
        source_role=kwargs.pop("source_role", "data"),
        source_artifact_id=source,
        **kwargs,
    )


@pytest.fixture(params=["creator", "curator"])
def execute_declared_result(request, tmp_path, monkeypatch):
    """Exercise real validation/resolution, replacing only final persistence."""
    store = MagicMock()
    store.provenance.load_type_map.side_effect = lambda ids: dict.fromkeys(ids, "data")

    def execute(result, *, inputs=None, specs=None):
        inputs = {"data": ["a" * 32, "b" * 32]} if inputs is None else inputs
        if specs is not None:
            monkeypatch.setattr(_TestOp, "outputs", specs)
        operation = _TestOp()
        if request.param == "curator":
            with patch(
                "artisan.execution.executors.curator.record_execution_success"
            ) as record:
                _handle_artifact_result(
                    result=result,
                    operation=operation,
                    artifact_store=store,
                    execution_context=MagicMock(
                        spec=ExecutionContext, execution_run_id="f" * 32
                    ),
                    inputs=inputs,
                    timestamp_end=datetime.now(UTC),
                    command_recording=CommandRecording.empty(),
                    replay_snapshot=ReplaySnapshot.unavailable(
                        "direct_recorder_fixture"
                    ),
                    replay_of_execution_run_id=None,
                )
            return record.call_args.kwargs["lineage_edges"]
        prepped = PreppedUnit(
            unit=MagicMock(step_number=1),
            execution_run_id="f" * 32,
            sandbox_path=str(tmp_path),
            postprocess_dir=str(tmp_path),
            log_path=str(tmp_path / "missing.log"),
            files_dir=None,
            operation=operation,
            input_artifacts={
                role: [DataArtifact(artifact_id=aid) for aid in ids]
                for role, ids in inputs.items()
            },
            associated={},
        )
        runtime = RuntimeEnvironment(
            delta_root=str(tmp_path / "delta"),
            staging_root=str(tmp_path / "staging"),
            working_root=str(tmp_path),
            preserve_working=True,
        )
        with patch.object(_TestOp, "postprocess", return_value=result):
            return post_unit(prepped, [], runtime).edges

    execute.store = store
    execute.kind = request.param
    return execute


@pytest.mark.parametrize(
    ("lineage", "error"),
    [
        ({}, LineageIntegrityError),
        ({"data": [], "extra": []}, LineageIntegrityError),
        ({"data": []}, LineageCompletenessError),
        ({"data": [_mapping(draft_index=1)]}, LineageIntegrityError),
        ({"data": [_mapping(source_role="reference")]}, LineageIntegrityError),
        ({"data": [_mapping("c" * 32)]}, LineageIntegrityError),
        ({"data": [_mapping(), _mapping()]}, LineageIntegrityError),
        (
            {
                "data": [
                    LineageMapping(
                        draft_index=0, source_role="data", source_output_index=0
                    )
                ]
            },
            LineageIntegrityError,
        ),
    ],
)
def test_invalid_declarations_fail_before_finalization(
    execute_declared_result, lineage, error
):
    output = _draft("a" * 32 + ".csv")
    with pytest.raises(error):
        execute_declared_result(
            ArtifactResult(artifacts={"data": [output]}, lineage=lineage)
        )
    assert output.artifact_id is None
    execute_declared_result.store.provenance.load_type_map.assert_not_called()


def test_every_derived_occurrence_requires_its_own_parent(execute_declared_result):
    result = ArtifactResult(
        artifacts={"data": [_draft("same.csv"), _draft("same.csv")]},
        lineage={"data": [_mapping()]},
    )
    with pytest.raises(LineageCompletenessError, match=r"data.*\[1\]"):
        execute_declared_result(result)


def test_required_parent_roles_are_enforced(execute_declared_result):
    result = ArtifactResult(
        artifacts={"data": [_draft()]}, lineage={"data": [_mapping()]}
    )
    with pytest.raises(LineageCompletenessError, match="reference"):
        execute_declared_result(
            result,
            inputs={"data": ["a" * 32], "reference": ["b" * 32]},
            specs={
                "data": OutputSpec(
                    artifact_type="data", derives_from={"inputs": ["data", "reference"]}
                )
            },
        )


def test_fan_in_uses_declared_parents_without_hydrating(execute_declared_result):
    output = _draft()
    result = ArtifactResult(
        artifacts={"data": [output]},
        lineage={"data": [_mapping(), _mapping("b" * 32)]},
    )
    edges = execute_declared_result(result)
    assert {edge.source_artifact_id for edge in edges} == {"a" * 32, "b" * 32}
    assert {edge.target_artifact_id for edge in edges} == {output.artifact_id}
    assert len({edge.group_id for edge in edges}) == 1
    assert edges[0].group_id is not None
    assert output.original_name == "unrelated"
    store = execute_declared_result.store
    if execute_declared_result.kind == "curator":
        store.provenance.load_type_map.assert_called_once_with(["a" * 32, "b" * 32])
    store.get_artifact.assert_not_called()
    store.get_artifacts_by_type.assert_not_called()


@pytest.mark.parametrize("execute_declared_result", ["curator"], indirect=True)
def test_missing_source_type_fails_before_recording(execute_declared_result):
    execute_declared_result.store.provenance.load_type_map.side_effect = None
    execute_declared_result.store.provenance.load_type_map.return_value = {}
    result = ArtifactResult(
        artifacts={"data": [_draft()]}, lineage={"data": [_mapping()]}
    )
    with pytest.raises(LineageIntegrityError, match="type"):
        execute_declared_result(result)


def test_root_requires_explicit_empty_role(execute_declared_result):
    specs = {"data": OutputSpec(artifact_type="data", derives_from={"inputs": []})}
    assert (
        execute_declared_result(
            ArtifactResult(artifacts={"data": [_draft()]}, lineage={"data": []}),
            specs=specs,
        )
        == []
    )
    with pytest.raises(LineageIntegrityError):
        execute_declared_result(
            ArtifactResult(artifacts={"data": [_draft()]}), specs=specs
        )


@pytest.mark.parametrize("execute_declared_result", ["curator"], indirect=True)
def test_dynamic_curator_roles_are_explicit_roots(execute_declared_result):
    assert (
        execute_declared_result(
            ArtifactResult(artifacts={"runtime": [_draft()]}, lineage={"runtime": []}),
            specs={},
        )
        == []
    )
    with pytest.raises(LineageIntegrityError, match="Root"):
        execute_declared_result(
            ArtifactResult(
                artifacts={"runtime": [_draft()]}, lineage={"runtime": [_mapping()]}
            ),
            specs={},
        )


@pytest.mark.parametrize("present", [False, True])
def test_optional_output_has_no_occurrence_to_cover(execute_declared_result, present):
    specs = {
        "data": OutputSpec(
            artifact_type="data", required=False, derives_from={"inputs": ["data"]}
        )
    }
    result = (
        ArtifactResult(artifacts={"data": []}, lineage={"data": []})
        if present
        else ArtifactResult()
    )
    assert execute_declared_result(result, specs=specs) == []


def test_artifact_without_original_name_has_explicit_lineage(
    execute_declared_result, monkeypatch
):
    monkeypatch.setattr(ArtifactTypeDef, "_registry", dict(ArtifactTypeDef._registry))
    monkeypatch.setattr(ArtifactTypes, "_registry", dict(ArtifactTypes._registry))
    monkeypatch.setattr(ArtifactTypes, "NAMELESS", None, raising=False)

    class NamelessTypeDef(ArtifactTypeDef):
        key = "nameless"
        table_path = "artifacts/nameless"
        model = _NamelessArtifact

    output = _NamelessArtifact(value="payload")
    result = ArtifactResult(
        artifacts={"data": [output]}, lineage={"data": [_mapping()]}
    )
    edges = execute_declared_result(
        result,
        specs={
            "data": OutputSpec(
                artifact_type=ArtifactTypes.ANY, derives_from={"inputs": ["data"]}
            )
        },
    )
    assert len(edges) == 1
    assert edges[0].target_artifact_id == output.artifact_id
    assert edges[0].target_artifact_type == "nameless"


def test_config_content_adds_no_undeclared_parent(execute_declared_result):
    output = ExecutionConfigArtifact.draft(
        {"input": {"$artifact": "a" * 32}, "other": {"$artifact": "b" * 32}},
        "config.json",
        1,
    )
    result = ArtifactResult(
        artifacts={"data": [output]}, lineage={"data": [_mapping()]}
    )
    edges = execute_declared_result(
        result,
        specs={
            "data": OutputSpec(
                artifact_type="config", derives_from={"inputs": ["data"]}
            )
        },
    )
    assert len(edges) == 1
    assert edges[0].source_artifact_id == "a" * 32
    assert edges[0].target_artifact_id == output.artifact_id


@pytest.mark.parametrize("execute_declared_result", ["creator"], indirect=True)
def test_creator_cannot_emit_undeclared_dynamic_roles(execute_declared_result):
    with pytest.raises(ArtifactValidationError, match="Unexpected output roles"):
        execute_declared_result(
            ArtifactResult(artifacts={"runtime": [_draft()]}, lineage={"runtime": []}),
            specs={},
        )


def test_executor_preserves_names_order_and_sibling_indices(execute_declared_result):
    first = _draft("z-last-alphabetically.csv")
    second = DataArtifact.draft(b"value\n2\n", "a-first-alphabetically.csv", 1)
    child = _draft("unrelated-child.csv")
    result = ArtifactResult(
        artifacts={"data": [first, second], "child": [child]},
        lineage={
            "data": [_mapping(draft_index=0), _mapping("b" * 32, draft_index=1)],
            "child": [
                LineageMapping(draft_index=0, source_role="data", source_output_index=1)
            ],
        },
    )
    edges = execute_declared_result(
        result,
        specs={
            "data": OutputSpec(artifact_type="data", derives_from={"inputs": ["data"]}),
            "child": OutputSpec(
                artifact_type="data", derives_from={"outputs": ["data"]}
            ),
        },
    )
    assert [artifact.original_name for artifact in result.artifacts["data"]] == [
        "z-last-alphabetically",
        "a-first-alphabetically",
    ]
    assert {(edge.source_artifact_id, edge.target_artifact_id) for edge in edges} == {
        ("a" * 32, first.artifact_id),
        ("b" * 32, second.artifact_id),
        (second.artifact_id, child.artifact_id),
    }


@pytest.mark.parametrize("execute_declared_result", ["curator"], indirect=True)
@pytest.mark.parametrize("source_exists", [True, False])
def test_identity_neutral_output_cannot_supply_missing_input_type(
    execute_declared_result, source_exists
):
    source_id = _draft().finalize().artifact_id
    output = _draft()
    result = ArtifactResult(
        artifacts={"data": [output]}, lineage={"data": [_mapping(source_id)]}
    )
    store = execute_declared_result.store
    store.provenance.load_type_map.side_effect = None
    store.provenance.load_type_map.return_value = (
        {source_id: "data"} if source_exists else {}
    )
    if source_exists:
        edges = execute_declared_result(result, inputs={"data": [source_id]})
        assert len(edges) == 1
        assert edges[0].source_artifact_id == edges[0].target_artifact_id == source_id
    else:
        with pytest.raises(LineageIntegrityError, match="type"):
            execute_declared_result(result, inputs={"data": [source_id]})
