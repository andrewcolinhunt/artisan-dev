"""Curator preparation freezes verified content membership without side effects."""

from __future__ import annotations

import json
from enum import StrEnum
from pathlib import Path
from typing import ClassVar

import polars as pl
import pytest
from fixtures.retained_acceptance import open_committer
from fixtures.retained_curators import admit_curator, stage_curator
from fixtures.retained_readers import admit_read_case
from fixtures.retained_results import RetainedCase, _derive, boundary_artifact

from artisan.errors import ArtifactIntegrityError, StoreIntegrityError
from artisan.execution.lineage.graph import OccurrenceGraph
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.operations.curator.merge import Merge
from artisan.orchestration.engine.curator_inputs import prepare_curator_inputs
from artisan.schemas import FileRefArtifact, InputSpec, MetricArtifact, OutputSpec
from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.schemas.enums import TablePath
from artisan.schemas.execution.curator_admission import CuratorReadSnapshot
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.schemas.orchestration.step_start_record import StepStartRecord
from artisan.schemas.provenance.projection import AcceptedCuratorOccurrence
from artisan.storage.cache.curator_dependencies import CuratorReadRecorder
from artisan.storage.core.curator_reader import CuratorReader
from artisan.storage.core.curator_view import CuratorView, capture_curator_view
from artisan.storage.core.retained_reader import AcceptedArtifact, RetainedReader
from artisan.storage.io.curator_admission import curator_admission_path
from artisan.storage.io.curator_plan import read_curator_plan
from artisan.storage.io.retained_commit import RetainedCommitter
from artisan.utils.hashing import compute_content_digest


class SelectCSV(Merge):
    name = "select_csv"
    runtime_defined_inputs = False

    class InputRole(StrEnum):
        data = "data"
        optional = "optional"

    inputs: ClassVar = {
        "data": InputSpec(artifact_type="csv", with_associated=("metric", "file_ref")),
        "optional": InputSpec(required=False),
    }


@pytest.fixture
def committer(tmp_path: Path) -> RetainedCommitter:
    storage = StorageConfig()
    return open_committer(
        str(tmp_path / "store"), storage.filesystem(), storage, initialize=True
    )


def _record(operation: OperationDefinition) -> StepStartRecord:
    return StepStartRecord(
        step_run_id="d" * 32,
        step_number=10,
        step_name=operation.name,
        operation_class=f"{type(operation).__module__}.{type(operation).__qualname__}",
        params_json="{}",
        input_refs_json="{}",
        compute_backend="local",
        compute_options_json=json.dumps(
            {"dynamic_output_roles": not operation.outputs}
        ),
        output_roles_json=json.dumps(list(operation.outputs)),
        output_types_json=json.dumps(
            {role: spec.artifact_type for role, spec in operation.outputs.items()}
        ),
    )


def _measurement(
    committer: RetainedCommitter,
    *,
    owner: str = "a",
    score: int = 1,
    external: Path | None = None,
) -> tuple[AcceptedArtifact, AcceptedArtifact]:
    graph = OccurrenceGraph(boundary_invocation=owner)
    primary = boundary_artifact()
    root = graph.add_input("data", primary)
    if external is None:
        related = MetricArtifact.draft(
            {"score": score}, f"score-{score}.json", 7
        ).finalize()
    else:
        data = b"associated external bytes"
        external.write_bytes(data)
        related = FileRefArtifact.draft(
            str(external), compute_content_digest(data), len(data), 7
        ).finalize()
    child = _derive(graph, f"measure-{owner}", related, [root])
    case = RetainedCase(
        graph.project(
            {"data": [root], "related": [child]},
            {"data": OutputSpec(), "related": OutputSpec()},
        ),
        {root: primary},
        {child: related},
    )
    _, receipt = admit_read_case(committer, case, step_run_id=owner * 32)
    committer.accept(receipt)
    result = RetainedReader(committer).read(receipt)
    return result.output("data")[0], result.output("related")[0]


def test_preparation_preserves_order_repeats_and_exact_producers_without_publication(
    committer: RetainedCommitter,
) -> None:
    first, _ = _measurement(committer)
    second, _ = _measurement(committer, owner="b", score=2)
    artifact_id = first.artifact.artifact_id
    assert artifact_id is not None
    assert artifact_id == second.artifact.artifact_id
    before = committer._controls()
    operation = Merge()
    admission = prepare_curator_inputs(
        operation,
        _record(operation),
        {"second": [second, artifact_id, first, second], "empty": []},
        software_digest="e" * 64,
        software_cacheable=False,
        committer=committer,
    )
    assert [group.role for group in admission.inputs] == ["second", "empty"]
    assert [member.producers for member in admission.inputs[0].members] == [
        (second.reference,),
        (),
        (first.reference,),
        (second.reference,),
    ]
    assert admission.inputs[1].members == ()
    assert admission.associations == ()
    assert not admission.software_cacheable
    assert admission.definition is not None
    assert committer._controls().equals(before)
    assert not committer.fs.exists(
        curator_admission_path(committer.delta_root, "d" * 32)
    )


@pytest.mark.parametrize("inputs", [{}, {"unknown": []}, {"data": [], "extra": []}])
def test_fixed_roles_reject_missing_or_unknown_before_store_capture(
    committer: RetainedCommitter,
    inputs: dict[str, list[str]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "artisan.orchestration.engine.curator_inputs.capture_curator_view",
        lambda _: pytest.fail("Captured an invalid input contract"),
    )
    operation = SelectCSV()
    with pytest.raises(ValueError, match="missing required or undeclared"):
        prepare_curator_inputs(
            operation,
            _record(operation),
            inputs,
            software_digest="e" * 64,
            software_cacheable=True,
            committer=committer,
        )


def test_explicit_empty_required_role_and_optional_omission_are_preserved(
    committer: RetainedCommitter,
) -> None:
    operation = SelectCSV()
    admission = prepare_curator_inputs(
        operation,
        _record(operation),
        {"data": []},
        software_digest="e" * 64,
        software_cacheable=True,
        committer=committer,
    )
    assert [(role.role, role.members) for role in admission.inputs] == [("data", ())]
    assert admission.associations == ()


@pytest.mark.parametrize("failure", ["type", "missing", "object", "producer"])
def test_preparation_rejects_wrong_type_missing_content_and_forged_accepted_values(
    committer: RetainedCommitter,
    failure: str,
) -> None:
    primary, metric = _measurement(committer)
    value: AcceptedArtifact | str = primary
    if failure == "type":
        value = metric
    elif failure == "missing":
        value = "f" * 32
    elif failure == "object":
        value = AcceptedArtifact(
            primary.reference,
            primary.artifact.model_copy(update={"origin_step_number": 99}),
        )
    elif failure == "producer":
        value = AcceptedArtifact(metric.reference, primary.artifact)
    operation = SelectCSV()
    with pytest.raises(
        (ValueError, StoreIntegrityError, ArtifactIntegrityError),
        match="type|content|origin|mutated|view",
    ):
        prepare_curator_inputs(
            operation,
            _record(operation),
            {"data": [value]},
            software_digest="e" * 64,
            software_cacheable=True,
            committer=committer,
        )


def test_associations_freeze_global_direct_content_membership_including_empty(
    committer: RetainedCommitter,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    primary, first_metric = _measurement(committer)
    snapshot = capture_curator_view(committer)
    _, second_metric = _measurement(committer, owner="b", score=2)
    assert first_metric.artifact.artifact_id is not None
    assert second_metric.artifact.artifact_id is not None
    operation = SelectCSV()
    current = prepare_curator_inputs(
        operation,
        _record(operation),
        {"data": [primary]},
        software_digest="e" * 64,
        software_cacheable=True,
        committer=committer,
    )
    assert current.associations[1].artifact_ids == tuple(
        sorted((first_metric.artifact.artifact_id, second_metric.artifact.artifact_id))
    )
    captures = []

    def captured(_: RetainedCommitter) -> CuratorReadSnapshot:
        captures.append(snapshot)
        return snapshot

    monkeypatch.setattr(
        "artisan.orchestration.engine.curator_inputs.capture_curator_view", captured
    )
    admission = prepare_curator_inputs(
        operation,
        _record(operation),
        {"data": [primary, primary]},
        software_digest="e" * 64,
        software_cacheable=True,
        committer=committer,
    )
    assert captures == [snapshot]
    assert [
        (item.artifact_type, item.artifact_ids) for item in admission.associations
    ] == [
        ("file_ref", ()),
        ("metric", (first_metric.artifact.artifact_id,)),
    ]
    assert (
        second_metric.artifact.artifact_id not in admission.associations[1].artifact_ids
    )


def test_producer_completed_after_captured_view_is_not_admitted_by_equal_content(
    committer: RetainedCommitter,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _measurement(committer)
    snapshot = capture_curator_view(committer)
    later, _ = _measurement(committer, owner="b", score=2)
    monkeypatch.setattr(
        "artisan.orchestration.engine.curator_inputs.capture_curator_view",
        lambda _: snapshot,
    )
    operation = Merge()
    with pytest.raises(StoreIntegrityError, match="admitted completed view"):
        prepare_curator_inputs(
            operation,
            _record(operation),
            {"data": [later]},
            software_digest="e" * 64,
            software_cacheable=True,
            committer=committer,
        )


def test_explicit_curator_producer_keeps_its_own_completion_authority(
    committer: RetainedCommitter,
) -> None:
    primary, _ = _measurement(committer)
    operation = Merge()
    coordinator, admission, reference = admit_curator(
        committer,
        operation,
        {"data": (primary,)},
        step_run_id="b" * 32,
    )
    staged, terminal = stage_curator(coordinator, admission, operation)
    assert staged.execution_run_id is not None
    coordinator.finish(reference, staged.execution_run_id, terminal)
    plan = read_curator_plan(committer.delta_root, "b" * 32, committer.fs)
    assert plan is not None
    output = (
        CuratorReader(committer)
        .read_result("b" * 32, plan.plan_digest)
        .output("merged")[0]
    )
    prepared = prepare_curator_inputs(
        operation,
        _record(operation),
        {"data": [output]},
        software_digest="e" * 64,
        software_cacheable=True,
        committer=committer,
    )
    assert prepared.inputs[0].members[0].producers == (output.reference,)
    assert isinstance(output.reference, AcceptedCuratorOccurrence)


@pytest.mark.parametrize("table", ["index", "content"])
def test_changed_registered_origin_rejects_before_admission(
    committer: RetainedCommitter,
    table: str,
) -> None:
    primary, _ = _measurement(committer)
    selected = (
        TablePath.ARTIFACT_INDEX.value
        if table == "index"
        else ArtifactTypeDef.get_table_path(primary.artifact.artifact_type)
    )
    path = f"{committer.delta_root}/{selected}"
    pl.read_delta(path).with_columns(
        pl.lit(99, dtype=pl.Int32).alias("origin_step_number")
    ).write_delta(path, mode="overwrite")
    operation = Merge()
    with pytest.raises(StoreIntegrityError, match="origin|conflicting"):
        prepare_curator_inputs(
            operation,
            _record(operation),
            {"data": [primary]},
            software_digest="e" * 64,
            software_cacheable=True,
            committer=committer,
        )


def test_changed_associated_content_cannot_enter_frozen_membership(
    committer: RetainedCommitter,
) -> None:
    primary, _ = _measurement(committer)
    path = f"{committer.delta_root}/{ArtifactTypeDef.get_table_path('metric')}"
    pl.read_delta(path).with_columns(
        pl.lit(b'{"score":999}', dtype=pl.Binary).alias("content")
    ).write_delta(path, mode="overwrite")
    operation = SelectCSV()
    with pytest.raises(StoreIntegrityError, match="conflicting"):
        prepare_curator_inputs(
            operation,
            _record(operation),
            {"data": [primary]},
            software_digest="e" * 64,
            software_cacheable=True,
            committer=committer,
        )


def test_historical_corruption_rejects_even_when_latest_version_is_repaired(
    committer: RetainedCommitter,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    primary, _ = _measurement(committer)
    path = f"{committer.delta_root}/{TablePath.ARTIFACT_INDEX.value}"
    original = pl.read_delta(path)
    original.with_columns(
        pl.lit(99, dtype=pl.Int32).alias("origin_step_number")
    ).write_delta(path, mode="overwrite")
    captured = capture_curator_view(committer)
    original.write_delta(path, mode="overwrite")
    operation = Merge()
    prepare_curator_inputs(
        operation,
        _record(operation),
        {"data": [primary]},
        software_digest="e" * 64,
        software_cacheable=True,
        committer=committer,
    )
    monkeypatch.setattr(
        "artisan.orchestration.engine.curator_inputs.capture_curator_view",
        lambda _: captured,
    )
    with pytest.raises(StoreIntegrityError, match="origin|conflicting"):
        prepare_curator_inputs(
            operation,
            _record(operation),
            {"data": [primary]},
            software_digest="e" * 64,
            software_cacheable=True,
            committer=committer,
        )


def test_external_content_is_not_read_for_ids_but_associated_hydration_stays_ineligible(
    committer: RetainedCommitter,
    tmp_path: Path,
) -> None:
    primary, external = _measurement(committer, external=tmp_path / "source.bin")
    assert isinstance(external.artifact, FileRefArtifact)
    assert external.artifact.path is not None
    committer.fs.rm(external.artifact.path)
    operation = Merge()
    ids_only = prepare_curator_inputs(
        operation,
        _record(operation),
        {"data": [external]},
        software_digest="e" * 64,
        software_cacheable=True,
        committer=committer,
    )
    assert not CuratorReadRecorder(
        CuratorView(committer, ids_only.view), ids_only
    ).exclusions
    associated = SelectCSV()
    admission = prepare_curator_inputs(
        associated,
        _record(associated),
        {"data": [primary]},
        software_digest="e" * 64,
        software_cacheable=True,
        committer=committer,
    )
    recorder = CuratorReadRecorder(CuratorView(committer, admission.view), admission)
    assert recorder.exclusions == {"untracked_external_content"}
