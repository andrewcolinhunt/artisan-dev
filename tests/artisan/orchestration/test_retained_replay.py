"""Frozen whole-recipe replay uses new diagnostic ownership and authorized code."""

from __future__ import annotations

import importlib
from pathlib import Path
from types import ModuleType

import pytest
from fixtures.replay_operations import Increment, TwoIncrements
from fixtures.retained_acceptance import open_committer

from artisan.composites.base.compiled import CompiledPlan
from artisan.composites.base.compiler import compile_definition
from artisan.errors import ArtisanError, StoreIntegrityError
from artisan.execution.composition import execute_compiled_plan
from artisan.execution.compute.local import LocalExecuteRouter
from artisan.execution.executor import Executor
from artisan.execution.lineage.graph import OccurrenceGraph
from artisan.execution.recording.retained_replay import (
    capture_replay_configuration,
    publish_replay_configuration,
    read_replay_configuration,
)
from artisan.orchestration.retained_replay import Definition, prepare_retained_replay
from artisan.schemas.artifact.metric import MetricArtifact
from artisan.schemas.enums import GroupByStrategy
from artisan.schemas.execution.admission import AdmissionReference
from artisan.schemas.execution.assignment import (
    AssignmentAttempt,
    AssignmentIdentity,
    DeliveryIdentity,
    SealedDelivery,
    SuccessfulDelivery,
)
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.storage.core.retained_reader import RetainedReader
from artisan.storage.io.admission import publish_admission
from artisan.storage.io.retained_seal import stage_retained_result
from artisan.utils.command_supervisor import close_command_scope

type AcceptedFixture = tuple[RetainedReader, SealedDelivery, AdmissionReference]


@pytest.fixture
def accepted(tmp_path: Path) -> AcceptedFixture:
    return _accept_plan(tmp_path, compile_definition(TwoIncrements))


def _accept_plan(tmp_path: Path, plan: CompiledPlan) -> AcceptedFixture:
    storage = StorageConfig()
    committer = open_committer(
        str(tmp_path / "store"), storage.filesystem(), storage, initialize=True
    )
    identity = AssignmentIdentity(step_run_id="a" * 32, unit_index=0)
    artifact = MetricArtifact.draft({"value": 3}, "source.json").finalize()
    graph = OccurrenceGraph(boundary_invocation=identity.assignment_id)
    graph.add_input("source", artifact)
    boundary = graph.project({}, {})
    attempt = AssignmentAttempt(
        assignment=identity,
        attempt=0,
        generation=0,
        recipe_digest=plan.recipe.digest,
        software_digest="b" * 64,
        input_context_digest=boundary.input_context_digest,
    )
    artifacts = {boundary.boundary_inputs[0].occurrences[0]: artifact}
    admission = publish_admission(
        attempt,
        plan.recipe,
        boundary,
        artifacts,
        step_number=1,
        delta_root=committer.delta_root,
        files_root=committer.files_root,
        fs=committer.fs,
    )
    committer.journal.admit(attempt, admission_digest=admission.record_digest)
    with Executor() as executor:
        result = execute_compiled_plan(
            plan,
            {"source": [artifact]},
            executor,
            invocation_id=identity.assignment_id,
            execute_adapter=lambda *_: LocalExecuteRouter(),
            input_attribution=boundary.input_attribution,
        )
        assert close_command_scope()
        receipt = stage_retained_result(
            str(tmp_path / "staging"),
            SuccessfulDelivery(
                delivery=DeliveryIdentity.new(attempt),
                computation_complete=True,
                cleanup_complete=True,
            ),
            result.projection,
            result.retained_artifacts,
            fs=committer.fs,
        )
    committer.accept(receipt)
    return RetainedReader(committer), receipt, admission


def _registered(*definitions: Definition) -> dict[tuple[str, str], Definition]:
    return {
        (definition.__module__, definition.__qualname__): definition
        for definition in definitions
    }


def test_frozen_replay_runs_complete_recipe_with_new_diagnostic_owner(
    accepted: AcceptedFixture,
) -> None:
    reader, receipt, admission = accepted
    configuration = capture_replay_configuration(receipt, {}, admission=admission)
    publish_replay_configuration(
        configuration, delta_root=reader.committer.delta_root, fs=reader.committer.fs
    )
    replay = prepare_retained_replay(
        reader.committer,
        receipt.delivery.delivery_id,
        registered_definitions=_registered(TwoIncrements, Increment),
        runtime_replacements={},
    )
    assert replay.identity != receipt.delivery.attempt.assignment
    assert replay.delivery.delivery_id == receipt.delivery.delivery_id
    assert replay.plan.recipe.digest == receipt.delivery.attempt.recipe_digest
    diagnostic = replay.validate_selection(reader.committer, "b" * 64)
    assert diagnostic.source_admission_digest == admission.record_digest
    roles = replay.boundary.input_roles()
    inputs = {
        role: [replay.artifacts[address] for address in addresses]
        for role, addresses in roles.items()
    }
    with Executor() as executor:
        result = execute_compiled_plan(
            replay.plan,
            inputs,
            executor,
            invocation_id=replay.identity.assignment_id,
            execute_adapter=lambda *_: LocalExecuteRouter(),
            frozen_context=replay.boundary.frozen_context,
            input_attribution=replay.boundary.input_attribution,
            expected_input_context_digest=replay.boundary.input_context_digest,
        )
    assert result.result.artifacts["metrics"][0].values == {"value": 5}


def test_replay_requires_exact_runtime_capture_and_never_persists_values(
    accepted: AcceptedFixture,
) -> None:
    reader, receipt, admission = accepted
    configuration = capture_replay_configuration(
        receipt, {"TOKEN": "synthetic-private"}, admission=admission
    )
    publish_replay_configuration(
        configuration, delta_root=reader.committer.delta_root, fs=reader.committer.fs
    )
    assert "synthetic-private" not in configuration.model_dump_json()
    assert (
        read_replay_configuration(
            receipt,
            admission=admission,
            delta_root=reader.committer.delta_root,
            fs=reader.committer.fs,
        )
        == configuration
    )
    with pytest.raises(ArtisanError, match="replacement"):
        prepare_retained_replay(
            reader.committer,
            receipt.delivery.delivery_id,
            registered_definitions=_registered(TwoIncrements, Increment),
            runtime_replacements={},
        )
    replay = prepare_retained_replay(
        reader.committer,
        receipt.delivery.delivery_id,
        registered_definitions=_registered(TwoIncrements, Increment),
        runtime_replacements={"TOKEN": "replacement"},
    )
    assert replay.runtime_env == {"TOKEN": "replacement"}


@pytest.mark.parametrize("failed", [False, True])
def test_runtime_capture_is_bound_to_the_exact_admission_and_optional_seal(
    accepted: AcceptedFixture, failed: bool
) -> None:
    reader, receipt, admission = accepted
    source = DeliveryIdentity.new(admission.attempt) if failed else receipt
    configuration = capture_replay_configuration(source, {}, admission=admission)
    publish_replay_configuration(
        configuration, delta_root=reader.committer.delta_root, fs=reader.committer.fs
    )
    assert configuration.seal_digest == (None if failed else receipt.seal_digest)
    wrong_record = admission.model_copy(update={"record_digest": "0" * 32})
    with pytest.raises(StoreIntegrityError, match="source owner"):
        read_replay_configuration(
            source,
            admission=wrong_record,
            delta_root=reader.committer.delta_root,
            fs=reader.committer.fs,
        )
    wrong_attempt = admission.model_copy(
        update={"attempt": admission.attempt.model_copy(update={"attempt": 1})}
    )
    with pytest.raises(ValueError, match="exact admitted"):
        capture_replay_configuration(source, {}, admission=wrong_attempt)
    if failed:
        assert isinstance(source, DeliveryIdentity)
        invented = SealedDelivery(
            delivery=source, seal_digest="a" * 32, seal_uri="memory://not-a-result"
        )
        with pytest.raises(StoreIntegrityError, match="source owner"):
            read_replay_configuration(
                invented,
                admission=admission,
                delta_root=reader.committer.delta_root,
                fs=reader.committer.fs,
            )
    else:
        corrupted = configuration.model_copy(update={"admission_digest": "0" * 32})
        path = f"{reader.committer.delta_root}/_artisan/replay/{receipt.delivery.delivery_id}/configuration.json"
        reader.committer.fs.pipe_file(path, corrupted.model_dump_json().encode())
        with pytest.raises(StoreIntegrityError, match="source owner"):
            prepare_retained_replay(
                reader.committer,
                receipt.delivery.delivery_id,
                registered_definitions=_registered(TwoIncrements, Increment),
                runtime_replacements={},
            )


def test_missing_configuration_is_unavailable_not_empty(
    accepted: AcceptedFixture,
) -> None:
    reader, receipt, admission = accepted
    configuration = read_replay_configuration(
        receipt,
        admission=admission,
        delta_root=reader.committer.delta_root,
        fs=reader.committer.fs,
    )
    assert configuration.status == "unavailable"
    with pytest.raises(ArtisanError, match="unavailable"):
        prepare_retained_replay(
            reader.committer,
            receipt.delivery.delivery_id,
            registered_definitions=_registered(TwoIncrements, Increment),
            runtime_replacements={},
        )


@pytest.mark.parametrize("definitions", [(), (TwoIncrements,)])
def test_root_and_nested_definitions_require_explicit_registration(
    accepted: AcceptedFixture,
    monkeypatch: pytest.MonkeyPatch,
    definitions: tuple[Definition, ...],
) -> None:
    reader, receipt, admission = accepted
    publish_replay_configuration(
        capture_replay_configuration(receipt, {}, admission=admission),
        delta_root=reader.committer.delta_root,
        fs=reader.committer.fs,
    )
    imported = []
    original = importlib.import_module

    def record(name: str, package: str | None = None) -> ModuleType:
        imported.append(name)
        return original(name, package)

    monkeypatch.setattr(importlib, "import_module", record)
    with pytest.raises(ArtisanError, match="not registered"):
        prepare_retained_replay(
            reader.committer,
            receipt.delivery.delivery_id,
            registered_definitions=_registered(*definitions),
            runtime_replacements={},
        )
    assert "fixtures.replay_operations" not in imported


def test_changed_software_requires_explicit_diagnostic_selection(
    accepted: AcceptedFixture,
) -> None:
    reader, receipt, admission = accepted
    publish_replay_configuration(
        capture_replay_configuration(receipt, {}, admission=admission),
        delta_root=reader.committer.delta_root,
        fs=reader.committer.fs,
    )
    options = {
        "registered_definitions": _registered(TwoIncrements, Increment),
        "runtime_replacements": {},
    }
    strict = prepare_retained_replay(
        reader.committer, receipt.delivery.delivery_id, **options
    )
    with pytest.raises(ArtisanError, match="changed"):
        strict.validate_selection(reader.committer, "c" * 64)
    changed = prepare_retained_replay(
        reader.committer,
        receipt.delivery.delivery_id,
        allow_code_change=True,
        **options,
    )
    diagnostic = changed.validate_selection(reader.committer, "c" * 64)
    assert diagnostic.source_delivery.attempt.software_digest == "b" * 64
    assert diagnostic.selected_software_digest == "c" * 64


def test_strict_parameter_types_restore_in_json_mode() -> None:
    from artisan.orchestration.retained_replay import _registered_plan

    original = compile_definition(Increment, params=Increment.Params(increments=(2, 3)))
    restored = _registered_plan(original.recipe, _registered(Increment))
    assert restored.recipe == original.recipe
    assert restored.instantiate().params.increments == (2, 3)


def test_changed_nested_code_requires_explicit_selection(
    accepted: AcceptedFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    reader, receipt, admission = accepted
    monkeypatch.setattr(Increment, "version", "2.0.0")
    configuration = capture_replay_configuration(receipt, {}, admission=admission)
    publish_replay_configuration(
        configuration, delta_root=reader.committer.delta_root, fs=reader.committer.fs
    )
    options = {
        "registered_definitions": _registered(TwoIncrements, Increment),
        "runtime_replacements": {},
    }
    with pytest.raises(ArtisanError, match="changed"):
        prepare_retained_replay(
            reader.committer, receipt.delivery.delivery_id, **options
        )
    replay = prepare_retained_replay(
        reader.committer,
        receipt.delivery.delivery_id,
        allow_code_change=True,
        **options,
    )
    assert replay.plan.recipe.digest != receipt.delivery.attempt.recipe_digest
    diagnostic = replay.validate_selection(reader.committer, "c" * 64)
    assert diagnostic.selected_recipe_digest == replay.plan.recipe.digest
    assert (
        diagnostic.source_delivery.attempt.recipe_digest
        == receipt.delivery.attempt.recipe_digest
    )


def test_replay_restores_root_batch_and_grouping_without_replacing_nested_defaults(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan = compile_definition(
        TwoIncrements,
        group_by=GroupByStrategy.ZIP,
        batch_strategy={"artifacts_per_unit": 4, "max_artifacts_per_unit": None},
    )
    reader, receipt, admission = _accept_plan(tmp_path, plan)
    configuration = capture_replay_configuration(receipt, {}, admission=admission)
    publish_replay_configuration(
        configuration, delta_root=reader.committer.delta_root, fs=reader.committer.fs
    )
    options = {
        "registered_definitions": _registered(TwoIncrements, Increment),
        "runtime_replacements": {},
    }
    replay = prepare_retained_replay(
        reader.committer, receipt.delivery.delivery_id, **options
    )
    assert replay.plan.recipe == plan.recipe
    assert replay.plan.recipe.policy.artifacts_per_unit == 4
    assert replay.plan.calls[0].plan.recipe.policy.artifacts_per_unit == 1
    defaults = Increment.model_fields["batch_strategy"].default
    monkeypatch.setattr(defaults, "artifacts_per_unit", 2)
    with pytest.raises(ArtisanError, match="changed"):
        prepare_retained_replay(
            reader.committer, receipt.delivery.delivery_id, **options
        )
    changed = prepare_retained_replay(
        reader.committer,
        receipt.delivery.delivery_id,
        allow_code_change=True,
        **options,
    )
    assert changed.plan.recipe.policy.artifacts_per_unit == 4
    assert changed.plan.calls[0].plan.recipe.policy.artifacts_per_unit == 2


@pytest.mark.parametrize("changed_field", ["per_artifact_dispatch", "execute_as_tool"])
def test_changed_root_execution_requires_explicit_replay_selection(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    changed_field: str,
) -> None:
    plan = compile_definition(Increment, batch_strategy={"artifacts_per_unit": 3})
    reader, receipt, admission = _accept_plan(tmp_path, plan)
    monkeypatch.setattr(Increment, changed_field, changed_field == "execute_as_tool")
    options = {
        "registered_definitions": _registered(Increment),
        "runtime_replacements": {},
    }
    configuration = capture_replay_configuration(receipt, {}, admission=admission)
    publish_replay_configuration(
        configuration, delta_root=reader.committer.delta_root, fs=reader.committer.fs
    )
    with pytest.raises(ArtisanError, match="changed"):
        prepare_retained_replay(
            reader.committer, receipt.delivery.delivery_id, **options
        )
    replay = prepare_retained_replay(
        reader.committer,
        receipt.delivery.delivery_id,
        allow_code_change=True,
        **options,
    )
    if changed_field == "per_artifact_dispatch":
        assert not replay.plan.recipe.policy.per_artifact_dispatch
    else:
        assert replay.plan.recipe.execution.mode == "python_adapter"
    assert replay.plan.recipe.policy.artifacts_per_unit == 3
    replay.plan.instantiate()


def test_diagnostic_recovery_preserves_diagnostic_owner_and_cache_exclusion(
    accepted: AcceptedFixture, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    reader, source, admission = accepted
    committer = reader.committer
    publish_replay_configuration(
        capture_replay_configuration(source, {}, admission=admission),
        delta_root=committer.delta_root,
        fs=committer.fs,
    )
    replay = prepare_retained_replay(
        reader.committer,
        source.delivery.delivery_id,
        registered_definitions=_registered(TwoIncrements, Increment),
        runtime_replacements={},
    )
    diagnostic = replay.validate_selection(committer, "b" * 64)
    attempt = AssignmentAttempt(
        assignment=replay.identity,
        attempt=0,
        generation=0,
        recipe_digest=replay.plan.recipe.digest,
        software_digest="b" * 64,
        input_context_digest=replay.boundary.input_context_digest,
        replay_of_execution_run_id=replay.delivery.delivery_id,
    )
    reference = publish_admission(
        attempt,
        replay.plan.recipe,
        replay.boundary,
        replay.artifacts,
        step_number=2,
        delta_root=committer.delta_root,
        files_root=committer.files_root,
        fs=committer.fs,
    )
    committer.journal.admit(attempt, admission_digest=reference.record_digest)
    inputs = {
        role: [replay.artifacts[address] for address in addresses]
        for role, addresses in replay.boundary.input_roles().items()
    }
    with Executor() as executor:
        result = execute_compiled_plan(
            replay.plan,
            inputs,
            executor,
            invocation_id=replay.identity.assignment_id,
            execute_adapter=lambda *_: LocalExecuteRouter(),
            input_attribution=replay.boundary.input_attribution,
            frozen_context=replay.boundary.frozen_context,
        )
        assert close_command_scope()
        receipt = stage_retained_result(
            str(tmp_path / "diagnostic"),
            SuccessfulDelivery(
                delivery=DeliveryIdentity.new(attempt),
                computation_complete=True,
                cleanup_complete=True,
            ),
            result.projection,
            result.retained_artifacts,
            fs=committer.fs,
        )

    def stop(boundary: str, _receipt: SealedDelivery, _table: str | None) -> None:
        if boundary == "claimed":
            msg = "diagnostic publication interrupted"
            raise RuntimeError(msg)

    with monkeypatch.context() as patch:
        patch.setattr(committer, "_checkpoint", stop)
        with pytest.raises(RuntimeError, match="interrupted"):
            committer.accept(receipt)
    committer.recover(receipt.delivery.attempt.assignment)
    from artisan.execution.recording.retained_replay import (
        publish_replay_diagnostic,
        read_replay_diagnostic,
    )
    from artisan.storage._retained_publication import export_retained_publication

    publish_replay_diagnostic(
        receipt, diagnostic, delta_root=committer.delta_root, fs=committer.fs
    )
    assert (
        read_replay_diagnostic(
            receipt, delta_root=committer.delta_root, fs=committer.fs
        )
        == diagnostic
    )
    inspection = export_retained_publication(reader, tmp_path / "inspection", redact=[])
    copied = next(
        item
        for item in inspection.metadata().sources
        if item.delivery == receipt.delivery
    )
    assert copied.replay_diagnostic == diagnostic
    assert (
        reader.read(receipt).receipt.delivery.attempt.replay_of_execution_run_id
        == source.delivery.delivery_id
    )
    assert [plan.receipt for plan in reader.completed_plans()] == [source]
    assert len(reader.completed_plans(ordinary=False)) == 2


def test_replay_rebinds_semantic_references_separately_from_empty_association_capture() -> (
    None
):
    from artisan.orchestration.retained_replay import _rebind_inputs
    from artisan.schemas.artifact.execution_config import ExecutionConfigArtifact
    from artisan.schemas.provenance.occurrence import OccurrenceAddress
    from artisan.schemas.provenance.projection import (
        BoundaryInputAttribution,
        BoundaryProjection,
        FrozenAssociation,
        FrozenBoundaryContext,
        FrozenReferences,
        ProjectedOccurrence,
        RoleOccurrences,
    )

    structure = MetricArtifact.draft({"value": 3}, "source.json").finalize()
    configuration = ExecutionConfigArtifact.draft(
        {"input": {"$artifact": structure.artifact_id}}, "config.json"
    ).finalize()
    source = OccurrenceAddress(invocation_id="original", role="config", index=0)
    member = OccurrenceAddress(
        invocation_id="original/context", role="retained", index=0
    )
    artifacts = {source: configuration, member: structure}
    boundary = BoundaryProjection(
        boundary_inputs=(RoleOccurrences(role="config", occurrences=(source,)),),
        input_attribution=tuple(
            BoundaryInputAttribution(boundary=address, producer=None)
            for address in artifacts
        ),
        occurrences=tuple(
            ProjectedOccurrence(
                address=address,
                artifact_id=artifact.artifact_id,
                artifact_type=artifact.artifact_type,
            )
            for address, artifact in artifacts.items()
        ),
        exports=(),
        frozen_context=FrozenBoundaryContext(
            associations=(
                FrozenAssociation(primary=source, artifact_type="metric", members=()),
            ),
            references=(
                FrozenReferences(
                    source=source, members=(member,), direct_parents=(member,)
                ),
            ),
        ),
    )
    rebound, recovered = _rebind_inputs(boundary, artifacts, "diagnostic")
    new_source = rebound.input_roles()["config"][0]
    reference = rebound.frozen_context.reference_capture(new_source)
    assert new_source.invocation_id == "diagnostic"
    assert reference.members == reference.direct_parents
    assert recovered[reference.members[0]] is structure
    assert reference.members[0] != member
    assert rebound.frozen_context.associated(new_source, "metric") == ()
    assert rebound.input_context_digest != boundary.input_context_digest
