"""Invocation-owned input references and frozen dependency membership."""

from __future__ import annotations

import pytest

from artisan.errors import ArtifactIntegrityError
from artisan.schemas import ArtifactResult, ExecutionConfigArtifact, MetricArtifact
from artisan.schemas.provenance.occurrence import InputReference
from artisan.schemas.specs.input_models import (
    FrozenInputContext,
    PostprocessInput,
    PreprocessInput,
)


def metric(value: int) -> MetricArtifact:
    return MetricArtifact.draft({"value": value}, "value.json", 1).finalize()


@pytest.mark.parametrize("phase", [PreprocessInput, PostprocessInput])
def test_equal_content_inputs_keep_exact_context(phase: type) -> None:
    primary, first, second = metric(1), metric(2), metric(3)
    context = FrozenInputContext(
        "call",
        {"data": [primary, primary]},
        associations={
            ("data", 0, "metric"): [first],
            ("data", 1, "metric"): [second],
        },
    )
    kwargs = (
        {"preprocess_dir": "/tmp"}
        if phase is PreprocessInput
        else {"postprocess_dir": "/tmp", "step_number": None}
    )
    inputs = phase(**kwargs, _context=context)
    left, right = inputs.ref("data", index=0), inputs.ref("data", index=1)
    assert left != right
    assert left.artifact_id == right.artifact_id == primary.artifact_id
    assert inputs.associated_artifacts(left, "metric") == [first]
    assert inputs.associated_artifacts(right, "metric") == [second]
    with pytest.raises(ValueError, match="ambiguous"):
        inputs.associated_artifacts(primary, "metric")


def test_context_snapshots_membership_and_distinguishes_missing() -> None:
    primary, member = metric(1), metric(2)
    values = [primary]
    members = [member]
    context = FrozenInputContext(
        "call",
        {"data": values},
        associations={
            ("data", 0, "metric"): members,
            ("data", 0, "config"): [],
        },
    )
    values.clear()
    members.clear()
    assert context.associated_artifacts(primary, "metric") == (member,)
    assert context.associated_artifacts(primary, "config") == ()
    with pytest.raises(ValueError, match="not captured"):
        context.associated_artifacts(primary, "file_ref")
    with pytest.raises(TypeError):
        context.artifacts["data"] = ()
    with pytest.raises(TypeError):
        context.associations[("data", 0, "metric")] = ()


def test_context_rejects_foreign_owner_and_identity() -> None:
    context = FrozenInputContext("call", {"data": [metric(1)]})
    reference = context.ref("data", index=0)
    for changes in ({"invocation_id": "other"}, {"artifact_id": "a" * 32}):
        wrong = InputReference(**(reference.model_dump() | changes))
        with pytest.raises(ValueError, match="owner or expected identity"):
            context.artifact(wrong)
    for index in (-1, 1, True):
        with pytest.raises(ValueError, match="position does not exist"):
            context.ref("data", index=index)


def test_configuration_context_is_exact_and_explicit() -> None:
    referent = metric(7)
    config = ExecutionConfigArtifact.draft(
        {"source": {"$artifact": referent.artifact_id}}, "config.json", 1
    ).finalize()
    captured = {referent.artifact_id: referent}
    context = FrozenInputContext(
        "call", {"config": [config]}, references={("config", 0): captured}
    )
    captured.clear()
    assert context.referenced_artifacts(config) == {referent.artifact_id: referent}
    with pytest.raises(TypeError):
        context.referenced_artifacts(config)[referent.artifact_id] = referent
    with pytest.raises(ValueError, match="exactly match"):
        FrozenInputContext("call", {"config": [config]}, references={("config", 0): {}})
    with pytest.raises(ValueError, match="resolved references"):
        FrozenInputContext("call", {"config": [config]}).referenced_artifacts(config)
    empty = ExecutionConfigArtifact.draft({}, "empty.json", 1).finalize()
    assert (
        FrozenInputContext(
            "call", {"config": [empty]}, references={("config", 0): {}}
        ).referenced_artifacts(empty)
        == {}
    )


def test_context_preserves_finalized_identity_and_optional_origin() -> None:
    artifact = metric(3)
    artifact_id = artifact.artifact_id
    context = FrozenInputContext("call", {"data": [artifact]})
    inputs = PostprocessInput(None, "/tmp", _context=context)
    assert inputs.step_number is None
    assert inputs.ref("data", index=0).artifact_id == artifact_id
    assert artifact.origin_step_number == 1
    artifact.metadata["mutated"] = True
    with pytest.raises(ArtifactIntegrityError):
        inputs.ref("data", index=0)


def test_context_rejects_drafts_and_inconsistent_phase_inputs() -> None:
    draft = MetricArtifact.draft({"value": 1}, "value.json", 1)
    with pytest.raises(ValueError, match="finalized"):
        FrozenInputContext("call", {"data": [draft]})
    context = FrozenInputContext("call", {"data": [metric(1)]})
    with pytest.raises(ValueError, match="do not match"):
        PreprocessInput("/tmp", {"data": [metric(2)]}, _context=context)
    with pytest.raises(ValueError, match="invocation context"):
        PreprocessInput("/tmp").ref("data", index=0)


def test_result_helper_preserves_exact_references_atomically() -> None:
    source = metric(1)
    inputs = PreprocessInput(
        "/tmp", _context=FrozenInputContext("call", {"data": [source]})
    )
    result = ArtifactResult()
    result.add_artifact(
        "metrics", metric(2), sources={"data": [inputs.ref("data", index=0)]}
    )
    mapping = result.lineage["metrics"][0]
    assert (mapping.source_input_index, mapping.source_invocation) == (0, "call")
    assert mapping.source_artifact_id == source.artifact_id
    before = result.model_dump()
    with pytest.raises(ValueError, match="source role"):
        result.add_artifact(
            "metrics", metric(3), sources={"wrong": [inputs.ref("data", index=0)]}
        )
    with pytest.raises(ValueError, match="Cannot mix"):
        result.add_artifact(
            "metrics", metric(3), sources={"data": [inputs.ref("data", index=0), 0]}
        )
    assert result.model_dump() == before


def test_exact_and_id_references_share_the_input_namespace() -> None:
    source, other = metric(1), metric(2)
    context = FrozenInputContext("call", {"data": [source, other]})
    result = ArtifactResult()
    result.add_artifact(
        "metrics",
        metric(3),
        sources={"data": [context.ref("data", index=0), other.artifact_id]},
    )
    assert [mapping.source_artifact_id for mapping in result.lineage["metrics"]] == [
        source.artifact_id,
        other.artifact_id,
    ]


def test_context_rejects_mutated_captured_member_on_read() -> None:
    primary, member = metric(1), metric(2)
    context = FrozenInputContext(
        "call",
        {"data": [primary]},
        associations={("data", 0, "metric"): [member]},
    )
    member.metadata["changed"] = True
    with pytest.raises(ArtifactIntegrityError):
        context.associated_artifacts(primary, "metric")
