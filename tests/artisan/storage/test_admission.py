"""Required frozen input evidence survives loss of controller and worker scratch."""

from __future__ import annotations

import json
import posixpath
import shutil
from pathlib import Path
from unittest.mock import patch

import pytest
from fixtures.retained_results import RetainedCase, retained_case
from fsspec import AbstractFileSystem

from artisan.errors import StoreIntegrityError
from artisan.execution.lineage.graph import OccurrenceGraph
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.execution_config import ExecutionConfigArtifact
from artisan.schemas.artifact.metric import MetricArtifact
from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.schemas.composites.plan import ExecutionRecipe
from artisan.schemas.execution.admission import AdmissionReference
from artisan.schemas.execution.assignment import AssignmentAttempt, AssignmentIdentity
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.schemas.provenance.occurrence import (
    OccurrenceAddress,
    ReferenceContext,
    RequiredReference,
)
from artisan.schemas.provenance.projection import (
    AcceptedOccurrence,
    AcceptedProducer,
    BoundaryInputAttribution,
    BoundaryProjection,
    FrozenBoundaryContext,
    FrozenReferences,
    ProjectedOccurrence,
)
from artisan.storage.core.store_format import initialize_retained_store
from artisan.storage.io.admission import (
    admission_directory,
    capture_admission,
    publish_admission,
)
from artisan.storage.io.assignment_journal import AssignmentJournal
from artisan.utils.hashing import canonical_json_bytes

type Backend = tuple[AbstractFileSystem, StorageConfig, str]
type BoundaryArtifacts = dict[OccurrenceAddress, Artifact]


def _inputs(
    scratch: Path, kind: str
) -> tuple[ExecutionRecipe, BoundaryProjection, BoundaryArtifacts]:
    case = retained_case(scratch, kind=kind)
    if kind in {"empty", "alias"}:
        return (
            case.recipe,
            case.projection.model_copy(update={"exports": ()}),
            case.boundary,
        )
    graph = OccurrenceGraph()
    config = case.artifacts[case.projection.exports[0].occurrences[0]]
    primary = graph.add_input("config", config)
    members = []
    for index, address in enumerate(case.projection.support):
        member = OccurrenceAddress(
            invocation_id="frozen-context", role="files", index=index
        )
        graph.add_context_input(member, case.artifacts[address])
        members.append(member)
    projection = graph.project(
        {},
        {},
        frozen_context=FrozenBoundaryContext(
            references=(FrozenReferences(source=primary, members=tuple(members)),),
        ),
    )
    recipe = ExecutionRecipe.model_validate(
        case.recipe.model_copy(
            update={
                "inputs": (
                    case.recipe.inputs[0].model_copy(
                        update={
                            "role": "config",
                            "artifact_type": "config",
                            "resolve_references": True,
                        }
                    ),
                ),
            }
        ).model_dump()
    )
    return (
        recipe,
        projection,
        {
            address: graph.nodes[address].artifact
            for address in projection.boundary_addresses()
        },
    )


def _prepare(
    backend_fs: Backend, scratch: Path, *, kind: str = "file"
) -> tuple[
    AdmissionReference, str, str, ExecutionRecipe, BoundaryProjection, BoundaryArtifacts
]:
    fs, _, root = backend_fs
    delta_root, files_root = f"{root}/candidate", f"{root}/managed"
    initialize_retained_store(delta_root, fs)
    recipe, boundary, artifacts = _inputs(scratch, kind)
    attempt = AssignmentAttempt(
        assignment=AssignmentIdentity(step_run_id="a" * 32, unit_index=0),
        attempt=0,
        generation=0,
        recipe_digest=recipe.digest,
        software_digest="c" * 64,
        input_context_digest=boundary.input_context_digest,
    )
    reference = publish_admission(
        attempt,
        recipe,
        boundary,
        artifacts,
        step_number=7,
        delta_root=delta_root,
        files_root=files_root,
        fs=fs,
    )
    return reference, delta_root, files_root, recipe, boundary, artifacts


@pytest.mark.parametrize("kind", ["file", "appendable", "alias", "empty"])
def test_admission_survives_original_scratch_loss_and_reuses_managed_carriers(
    backend_fs: Backend,
    tmp_path: Path,
    kind: str,
) -> None:
    fs, storage, _ = backend_fs
    scratch = tmp_path / "inputs"
    reference, root, files_root, recipe, boundary, original = _prepare(
        backend_fs, scratch, kind=kind
    )
    rows = {address: artifact.to_row() for address, artifact in original.items()}
    locators = {
        address: getattr(artifact, "path", None)
        for address, artifact in original.items()
    }
    journal = AssignmentJournal(
        root, fs=fs, storage_options=storage.delta_storage_options()
    )
    journal.initialize()
    journal.admit(reference.attempt, admission_digest=reference.record_digest)
    if scratch.exists():
        shutil.rmtree(scratch)
    state = journal.state(reference.attempt.assignment)
    recovered = AdmissionReference(
        attempt=state.attempt, record_digest=state.admission_digest
    )
    with capture_admission(
        recovered, delta_root=root, files_root=files_root, fs=fs
    ) as captured:
        assert captured.record.step_number == 7
        assert captured.record.boundary == boundary
        assert {
            address: artifact.to_row()
            for address, artifact in captured.artifacts.items()
        } == rows
        assert len(captured.record.carriers) == (
            1 if kind in {"file", "appendable"} else 0
        )
        for artifact in captured.artifacts.values():
            artifact.verify_external_content(fs=fs)
        next_attempt = reference.attempt.model_copy(
            update={"attempt": 1, "generation": 1}
        )
        with patch(
            "artisan.storage.io.admission.publish_immutable_file",
            side_effect=AssertionError("Managed carrier was copied again"),
        ):
            repeated = publish_admission(
                next_attempt,
                recipe,
                boundary,
                captured.artifacts,
                step_number=7,
                delta_root=root,
                files_root=files_root,
                fs=fs,
            )
        assert repeated.attempt == next_attempt
    assert {
        address: getattr(artifact, "path", None)
        for address, artifact in original.items()
    } == locators


@pytest.mark.parametrize("change", ["record", "table", "carrier"])
def test_admission_rejects_changed_required_evidence(
    backend_fs: Backend, tmp_path: Path, change: str
) -> None:
    fs, _, _ = backend_fs
    reference, root, files_root, _, _, _ = _prepare(backend_fs, tmp_path / "inputs")
    directory = admission_directory(root, reference.attempt)
    with capture_admission(
        reference, delta_root=root, files_root=files_root, fs=fs
    ) as captured:
        if change == "record":
            path = f"{directory}/admission.json"
            data = captured.record.model_dump(mode="json")
            data["step_number"] = 9
            encoded = canonical_json_bytes(data)
        elif change == "table":
            path = f"{directory}/{captured.record.tables[0].filename}"
            encoded = b"changed parquet"
        else:
            path = f"{files_root}/{captured.record.carriers[0].relative_path}"
            encoded = b"changed carrier"
    with fs.open(path, "wb") as stream:
        stream.write(encoded)
    with (
        pytest.raises(StoreIntegrityError),
        capture_admission(reference, delta_root=root, files_root=files_root, fs=fs),
    ):
        pytest.fail("Changed required input evidence was accepted")


def test_admission_requires_exact_accepted_producer_verification(
    backend_fs: Backend, tmp_path: Path
) -> None:
    fs, _, root = backend_fs
    delta_root, files_root = f"{root}/candidate", f"{root}/managed"
    initialize_retained_store(delta_root, fs)
    recipe, boundary, artifacts = _inputs(tmp_path, "alias")
    address = boundary.boundary_inputs[0].occurrences[0]
    producer = AcceptedOccurrence(
        delivery_id="d" * 32,
        seal_digest="e" * 32,
        address=OccurrenceAddress(invocation_id="prior", role="data", index=0),
    )
    boundary = boundary.model_copy(
        update={
            "input_attribution": (
                BoundaryInputAttribution(boundary=address, producer=producer),
            )
        }
    )
    attempt = AssignmentAttempt(
        assignment=AssignmentIdentity(step_run_id="a" * 32, unit_index=0),
        attempt=0,
        generation=0,
        recipe_digest=recipe.digest,
        software_digest="c" * 64,
        input_context_digest=boundary.input_context_digest,
    )
    arguments = {
        "step_number": 7,
        "delta_root": delta_root,
        "files_root": files_root,
        "fs": fs,
    }
    with pytest.raises(StoreIntegrityError, match="accepted-winner"):
        publish_admission(attempt, recipe, boundary, artifacts, **arguments)
    checked = []

    def verify(actual: AcceptedProducer, expected: ProjectedOccurrence) -> None:
        assert actual == producer
        assert expected.artifact_id == artifacts[address].artifact_id
        assert expected.address == address
        checked.append(actual)

    reference = publish_admission(
        attempt, recipe, boundary, artifacts, **arguments, verify_producer=verify
    )
    with (
        pytest.raises(StoreIntegrityError, match="accepted-winner"),
        capture_admission(
            reference, delta_root=delta_root, files_root=files_root, fs=fs
        ),
    ):
        pytest.fail("Producer claim was silently trusted")
    with capture_admission(
        reference,
        delta_root=delta_root,
        files_root=files_root,
        fs=fs,
        verify_producer=verify,
    ):
        pass
    assert checked == [producer, producer]


def test_admission_rejects_self_consistent_record_from_another_attempt(
    backend_fs: Backend, tmp_path: Path
) -> None:
    fs, _, _ = backend_fs
    reference, root, files_root, _, _, _ = _prepare(backend_fs, tmp_path, kind="empty")
    changed = reference.model_copy(
        update={"attempt": reference.attempt.model_copy(update={"generation": 1})}
    )
    source = f"{admission_directory(root, reference.attempt)}/admission.json"
    target = f"{admission_directory(root, changed.attempt)}/admission.json"
    fs.makedirs(posixpath.dirname(target), exist_ok=True)
    with fs.open(source, "rb") as stream:
        data = stream.read()
    assert json.loads(data)["step_number"] == 7
    with fs.open(target, "wb") as stream:
        stream.write(data)
    with (
        pytest.raises(StoreIntegrityError, match="ownership"),
        capture_admission(changed, delta_root=root, files_root=files_root, fs=fs),
    ):
        pytest.fail("Another attempt's immutable record was accepted")


@pytest.mark.parametrize(("materialize", "resolve"), [(True, False), (False, True)])
def test_admission_requires_explicit_empty_config_reference_capture(
    backend_fs: Backend,
    tmp_path: Path,
    materialize: bool,
    resolve: bool,
) -> None:
    fs, _, root = backend_fs
    delta_root, files_root = f"{root}/candidate", f"{root}/managed"
    initialize_retained_store(delta_root, fs)
    recipe = retained_case(tmp_path, kind="alias").recipe
    recipe = recipe.model_copy(
        update={
            "inputs": (
                recipe.inputs[0].model_copy(
                    update={
                        "role": "config",
                        "artifact_type": "config",
                        "materialize": materialize,
                        "resolve_references": resolve,
                    }
                ),
            )
        }
    )
    graph = OccurrenceGraph()
    config = ExecutionConfigArtifact.draft({}, "empty.json", 3).finalize()
    address = graph.add_input("config", config)
    captured = graph.project(
        {},
        {},
        frozen_context=FrozenBoundaryContext(
            references=(FrozenReferences(source=address, members=()),),
        ),
    )
    missing = captured.model_copy(update={"frozen_context": FrozenBoundaryContext()})
    attempt = AssignmentAttempt(
        assignment=AssignmentIdentity(step_run_id="a" * 32, unit_index=0),
        attempt=0,
        generation=0,
        recipe_digest=recipe.digest,
        software_digest="c" * 64,
        input_context_digest=missing.input_context_digest,
    )
    with pytest.raises(ValueError, match="not captured"):
        publish_admission(
            attempt,
            recipe,
            missing,
            {address: config},
            step_number=7,
            delta_root=delta_root,
            files_root=files_root,
            fs=fs,
        )
    attempt = attempt.model_copy(
        update={"input_context_digest": captured.input_context_digest}
    )
    reference = publish_admission(
        attempt,
        recipe,
        captured,
        {address: config},
        step_number=7,
        delta_root=delta_root,
        files_root=files_root,
        fs=fs,
    )
    with capture_admission(
        reference, delta_root=delta_root, files_root=files_root, fs=fs
    ) as admission:
        assert admission.record.boundary.frozen_context.referenced(address) == ()


def test_nonconfig_required_referents_roundtrip_without_association_query(
    backend_fs: Backend, monkeypatch: pytest.MonkeyPatch
) -> None:
    def policy(
        _artifact: Artifact, context: ReferenceContext
    ) -> tuple[RequiredReference, ...]:
        return tuple(
            RequiredReference(
                address=parent,
                artifact_id=context.artifacts[parent].artifact_id,
                artifact_type=context.artifacts[parent].artifact_type,
            )
            for parent in context.direct_parents
        )

    monkeypatch.setattr(
        ArtifactTypeDef.get("metric"), "required_references", staticmethod(policy)
    )
    graph = OccurrenceGraph()
    primary_artifact = MetricArtifact.draft({"score": 1}, "metric.json", 2).finalize()
    target_artifact = ExecutionConfigArtifact.draft({}, "config.json", 1).finalize()
    primary = graph.add_input("metric", primary_artifact)
    target = graph.add_context_input(
        OccurrenceAddress(invocation_id="upstream", role="config", index=0),
        target_artifact,
    )
    context = FrozenBoundaryContext(
        references=(
            FrozenReferences(
                source=primary, members=(target,), direct_parents=(target,)
            ),
            FrozenReferences(source=target, members=()),
        )
    )
    boundary = graph.project({}, {}, frozen_context=context)
    artifacts = {primary: primary_artifact, target: target_artifact}
    recipe = RetainedCase(boundary, artifacts, {}).recipe
    attempt = AssignmentAttempt(
        assignment=AssignmentIdentity(step_run_id="a" * 32, unit_index=0),
        attempt=0,
        generation=0,
        recipe_digest=recipe.digest,
        software_digest="c" * 64,
        input_context_digest=boundary.input_context_digest,
    )
    fs, _, root = backend_fs
    delta_root, files_root = f"{root}/candidate", f"{root}/managed"
    initialize_retained_store(delta_root, fs)
    reference = publish_admission(
        attempt,
        recipe,
        boundary,
        artifacts,
        step_number=3,
        delta_root=delta_root,
        files_root=files_root,
        fs=fs,
    )
    with capture_admission(
        reference, delta_root=delta_root, files_root=files_root, fs=fs
    ) as admission:
        assert admission.record.boundary.frozen_context == context
        assert set(admission.artifacts) == {primary, target}
        assert not admission.record.boundary.associations
        assert not admission.record.recipe.inputs[0].with_associated
        assert not admission.record.recipe.inputs[0].resolve_references
