"""Put authored scientific shapes through the actual candidate storage boundary."""

from __future__ import annotations

import json

import polars as pl
import pytest
from fsspec import AbstractFileSystem
from pydantic import ValidationError
from qualification.lineage.fixtures import (
    ProjectionCase,
    alias_identity_case,
    required_support_case,
    scientific_cases,
)
from qualification.lineage.projection import (
    BoundaryProjection,
    FrozenContext,
    FrozenReferences,
    Occurrence,
)
from qualification.persistence.prototype import (
    INPUT_SCHEMA,
    CandidateStore,
    Envelope,
    stage_delivery,
)

from artisan.errors import ArtifactIntegrityError, StoreIntegrityError
from artisan.schemas.artifact.execution_config import ExecutionConfigArtifact
from artisan.schemas.artifact.metric import MetricArtifact
from artisan.schemas.execution.storage_config import StorageConfig


@pytest.mark.parametrize(
    "case",
    [*scientific_cases(), required_support_case(), alias_identity_case()],
    ids=lambda case: case.name,
)
def test_scientific_projection_survives_storage_and_cache(
    persistence_backend: tuple[AbstractFileSystem, StorageConfig, str],
    case: ProjectionCase,
) -> None:
    fs, storage, root = persistence_backend
    store = CandidateStore(
        f"{root}/candidate", fs=fs, storage_options=storage.delta_storage_options()
    )
    store.seed_inputs(case.boundary_payloads())
    identity = Envelope(
        unit_id=case.name,
        attempt=0,
        generation=0,
        delivery_id="delivery-1",
        recipe_digest=case.name,
        software_digest="fixture-software",
    )
    store.activate(identity, case.projection)
    directory = f"{root}/delivery"
    stage_delivery(directory, identity, case.projection, case.staged_payloads(), fs=fs)
    store.accept(directory)
    result = store.result(identity.unit_id)
    assert result is not None
    assert result.exports == case.projection.exports
    assert result.support == case.projection.support
    assert result.parents == case.projection.parents
    assert result.associations == case.projection.associations
    assert result.references == case.projection.references
    assert result.frozen_context == case.projection.frozen_context
    assert {item.key: item for item in result.occurrences} == {
        item.key: item for item in case.projection.occurrences
    }
    assert store.cache(identity, case.projection) == [result]
    artifacts = store._read("artifacts", store._physical_schema("artifacts"))
    assert set(artifacts["artifact_id"]) == set(case.staged_payloads())
    assert store.journal().filter(pl.col("kind") == "claim").height == 1
    for artifact_id in case.artifacts:
        hydrated = store.hydrate(identity.unit_id, artifact_id)
        assert hydrated.artifact_id == artifact_id


@pytest.mark.parametrize("damage", ["missing-reference-edge", "foreign-staging-only"])
def test_required_support_cannot_be_omitted_or_borrowed_from_another_delivery(
    persistence_backend: tuple[AbstractFileSystem, StorageConfig, str], damage: str
) -> None:
    fs, storage, root = persistence_backend
    case = required_support_case()
    store = CandidateStore(
        f"{root}/candidate", fs=fs, storage_options=storage.delta_storage_options()
    )
    store.seed_inputs(case.boundary_payloads())
    identity = Envelope(
        unit_id=case.name,
        attempt=0,
        generation=0,
        delivery_id="selected",
        recipe_digest=case.name,
        software_digest="fixture-software",
    )
    store.activate(identity, case.projection)
    projection, payloads = case.projection, case.staged_payloads()
    if damage == "missing-reference-edge":
        projection = projection.model_copy(update={"references": ()})
    else:
        other = identity.model_copy(update={"delivery_id": "foreign"})
        stage_delivery(f"{root}/foreign", other, projection, payloads, fs=fs)
        support = projection.support[0]
        support_id = next(
            item.artifact_id for item in projection.occurrences if item.key == support
        )
        payloads.pop(support_id)
    directory = f"{root}/selected"
    with pytest.raises(
        (StoreIntegrityError, ValidationError), match="membership|Support"
    ):
        stage_delivery(directory, identity, projection, payloads, fs=fs)
        store.accept(directory)
    assert store.result(identity.unit_id) is None
    assert store.cache(identity, case.projection) == []


def test_changed_trusted_boundary_payload_blocks_all_accepted_reads(
    persistence_backend: tuple[AbstractFileSystem, StorageConfig, str],
) -> None:
    fs, storage, root = persistence_backend
    case = alias_identity_case()
    store = CandidateStore(
        f"{root}/candidate", fs=fs, storage_options=storage.delta_storage_options()
    )
    store.seed_inputs(case.boundary_payloads())
    identity = Envelope(
        unit_id=case.name,
        attempt=0,
        generation=0,
        delivery_id="delivery",
        recipe_digest=case.name,
        software_digest="fixture-software",
    )
    store.activate(identity, case.projection)
    stage_delivery(f"{root}/delivery", identity, case.projection, {}, fs=fs)
    store.accept(f"{root}/delivery")
    rows = store._read("inputs", INPUT_SCHEMA).to_dicts()
    payload = json.loads(rows[0]["content"])
    payload["original_name"] = "changed-identity-metadata"
    rows[0]["content"] = json.dumps(payload).encode()
    pl.DataFrame(rows, schema=INPUT_SCHEMA).write_delta(
        f"{store.root}/inputs", mode="overwrite", storage_options=store.storage_options
    )
    with pytest.raises(ArtifactIntegrityError):
        store.result(identity.unit_id)
    with pytest.raises(ArtifactIntegrityError):
        store.cache(identity, case.projection)
    with pytest.raises(ArtifactIntegrityError):
        store.recover()


@pytest.mark.parametrize("has_reference", [False, True])
def test_boundary_configuration_uses_its_frozen_reference_capture(
    persistence_backend: tuple[AbstractFileSystem, StorageConfig, str],
    has_reference: bool,
) -> None:
    fs, storage, root = persistence_backend
    source = MetricArtifact.draft(
        content={"value": 1}, original_name="source", step_number=0
    ).finalize()
    config = ExecutionConfigArtifact.draft(
        content={"dependency": {"$artifact": source.artifact_id}}
        if has_reference
        else {},
        original_name="configuration",
        step_number=0,
    ).finalize()
    occurrences = [
        Occurrence(key="config", artifact_id=config.artifact_id, artifact_type="config")
    ]
    if has_reference:
        occurrences.append(
            Occurrence(
                key="source", artifact_id=source.artifact_id, artifact_type="metric"
            )
        )
    projection = BoundaryProjection(
        boundary_inputs={"config": ("config",)},
        occurrences=tuple(occurrences),
        exports={"config": ("config",)},
        frozen_context=FrozenContext(
            references=(
                FrozenReferences(
                    config="config", members=("source",) if has_reference else ()
                ),
            )
        ),
    )
    store = CandidateStore(
        f"{root}/candidate", fs=fs, storage_options=storage.delta_storage_options()
    )
    store.seed_inputs(
        {
            config.artifact_id: ("config", config.model_dump_json().encode()),
            source.artifact_id: ("metric", source.model_dump_json().encode()),
        }
    )
    identity = Envelope(
        unit_id="frozen-config",
        attempt=0,
        generation=0,
        delivery_id="delivery",
        recipe_digest="recipe",
        software_digest="software",
    )
    store.activate(identity, projection)
    stage_delivery(f"{root}/delivery", identity, projection, {}, fs=fs)
    store.accept(f"{root}/delivery")
    assert (
        store.hydrate(identity.unit_id, config.artifact_id).artifact_id
        == config.artifact_id
    )
    assert store.cache(identity, projection)
    changed = projection.model_copy(update={"frozen_context": FrozenContext()})
    assert store.cache(identity, changed) == []
