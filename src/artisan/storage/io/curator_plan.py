"""Typed curator completion authority around existing exact logical effects."""

from __future__ import annotations

import json
from typing import Annotated, Any, Literal

from fsspec import AbstractFileSystem
from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, model_validator

from artisan.errors import StoreIntegrityError
from artisan.schemas.enums import TablePath
from artisan.schemas.execution.admission import ManagedCarrier
from artisan.schemas.execution.assignment import ContentDigest
from artisan.schemas.execution.curator_admission import CuratorAdmissionReference
from artisan.schemas.execution.curator_cache import (
    CURATOR_CACHE_PROOF_KEY,
    CuratorCacheProof,
)
from artisan.schemas.execution.curator_carriers import CuratorCarrierInventory
from artisan.schemas.provenance.curator_projection import CuratorProjection
from artisan.storage.core.table_schemas import FRAMEWORK_SCHEMAS
from artisan.storage.io.curator_effects import CuratorEffects
from artisan.storage.io.curator_reuse_plan import CuratorReusePlan
from artisan.storage.io.plan_evidence import PlannedTable
from artisan.storage.io.publication import publish_immutable_bytes
from artisan.storage.io.retained_plan import retained_table_paths
from artisan.utils.hashing import canonical_json_bytes, compute_content_digest
from artisan.utils.path import uri_join


class CuratorCommitPlan(BaseModel):
    """One curator completion, distinct from scientific assignment acceptance.

    The existing effects retain their sealed worker membership and uniform new
    content origin. This outer digest additionally binds content-level source
    sets and frozen queries. It is the digest written to the logical journal.
    """

    model_config = ConfigDict(
        extra="forbid", frozen=True, revalidate_instances="always"
    )

    domain: Literal["artisan.curator-commit"] = "artisan.curator-commit"
    admission: CuratorAdmissionReference
    effects: CuratorEffects
    projection: CuratorProjection
    cache_proof: CuratorCacheProof | None = None
    carrier_inventory: CuratorCarrierInventory | None = None
    managed_locations: PlannedTable | None = None
    plan_digest: ContentDigest

    @model_validator(mode="after")
    def _validate_contract(self) -> CuratorCommitPlan:
        CuratorEffects.model_validate(self.effects.model_dump())
        if self.effects.step_run_id != self.admission.step_run_id:
            msg = "Curator effects must own their admitted terminal step"
            raise ValueError(msg)
        if any(
            item.occurrence.address.invocation_id != self.step_run_id
            for item in self.projection.occurrences
        ):
            msg = "Curator content membership belongs to another completion"
            raise ValueError(msg)
        bindings = (
            self.carrier_inventory.external_bindings
            if self.carrier_inventory is not None
            else ()
        )
        if bool(bindings) != (self.managed_locations is not None) or bool(bindings) != (
            self.effects.table(TablePath.ARTIFACT_LOCATIONS.value) is not None
        ):
            msg = "Curator managed locations require the exact sealed carrier bindings"
            raise ValueError(msg)
        if self.managed_locations is not None and (
            self.managed_locations.table_path != TablePath.ARTIFACT_LOCATIONS.value
            or self.effects.table(TablePath.ARTIFACT_LOCATIONS.value) is None
            or {key[0] for key in self.managed_locations.row_keys}
            != {item.artifact_id for item in bindings}
            or self.managed_locations.row_count != len(bindings)
        ):
            msg = "Curator managed locations disagree with sealed external membership"
            raise ValueError(msg)
        if self.plan_digest != curator_plan_digest(
            self.model_dump(mode="json", exclude={"plan_digest"})
        ):
            msg = "Curator plan digest disagrees with its exact effects and projection"
            raise ValueError(msg)
        return self

    @property
    def logical_commit_id(self) -> str:
        return self.effects.logical_commit_id

    @property
    def commit_kind(self) -> str:
        return "step_result"

    @property
    def step_run_id(self) -> str:
        return self.admission.step_run_id

    @property
    def recovery_batch_id(self) -> None:
        return None

    @property
    def step_number(self) -> int:
        return self.effects.step_number

    def table(self, table_path: str) -> PlannedTable | None:
        if table_path == TablePath.ARTIFACT_LOCATIONS.value and self.managed_locations:
            return self.managed_locations
        return self.effects.table(table_path)

    @property
    def tables(self) -> tuple[PlannedTable, ...]:
        """Return applied effects while preserving original sealed worker evidence."""
        return tuple(
            self.managed_locations
            if table.table_path == TablePath.ARTIFACT_LOCATIONS.value
            and self.managed_locations is not None
            else table
            for table in self.effects.tables
        )

    @property
    def carriers(self) -> tuple[ManagedCarrier, ...]:
        """Expose complete managed identities for exact selected content readers."""
        if self.carrier_inventory is None:
            return ()
        return tuple(
            ManagedCarrier(digest=item.digest, size_bytes=item.size_bytes)
            for item in self.carrier_inventory.carriers
        )

    def expected_origin(self, table_path: str, key: tuple[Any, ...]) -> int:
        return self.effects.expected_origin(table_path, key)


type CuratorPlan = CuratorCommitPlan | CuratorReusePlan
_PLAN_ADAPTER: TypeAdapter[CuratorPlan] = TypeAdapter(
    Annotated[CuratorPlan, Field(discriminator="domain")]
)


def curator_plan_digest(payload: dict[str, Any]) -> str:
    return compute_content_digest(
        b"artisan.curator-commit-v1\0" + canonical_json_bytes(payload)
    )


def sealed_curator_cache_proof(
    metadata: str | None, *, success: bool
) -> CuratorCacheProof | None:
    """Parse framework proof from an already verified exact execution row."""
    payload = {} if metadata is None else json.loads(metadata)
    if not isinstance(payload, dict):
        msg = "Curator execution metadata must be an object"
        raise StoreIntegrityError(msg)
    if CURATOR_CACHE_PROOF_KEY not in payload:
        return None
    if not success:
        msg = "Failed curator execution cannot contain reusable read evidence"
        raise StoreIntegrityError(msg)
    return CuratorCacheProof.model_validate(payload[CURATOR_CACHE_PROOF_KEY])


def curator_table_paths() -> tuple[str, ...]:
    """Audit all registered effects, including unplanned rows under this owner."""
    return tuple(
        sorted(
            set(retained_table_paths())
            | {
                table.value
                for table in FRAMEWORK_SCHEMAS
                if table != TablePath.LOGICAL_COMMITS
            }
        )
    )


def curator_plan_path(delta_root: str, step_run_id: str) -> str:
    return uri_join(delta_root, "_artisan", "curator-commits", step_run_id, "plan.json")


def publish_curator_plan(
    plan: CuratorPlan, *, delta_root: str, fs: AbstractFileSystem
) -> None:
    """Select immutable curator terminal effects after all evidence is validated."""
    plan = _PLAN_ADAPTER.validate_python(plan)
    publish_immutable_bytes(
        fs,
        curator_plan_path(delta_root, plan.step_run_id),
        canonical_json_bytes(plan.model_dump(mode="json")),
    )


def read_curator_plan(
    delta_root: str, step_run_id: str, fs: AbstractFileSystem
) -> CuratorPlan | None:
    """Read canonical metadata without treating a plan as completed authority."""
    path = curator_plan_path(delta_root, step_run_id)
    if not fs.exists(path):
        return None
    with fs.open(path, "rb", cache_type="none") as stream:
        encoded = stream.read()
    plan = _PLAN_ADAPTER.validate_json(encoded)
    if (
        plan.step_run_id != step_run_id
        or canonical_json_bytes(plan.model_dump(mode="json")) != encoded
    ):
        msg = "Curator plan has another owner or noncanonical bytes"
        raise StoreIntegrityError(msg)
    return plan
