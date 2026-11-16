"""Current-step cache membership referencing an original curator completion."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, model_validator

from artisan.schemas.enums import TablePath
from artisan.schemas.execution.assignment import ContentDigest
from artisan.schemas.execution.curator_admission import (
    CuratorAdmission,
    CuratorAdmissionReference,
)
from artisan.schemas.orchestration.step_lifecycle import (
    CancellationStatus,
    StepDisposition,
    StepStatus,
)
from artisan.schemas.orchestration.step_result import StepResult
from artisan.storage.io.curator_effects import CuratorEffects
from artisan.storage.io.plan_evidence import PlannedTable
from artisan.utils.hashing import canonical_json_bytes, compute_content_digest


class CuratorReuseSource(BaseModel):
    """Exact executed authority; reuse never creates another artifact producer."""

    model_config = ConfigDict(
        extra="forbid", frozen=True, revalidate_instances="always"
    )

    step_run_id: ContentDigest
    plan_digest: ContentDigest
    execution_run_id: ContentDigest


class CuratorReusePlan(BaseModel):
    """Publish only terminal and cache membership under the current step owner."""

    model_config = ConfigDict(
        extra="forbid", frozen=True, revalidate_instances="always"
    )

    domain: Literal["artisan.curator-reuse"] = "artisan.curator-reuse"
    admission: CuratorAdmissionReference
    source: CuratorReuseSource
    effects: CuratorEffects
    plan_digest: ContentDigest

    @model_validator(mode="after")
    def _validate_contract(self) -> CuratorReusePlan:
        CuratorEffects.model_validate(self.effects.model_dump())
        if (
            self.effects.step_run_id != self.admission.step_run_id
            or self.source.step_run_id == self.step_run_id
        ):
            msg = "Curator reuse must bind a different original source and current step"
            raise ValueError(msg)
        if {table.table_path for table in self.tables} != {
            TablePath.STEPS.value,
            TablePath.CACHE_REUSE.value,
        }:
            msg = "Curator reuse publishes only terminal and cache membership effects"
            raise ValueError(msg)
        reuse = self.table(TablePath.CACHE_REUSE.value)
        assert reuse is not None
        if reuse.row_keys != ((self.step_run_id, self.source.execution_run_id),):
            msg = "Curator reuse membership differs from its exact original execution"
            raise ValueError(msg)
        if self.plan_digest != curator_reuse_digest(
            self.model_dump(mode="json", exclude={"plan_digest"})
        ):
            msg = "Curator reuse digest differs from its exact source and effects"
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

    @property
    def tables(self) -> tuple[PlannedTable, ...]:
        return self.effects.tables

    def table(self, table_path: str) -> PlannedTable | None:
        return self.effects.table(table_path)

    def expected_origin(self, table_path: str, key: tuple[Any, ...]) -> int:
        return self.effects.expected_origin(table_path, key)


def curator_reuse_digest(payload: dict[str, Any]) -> str:
    """Bind current admission, original source and exact staged terminal bytes."""
    return compute_content_digest(
        b"artisan.curator-reuse-v1\0" + canonical_json_bytes(payload)
    )


def curator_reuse_result(
    admission: CuratorAdmission,
    source: StepResult,
    *,
    cancellation_status: CancellationStatus | None = None,
    error: str | None = None,
) -> StepResult:
    """Preserve source output/metadata under the current admitted step identity."""
    if source.status != StepStatus.SUCCEEDED:
        msg = "Curator reuse requires an entirely successful original result"
        raise ValueError(msg)
    return StepResult(
        step_run_id=admission.record.step_run_id,
        step_number=admission.record.step_number,
        step_name=admission.record.step_name,
        status=StepStatus.SUCCEEDED,
        disposition=StepDisposition.CACHE_HIT,
        cancellation_status=cancellation_status,
        error=error,
        total_count=source.total_count,
        succeeded_count=source.succeeded_count,
        failed_count=0,
        output_roles=source.output_roles,
        output_types=source.output_types,
        metadata=source.metadata,
    )
