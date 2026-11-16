"""Closed historical run assertions, independent of live acceptance controls."""

from __future__ import annotations

from typing import TYPE_CHECKING, Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from artisan.schemas.composites.plan import ExecutionRecipe
from artisan.schemas.enums import CachePolicy, FailurePolicy, TablePath
from artisan.schemas.execution.assignment import ContentDigest, DeliveryIdentity
from artisan.schemas.execution.retained_result import RetainedTableFile
from artisan.schemas.orchestration.step_lifecycle import (
    StepDisposition,
    StepStatus,
    classify_step_status,
)
from artisan.schemas.orchestration.step_result import StepResult

if TYPE_CHECKING:
    from artisan.storage._retained_publication import RetainedInspectionManifest


class _Record(BaseModel):
    model_config = ConfigDict(
        extra="forbid", frozen=True, revalidate_instances="always"
    )


class InspectedFailedResult(_Record):
    """Original closed failed execution, with no seal or accepted output authority."""

    delivery: DeliveryIdentity
    step_run_id: ContentDigest
    plan_digest: ContentDigest
    admission_digest: ContentDigest
    recipe: ExecutionRecipe

    @model_validator(mode="after")
    def _source_identity(self) -> InspectedFailedResult:
        if (
            self.delivery.attempt.assignment.step_run_id != self.step_run_id
            or self.delivery.attempt.recipe_digest != self.recipe.digest
        ):
            msg = "Failed inspection source identity is inconsistent"
            raise ValueError(msg)
        return self


class InspectedUnit(_Record):
    """One ordered scientific outcome; infrastructure failure has no execution."""

    unit_index: int = Field(ge=0, strict=True)
    execution_run_id: ContentDigest | None
    reused: bool = Field(strict=True)
    error: str | None = Field(min_length=1)

    @model_validator(mode="after")
    def _outcome(self) -> InspectedUnit:
        if (self.error is None and self.execution_run_id is None) or (
            self.reused and self.error is not None
        ):
            msg = "Inspection unit lacks a valid execution or failure"
            raise ValueError(msg)
        return self


class InspectedCurrentUnits(_Record):
    kind: Literal["current_units"] = "current_units"
    units: tuple[InspectedUnit, ...] = Field(min_length=1)
    failure_policy: FailurePolicy
    infrastructure_error: str | None = Field(min_length=1)


class InspectedWholeReuse(_Record):
    kind: Literal["whole_step_reuse"] = "whole_step_reuse"
    source_step_run_id: ContentDigest
    source_plan_digest: ContentDigest
    cache_policy: CachePolicy


class InspectedCuratorSelection(_Record):
    kind: Literal["curator"] = "curator"
    source_step_run_id: ContentDigest
    source_plan_digest: ContentDigest
    execution_run_id: ContentDigest


type InspectedSelection = Annotated[
    InspectedCurrentUnits | InspectedWholeReuse | InspectedCuratorSelection,
    Field(discriminator="kind"),
]


class InspectedStep(_Record):
    """One original terminal selection assertion, not a replayable commit plan."""

    step_run_id: ContentDigest
    plan_digest: ContentDigest
    result: StepResult
    selection: InspectedSelection

    @model_validator(mode="after")
    def _terminal(self) -> InspectedStep:
        if self.result.step_run_id != self.step_run_id:
            msg = "Inspection terminal belongs to another attempt"
            raise ValueError(msg)
        if isinstance(self.selection, InspectedCurrentUnits):
            _validate_counts(self.result, self.selection)
        return self


class InspectedRedaction(_Record):
    """An explicitly transformed diagnostic field, never a changed source ID."""

    step_run_id: ContentDigest
    location: Literal["lifecycle", "result", "unit", "selection"]
    index: int = Field(ge=0, strict=True)
    field: Literal["error", "input_refs_json", "infrastructure_error"]


class InspectedRuns(_Record):
    """Finite lifecycle and selections checked using only copied inspection data."""

    lifecycle: RetainedTableFile
    selections: tuple[InspectedStep, ...]
    failed_sources: tuple[InspectedFailedResult, ...]
    redactions: tuple[InspectedRedaction, ...]

    @model_validator(mode="after")
    def _inventory(self) -> InspectedRuns:
        if self.lifecycle.table_path != TablePath.STEPS.value:
            msg = "Inspection lifecycle has another table schema"
            raise ValueError(msg)
        selections = [item.step_run_id for item in self.selections]
        failed = [item.delivery.delivery_id for item in self.failed_sources]
        redactions = [
            (item.step_run_id, item.location, item.index, item.field)
            for item in self.redactions
        ]
        if (
            selections != sorted(set(selections))
            or failed != sorted(set(failed))
            or redactions != sorted(set(redactions))
        ):
            msg = "Run inspection inventories must be canonical and unique"
            raise ValueError(msg)
        return self

    def validate_sources(self, manifest: RetainedInspectionManifest) -> None:
        """Cross-check original source links without reconstructing control records."""
        successful = {item.delivery.delivery_id: item for item in manifest.sources}
        failed = {item.delivery.delivery_id: item for item in self.failed_sources}
        curators = {item.execution_run_id: item for item in manifest.curator_sources}
        if set(successful) & set(failed) or (set(successful) | set(failed)) & set(
            curators
        ):
            msg = "Inspection execution IDs have conflicting source kinds"
            raise ValueError(msg)
        steps = {item.step_run_id: item for item in self.selections}
        used_failures = set()
        for step in self.selections:
            selection = step.selection
            if isinstance(selection, InspectedWholeReuse):
                _validate_whole(step, steps)
            elif isinstance(selection, InspectedCuratorSelection):
                source = curators.get(selection.execution_run_id)
                if source is None or (source.step_run_id, source.plan_digest) != (
                    selection.source_step_run_id,
                    selection.source_plan_digest,
                ):
                    msg = "Inspection curator selection has no exact source"
                    raise ValueError(msg)
                if source.success != (
                    step.result.status in {StepStatus.SUCCEEDED, StepStatus.PARTIAL}
                ):
                    msg = "Inspection curator selection changed its outcome"
                    raise ValueError(msg)
                reused = source.step_run_id != step.step_run_id
                if source.success and step.result.disposition != (
                    StepDisposition.CACHE_HIT if reused else StepDisposition.EXECUTED
                ):
                    msg = "Inspection curator selection changed reuse status"
                    raise ValueError(msg)
                if not reused and step.plan_digest != source.plan_digest:
                    msg = "Executed curator selection changed its original plan"
                    raise ValueError(msg)
                original = steps.get(source.step_run_id)
                if reused and (
                    not source.success
                    or original is None
                    or original.plan_digest != source.plan_digest
                    or original.selection != selection
                    or any(
                        getattr(original.result, field) != getattr(step.result, field)
                        for field in (
                            "status",
                            "total_count",
                            "succeeded_count",
                            "failed_count",
                            "output_roles",
                            "output_types",
                        )
                    )
                ):
                    msg = "Curator reuse changed its original selection or result"
                    raise ValueError(msg)
            else:
                for unit in selection.units:
                    identity = unit.execution_run_id
                    if unit.error is None:
                        if identity not in successful:
                            msg = "Inspection unit has no accepted original source"
                            raise ValueError(msg)
                        assert identity is not None
                        assignment = successful[identity].delivery.attempt.assignment
                        if not unit.reused and (
                            assignment.step_run_id,
                            assignment.unit_index,
                        ) != (step.step_run_id, unit.unit_index):
                            msg = "Executed inspection unit changed its original owner"
                            raise ValueError(msg)
                    elif identity is not None:
                        source_failure = failed.get(identity)
                        if source_failure is None or (
                            source_failure.step_run_id,
                            source_failure.plan_digest,
                            source_failure.delivery.attempt.assignment.unit_index,
                        ) != (step.step_run_id, step.plan_digest, unit.unit_index):
                            msg = "Inspection failure has no exact original source"
                            raise ValueError(msg)
                        used_failures.add(identity)
        if used_failures != set(failed):
            msg = "Inspection contains unselected failed execution sources"
            raise ValueError(msg)


def _validate_counts(result: StepResult, selected: InspectedCurrentUnits) -> None:
    units = selected.units
    if tuple(item.unit_index for item in units) != tuple(range(len(units))):
        msg = "Inspection scientific units must preserve complete order"
        raise ValueError(msg)
    succeeded = sum(item.error is None for item in units)
    status = classify_step_status(
        succeeded,
        len(units) - succeeded,
        selected.failure_policy,
        infrastructure_error=selected.infrastructure_error is not None,
        cancellation_status=result.cancellation_status,
    )
    counts = (
        (0, 0, 0)
        if status == StepStatus.CANCELLED
        else (len(units), succeeded, len(units) - succeeded)
    )
    usable = status in {StepStatus.SUCCEEDED, StepStatus.PARTIAL}
    disposition = (
        (
            StepDisposition.CACHE_HIT
            if all(item.reused for item in units)
            else StepDisposition.EXECUTED
        )
        if usable
        else None
    )
    if (
        result.status,
        result.disposition,
        result.total_count,
        result.succeeded_count,
        result.failed_count,
    ) != (status, disposition, *counts):
        msg = "Inspection result differs from its ordered unit outcomes"
        raise ValueError(msg)


def _validate_whole(step: InspectedStep, steps: dict[str, InspectedStep]) -> None:
    selected = step.selection
    assert isinstance(selected, InspectedWholeReuse)
    source = steps.get(selected.source_step_run_id)
    if (
        source is None
        or source is step
        or source.plan_digest != selected.source_plan_digest
        or not isinstance(source.selection, InspectedCurrentUnits)
        or source.result.status not in {StepStatus.SUCCEEDED, StepStatus.PARTIAL}
        or step.result.disposition != StepDisposition.CACHE_HIT
        or step.result.cancellation_status is not None
        or (
            selected.cache_policy == CachePolicy.ALL_SUCCEEDED
            and source.result.status != StepStatus.SUCCEEDED
        )
    ):
        msg = "Whole-step inspection has no original eligible selection"
        raise ValueError(msg)
    fields = (
        "status",
        "total_count",
        "succeeded_count",
        "failed_count",
        "output_roles",
        "output_types",
    )
    if any(
        getattr(step.result, name) != getattr(source.result, name) for name in fields
    ):
        msg = "Whole-step inspection changed historical results"
        raise ValueError(msg)
