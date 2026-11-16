"""Exact terminal step effects and ordered selections of accepted unit results."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Annotated, Any, Literal

import polars as pl
from fsspec import AbstractFileSystem
from fsspec.implementations.local import LocalFileSystem
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from artisan.errors import StoreIntegrityError
from artisan.schemas.enums import CachePolicy, FailurePolicy, TablePath
from artisan.schemas.execution.admission import AdmissionReference
from artisan.schemas.execution.assignment import (
    AssignmentIdentity,
    ContentDigest,
    DeliveryIdentity,
    SealedDelivery,
)
from artisan.schemas.execution.step_admission import StepAdmission
from artisan.schemas.orchestration.step_lifecycle import (
    StepDisposition,
    StepStatus,
    classify_step_status,
)
from artisan.schemas.orchestration.step_result import StepResult
from artisan.storage.core.table_schemas import STEPS_SCHEMA
from artisan.storage.io.assignment_journal import AssignmentState, WinningDelivery
from artisan.storage.io.bounded_evidence import DiskTable
from artisan.storage.io.bounded_io import capture_parquet
from artisan.storage.io.plan_evidence import PlannedFile, PlannedTable
from artisan.storage.io.publication import (
    publish_immutable_bytes,
    publish_immutable_file,
)
from artisan.utils.hashing import canonical_json_bytes, compute_content_digest
from artisan.utils.path import uri_join


class StepUnitFailure(BaseModel):
    """Observed failure; delivery identity requires confirmed execution ownership."""

    model_config = ConfigDict(
        extra="forbid", frozen=True, revalidate_instances="always"
    )
    error: str = Field(min_length=1, strict=True)
    failed_delivery: DeliveryIdentity | None = None


class StepUnitResolution(BaseModel):
    """One ordered unit's selected result or durably fenced failure."""

    model_config = ConfigDict(
        extra="forbid", frozen=True, revalidate_instances="always"
    )
    admission: AdmissionReference
    decision_sequence: int = Field(ge=0, strict=True)
    receipt: SealedDelivery | None = None
    reused: bool = Field(default=False, strict=True)
    error: str | None = Field(default=None, min_length=1, strict=True)
    failed_delivery: DeliveryIdentity | None = None

    @model_validator(mode="after")
    def _validate_outcome(self) -> StepUnitResolution:
        if (self.receipt is None) != (self.error is not None) or (
            self.reused and self.receipt is None
        ):
            msg = "A unit resolution requires exactly one selected receipt or failure"
            raise ValueError(msg)
        if self.failed_delivery is not None and (
            self.receipt is not None
            or self.error is None
            or self.failed_delivery.attempt != self.admission.attempt
        ):
            msg = "Failed delivery must name the exact fenced unit admission"
            raise ValueError(msg)
        return self


class StepUnitFence(BaseModel):
    """An administrative current-unit fence, without a scientific outcome."""

    model_config = ConfigDict(
        extra="forbid", frozen=True, revalidate_instances="always"
    )
    admission: AdmissionReference
    decision_sequence: int = Field(ge=0, strict=True)


class CurrentStepUnits(BaseModel):
    """Results observed for the current finite assignment membership."""

    model_config = ConfigDict(
        extra="forbid", frozen=True, revalidate_instances="always"
    )
    kind: Literal["current_units"] = "current_units"
    units: tuple[StepUnitResolution, ...]
    failure_policy: FailurePolicy
    infrastructure_error: str | None = Field(default=None, min_length=1, strict=True)

    @model_validator(mode="after")
    def _unique_deliveries(self) -> CurrentStepUnits:
        failed = [
            unit.failed_delivery.delivery_id
            for unit in self.units
            if unit.failed_delivery is not None
        ]
        accepted = {
            unit.receipt.delivery.delivery_id
            for unit in self.units
            if unit.receipt is not None
        }
        if len(failed) != len(set(failed)) or accepted.intersection(failed):
            msg = "Failed executions cannot select duplicate delivery identities"
            raise ValueError(msg)
        return self


class WholeStepReuse(BaseModel):
    """Select one original completed step; current fences are not failures."""

    model_config = ConfigDict(
        extra="forbid", frozen=True, revalidate_instances="always"
    )
    kind: Literal["whole_step_reuse"] = "whole_step_reuse"
    source_step_run_id: ContentDigest
    source_plan_digest: ContentDigest
    cache_policy: CachePolicy
    fences: tuple[StepUnitFence, ...]


type StepSelection = Annotated[
    CurrentStepUnits | WholeStepReuse, Field(discriminator="kind")
]


class StepResultPlan(BaseModel):
    """One current terminal effect with exact current or historical selection."""

    model_config = ConfigDict(
        extra="forbid", frozen=True, revalidate_instances="always"
    )
    domain: Literal["artisan.retained-step-result"] = "artisan.retained-step-result"
    schema_version: Literal[1] = 1
    admission: StepAdmission
    selection: StepSelection
    result: StepResult
    terminal: PlannedTable
    plan_digest: ContentDigest

    @field_validator("schema_version", mode="before")
    @classmethod
    def _strict_version(cls, value: object) -> object:
        if type(value) is not int:
            msg = "Step result plan version must be an integer"
            raise ValueError(msg)
        return value

    @model_validator(mode="after")
    def _validate_plan(self) -> StepResultPlan:
        count = len(self.admission.assignments)
        if (
            not count
            or len(self.decision_admissions) != count
            or self.result.step_run_id != self.step_run_id
            or self.result.step_number != self.step_number
        ):
            msg = (
                "Step result must summarize its complete current assignment membership"
            )
            raise ValueError(msg)
        for initial, selected in zip(
            self.admission.assignments, self.decision_admissions, strict=True
        ):
            validate_unit_admission(initial, selected)
        if isinstance(self.selection, CurrentStepUnits):
            self._validate_current_result()
        elif (
            self.selection.source_step_run_id == self.step_run_id
            or self.result.status not in {StepStatus.SUCCEEDED, StepStatus.PARTIAL}
            or self.result.disposition != StepDisposition.CACHE_HIT
            or self.result.cancellation_status is not None
            or self.result.total_count != count
            or (
                self.selection.cache_policy == CachePolicy.ALL_SUCCEEDED
                and self.result.status != StepStatus.SUCCEEDED
            )
        ):
            msg = "Whole-step reuse requires a distinct eligible completed source"
            raise ValueError(msg)
        if (
            self.terminal.table_path != TablePath.STEPS.value
            or self.terminal.row_count != 1
            or len(self.terminal.files) != 1
        ):
            msg = "Step result requires exactly one terminal snapshot"
            raise ValueError(msg)
        if self.plan_digest != step_result_digest(
            self.model_dump(mode="json", exclude={"plan_digest"})
        ):
            msg = "Step result plan digest differs from its exact effects"
            raise ValueError(msg)
        return self

    def _validate_current_result(self) -> None:
        selection = self.selection
        assert isinstance(selection, CurrentStepUnits)
        count = len(selection.units)
        succeeded = sum(unit.receipt is not None for unit in selection.units)
        status = classify_step_status(
            succeeded,
            count - succeeded,
            selection.failure_policy,
            infrastructure_error=selection.infrastructure_error is not None,
            cancellation_status=self.result.cancellation_status,
        )
        usable = status in {StepStatus.SUCCEEDED, StepStatus.PARTIAL}
        disposition = (
            (
                StepDisposition.CACHE_HIT
                if all(unit.reused for unit in selection.units)
                else StepDisposition.EXECUTED
            )
            if usable
            else None
        )
        counts = (
            (0, 0, 0)
            if status == StepStatus.CANCELLED
            else (count, succeeded, count - succeeded)
        )
        if (
            self.result.status != status
            or self.result.disposition != disposition
            or (
                self.result.total_count,
                self.result.succeeded_count,
                self.result.failed_count,
            )
            != counts
        ):
            msg = (
                "Step result must summarize its complete current assignment membership"
            )
            raise ValueError(msg)

    @property
    def decision_admissions(self) -> tuple[AdmissionReference, ...]:
        """Current journal owners, including administrative whole-step fences."""
        selection = self.selection
        records = (
            selection.units
            if isinstance(selection, CurrentStepUnits)
            else selection.fences
        )
        return tuple(item.admission for item in records)

    @property
    def selected_receipts(self) -> tuple[SealedDelivery, ...]:
        """Current-unit receipts; whole-step reuse refers to its source plan."""
        if not isinstance(self.selection, CurrentStepUnits):
            return ()
        return tuple(
            unit.receipt for unit in self.selection.units if unit.receipt is not None
        )

    @property
    def logical_commit_id(self) -> str:
        return f"step_result:{self.step_run_id}"

    @property
    def commit_kind(self) -> str:
        return "step_result"

    @property
    def step_run_id(self) -> str:
        return self.admission.step_run_id

    @property
    def step_number(self) -> int:
        return self.admission.step_number

    @property
    def recovery_batch_id(self) -> None:
        return None

    def table(self, table_path: str) -> PlannedTable | None:
        return self.terminal if table_path == TablePath.STEPS.value else None

    def expected_origin(self, table_path: str, key: tuple[Any, ...]) -> None:
        msg = "Step snapshots do not carry artifact origins"
        raise ValueError(msg)


def verify_step_decisions(
    plan: StepResultPlan,
    states: Mapping[AssignmentIdentity, AssignmentState | None],
    winners: Mapping[SealedDelivery, WinningDelivery],
) -> None:
    """Check a fresh decision snapshot, including each selected source's winner."""
    if isinstance(plan.selection, WholeStepReuse):
        for fence in plan.selection.fences:
            state = states[fence.admission.attempt.assignment]
            if (
                state is None
                or state.attempt != fence.admission.attempt
                or state.admission_digest != fence.admission.record_digest
                or not state.cancelled
                or state.winner is not None
                or state.reuse is not None
                or state.sequence != fence.decision_sequence
            ):
                msg = "Whole-step reuse changed its exact administrative fences"
                raise StoreIntegrityError(msg)
        return
    for resolution in plan.selection.units:
        reference = resolution.admission
        state = states[reference.attempt.assignment]
        if state is None or (state.attempt, state.admission_digest) != (
            reference.attempt,
            reference.record_digest,
        ):
            msg = "Step result has an obsolete assignment admission"
            raise StoreIntegrityError(msg)
        selected = state.reuse if resolution.reused else state.winner
        if resolution.receipt is not None:
            if resolution.receipt not in winners:
                msg = "Step selection has no verified source winner"
                raise StoreIntegrityError(msg)
            if selected is None or (selected.receipt, selected.decision_sequence) != (
                resolution.receipt,
                resolution.decision_sequence,
            ):
                msg = "Step result differs from the durable unit selection"
                raise StoreIntegrityError(msg)
        elif (
            not state.cancelled
            or state.winner is not None
            or state.reuse is not None
            or state.sequence != resolution.decision_sequence
        ):
            msg = "Failed step unit must have an exact durable fence"
            raise StoreIntegrityError(msg)


def validate_unit_admission(
    initial: AdmissionReference, selected: AdmissionReference
) -> None:
    """Permit a journaled retry without changing finite unit order or science."""
    counters = {"attempt", "generation"}
    if (
        selected.attempt.model_dump(exclude=counters)
        != initial.attempt.model_dump(exclude=counters)
        or selected.attempt.attempt < initial.attempt.attempt
        or selected.attempt.generation < initial.attempt.generation
        or (
            selected.attempt.generation == initial.attempt.generation
            and selected != initial
        )
    ):
        msg = "Selected retry changed the finite unit's admitted scientific identity"
        raise StoreIntegrityError(msg)


def step_result_directory(delta_root: str, step_run_id: str) -> str:
    if len(step_run_id) != 32 or any(
        char not in "0123456789abcdef" for char in step_run_id
    ):
        msg = "Step result requires a canonical step identity"
        raise ValueError(msg)
    return uri_join(delta_root, "_artisan", "step-results", step_run_id)


def read_step_result_plan(
    delta_root: str, step_run_id: str, fs: AbstractFileSystem
) -> StepResultPlan | None:
    """Read canonical effects; file presence alone grants no result visibility."""
    path = uri_join(step_result_directory(delta_root, step_run_id), "plan.json")
    if not fs.exists(path):
        return None
    with fs.open(path, "rb", cache_type="none") as stream:
        encoded = stream.read()
    plan = StepResultPlan.model_validate_json(encoded)
    if (
        plan.step_run_id != step_run_id
        or canonical_json_bytes(plan.model_dump(mode="json")) != encoded
    ):
        msg = "Step result has another owner or noncanonical bytes"
        raise StoreIntegrityError(msg)
    return plan


def publish_step_result_plan(
    admission: StepAdmission,
    selection: CurrentStepUnits | WholeStepReuse,
    result: StepResult,
    terminal: pl.DataFrame,
    *,
    delta_root: str,
    fs: AbstractFileSystem,
) -> StepResultPlan:
    """Publish the exact terminal table and immutable plan before logical visibility."""
    directory = step_result_directory(delta_root, admission.step_run_id)
    with TemporaryDirectory(prefix="artisan-step-result-") as scratch:
        path = str(Path(scratch) / "terminal.parquet")
        terminal.write_parquet(path)
        captured = capture_parquet(
            path, LocalFileSystem(), str(Path(scratch) / "captured.parquet")
        )
        evidence = DiskTable(TablePath.STEPS.value, [captured.path], scratch)
        relative_path = f"terminal-{captured.digest}.parquet"
        table = PlannedTable(
            table_path=TablePath.STEPS.value,
            natural_key=evidence.natural_key,
            row_keys=evidence.keys,
            row_count=evidence.row_count,
            table_plan_key=evidence.canonical_key(
                f"step_result:{admission.step_run_id}"
            ),
            files=(
                PlannedFile(
                    relative_path=relative_path,
                    size_bytes=captured.size_bytes,
                    digest=captured.digest,
                    row_count=captured.row_count,
                    schema_signature=tuple(
                        (name, str(dtype)) for name, dtype in captured.schema.items()
                    ),
                ),
            ),
        )
        payload = {
            "domain": "artisan.retained-step-result",
            "schema_version": 1,
            "admission": admission.model_dump(mode="json"),
            "selection": selection.model_dump(mode="json"),
            "result": result.model_dump(mode="json"),
            "terminal": table.model_dump(mode="json"),
        }
        plan = StepResultPlan.model_validate(
            {**payload, "plan_digest": step_result_digest(payload)}
        )
        publish_immutable_file(
            fs,
            uri_join(directory, relative_path),
            captured.path,
            source_fs=LocalFileSystem(),
        )
    publish_immutable_bytes(
        fs,
        uri_join(directory, "plan.json"),
        canonical_json_bytes(plan.model_dump(mode="json")),
    )
    return plan


@contextmanager
def capture_step_terminal(
    plan: StepResultPlan, *, delta_root: str, fs: AbstractFileSystem
) -> Iterator[DiskTable]:
    """Recapture one exact terminal effect through the shared bounded-table reader."""
    if read_step_result_plan(delta_root, plan.step_run_id, fs) != plan:
        msg = "Persisted step effects changed"
        raise StoreIntegrityError(msg)
    file = plan.terminal.files[0]
    with TemporaryDirectory(prefix="artisan-step-terminal-") as scratch:
        captured = capture_parquet(
            uri_join(
                step_result_directory(delta_root, plan.step_run_id), file.relative_path
            ),
            fs,
            str(Path(scratch) / "terminal.parquet"),
        )
        if (
            captured.digest,
            captured.size_bytes,
            captured.row_count,
            captured.schema,
        ) != (file.digest, file.size_bytes, file.row_count, STEPS_SCHEMA) or tuple(
            (name, str(dtype)) for name, dtype in captured.schema.items()
        ) != file.schema_signature:
            msg = "Step terminal evidence changed"
            raise StoreIntegrityError(msg)
        table = DiskTable(TablePath.STEPS.value, [captured.path], scratch)
        if (
            table.keys != plan.terminal.row_keys
            or table.canonical_key(plan.logical_commit_id)
            != plan.terminal.table_plan_key
        ):
            msg = "Step terminal effect differs from its exact plan"
            raise StoreIntegrityError(msg)
        yield table


def step_result_digest(payload: dict[str, Any]) -> str:
    return compute_content_digest(
        b"artisan.retained-step-result-v1\0" + canonical_json_bytes(payload)
    )
