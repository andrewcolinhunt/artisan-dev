"""Step result schema and builder for pipeline orchestration."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel, Field, model_validator

from artisan.schemas.artifact.types import ArtifactTypes
from artisan.schemas.orchestration.output_reference import OutputReference
from artisan.schemas.orchestration.step_lifecycle import (
    TERMINAL_STEP_STATUSES,
    CancellationStatus,
    StepDisposition,
    StepStatus,
)


class StepResult(BaseModel):
    """Authoritative terminal result of one pipeline step attempt."""

    step_name: str = Field(..., description="Human-readable step name.")
    step_number: int = Field(..., description="Sequential pipeline step number.")
    status: StepStatus = Field(..., description="Authoritative terminal state.")
    disposition: StepDisposition | None = Field(
        default=None,
        description="Whether usable output was executed or reused from cache.",
    )
    cancellation_status: CancellationStatus | None = Field(
        default=None,
        description="Final cancellation acknowledgement, when requested.",
    )
    error: str | None = Field(default=None, description="Outcome diagnostic.")
    total_count: int = Field(default=0, ge=0, description="Total items processed.")
    succeeded_count: int = Field(default=0, ge=0, description="Successful items.")
    failed_count: int = Field(default=0, ge=0, description="Failed items.")
    output_roles: frozenset[str] = Field(
        default_factory=frozenset,
        description="Available output role names.",
    )
    output_types: dict[str, str | None] = Field(
        default_factory=dict,
        description="Mapping of output role to artifact type.",
    )
    duration_seconds: float | None = Field(
        default=None,
        description="Wall-clock duration in seconds.",
    )
    metadata: dict[str, Any] = Field(
        default_factory=dict,
        description="Step metadata (timings, diagnostics, etc.).",
    )
    step_run_id: str | None = Field(
        default=None,
        description="Unique ID for this step execution attempt.",
    )

    def output(self, role: str) -> OutputReference:
        """Return a lazy reference to outputs for the given role.

        Args:
            role: Output role name to reference.

        Returns:
            An ``OutputReference`` resolved at dispatch time.

        Raises:
            ValueError: If role is not in ``output_roles``.
        """
        if role not in self.output_roles:
            available = ", ".join(sorted(self.output_roles)) or "(none)"
            msg = f"Output role '{role}' not available. Available roles: {available}"
            raise ValueError(msg)

        return OutputReference(
            source_step=self.step_number,
            role=role,
            artifact_type=self.output_types.get(role) or ArtifactTypes.ANY,
        )

    @property
    def has_failures(self) -> bool:
        """True if at least one item failed during this step."""
        return self.failed_count > 0

    @model_validator(mode="after")
    def _validate_terminal_result(self) -> StepResult:
        """Enforce the terminal lifecycle and count invariants."""
        if self.status not in TERMINAL_STEP_STATUSES:
            msg = f"StepResult requires a terminal status, got {self.status.value!r}"
            raise ValueError(msg)
        if self.total_count != self.succeeded_count + self.failed_count:
            msg = "total_count must equal succeeded_count + failed_count"
            raise ValueError(msg)
        if any(key in self.metadata for key in ("cancelled", "skipped", "status")):
            msg = "Lifecycle facts must be represented by StepResult.status"
            raise ValueError(msg)

        if self.status == StepStatus.SUCCEEDED:
            if self.failed_count != 0:
                msg = "succeeded requires failed_count == 0"
                raise ValueError(msg)
            self._require_disposition()
            if (
                self.error is not None
                and self.cancellation_status != CancellationStatus.REJECTED
            ):
                msg = "succeeded carries an error only after rejected cancellation"
                raise ValueError(msg)
        elif self.status == StepStatus.PARTIAL:
            if self.succeeded_count == 0 or self.failed_count == 0:
                msg = "partial requires both successful and failed items"
                raise ValueError(msg)
            self._require_disposition()
        elif self.status == StepStatus.FAILED:
            if not self.error:
                msg = "failed requires an error"
                raise ValueError(msg)
            if self.disposition is not None:
                msg = "failed must not carry a disposition"
                raise ValueError(msg)
        elif self.status == StepStatus.CANCELLED:
            if self.total_count != 0:
                msg = "cancelled requires zero counts"
                raise ValueError(msg)
            if self.cancellation_status != CancellationStatus.CONFIRMED:
                msg = "cancelled requires confirmed cancellation"
                raise ValueError(msg)
            if self.disposition is not None:
                msg = "cancelled must not carry a disposition"
                raise ValueError(msg)
        elif self.status == StepStatus.SKIPPED:
            if self.total_count != 0:
                msg = "skipped requires zero counts"
                raise ValueError(msg)
            if self.disposition is not None or self.cancellation_status is not None:
                msg = "skipped cannot carry disposition or cancellation evidence"
                raise ValueError(msg)
            if self.error is not None:
                msg = "skipped must not carry an error"
                raise ValueError(msg)

        if self.status in {
            StepStatus.FAILED,
            StepStatus.CANCELLED,
            StepStatus.SKIPPED,
        } and (self.output_roles or self.output_types):
            msg = f"{self.status.value} must not expose outputs"
            raise ValueError(msg)

        if self.cancellation_status == CancellationStatus.REQUESTED:
            msg = "Terminal results cannot carry requested cancellation"
            raise ValueError(msg)
        if (
            self.cancellation_status == CancellationStatus.CONFIRMED
            and self.status != StepStatus.CANCELLED
        ):
            msg = "Confirmed cancellation requires cancelled status"
            raise ValueError(msg)
        if (
            self.cancellation_status == CancellationStatus.UNKNOWN
            and self.status != StepStatus.FAILED
        ):
            msg = "Unknown cancellation requires failed status"
            raise ValueError(msg)
        return self

    def _require_disposition(self) -> None:
        """Require an execution disposition for a usable output set."""
        if self.disposition is None:
            msg = f"{self.status.value} requires a disposition"
            raise ValueError(msg)

    model_config = {"frozen": True}


@dataclass
class StepResultBuilder:
    """Builder for constructing StepResult during step execution."""

    step_name: str
    step_number: int
    operation_outputs: dict[str, str | None]  # role -> artifact_type from outputs
    step_run_id: str | None = None

    _total_count: int = 0
    _succeeded_count: int = 0
    _failed_count: int = 0

    def add_success(self, count: int = 1) -> None:
        """Record successful item(s)."""
        self._total_count += count
        self._succeeded_count += count

    def add_failure(self, count: int = 1) -> None:
        """Record failed item(s)."""
        self._total_count += count
        self._failed_count += count

    def build(
        self,
        status: StepStatus,
        disposition: StepDisposition | None = None,
        cancellation_status: CancellationStatus | None = None,
        error: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> StepResult:
        """Build the final StepResult from accumulated counts.

        Args:
            status: Authoritative terminal lifecycle state.
            disposition: Execution disposition for usable output states.
            cancellation_status: Final cancellation acknowledgement.
            error: Outcome diagnostic.
            metadata: Optional metadata dict (timings, diagnostics, etc.).

        Returns:
            Frozen StepResult with final counts and output role info.
        """
        exposes_outputs = status in {StepStatus.SUCCEEDED, StepStatus.PARTIAL}
        return StepResult(
            step_name=self.step_name,
            step_number=self.step_number,
            status=status,
            disposition=disposition,
            cancellation_status=cancellation_status,
            error=error,
            total_count=self._total_count,
            succeeded_count=self._succeeded_count,
            failed_count=self._failed_count,
            output_roles=(
                frozenset(self.operation_outputs.keys())
                if exposes_outputs
                else frozenset()
            ),
            output_types=dict(self.operation_outputs) if exposes_outputs else {},
            metadata=metadata or {},
            step_run_id=self.step_run_id,
        )
