"""Step result schema and builder for pipeline orchestration."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel, Field

from artisan.schemas.orchestration.output_reference import OutputReference


class StepResult(BaseModel):
    """Result of a completed pipeline step."""

    step_name: str = Field(..., description="Human-readable step name.")
    step_number: int = Field(..., description="Sequential pipeline step number.")
    success: bool = Field(..., description="Whether the step succeeded.")
    total_count: int = Field(default=0, description="Total items processed.")
    succeeded_count: int = Field(default=0, description="Successful items.")
    failed_count: int = Field(default=0, description="Failed items.")
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
            artifact_type=self.output_types.get(role),
        )

    @property
    def has_failures(self) -> bool:
        """True if at least one item failed during this step."""
        return self.failed_count > 0

    model_config = {"frozen": True}


@dataclass
class StepResultBuilder:
    """Builder for constructing StepResult during step execution."""

    step_name: str
    step_number: int
    operation_outputs: dict[str, str | None]  # role -> artifact_type from outputs

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
        success_override: bool | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> StepResult:
        """Build the final StepResult from accumulated counts.

        Args:
            success_override: Force success/failure regardless of counts.
            metadata: Optional metadata dict (timings, diagnostics, etc.).

        Returns:
            Frozen StepResult with final counts and output role info.
        """
        success = (
            success_override
            if success_override is not None
            else (self._failed_count == 0)
        )

        return StepResult(
            step_name=self.step_name,
            step_number=self.step_number,
            success=success,
            total_count=self._total_count,
            succeeded_count=self._succeeded_count,
            failed_count=self._failed_count,
            output_roles=frozenset(self.operation_outputs.keys()),
            output_types=dict(self.operation_outputs),
            metadata=metadata or {},
        )
