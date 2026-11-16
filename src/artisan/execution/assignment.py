"""Execute one complete assignment with provider-owned software and command routing."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import Protocol

from artisan.composites.base.compiled import CompiledPlan
from artisan.execution.assignment_transport import AdmittedAssignment
from artisan.execution.composition import (
    ExecuteAdapter,
    PlanExecution,
    execute_compiled_plan,
)
from artisan.execution.compute.prepared import freeze_runtime_injections
from artisan.execution.exceptions import AssignmentExecutionError
from artisan.execution.recording.delivery_diagnostics import (
    capture_delivery_diagnostics,
)
from artisan.execution.runtime_injections import runtime_injections
from artisan.execution.scientific_failure import assignment_science
from artisan.execution.session import ExecutionSession
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.execution.assignment import (
    AssignmentAttempt,
    DeliveryIdentity,
    SealedDelivery,
    SuccessfulDelivery,
)
from artisan.schemas.execution.delivery_diagnostics import DeliveryDiagnostics
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.schemas.execution.worker_transport import RuntimeCompatibility
from artisan.schemas.provenance.occurrence import OccurrenceAddress
from artisan.schemas.provenance.projection import (
    BoundaryInputAttribution,
    FrozenBoundaryContext,
)
from artisan.utils.command_supervisor import CommandOwnershipError, close_command_scope
from artisan.utils.external_tools import redact_values


class PreparedAssignmentSoftware(Protocol):
    """Closed provider software and assignment-specific execution preflight."""

    @property
    def software_digest(self) -> str: ...

    @property
    def software_cacheable(self) -> bool: ...

    @property
    def compatibility(self) -> RuntimeCompatibility: ...

    @property
    def application_modules(self) -> tuple[str, ...]: ...

    def verify(self, plan: CompiledPlan) -> None: ...

    def create_adapter(
        self,
        assignment: Assignment,
        *,
        working_root: Path,
        runtime_env: Mapping[str, str],
    ) -> ExecuteAdapter:
        """Validate provider paths and create routing before scientific execution."""
        ...


@dataclass(frozen=True)
class Assignment:
    """Trusted live scientific inputs paired with controller-frozen admission."""

    attempt: AssignmentAttempt
    plan: CompiledPlan
    inputs: Mapping[str, Sequence[Artifact]]
    frozen_context: FrozenBoundaryContext = field(default_factory=FrozenBoundaryContext)
    context_artifacts: Mapping[OccurrenceAddress, Artifact] = field(
        default_factory=dict
    )
    input_attribution: tuple[BoundaryInputAttribution, ...] = ()
    step_number: int | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "inputs",
            MappingProxyType(
                {role: tuple(values) for role, values in self.inputs.items()}
            ),
        )
        object.__setattr__(
            self, "context_artifacts", MappingProxyType(dict(self.context_artifacts))
        )
        if self.plan.recipe.digest != self.attempt.recipe_digest:
            msg = "Assignment recipe differs from its admitted attempt"
            raise ValueError(msg)


class RetainedResultSealer(Protocol):
    """Stage retained bytes while files exist, using storage I/O only."""

    def __call__(
        self, successful: SuccessfulDelivery, result: PlanExecution
    ) -> SealedDelivery:
        """Write the complete seal last and return its typed locator and digest."""
        ...


def execute_assignment(
    assignment: Assignment,
    bindings: PreparedAssignmentSoftware,
    sealer: RetainedResultSealer,
    *,
    working_root: Path,
    runtime_env: Mapping[str, str],
    preserve_working: bool = False,
    diagnostic_sink: Callable[[DeliveryDiagnostics], None] | None = None,
    diagnostic_storage: StorageConfig | None = None,
) -> SealedDelivery:
    """Enter one fresh delivery, execute science, close commands, then seal.

    Fresh delivery identity owns scratch and staging. The stable assignment ID
    owns consuming input positions, preserving admission snapshots across retry.
    The caller must still observe worker exit and owner closure before success.
    """
    delivery = DeliveryIdentity.new(assignment.attempt)
    runtime_env = freeze_runtime_injections(runtime_env)
    verify_assignment(assignment, bindings)
    adapter = bindings.create_adapter(
        assignment, working_root=working_root, runtime_env=runtime_env
    )
    with (
        capture_delivery_diagnostics(
            delivery,
            diagnostic_sink,
            sensitive_values=tuple(runtime_env.values()),
            storage=diagnostic_storage,
        ) as diagnostics,
        runtime_injections(runtime_env),
    ):
        session = ExecutionSession(
            working_root=working_root / delivery.delivery_id,
            preserve_working=preserve_working,
            diagnostics=diagnostics,
        )
        authored_failure: AssignmentExecutionError | None = None
        try:
            with session:
                try:
                    with assignment_science(
                        sensitive_values=tuple(runtime_env.values())
                    ):
                        result = execute_compiled_plan(
                            assignment.plan,
                            assignment.inputs,
                            session,
                            invocation_id=assignment.attempt.assignment.assignment_id,
                            execute_adapter=adapter,
                            context_artifacts=assignment.context_artifacts,
                            frozen_context=assignment.frozen_context,
                            input_attribution=assignment.input_attribution,
                            expected_input_context_digest=assignment.attempt.input_context_digest,
                            step_number=assignment.step_number,
                        )
                except AssignmentExecutionError as error:
                    authored_failure = AssignmentExecutionError(
                        redact_values(str(error), tuple(runtime_env.values())),
                        phase=error.phase,
                        call_path=error.call_path,
                        error_envelope=error.error_envelope,
                    )
                    try:
                        closed = close_command_scope(error)
                    except CommandOwnershipError:
                        raise CommandOwnershipError() from authored_failure
                    if not closed:
                        raise CommandOwnershipError() from authored_failure
                    raise authored_failure from None
                if not close_command_scope():
                    raise CommandOwnershipError()
                successful = SuccessfulDelivery(
                    delivery=delivery, computation_complete=True, cleanup_complete=True
                )
                receipt = SealedDelivery.model_validate(sealer(successful, result))
                if receipt.delivery != delivery:
                    msg = "Sealer returned a receipt for a different delivery"
                    raise ValueError(msg)
                if diagnostics is not None:
                    diagnostics.receipt = receipt
        except AssignmentExecutionError as error:
            if error is not authored_failure:
                msg = "Assignment infrastructure failed outside scientific execution"
                raise RuntimeError(msg) from error
            if not session.cleanup_complete:
                raise CommandOwnershipError() from error
            raise AssignmentExecutionError(
                str(error),
                phase=error.phase,
                call_path=error.call_path,
                delivery=delivery,
                error_envelope=error.error_envelope,
            ) from None
    return receipt


def verify_assignment(
    assignment: Assignment | AdmittedAssignment, bindings: PreparedAssignmentSoftware
) -> None:
    """Check selected science/software at each transport boundary."""
    if assignment.plan.recipe.digest != assignment.attempt.recipe_digest:
        msg = "Assignment recipe differs from its admitted attempt"
        raise ValueError(msg)
    if (
        bindings.software_digest != assignment.attempt.software_digest
        or bindings.software_cacheable != assignment.attempt.software_cacheable
    ):
        msg = "Prepared software differs from the admitted assignment"
        raise ValueError(msg)
    bindings.verify(assignment.plan)
