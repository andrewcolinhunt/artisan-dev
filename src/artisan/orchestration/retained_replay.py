"""Prepare provider-neutral diagnostic replay from exact accepted input evidence."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field
from types import MappingProxyType
from uuid import uuid4

from artisan.composites.base.compiled import CompiledPlan
from artisan.composites.base.compiler import compile_captured_definition
from artisan.composites.base.composite_definition import CompositeDefinition
from artisan.errors import ArtisanError, ErrorCode, StoreIntegrityError
from artisan.execution.compute.prepared import freeze_runtime_injections
from artisan.execution.recording.retained_replay import read_replay_configuration
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.orchestration.original_selection import (
    OriginalSelectionProof,
    resolve_original_selection,
)
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.composites.grouping import AnchoredNameGrouping
from artisan.schemas.composites.plan import ExecutionRecipe
from artisan.schemas.execution.admission import AdmissionReference
from artisan.schemas.execution.assignment import (
    AssignmentIdentity,
    DeliveryIdentity,
)
from artisan.schemas.execution.retained_replay import (
    RetainedReplayConfiguration,
    RetainedReplayDiagnostic,
)
from artisan.schemas.provenance.occurrence import OccurrenceAddress
from artisan.schemas.provenance.projection import (
    BoundaryInputAttribution,
    BoundaryProjection,
    FrozenAssociation,
    FrozenBoundaryContext,
    FrozenReferences,
    ProjectedOccurrence,
    RoleOccurrences,
)
from artisan.storage.core.execution_source import (
    FailedScientificExecutionSource,
    ScientificExecutionSource,
    read_execution_source,
)
from artisan.storage.core.retained_reader import RetainedReader
from artisan.storage.io.admission import (
    CapturedAdmission,
    _validate_input_contract,
    capture_admission,
)
from artisan.storage.io.retained_commit import RetainedCommitter

type Definition = type[OperationDefinition] | type[CompositeDefinition]


@dataclass(frozen=True)
class RetainedReplayPreparation:
    """Frozen diagnostic science; the controller still resolves actual software."""

    identity: AssignmentIdentity
    source: ScientificExecutionSource | FailedScientificExecutionSource
    plan: CompiledPlan
    boundary: BoundaryProjection
    artifacts: Mapping[OccurrenceAddress, Artifact]
    runtime_env: Mapping[str, str] = field(repr=False)
    allow_code_change: bool
    original_selection: OriginalSelectionProof | None = None

    @property
    def delivery(self) -> DeliveryIdentity:
        """Return the original successful or closed failed execution identity."""
        if isinstance(self.source, ScientificExecutionSource):
            return self.source.plan.receipt.delivery
        assert self.source.unit.failed_delivery is not None
        return self.source.unit.failed_delivery

    @property
    def admission(self) -> AdmissionReference:
        """Return the required frozen input owner, independently of any result seal."""
        return (
            self.source.plan.admission
            if isinstance(self.source, ScientificExecutionSource)
            else self.source.unit.admission
        )

    def validate_selection(
        self, committer: RetainedCommitter, software_digest: str
    ) -> RetainedReplayDiagnostic:
        """Recheck authority and the actual resolved software before publication."""
        if read_execution_source(committer, self.delivery.delivery_id) != self.source:
            msg = "Replay source changed after preparation"
            raise StoreIntegrityError(msg)
        if (
            self.original_selection is not None
            and resolve_original_selection(
                committer, self.delivery.delivery_id, self.plan.recipe
            )
            != self.original_selection
        ):
            msg = "Original replay selection changed after preparation"
            raise StoreIntegrityError(msg)
        if isinstance(self.source, ScientificExecutionSource):
            RetainedReader(committer).read(self.source.plan.receipt)
        configuration = read_replay_configuration(
            self.source.plan.receipt
            if isinstance(self.source, ScientificExecutionSource)
            else self.delivery,
            admission=self.admission,
            delta_root=committer.delta_root,
            fs=committer.fs,
        )
        _runtime_replacements(configuration, self.runtime_env)
        if (
            not self.allow_code_change
            and software_digest != self.delivery.attempt.software_digest
        ):
            raise _error(
                ErrorCode.REPLAY_CODE_CHANGED, "Selected replay software changed"
            )
        return RetainedReplayDiagnostic(
            source_delivery=self.delivery,
            source_admission_digest=self.admission.record_digest,
            source_seal_digest=(
                self.source.plan.receipt.seal_digest
                if isinstance(self.source, ScientificExecutionSource)
                else None
            ),
            selected_recipe_digest=self.plan.recipe.digest,
            selected_software_digest=software_digest,
            allow_code_change=self.allow_code_change,
        )


def prepare_retained_replay(
    committer: RetainedCommitter,
    execution_run_id: str,
    *,
    registered_definitions: Mapping[tuple[str, str], Definition],
    runtime_replacements: Mapping[str, str],
    allow_code_change: bool = False,
) -> RetainedReplayPreparation:
    """Revalidate source authority and restore trusted code and exact captured inputs.

    Persisted runtime-key evidence is required, including for an empty key set.
    Recorded names never authorize imports, installation or deployment. Software
    equivalence is checked against the controller's subsequent actual resolution.
    """
    source = read_execution_source(committer, execution_run_id)
    if not isinstance(
        source, (ScientificExecutionSource, FailedScientificExecutionSource)
    ):
        raise _error(
            ErrorCode.REPLAY_EXECUTION_NOT_FOUND,
            "Replay requires a completed scientific execution source",
        )
    with _capture_source(committer, source) as captured:
        selected = (
            source.plan.receipt
            if isinstance(source, ScientificExecutionSource)
            else source.unit.failed_delivery
        )
        assert selected is not None
        configuration = read_replay_configuration(
            selected,
            admission=captured.reference,
            delta_root=committer.delta_root,
            fs=committer.fs,
        )
        runtime = _runtime_replacements(configuration, runtime_replacements)
        plan = _registered_plan(captured.record.recipe, registered_definitions)
        if not allow_code_change and plan.recipe != captured.record.recipe:
            raise _error(
                ErrorCode.REPLAY_CODE_CHANGED,
                "Replay recipe changed; explicit changed-code replay is required",
            )
        original_selection = (
            resolve_original_selection(committer, execution_run_id, plan.recipe)
            if isinstance(plan.recipe.policy.group_by, AnchoredNameGrouping)
            or isinstance(captured.record.recipe.policy.group_by, AnchoredNameGrouping)
            else None
        )
        identity = AssignmentIdentity(step_run_id=uuid4().hex, unit_index=0)
        boundary, artifacts = _rebind_inputs(
            captured.record.boundary, captured.artifacts, identity.assignment_id
        )
        _validate_input_contract(plan.recipe, boundary, artifacts)
    return RetainedReplayPreparation(
        identity,
        source,
        plan,
        boundary,
        artifacts,
        runtime,
        allow_code_change,
        original_selection,
    )


@contextmanager
def _capture_source(
    committer: RetainedCommitter,
    source: ScientificExecutionSource | FailedScientificExecutionSource,
) -> Iterator[CapturedAdmission]:
    if isinstance(source, ScientificExecutionSource):
        reader = RetainedReader(committer)
        reader.read(source.plan.receipt)
        with reader.capture_inputs(source.plan.receipt) as captured:
            yield captured
        return
    with capture_admission(
        source.unit.admission,
        delta_root=committer.delta_root,
        files_root=committer.files_root,
        fs=committer.fs,
        verify_producer=committer._verify_producer,
    ) as captured:
        yield captured


def _runtime_replacements(
    configuration: RetainedReplayConfiguration,
    replacements: Mapping[str, str],
) -> Mapping[str, str]:
    if configuration.status == "unavailable":
        raise _error(
            ErrorCode.REPLAY_EVIDENCE_UNAVAILABLE,
            "Replay runtime configuration is unavailable",
        )
    if set(replacements) != set(configuration.required_runtime_keys):
        raise _error(
            ErrorCode.REPLAY_REPLACEMENT_REQUIRED,
            "Supply exactly the captured runtime replacement keys",
        )
    return freeze_runtime_injections(replacements)


def _registered_plan(
    recipe: ExecutionRecipe, registered: Mapping[tuple[str, str], Definition]
) -> CompiledPlan:
    key = recipe.definition.module, recipe.definition.qualname
    if key not in registered:
        raise _error(
            ErrorCode.REPLAY_DEPENDENCY_UNAVAILABLE,
            "Replay root definition is not registered by the caller",
        )
    definition = registered[key]
    plan = compile_captured_definition(definition, recipe)
    trusted = set(registered.values())
    pending = [plan]
    while pending:
        child = pending.pop()
        if child.definition not in trusted:
            raise _error(
                ErrorCode.REPLAY_DEPENDENCY_UNAVAILABLE,
                "Replay nested definition is not registered by the caller",
            )
        pending.extend(call.plan for call in child.calls)
    return plan


def _rebind_inputs(
    boundary: BoundaryProjection,
    artifacts: Mapping[OccurrenceAddress, Artifact],
    invocation_id: str,
) -> tuple[BoundaryProjection, Mapping[OccurrenceAddress, Artifact]]:
    addresses: dict[OccurrenceAddress, OccurrenceAddress] = {}
    roles = []
    for group in boundary.boundary_inputs:
        members = []
        for index, original in enumerate(group.occurrences):
            if original in addresses:
                raise _error(
                    ErrorCode.REPLAY_CONFIGURATION_INVALID,
                    "Replay boundary repeats a consuming slot",
                )
            address = OccurrenceAddress(
                invocation_id=invocation_id, role=group.role, index=index
            )
            addresses[original] = address
            members.append(address)
        roles.append(RoleOccurrences(role=group.role, occurrences=tuple(members)))
    for item in boundary.occurrences:
        if item.address not in addresses:
            addresses[item.address] = OccurrenceAddress(
                invocation_id=f"{invocation_id}/context",
                role="captured",
                index=len(addresses),
            )
    context = FrozenBoundaryContext(
        associations=tuple(
            FrozenAssociation(
                primary=addresses[item.primary],
                artifact_type=item.artifact_type,
                members=tuple(addresses[member] for member in item.members),
            )
            for item in boundary.frozen_context.associations
        ),
        references=tuple(
            FrozenReferences(
                source=addresses[item.source],
                members=tuple(addresses[member] for member in item.members),
                direct_parents=tuple(
                    addresses[parent] for parent in item.direct_parents
                ),
            )
            for item in boundary.frozen_context.references
        ),
    )
    rebound = BoundaryProjection(
        boundary_inputs=tuple(roles),
        occurrences=tuple(
            ProjectedOccurrence(
                address=addresses[item.address],
                artifact_id=item.artifact_id,
                artifact_type=item.artifact_type,
            )
            for item in boundary.occurrences
        ),
        input_attribution=tuple(
            BoundaryInputAttribution(
                boundary=addresses[item.boundary], producer=item.producer
            )
            for item in boundary.input_attribution
        ),
        frozen_context=context,
        exports=(),
        support=(),
        parents=(),
        associations=(),
        references=(),
    )
    return rebound, MappingProxyType(
        {addresses[address]: artifact for address, artifact in artifacts.items()}
    )


def _error(code: str, message: str) -> ArtisanError:
    return ArtisanError(
        code, message, error_type="validation", recovery_hint="CHECK_INPUT"
    )
