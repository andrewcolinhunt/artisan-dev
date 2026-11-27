"""Exact accepted occurrences, ordered outputs and direct semantic associations."""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from functools import partial
from types import MappingProxyType
from typing import TYPE_CHECKING, cast

import polars as pl

from artisan.errors import StoreIntegrityError
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.schemas.enums import TablePath
from artisan.schemas.execution.assignment import AssignmentIdentity, SealedDelivery
from artisan.schemas.provenance.occurrence import ArtifactOccurrence, OccurrenceAddress
from artisan.schemas.provenance.projection import (
    AcceptedCuratorOccurrence,
    AcceptedOccurrence,
    AcceptedProducer,
    BoundaryProjection,
    ProjectedOccurrence,
)
from artisan.storage.core.committed_scan import scan_plan_keys
from artisan.storage.io.admission import (
    CapturedAdmission,
    capture_admission,
    read_admission_record,
)
from artisan.storage.io.curator_admission import read_curator_admission
from artisan.storage.io.curator_plan import CuratorCommitPlan
from artisan.storage.io.retained_commit import RetainedCommitter
from artisan.storage.io.retained_plan import RetainedCommitPlan

if TYPE_CHECKING:
    from artisan.storage.core.curator_reader import AcceptedCuratorResult, CuratorReader
    from artisan.storage.io.retained_read import RetainedReadSnapshot


@dataclass(frozen=True)
class AcceptedArtifact:
    """Hydrated content with its exact accepted-result occurrence authority."""

    reference: AcceptedProducer
    artifact: Artifact


@dataclass(frozen=True)
class AcceptedRole:
    role: str
    occurrences: tuple[AcceptedArtifact, ...]


@dataclass(frozen=True)
class AcceptedResult:
    """Ordered public membership is distinct from retained semantic support."""

    receipt: SealedDelivery
    projection: BoundaryProjection
    artifacts: Mapping[OccurrenceAddress, Artifact]
    outputs: tuple[AcceptedRole, ...]
    support: tuple[AcceptedArtifact, ...]

    def output(self, role: str) -> tuple[AcceptedArtifact, ...]:
        for group in self.outputs:
            if group.role == role:
                return group.occurrences
        msg = f"Result has no declared output role {role!r}"
        raise KeyError(msg)


@dataclass(frozen=True)
class ResultSelection:
    """Current unit order and selected source; visibility is checked independently."""

    assignment: AssignmentIdentity
    receipt: SealedDelivery


@dataclass(frozen=True)
class AssociationQuery:
    primary: AcceptedProducer
    artifact_type: str


class RetainedReader:
    """Read through winner/completion evidence without importing recorded code.

    ``associated_many`` builds one exact direct-relation view for a batch of
    boundary inputs. Curator memberships retain their original query seed and
    acquire only later edges attributed to that exact owner. Frozen creator input
    captures never become additional derivation or association edges.
    """

    def __init__(self, committer: RetainedCommitter) -> None:
        self.committer = committer

    def read(self, receipt: SealedDelivery) -> AcceptedResult:
        """Hydrate registered content while preserving duplicate output occurrences."""
        return self.read_many((receipt,))[0]

    def read_many(
        self, receipts: Sequence[SealedDelivery]
    ) -> tuple[AcceptedResult, ...]:
        """Read ordered selections through one fresh proof and fixed table versions."""
        if not receipts:
            return ()
        with self.committer._read_snapshot() as snapshot:
            results = {}
            for receipt in receipts:
                if receipt not in results:
                    results[receipt] = self._read(receipt, snapshot)
            return tuple(results[receipt] for receipt in receipts)

    def _read(
        self, receipt: SealedDelivery, snapshot: RetainedReadSnapshot
    ) -> AcceptedResult:
        projection = snapshot.projection(receipt)
        plan = snapshot.plan(receipt)
        content = self._content(plan, snapshot)
        artifacts = {}
        for item in projection.occurrences:
            artifact = content[item.artifact_id]
            ArtifactOccurrence(item.address, artifact)
            if artifact.artifact_type != item.artifact_type:
                msg = "Accepted occurrence disagrees with its registered content type"
                raise StoreIntegrityError(msg)
            artifacts[item.address] = artifact
        accepted = {
            address: AcceptedArtifact(_reference(receipt, address), artifact)
            for address, artifact in artifacts.items()
        }
        return AcceptedResult(
            receipt,
            projection,
            MappingProxyType(artifacts),
            tuple(
                AcceptedRole(
                    group.role,
                    tuple(accepted[address] for address in group.occurrences),
                )
                for group in projection.exports
            ),
            tuple(accepted[address] for address in projection.support),
        )

    @contextmanager
    def capture_inputs(self, receipt: SealedDelivery) -> Iterator[CapturedAdmission]:
        """Capture the exact required recipe/context, including diagnostic results."""
        with (
            self.committer._read_snapshot() as snapshot,
            self._capture_inputs(receipt, snapshot) as captured,
        ):
            yield captured

    @contextmanager
    def _capture_inputs(
        self, receipt: SealedDelivery, snapshot: RetainedReadSnapshot
    ) -> Iterator[CapturedAdmission]:
        snapshot.require_committer(self.committer)
        snapshot.projection(receipt)
        plan = snapshot.plan(receipt)
        with capture_admission(
            plan.admission,
            delta_root=self.committer.delta_root,
            files_root=self.committer.files_root,
            fs=self.committer.fs,
            verify_producer=partial(self.committer._verify_producer, _read=snapshot),
        ) as captured:
            yield captured

    def step_outputs(
        self, selections: Sequence[ResultSelection]
    ) -> tuple[AcceptedRole, ...]:
        """Concatenate current unit order and each role's original occurrence order.

        This validates selected results; the controller separately persists the
        current step's finite admission and reuse membership before exposing it.
        """
        if not selections:
            return ()
        _validate_unit_order(tuple(item.assignment for item in selections))
        with self.committer._read_snapshot() as snapshot:
            results = [self._read(item.receipt, snapshot) for item in selections]
            contracts = [
                read_admission_record(
                    snapshot.plan(item.receipt).admission,
                    delta_root=self.committer.delta_root,
                    fs=self.committer.fs,
                ).recipe.outputs
                for item in selections
            ]
        if any(contract != contracts[0] for contract in contracts):
            msg = "Selected units disagree on their admitted output contract"
            raise StoreIntegrityError(msg)
        return tuple(
            AcceptedRole(
                spec.role,
                tuple(
                    item
                    for result in results
                    for group in result.outputs
                    if group.role == spec.role
                    for item in group.occurrences
                ),
            )
            for spec in contracts[0]
        )

    def completed_plans(
        self, *, ordinary: bool = True
    ) -> tuple[RetainedCommitPlan, ...]:
        """Discover complete candidates; each use still requires exact result proof."""
        with self.committer._read_snapshot() as snapshot:
            return self._completed_plans(snapshot, ordinary=ordinary)

    def _completed_plans(
        self, snapshot: RetainedReadSnapshot, *, ordinary: bool = True
    ) -> tuple[RetainedCommitPlan, ...]:
        snapshot.require_committer(self.committer)
        controls = snapshot.controls.filter(
            (pl.col("state") == "complete")
            & (pl.col("commit_kind") == "retained_result")
        )
        plans = []
        for logical_id in sorted(controls["logical_commit_id"].to_list()):
            plan = snapshot.completed_control_plan(logical_id)
            if not isinstance(plan, RetainedCommitPlan):
                msg = "Completed retained control has another plan type"
                raise StoreIntegrityError(msg)
            if (
                not ordinary
                or plan.receipt.delivery.attempt.replay_of_execution_run_id is None
            ):
                plans.append(plan)
        return tuple(plans)

    def associated(
        self, primary: AcceptedProducer, artifact_type: str
    ) -> tuple[AcceptedArtifact, ...]:
        return self.associated_many((AssociationQuery(primary, artifact_type),))[0]

    def associated_many(
        self, queries: Sequence[AssociationQuery]
    ) -> tuple[tuple[AcceptedArtifact, ...], ...]:
        """Combine exact direct edges with curator seeds and verified input aliases."""
        from artisan.storage.core.curator_reader import CuratorReader
        from artisan.storage.core.curator_view import CuratorView, capture_curator_view

        if not queries:
            return ()
        view = CuratorView(self.committer, capture_curator_view(self.committer))
        projections = view.projections()
        diagnostics = frozenset(
            plan.receipt.delivery.delivery_id
            for plan in self.completed_plans(ordinary=False)
            if plan.receipt.delivery.attempt.replay_of_execution_run_id is not None
        )
        index = _Associations(projections, CuratorReader(self.committer), diagnostics)
        selected = [index.lookup(query) for query in queries]
        results: dict[str, AcceptedResult] = {}
        curators: dict[tuple[str, str], AcceptedCuratorResult] = {}
        output = []
        for references in selected:
            artifacts = []
            for reference in references:
                if isinstance(reference, AcceptedCuratorOccurrence):
                    owner = (reference.step_run_id, reference.plan_digest)
                    if owner not in curators:
                        curators[owner] = index.curator_reader.read_result(*owner)
                    curator = curators[owner]
                    artifacts.append(
                        AcceptedArtifact(
                            reference, curator.artifacts[reference.address]
                        )
                    )
                    continue
                if reference.delivery_id not in results:
                    results[reference.delivery_id] = self.read(
                        projections[reference.delivery_id][0]
                    )
                result = results[reference.delivery_id]
                artifacts.append(
                    AcceptedArtifact(reference, result.artifacts[reference.address])
                )
            output.append(tuple(artifacts))
        return tuple(output)

    def _content(
        self, plan: RetainedCommitPlan, proof: RetainedReadSnapshot
    ) -> dict[str, Artifact]:
        locations = plan.table(TablePath.ARTIFACT_LOCATIONS.value)
        uris: dict[str, str] = {}
        if locations is not None:
            for artifact_id, uri in locations.row_keys:
                if not isinstance(artifact_id, str) or not isinstance(uri, str):
                    msg = "Accepted location keys must be artifact IDs and URI strings"
                    raise StoreIntegrityError(msg)
                uris[artifact_id] = uri
        if locations is not None and len(uris) != locations.row_count:
            msg = "Accepted plan gives one artifact multiple carrier locations"
            raise StoreIntegrityError(msg)
        content: dict[str, Artifact] = {}
        for definition in ArtifactTypeDef.get_all().values():
            table = plan.table(definition.table_path)
            if table is None:
                continue
            snapshot = proof.snapshot(definition.table_path)
            selected = scan_plan_keys(plan, table, snapshot, proof.controls)
            for frame in snapshot.iter_frames(rows_by_file=selected.eligible):
                for row in frame.drop("logical_commit_id").iter_rows(named=True):
                    artifact = cast(type[Artifact], definition.model).from_row(row)
                    assert artifact.artifact_id is not None
                    if artifact.EXTERNALLY_BACKED:
                        if artifact.artifact_id not in uris:
                            msg = "Accepted external artifact has no planned managed carrier"
                            raise StoreIntegrityError(msg)
                        setattr(
                            artifact,
                            next(iter(artifact.LOCATOR_FIELDS)),
                            uris[artifact.artifact_id],
                        )
                        artifact.verify_external_content(fs=self.committer.fs)
                    content[artifact.artifact_id] = artifact
        return content


class _Associations:
    """One finite lookup view; content equality never joins producer identities."""

    def __init__(
        self,
        projections: Mapping[str, tuple[SealedDelivery, BoundaryProjection]],
        curator_reader: CuratorReader,
        diagnostic_deliveries: frozenset[str],
    ) -> None:
        self.projections = projections
        self.curator_reader = curator_reader
        self.curators: dict[tuple[str, str], CuratorCommitPlan] = {}
        self.diagnostic_deliveries = diagnostic_deliveries
        self.diagnostic_curators: set[tuple[str, str]] = set()
        self.canonical: dict[AcceptedProducer, AcceptedProducer | None] = {}
        self.edges: dict[AcceptedProducer, list[AcceptedProducer]] = {}
        for receipt, projection in projections.values():
            for relation in projection.associations:
                source = self.resolve(_reference(receipt, relation.source))
                target = self.resolve(_reference(receipt, relation.target))
                if source is None or target is None:
                    continue
                members = self.edges.setdefault(source, [])
                if target not in members:
                    members.append(target)

    def curator_plan(self, reference: AcceptedCuratorOccurrence) -> CuratorCommitPlan:
        owner = (reference.step_run_id, reference.plan_digest)
        if owner not in self.curators:
            plan = self.curator_reader.metadata(*owner)
            if not isinstance(plan, CuratorCommitPlan):
                msg = "Curator cache membership is not an artifact producer"
                raise StoreIntegrityError(msg)
            committer = self.curator_reader.committer
            admission = read_curator_admission(
                plan.admission, delta_root=committer.delta_root, fs=committer.fs
            )
            if admission.record.replay_of_execution_run_id is not None:
                self.diagnostic_curators.add(owner)
            self.curators[owner] = plan
        plan = self.curators[owner]
        if not any(
            item.occurrence.address == reference.address
            for item in plan.projection.occurrences
        ):
            msg = "Association refers to an absent curator content occurrence"
            raise StoreIntegrityError(msg)
        return plan

    def diagnostic(self, reference: AcceptedProducer) -> bool:
        if isinstance(reference, AcceptedCuratorOccurrence):
            self.curator_plan(reference)
            return (
                reference.step_run_id,
                reference.plan_digest,
            ) in self.diagnostic_curators
        return reference.delivery_id in self.diagnostic_deliveries

    def occurrence(self, reference: AcceptedProducer) -> ProjectedOccurrence:
        if isinstance(reference, AcceptedCuratorOccurrence):
            return next(
                item.occurrence
                for item in self.curator_plan(reference).projection.occurrences
                if item.occurrence.address == reference.address
            )
        receipt, projection = self.projections.get(reference.delivery_id, (None, None))
        if (
            receipt is None
            or projection is None
            or receipt.seal_digest != reference.seal_digest
        ):
            msg = "Association refers to an unavailable or diagnostic accepted producer"
            raise StoreIntegrityError(msg)
        occurrence = next(
            (
                item
                for item in projection.occurrences
                if item.address == reference.address
            ),
            None,
        )
        if occurrence is None:
            msg = "Association refers to an absent accepted occurrence"
            raise StoreIntegrityError(msg)
        return occurrence

    def resolve(self, reference: AcceptedProducer) -> AcceptedProducer | None:
        chain: list[AcceptedProducer] = []
        current = reference
        while current not in self.canonical:
            if current in chain:
                msg = "Accepted input attribution contains a cycle"
                raise StoreIntegrityError(msg)
            chain.append(current)
            if self.diagnostic(current):
                self.canonical[current] = None
                break
            occurrence = self.occurrence(current)
            if isinstance(current, AcceptedCuratorOccurrence):
                self.canonical[current] = current
                break
            projection = self.projections[current.delivery_id][1]
            producer = next(
                (
                    item.producer
                    for item in projection.input_attribution
                    if item.boundary == current.address
                ),
                None,
            )
            if producer is None:
                self.canonical[current] = current
                break
            if self.diagnostic(producer):
                self.canonical[producer] = None
                current = producer
                break
            selected = self.occurrence(producer)
            if (selected.artifact_id, selected.artifact_type) != (
                occurrence.artifact_id,
                occurrence.artifact_type,
            ):
                msg = "Accepted alias disagrees with its selected previous content"
                raise StoreIntegrityError(msg)
            current = producer
        target = self.canonical[current]
        self.canonical.update(dict.fromkeys(chain, target))
        return target

    def lookup(self, query: AssociationQuery) -> tuple[AcceptedProducer, ...]:
        source = self.resolve(query.primary)
        if source is None:
            msg = (
                "Diagnostic producers are excluded from ordinary association discovery"
            )
            raise StoreIntegrityError(msg)
        references: list[AcceptedProducer] = []
        if isinstance(source, AcceptedCuratorOccurrence):
            plan = self.curator_plan(source)
            capture = next(
                (
                    item
                    for item in plan.projection.queries
                    if (item.primary, item.artifact_type)
                    == (source.address, query.artifact_type)
                ),
                None,
            )
            if capture is None:
                msg = "Curator association type was not captured for this membership"
                raise StoreIntegrityError(msg)
            references.extend(
                source.model_copy(update={"address": address})
                for address in capture.members
            )
        references.extend(self.edges.get(source, ()))
        return tuple(
            reference
            for reference in dict.fromkeys(references)
            if self.occurrence(reference).artifact_type == query.artifact_type
        )


def _reference(
    receipt: SealedDelivery, address: OccurrenceAddress
) -> AcceptedOccurrence:
    return AcceptedOccurrence(
        delivery_id=receipt.delivery.delivery_id,
        seal_digest=receipt.seal_digest,
        address=address,
    )


def _validate_unit_order(assignments: tuple[AssignmentIdentity, ...]) -> None:
    if len({item.step_run_id for item in assignments}) != 1 or tuple(
        item.unit_index for item in assignments
    ) != tuple(range(len(assignments))):
        msg = "Selected result units must preserve one current step's complete order"
        raise ValueError(msg)
