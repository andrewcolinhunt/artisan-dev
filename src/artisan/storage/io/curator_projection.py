"""Build content-selection evidence from sealed curator output memberships."""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping, Sequence

import polars as pl

from artisan.errors import StoreIntegrityError
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.schemas.enums import TablePath
from artisan.schemas.execution.curator_admission import CuratorAdmission
from artisan.schemas.provenance.curator_projection import (
    CuratorContentOccurrence,
    CuratorProjection,
    CuratorQueryMembership,
)
from artisan.schemas.provenance.occurrence import OccurrenceAddress, ReferenceContext
from artisan.schemas.provenance.projection import (
    AcceptedCuratorOccurrence,
    AcceptedOccurrence,
    AcceptedProducer,
    FrozenReferences,
    OccurrenceRelation,
    ProjectedOccurrence,
    RoleOccurrences,
)
from artisan.storage.core.artifact_store import ArtifactStore
from artisan.storage.core.curator_provenance import RetainedProjections
from artisan.storage.core.curator_view import CuratorView
from artisan.storage.core.table_schemas import get_schema
from artisan.storage.io.bounded_evidence import DiskTable
from artisan.storage.io.curator_control import _retained_projection
from artisan.storage.io.curator_plan import CuratorCommitPlan
from artisan.storage.io.retained_plan import RetainedCommitPlan


def visible_sources(
    projections: RetainedProjections, curator_plans: Sequence[CuratorCommitPlan]
) -> dict[str, tuple[AcceptedProducer, ...]]:
    """Index actual accepted public/support memberships by their content IDs.

    This index supports the curator's explicit content aggregation. Scientific
    consumers must continue to use exact occurrence selection instead.
    """
    found: dict[str, set[AcceptedProducer]] = {}
    for receipt, projection in projections.values():
        selected = set(projection.support) | {
            address for role in projection.exports for address in role.occurrences
        }
        for item in projection.occurrences:
            if item.address in selected:
                found.setdefault(item.artifact_id, set()).add(
                    AcceptedOccurrence(
                        delivery_id=receipt.delivery.delivery_id,
                        seal_digest=receipt.seal_digest,
                        address=item.address,
                    )
                )
    for plan in curator_plans:
        for member in plan.projection.occurrences:
            found.setdefault(member.occurrence.artifact_id, set()).add(
                AcceptedCuratorOccurrence(
                    step_run_id=plan.step_run_id,
                    plan_digest=plan.plan_digest,
                    address=member.occurrence.address,
                )
            )
    return {
        artifact_id: _ordered(producers) for artifact_id, producers in found.items()
    }


def _ordered(producers: Iterable[AcceptedProducer]) -> tuple[AcceptedProducer, ...]:
    return tuple(sorted(set(producers), key=lambda item: item.model_dump_json()))


class _Content:
    """Hydrate only selected content, releasing transient models between uses."""

    def __init__(self, view: CuratorView, tables: Mapping[str, DiskTable]) -> None:
        self.view = view
        self.tables = tables
        self.store = ArtifactStore(
            view.committer.delta_root,
            fs=view.committer.fs,
            storage_options=view.committer.storage_options,
            files_root=view.committer.files_root,
            _view=view,
        )
        index = pl.concat(
            [
                view.scan(TablePath.ARTIFACT_INDEX)
                .select("artifact_id", "artifact_type")
                .collect(),
                self.frame(TablePath.ARTIFACT_INDEX).select(
                    "artifact_id", "artifact_type"
                ),
            ]
        ).unique()
        if index["artifact_id"].n_unique() != index.height:
            msg = "Curator staged content has conflicting registered types"
            raise StoreIntegrityError(msg)
        self.types = dict(index.iter_rows())

    def frame(self, table: TablePath) -> pl.DataFrame:
        candidate = self.tables.get(table.value)
        return (
            _collect(candidate) if candidate else pl.DataFrame(schema=get_schema(table))
        )

    def artifact(self, artifact_id: str) -> Artifact:
        kind = self.types[artifact_id]
        path = ArtifactTypeDef.get_table_path(kind)
        staged = self.tables.get(path)
        if staged is None or (artifact_id,) not in staged.keys:
            artifact = self.store.get_artifact(artifact_id)
            if artifact is None:
                msg = "Curator selected content is not readable in its frozen view"
                raise StoreIntegrityError(msg)
            return artifact
        frame = pl.DataFrame([staged.get_row((artifact_id,))], schema=staged.schema)
        locations = pl.concat(
            [
                self.view.scan(TablePath.ARTIFACT_LOCATIONS)
                .filter(pl.col("artifact_id") == artifact_id)
                .collect(),
                self.frame(TablePath.ARTIFACT_LOCATIONS).filter(
                    pl.col("artifact_id") == artifact_id
                ),
            ]
        )
        return self.store._hydrate_rows(kind, frame, locations)[artifact_id]


def _collect(table: DiskTable) -> pl.DataFrame:
    frames = list(table.iter_frames())
    return pl.concat(frames) if frames else pl.DataFrame(schema=table.schema)


def build_curator_projection(
    admission: CuratorAdmission, view: CuratorView, tables: Mapping[str, DiskTable]
) -> CuratorProjection:
    """Preserve ID deduplication, source sets, direct queries and required closure."""
    if admission.view != view.snapshot:
        msg = "Curator projection must use exactly its admitted store view"
        raise StoreIntegrityError(msg)
    content = _Content(view, tables)
    edges = content.frame(TablePath.EXECUTION_EDGES).filter(
        pl.col("direction") == "output"
    )
    outputs = {
        group["role"][0]: tuple(sorted(set(group["artifact_id"])))
        for group in edges.partition_by("role")
    }
    terminal = content.frame(TablePath.STEPS).row(0, named=True)
    declared = json.loads(terminal["output_roles_json"])
    if not set(outputs) <= set(declared):
        msg = "Curator sealed output roles differ from its terminal contract"
        raise StoreIntegrityError(msg)
    for role in declared:
        outputs.setdefault(role, ())
    authored = content.frame(TablePath.ARTIFACT_EDGES)
    lineage = {
        (source, target)
        for source, target in authored.select(
            "source_artifact_id", "target_artifact_id"
        ).iter_rows()
        if source != target
    }
    selected = {artifact_id for members in outputs.values() for artifact_id in members}
    selected.update(artifact_id for pair in lineage for artifact_id in pair)
    if not selected <= content.types.keys():
        msg = "Curator membership contains absent content"
        raise StoreIntegrityError(msg)
    direct = pl.concat([view.artifact_edges(direct=True).collect(), authored]).unique()
    queries = _queries(outputs, direct)
    selected.update(
        artifact_id for members in queries.values() for artifact_id in members
    )
    projections = _admitted_projections(view, admission)
    curator_plans = _admitted_curator_plans(view, admission)
    captures = _reference_closure(
        content,
        selected,
        direct,
        admission,
        _reference_evidence(projections, curator_plans),
    )
    addresses = {
        artifact_id: OccurrenceAddress(
            invocation_id=admission.record.step_run_id, role="content", index=index
        )
        for index, artifact_id in enumerate(sorted(selected))
    }
    sources = visible_sources(projections, curator_plans)
    supplied: dict[str, set[AcceptedProducer]] = {}
    for role in admission.inputs:
        for member in role.members:
            supplied.setdefault(member.artifact_id, set()).update(member.producers)
    public = {artifact_id for members in outputs.values() for artifact_id in members}
    return CuratorProjection(
        occurrences=tuple(
            CuratorContentOccurrence(
                occurrence=ProjectedOccurrence(
                    address=address,
                    artifact_id=artifact_id,
                    artifact_type=content.types[artifact_id],
                ),
                sources=_ordered(supplied.get(artifact_id, ()))
                if artifact_id in public
                else sources.get(artifact_id, ()),
            )
            for artifact_id, address in addresses.items()
        ),
        outputs=tuple(
            RoleOccurrences(
                role=role, occurrences=tuple(addresses[item] for item in outputs[role])
            )
            for role in sorted(outputs)
        ),
        queries=tuple(
            CuratorQueryMembership(
                primary=addresses[primary],
                artifact_type=kind,
                members=tuple(addresses[item] for item in members),
            )
            for (primary, kind), members in sorted(queries.items())
        ),
        references=tuple(
            FrozenReferences(
                source=addresses[source],
                members=tuple(addresses[item] for item in members),
                direct_parents=tuple(
                    addresses[item] for item in parents if item in selected
                ),
            )
            for source, (members, parents) in sorted(captures.items())
        ),
        lineage=tuple(
            OccurrenceRelation(source=addresses[source], target=addresses[target])
            for source, target in sorted(lineage)
        ),
    )


def _queries(
    outputs: dict[str, tuple[str, ...]], direct: pl.DataFrame
) -> dict[tuple[str, str], tuple[str, ...]]:
    selected = {artifact_id for members in outputs.values() for artifact_id in members}
    queries: dict[tuple[str, str], set[str]] = {
        (artifact_id, kind): set()
        for artifact_id in selected
        for kind in ArtifactTypeDef.get_all()
    }
    for primary, kind, member in (
        direct.filter(pl.col("source_artifact_id").is_in(selected))
        .select("source_artifact_id", "target_artifact_type", "target_artifact_id")
        .iter_rows()
    ):
        queries[(primary, kind)].add(member)
    return {key: tuple(sorted(value)) for key, value in queries.items()}


def _reference_closure(
    content: _Content,
    selected: set[str],
    direct: pl.DataFrame,
    admission: CuratorAdmission,
    evidence: Mapping[str, tuple[set[str], set[str]]],
) -> dict[str, tuple[tuple[str, ...], tuple[str, ...]]]:
    """Run type policies against supplied content and genuine direct relationships."""
    candidates = set(selected) | {
        member.artifact_id for role in admission.inputs for member in role.members
    }
    pending = list(selected)
    captured: dict[str, tuple[tuple[str, ...], tuple[str, ...]]] = {}
    while pending:
        source = pending.pop()
        definition = ArtifactTypeDef.get(content.types[source])
        if source in captured or definition.reference_policy() is None:
            continue
        captured_members, captured_parents = evidence.get(source, (set(), set()))
        parents = tuple(
            sorted(
                set(
                    direct.filter(pl.col("target_artifact_id") == source)[
                        "source_artifact_id"
                    ]
                )
                | captured_parents
            )
        )
        available = candidates | set(parents) | captured_members
        addresses = {
            item: OccurrenceAddress(
                invocation_id="curator-policy", role="content", index=index
            )
            for index, item in enumerate(sorted(available))
        }
        inventory = {addresses[item]: content.artifact(item) for item in available}
        references = definition.resolve_required_references(
            content.artifact(source),
            ReferenceContext(
                inventory, direct_parents=tuple(addresses[item] for item in parents)
            ),
        )
        members = tuple(sorted({item.artifact_id for item in references}))
        captured[source] = members, parents
        pending.extend(set(members) - selected)
        selected.update(members)
        candidates.update(members)
    return captured


def _admitted_projections(
    view: CuratorView, admission: CuratorAdmission
) -> RetainedProjections:
    """Include explicitly selected diagnostic sources without changing queries."""
    projections = dict(view.projections())
    explicit = {
        source.delivery_id
        for role in admission.inputs
        for member in role.members
        for source in member.producers
        if isinstance(source, AcceptedOccurrence)
    }
    plans = [
        view.committer._control_plan(row) for row in view.controls.iter_rows(named=True)
    ]
    for plan in plans:
        if isinstance(plan, RetainedCommitPlan):
            delivery = plan.receipt.delivery.delivery_id
            if delivery in explicit and delivery not in projections:
                projections[delivery] = (
                    plan.receipt,
                    _retained_projection(
                        plan, admission, view.committer, view.controls
                    ),
                )
    return projections


def _reference_evidence(
    projections: RetainedProjections, curator_plans: Sequence[CuratorCommitPlan]
) -> dict[str, tuple[set[str], set[str]]]:
    """Read prior hard-reference context without turning it into query edges."""
    evidence: dict[str, tuple[set[str], set[str]]] = {}
    for _, projection in projections.values():
        ids = {item.address: item.artifact_id for item in projection.occurrences}
        for reference in projection.references:
            members, parents = evidence.setdefault(
                ids[reference.source], (set(), set())
            )
            members.add(ids[reference.target])
            parents.update(
                ids[edge.source]
                for edge in projection.associations
                if edge.target == reference.source
            )
        _add_captured_references(evidence, ids, projection.frozen_context.references)
    for plan in curator_plans:
        ids = {
            item.occurrence.address: item.occurrence.artifact_id
            for item in plan.projection.occurrences
        }
        _add_captured_references(evidence, ids, plan.projection.references)
    return evidence


def _admitted_curator_plans(
    view: CuratorView, admission: CuratorAdmission
) -> tuple[CuratorCommitPlan, ...]:
    explicit = {
        f"step_result:{source.step_run_id}"
        for role in admission.inputs
        for member in role.members
        for source in member.producers
        if isinstance(source, AcceptedCuratorOccurrence)
    }
    excluded = set(view.diagnostic_curator_owners()) - explicit
    plans = (
        view.committer._control_plan(row)
        for row in view.controls.filter(
            pl.col("commit_kind") == "step_result"
        ).iter_rows(named=True)
        if row["logical_commit_id"] not in excluded
    )
    return tuple(plan for plan in plans if isinstance(plan, CuratorCommitPlan))


def _add_captured_references(
    evidence: dict[str, tuple[set[str], set[str]]],
    ids: Mapping[OccurrenceAddress, str],
    references: Iterable[FrozenReferences],
) -> None:
    for reference in references:
        members, parents = evidence.setdefault(ids[reference.source], (set(), set()))
        members.update(ids[key] for key in reference.members)
        parents.update(ids[key] for key in reference.direct_parents)
