"""Read completed curator content selections without inventing assignment owners."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import cast

import polars as pl

from artisan.errors import StoreIntegrityError
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.schemas.enums import TablePath
from artisan.schemas.orchestration.step_result import StepResult
from artisan.schemas.provenance.curator_projection import CuratorProjection
from artisan.schemas.provenance.occurrence import OccurrenceAddress, ReferenceContext
from artisan.schemas.provenance.projection import (
    AcceptedCuratorOccurrence,
    AcceptedProducer,
)
from artisan.storage.cache.curator_reuse_proof import curator_source_matches
from artisan.storage.core.artifact_store import ArtifactStore
from artisan.storage.core.curator_view import CuratorView, capture_curator_view
from artisan.storage.core.retained_reader import AcceptedArtifact, AcceptedRole
from artisan.storage.io.curator_admission import read_curator_admission
from artisan.storage.io.curator_carrier_effects import sealed_curator_carriers
from artisan.storage.io.curator_plan import (
    CuratorCommitPlan,
    CuratorPlan,
    curator_table_paths,
    sealed_curator_cache_proof,
)
from artisan.storage.io.curator_reuse_plan import CuratorReusePlan, curator_reuse_result
from artisan.storage.io.input_registration_plan import InputRegistrationPlan
from artisan.storage.io.retained_commit import RetainedCommitter
from artisan.storage.io.retained_plan import RetainedCommitPlan
from artisan.storage.io.retained_read import RetainedReadSnapshot
from artisan.utils.hashing import compute_stream_digest
from artisan.utils.path import uri_join


@dataclass(frozen=True)
class AcceptedCuratorResult:
    plan: CuratorPlan
    projection: CuratorProjection
    artifacts: Mapping[OccurrenceAddress, Artifact]
    outputs: tuple[AcceptedRole, ...]
    result: StepResult

    def output(self, role: str) -> tuple[AcceptedArtifact, ...]:
        for group in self.outputs:
            if group.role == role:
                return group.occurrences
        raise KeyError(role)

    def reference(self, address: OccurrenceAddress) -> AcceptedCuratorOccurrence:
        if address not in self.artifacts:
            msg = "Curator occurrence is outside its accepted content membership"
            raise StoreIntegrityError(msg)
        source = (
            self.plan.source if isinstance(self.plan, CuratorReusePlan) else self.plan
        )
        return AcceptedCuratorOccurrence(
            step_run_id=source.step_run_id,
            plan_digest=source.plan_digest,
            address=address,
        )


class CuratorReader:
    """Verify complete exact effects before exposing curator-owned occurrences."""

    def __init__(self, committer: RetainedCommitter) -> None:
        self.committer = committer

    def read(self, reference: AcceptedCuratorOccurrence) -> AcceptedCuratorResult:
        """Verify a completed result and the exact requested occurrence membership."""
        with self.committer._read_snapshot() as read:
            return self._read(reference, read)

    def _read(
        self, reference: AcceptedCuratorOccurrence, read: RetainedReadSnapshot
    ) -> AcceptedCuratorResult:
        read.require_committer(self.committer)
        result = self._read_result(reference.step_run_id, reference.plan_digest, read)
        if (
            reference.address not in result.artifacts
            or result.reference(reference.address) != reference
        ):
            msg = "Selected curator occurrence is absent from completed membership"
            raise StoreIntegrityError(msg)
        return result

    def metadata(self, step_run_id: str, plan_digest: str) -> CuratorPlan:
        """Verify compact completed membership without hydrating artifact payloads."""
        with self.committer._read_snapshot() as read:
            return self._metadata(step_run_id, plan_digest, read)

    def _metadata(
        self, step_run_id: str, plan_digest: str, read: RetainedReadSnapshot
    ) -> CuratorPlan:
        plan = read.completed_control_plan(f"step_result:{step_run_id}")
        if not isinstance(plan, (CuratorCommitPlan, CuratorReusePlan)) or (
            plan.plan_digest != plan_digest
        ):
            msg = "Curator producer has no matching immutable completion plan"
            raise StoreIntegrityError(msg)
        if isinstance(plan, CuratorReusePlan):
            self._reuse_metadata(plan, read)
            return plan
        for path in (TablePath.EXECUTIONS.value, TablePath.STEPS.value):
            read.verify_effect(plan, path)
        execution = plan.table(TablePath.EXECUTIONS.value)
        proof = None
        inventory = None
        if execution is not None:
            row = read.effect_row(plan, TablePath.EXECUTIONS.value)
            proof = sealed_curator_cache_proof(row["metadata"], success=row["success"])
            inventory = sealed_curator_carriers(row["metadata"], success=row["success"])
        if proof != plan.cache_proof or inventory != plan.carrier_inventory:
            msg = "Curator proof or inventory differs from its completed execution evidence"
            raise StoreIntegrityError(msg)
        self._terminal(plan, read)
        return plan

    def read_result(self, step_run_id: str, plan_digest: str) -> AcceptedCuratorResult:
        """Verify complete effects, including a successful empty output result."""
        with self.committer._read_snapshot() as read:
            return self._read_result(step_run_id, plan_digest, read)

    def _read_result(
        self, step_run_id: str, plan_digest: str, read: RetainedReadSnapshot
    ) -> AcceptedCuratorResult:
        plan = self._metadata(step_run_id, plan_digest, read)
        for path in curator_table_paths():
            if path not in {TablePath.EXECUTIONS.value, TablePath.STEPS.value}:
                read.verify_effect(plan, path)
        if isinstance(plan, CuratorReusePlan):
            source = self._read_result(
                plan.source.step_run_id, plan.source.plan_digest, read
            )
            return AcceptedCuratorResult(
                plan,
                source.projection,
                source.artifacts,
                source.outputs,
                self._terminal(plan, read),
            )
        view = CuratorView(
            self.committer,
            capture_curator_view(self.committer, _read=read),
            _read=read,
        )
        self._verify_content_owners(plan, view, read)
        store = ArtifactStore(
            self.committer.delta_root,
            fs=self.committer.fs,
            storage_options=self.committer.storage_options,
            files_root=self.committer.files_root,
            _view=view,
        )
        content = {}
        for kind in sorted(
            {item.occurrence.artifact_type for item in plan.projection.occurrences}
        ):
            ids = [
                item.occurrence.artifact_id
                for item in plan.projection.occurrences
                if item.occurrence.artifact_type == kind
            ]
            content.update(store.get_artifacts_by_type(ids, kind))
        self._verify_hydrated_carriers(content, view, read)
        artifacts = {}
        for item in plan.projection.occurrences:
            artifact = content.get(item.occurrence.artifact_id)
            if (
                artifact is None
                or artifact.artifact_type != item.occurrence.artifact_type
            ):
                msg = "Curator occurrence has missing or conflicting content"
                raise StoreIntegrityError(msg)
            artifacts[item.occurrence.address] = artifact
        _validate_references(plan.projection, artifacts)
        return AcceptedCuratorResult(
            plan,
            plan.projection,
            MappingProxyType(artifacts),
            tuple(
                AcceptedRole(
                    group.role,
                    tuple(
                        AcceptedArtifact(
                            AcceptedCuratorOccurrence(
                                step_run_id=plan.step_run_id,
                                plan_digest=plan.plan_digest,
                                address=address,
                            ),
                            artifacts[address],
                        )
                        for address in group.occurrences
                    ),
                )
                for group in plan.projection.outputs
            ),
            self._terminal(plan, read),
        )

    def _reuse_metadata(
        self, plan: CuratorReusePlan, read: RetainedReadSnapshot
    ) -> None:
        for path in curator_table_paths():
            read.verify_effect(plan, path)
        source = self._metadata(plan.source.step_run_id, plan.source.plan_digest, read)
        if not isinstance(source, CuratorCommitPlan):
            msg = "Curator reuse must reference an original executed source"
            raise StoreIntegrityError(msg)
        admission = read_curator_admission(
            plan.admission, delta_root=self.committer.delta_root, fs=self.committer.fs
        )
        source_admission = read_curator_admission(
            source.admission, delta_root=self.committer.delta_root, fs=self.committer.fs
        )
        if not curator_source_matches(
            admission,
            source_admission,
            source.cache_proof,
            CuratorView(self.committer, admission.view, _read=read),
        ):
            msg = "Curator reuse no longer matches its exact admitted source proof"
            raise StoreIntegrityError(msg)
        terminal = self._terminal(plan, read)
        expected = curator_reuse_result(
            admission,
            self._terminal(source, read),
            cancellation_status=terminal.cancellation_status,
            error=terminal.error,
        )
        if terminal != expected:
            msg = "Curator reuse terminal differs from original source output and metadata"
            raise StoreIntegrityError(msg)

    def _verify_content_owners(
        self,
        plan: CuratorCommitPlan,
        view: CuratorView,
        read: RetainedReadSnapshot,
    ) -> None:
        """Verify selected reused rows without hydrating unrelated source outputs."""
        paths: dict[str, set[str]] = {
            TablePath.ARTIFACT_INDEX.value: set(),
            TablePath.ARTIFACT_LOCATIONS.value: set(),
        }
        for member in plan.projection.occurrences:
            item = member.occurrence
            paths[TablePath.ARTIFACT_INDEX.value].add(item.artifact_id)
            paths.setdefault(
                ArtifactTypeDef.get_table_path(item.artifact_type), set()
            ).add(item.artifact_id)
            model = cast(type[Artifact], ArtifactTypeDef.get_model(item.artifact_type))
            if model.EXTERNALLY_BACKED:
                paths[TablePath.ARTIFACT_LOCATIONS.value].add(item.artifact_id)
        for path, ids in paths.items():
            columns = ["artifact_id", "logical_commit_id"]
            if path == TablePath.ARTIFACT_LOCATIONS.value:
                columns.append("uri")
            rows = (
                view.physical(path)
                .filter(pl.col("artifact_id").is_in(ids))
                .select(columns)
                .collect()
            )
            if set(rows["artifact_id"]) != ids or (
                path != TablePath.ARTIFACT_LOCATIONS.value and rows.height != len(ids)
            ):
                msg = "Curator selected content has missing or duplicate physical rows"
                raise StoreIntegrityError(msg)
            for owner in rows["logical_commit_id"].unique():
                source = read.completed_control_plan(owner)
                if owner != plan.logical_commit_id:
                    read.verify_effect(source, path)

    def _verify_hydrated_carriers(
        self,
        content: Mapping[str, Artifact],
        view: CuratorView,
        read: RetainedReadSnapshot,
    ) -> None:
        """Bind only the readable URI selected by ordinary artifact hydration."""
        selected = pl.DataFrame(
            [
                (artifact_id, getattr(artifact, next(iter(artifact.LOCATOR_FIELDS))))
                for artifact_id, artifact in content.items()
                if artifact.EXTERNALLY_BACKED
            ],
            schema={"artifact_id": pl.String, "uri": pl.String},
            orient="row",
        )
        if selected.is_empty():
            return
        rows = (
            view.physical(TablePath.ARTIFACT_LOCATIONS.value)
            .join(selected.lazy(), on=["artifact_id", "uri"], how="inner")
            .select("artifact_id", "uri", "logical_commit_id")
            .collect()
        )
        if rows.height != selected.height:
            msg = "Curator selected carrier lacks one exact physical location owner"
            raise StoreIntegrityError(msg)
        for owner in rows["logical_commit_id"].unique():
            source = read.completed_control_plan(owner)
            if not isinstance(
                source, (RetainedCommitPlan, CuratorCommitPlan, InputRegistrationPlan)
            ):
                msg = "Curator external content lacks a complete carrier inventory"
                raise StoreIntegrityError(msg)
            uris = set(rows.filter(pl.col("logical_commit_id") == owner)["uri"])
            self._verify_selected_carriers(source, uris)

    def _verify_selected_carriers(
        self,
        plan: RetainedCommitPlan | CuratorCommitPlan | InputRegistrationPlan,
        uris: set[str],
    ) -> None:
        """Bind the complete selected carrier, beyond appendable record identity."""
        carriers = {
            uri_join(self.committer.files_root, carrier.relative_path): carrier
            for carrier in plan.carriers
        }
        for uri in uris:
            carrier = carriers.get(uri)
            if carrier is None:
                msg = "Curator selected location has no managed carrier identity"
                raise StoreIntegrityError(msg)
            with self.committer.fs.open(uri, "rb", cache_type="none") as stream:
                observed = compute_stream_digest(stream)
            if observed != (carrier.digest, carrier.size_bytes):
                msg = "Curator selected full carrier changed"
                raise StoreIntegrityError(msg)

    def _terminal(self, plan: CuratorPlan, read: RetainedReadSnapshot) -> StepResult:
        from artisan.orchestration.engine.step_tracker import StepTracker

        return StepTracker._validated_snapshot(
            read.effect_row(plan, TablePath.STEPS.value)
        ).to_step_result()

    def associated(
        self, reference: AcceptedCuratorOccurrence, artifact_type: str
    ) -> tuple[AcceptedArtifact, ...]:
        """Use the frozen content query; later global matches do not change it."""
        result = self.read(reference)
        for query in result.projection.queries:
            if (query.primary, query.artifact_type) == (
                reference.address,
                artifact_type,
            ):
                return tuple(
                    AcceptedArtifact(
                        result.reference(address), result.artifacts[address]
                    )
                    for address in query.members
                )
        msg = "Curator association type was not captured for this membership"
        raise StoreIntegrityError(msg)

    def parents(
        self, reference: AcceptedCuratorOccurrence
    ) -> tuple[AcceptedProducer, ...]:
        """Return selection ancestry and authored lineage without aliasing either."""
        result = self.read(reference)
        member = next(
            item
            for item in result.projection.occurrences
            if item.occurrence.address == reference.address
        )
        parents = set(member.sources)
        parents.update(
            result.reference(edge.source)
            for edge in result.projection.lineage
            if edge.target == reference.address
        )
        return tuple(sorted(parents, key=lambda item: item.model_dump_json()))


def _validate_references(
    projection: CuratorProjection, artifacts: Mapping[OccurrenceAddress, Artifact]
) -> None:
    captures = {item.source: item for item in projection.references}
    for address, artifact in artifacts.items():
        definition = ArtifactTypeDef.get(artifact.artifact_type)
        if definition.reference_policy() is None:
            if address in captures:
                msg = "Curator captured references for a type without a policy"
                raise StoreIntegrityError(msg)
            continue
        capture = captures.get(address)
        if capture is None:
            msg = "Curator semantic references lack a captured policy result"
            raise StoreIntegrityError(msg)
        context = ReferenceContext(
            {
                key: artifacts[key]
                for key in set(capture.members) | set(capture.direct_parents)
            },
            direct_parents=capture.direct_parents,
        )
        references = definition.resolve_required_references(artifact, context)
        if {item.address for item in references} != set(capture.members):
            msg = "Curator semantic references differ from their captured closure"
            raise StoreIntegrityError(msg)
