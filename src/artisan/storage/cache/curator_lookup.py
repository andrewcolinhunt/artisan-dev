"""Verify completed curator sources against current admitted logical-view reads."""

from __future__ import annotations

from dataclasses import dataclass

from artisan.errors import StoreIntegrityError
from artisan.schemas.enums import TablePath
from artisan.schemas.execution.curator_admission import (
    CuratorAdmission,
    CuratorAdmissionReference,
)
from artisan.schemas.orchestration.step_lifecycle import StepStatus
from artisan.storage.cache.curator_identity import curator_cache_key
from artisan.storage.cache.curator_reuse_proof import curator_source_matches
from artisan.storage.core.curator_reader import AcceptedCuratorResult, CuratorReader
from artisan.storage.core.curator_view import CuratorView
from artisan.storage.io.curator_admission import read_curator_admission
from artisan.storage.io.curator_plan import CuratorCommitPlan
from artisan.storage.io.curator_reuse_plan import CuratorReuseSource


@dataclass(frozen=True)
class CuratorCacheHit:
    """Verified reuse source; current-step publication remains controller-owned."""

    admission: CuratorAdmissionReference
    cache_key: str
    source: AcceptedCuratorResult


def lookup_curator(
    admission: CuratorAdmissionReference,
    *,
    reader: CuratorReader,
) -> CuratorCacheHit | None:
    """Reuse only exact completed ordinary results with qualified observed reads."""
    committer = reader.committer
    current = read_curator_admission(
        admission, delta_root=committer.delta_root, fs=committer.fs
    )
    key = curator_cache_key(current)
    if not _eligible(current) or key is None:
        return None
    view = CuratorView(committer, current.view)
    for row in committer._controls().sort("logical_commit_id").iter_rows(named=True):
        if row["state"] != "complete":
            continue
        candidate = committer._control_plan(row)
        if not isinstance(candidate, CuratorCommitPlan):
            continue
        accepted = _matching_source(candidate, current, view, reader)
        if accepted is not None:
            return CuratorCacheHit(admission, key, accepted)
    return None


def validate_curator_reuse_source(
    admission: CuratorAdmissionReference,
    source: CuratorReuseSource,
    *,
    reader: CuratorReader,
) -> AcceptedCuratorResult:
    """Revalidate an exact selected source without trusting a prior lookup object."""
    committer = reader.committer
    current = read_curator_admission(
        admission, delta_root=committer.delta_root, fs=committer.fs
    )
    plan = reader.metadata(source.step_run_id, source.plan_digest)
    execution = plan.table(TablePath.EXECUTIONS.value)
    if (
        not isinstance(plan, CuratorCommitPlan)
        or execution is None
        or execution.row_keys != ((source.execution_run_id,),)
    ):
        msg = "Curator reuse source must name its original executed completion"
        raise StoreIntegrityError(msg)
    accepted = _matching_source(
        plan, current, CuratorView(committer, current.view), reader
    )
    if accepted is None:
        msg = "Curator reuse source is not eligible for this exact current admission"
        raise StoreIntegrityError(msg)
    return accepted


def _eligible(admission: CuratorAdmission) -> bool:
    return (
        admission.cacheable
        and admission.software_cacheable
        and admission.record.replay_of_execution_run_id is None
        and curator_cache_key(admission) is not None
    )


def _matching_source(
    candidate: CuratorCommitPlan,
    current: CuratorAdmission,
    view: CuratorView,
    reader: CuratorReader,
) -> AcceptedCuratorResult | None:
    proof = candidate.cache_proof
    if (
        not _eligible(current)
        or candidate.step_run_id == current.record.step_run_id
        or proof is None
        or proof.ineligible_reasons
        or proof.cache_key != curator_cache_key(current)
    ):
        return None
    committer = reader.committer
    source = read_curator_admission(
        candidate.admission, delta_root=committer.delta_root, fs=committer.fs
    )
    if not curator_source_matches(current, source, proof, view):
        return None
    accepted = reader.read_result(candidate.step_run_id, candidate.plan_digest)
    return accepted if accepted.result.status == StepStatus.SUCCEEDED else None
