"""Shared reuse eligibility over exact admitted curator query snapshots."""

from __future__ import annotations

from artisan.schemas.execution.curator_admission import CuratorAdmission
from artisan.schemas.execution.curator_cache import CuratorCacheProof
from artisan.storage.cache.curator_dependencies import (
    curator_views_match,
    validate_curator_cache_proof,
)
from artisan.storage.cache.curator_identity import curator_cache_key
from artisan.storage.core.curator_view import CuratorView


def curator_source_matches(
    current: CuratorAdmission,
    source: CuratorAdmission,
    proof: CuratorCacheProof | None,
    view: CuratorView,
) -> bool:
    """Qualify both admissions and compare observed reads without loading artifacts."""
    if (
        not current.cacheable
        or not current.software_cacheable
        or not source.software_cacheable
        or not source.cacheable
        or current.record.replay_of_execution_run_id is not None
        or source.record.replay_of_execution_run_id is not None
        or current.record.step_run_id == source.record.step_run_id
        or proof is None
        or proof.ineligible_reasons
        or proof.cache_key != curator_cache_key(current)
    ):
        return False
    validate_curator_cache_proof(proof, view._at(source.view), admission=source)
    view.bind_admission(current)
    return curator_views_match(proof, view)
