"""Cache lookup result models.

``CacheHit`` and ``CacheMiss`` represent the two outcomes of a cache
lookup during execution dispatch.
"""

from __future__ import annotations

from dataclasses import dataclass

from artisan.schemas.enums import CacheValidationReason


@dataclass
class CacheHit:
    """Successful cache lookup with reusable execution results.

    Attributes:
        execution_run_id: Run ID of the cached execution.
        execution_spec_id: Deterministic cache key that matched.
        inputs: Input role/artifact_id pairs from the cached run.
        outputs: Output role/artifact_id pairs from the cached run.
    """

    execution_run_id: str
    execution_spec_id: str
    inputs: list[dict[str, str]]  # [{"role": str, "artifact_id": str}, ...]
    outputs: list[dict[str, str]]  # [{"role": str, "artifact_id": str}, ...]


@dataclass
class CacheMiss:
    """Cache lookup found no reusable result.

    Attributes:
        execution_spec_id: Deterministic cache key that was queried.
        reason: Why the cache could not satisfy the request.
    """

    execution_spec_id: str
    reason: CacheValidationReason
