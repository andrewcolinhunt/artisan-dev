"""Successful cache lookup result for execution dispatch."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class CacheHit:
    """Successful cache lookup identifying a reusable execution.

    Attributes:
        execution_run_id: Run ID of the cached execution.
        execution_spec_id: Deterministic cache key that matched.
    """

    execution_run_id: str
    execution_spec_id: str
