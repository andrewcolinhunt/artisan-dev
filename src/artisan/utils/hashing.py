"""Hashing utilities for content-addressed artifact and execution IDs.

Provides xxh3_128-based hash functions for artifact content addressing,
execution spec deduplication, and step-level cache keys.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import xxhash


def compute_artifact_id(content: bytes) -> str:
    """Compute xxh3_128 hash for content-addressed artifact ID.

    Args:
        content: Raw bytes to hash.

    Returns:
        32-character hexadecimal hash string.
    """
    return xxhash.xxh3_128(content).hexdigest()


def compute_execution_spec_id(
    operation_name: str,
    inputs: dict[str, list[str]],
    params: dict[str, Any] | None = None,
    config_overrides: dict[str, Any] | None = None,
) -> str:
    """Compute deterministic execution_spec_id with canonicalization.

    The spec_id uniquely identifies an execution based on:
    - operation_name: The operation's name attribute
    - inputs: All artifact IDs being processed
    - params: Merged parameters (defaults + overrides)
    - config_overrides: Runtime config overrides (environment, tool, etc.)

    Args:
        operation_name: The operation's name attribute.
        inputs: Dict mapping role to list of artifact IDs (the batch).
        params: Merged parameters dict (defaults + runtime overrides).
            Will be JSON-canonicalized for deterministic hashing.
        config_overrides: Optional config overrides that affect execution
            behavior (merged environment + tool overrides).

    Returns:
        32-character xxh3_128 hex string.
    """
    # Collect all artifact IDs from all roles, deduplicate, sort
    all_ids: set[str] = set()
    for role_ids in inputs.values():
        all_ids.update(role_ids)
    sorted_ids = ",".join(sorted(all_ids)) if all_ids else ""

    # Canonicalize params dict
    params_json = _canonicalize_dict(params)

    # Canonicalize config overrides
    config_json = _canonicalize_dict(config_overrides)

    # Hash: operation_name | sorted_artifact_ids | params_json | config_json
    hash_input = f"{operation_name}|{sorted_ids}|{params_json}|{config_json}"

    return xxhash.xxh3_128(hash_input.encode()).hexdigest()


def compute_step_spec_id(
    operation_name: str,
    step_number: int,
    params: dict[str, Any] | None,
    input_spec: dict[str, tuple[str, str]],
    config_overrides: dict[str, Any] | None = None,
) -> str:
    """Compute deterministic step_spec_id for step-level caching.

    Mirrors compute_execution_spec_id() but operates on step-level
    references (upstream spec_ids) instead of resolved artifact IDs.
    Includes step_number to prevent cross-position cache hits.

    Args:
        operation_name: The operation's name attribute.
        step_number: Position in the pipeline (0-based).
        params: Merged parameters dict.
        input_spec: Maps each input role to a (upstream_step_spec_id,
            upstream_role) tuple.
        config_overrides: Optional config overrides that affect execution
            behavior (merged environment + tool overrides).

    Returns:
        32-character xxh3_128 hex string.
    """
    input_parts = []
    for role in sorted(input_spec.keys()):
        upstream_spec_id, upstream_role = input_spec[role]
        input_parts.append(f"{role}:{upstream_spec_id}:{upstream_role}")
    input_str = ",".join(input_parts)

    params_json = _canonicalize_dict(params)
    config_json = _canonicalize_dict(config_overrides)

    hash_input = (
        f"{operation_name}|{step_number}|{input_str}|{params_json}|{config_json}"
    )
    return xxhash.xxh3_128(hash_input.encode()).hexdigest()


def compute_chain_spec_id(
    operations: list[tuple[str, dict[str, Any] | None]],
    initial_inputs: dict[str, list[str]],
) -> str:
    """Compute deterministic spec ID for a chain of operations.

    Hashes all operation names and params in order, plus the sorted
    initial input artifact IDs. Any change to any operation's params
    or the input set invalidates the cache.

    Args:
        operations: List of (operation_name, params_dict) tuples in order.
        initial_inputs: Initial input artifact IDs keyed by role.

    Returns:
        32-character xxh3_128 hex string.
    """
    # Hash operations in order
    op_parts: list[str] = []
    for name, params in operations:
        params_json = _canonicalize_dict(params)
        op_parts.append(f"{name}:{params_json}")
    ops_str = "|".join(op_parts)

    # Hash sorted initial input IDs
    all_ids: set[str] = set()
    for role_ids in initial_inputs.values():
        all_ids.update(role_ids)
    sorted_ids = ",".join(sorted(all_ids)) if all_ids else ""

    hash_input = f"chain|{ops_str}|{sorted_ids}"
    return xxhash.xxh3_128(hash_input.encode()).hexdigest()


class _CanonicalEncoder(json.JSONEncoder):
    """JSON encoder that handles sets and Paths for deterministic output."""

    def default(self, o: Any) -> Any:
        if isinstance(o, set):
            return sorted(o)
        if isinstance(o, Path):
            return str(o)
        return super().default(o)


def _canonicalize_dict(input_dict: dict[str, Any] | None) -> str:
    """Canonicalize a dict to a deterministic JSON string.

    Args:
        input_dict: Dict to canonicalize, or None.

    Returns:
        JSON string with sorted keys and minimal whitespace.
        Returns empty string for None or empty dict.
    """
    if not input_dict:
        return ""
    return json.dumps(
        input_dict, sort_keys=True, separators=(",", ":"), cls=_CanonicalEncoder
    )
