"""Hashing utilities for content-addressed artifact and execution IDs.

Provides xxh3_128-based hash functions for artifact content addressing,
execution spec deduplication, and step-level cache keys.
"""

from __future__ import annotations

import json
from typing import Any

import xxhash

from artisan.utils.json import artisan_json_default


def compute_artifact_id(content: bytes) -> str:
    """Compute xxh3_128 hash for content-addressed artifact ID.

    Args:
        content: Raw bytes to hash.

    Returns:
        32-character hexadecimal hash string.
    """
    return xxhash.xxh3_128(content).hexdigest()


def digest_utf8(s: str) -> str:
    """Compute xxh3_128 hex digest of a UTF-8 string.

    Args:
        s: String to hash.

    Returns:
        32-character hexadecimal hash string.
    """
    return xxhash.xxh3_128(s.encode()).hexdigest()


def serialize_params(operation: Any) -> dict[str, Any]:
    """Safely serialize operation/composite params to a JSON-ready dict.

    Args:
        operation: An OperationDefinition or CompositeDefinition instance.

    Returns:
        JSON-serializable dict, or empty dict if no params.
    """
    params = getattr(operation, "params", None)
    if params is None or not hasattr(params, "model_dump"):
        return {}
    dumped: dict[str, Any] = params.model_dump(mode="json")
    return dumped


def effective_config_payload(operation: Any) -> dict[str, Any]:
    """Serialize the effective, cache-affecting config off an op instance.

    The config counterpart to ``serialize_params``: both read the merged
    values (class defaults + applied overrides) off an already-instantiated
    operation, so the cache key reflects what actually runs — not just what
    the caller typed at the call site. Every key is always present; the
    legacy omit-when-unset rule is dropped.

    Args:
        operation: A fully-instantiated ``OperationDefinition`` (post
            ``instantiate_operation``). Duck-typed to keep ``utils`` free
            of an ``operations`` import, mirroring ``serialize_params``.

    Returns:
        JSON-ready dict fed to ``compute_step_spec_id`` /
        ``compute_execution_spec_id`` as ``config_overrides``.
    """
    tool = operation.tool
    group_by = operation.group_by
    return {
        "version": type(operation).version,
        "environments": operation.environments.model_dump(mode="json"),
        "tool": tool.model_dump(mode="json") if tool is not None else None,
        "compute_provider": operation.compute_provider.model_dump(mode="json"),
        "compute_resources": operation.compute_resources.model_dump(mode="json"),
        "group_by": group_by.value if group_by is not None else None,
    }


def compute_execution_spec_id(
    operation_name: str,
    inputs: dict[str, list[str]],
    params: dict[str, Any] | None = None,
    config_overrides: dict[str, Any] | None = None,
) -> str:
    """Compute deterministic execution_spec_id with canonicalization.

    The spec_id uniquely identifies an execution based on:
    - operation_name: The operation's name attribute
    - inputs: All artifact IDs being processed, keyed by role
    - params: Merged parameters (defaults + overrides)
    - config_overrides: Runtime config overrides (environment, tool, etc.)

    Inputs are canonicalized as a role-sorted list of sorted IDs.
    Multiplicity within a role is preserved (``[A, A]`` differs from
    ``[A]``) and role assignment matters (``{primary:[A], secondary:[B]}``
    differs from ``{primary:[B], secondary:[A]}``). Role-key ordering of
    the input dict is irrelevant — roles are sorted before hashing.

    Args:
        operation_name: The operation's name attribute.
        inputs: Dict mapping role to list of artifact IDs (the batch).
            Multiplicity within each role list is preserved.
        params: Merged parameters dict (defaults + runtime overrides).
            Will be JSON-canonicalized for deterministic hashing.
        config_overrides: Optional config overrides that affect execution
            behavior (merged environment + tool overrides).

    Returns:
        32-character xxh3_128 hex string.
    """
    if inputs:
        parts = [
            f"{role}=[{','.join(sorted(inputs[role]))}]"
            for role in sorted(inputs.keys())
        ]
        inputs_str = "|".join(parts)
    else:
        inputs_str = ""

    params_json = _canonicalize_dict(params)
    config_json = _canonicalize_dict(config_overrides)

    hash_input = f"{operation_name}|{inputs_str}|{params_json}|{config_json}"

    return digest_utf8(hash_input)


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
    input_str = _serialize_input_spec(input_spec)

    params_json = _canonicalize_dict(params)
    config_json = _canonicalize_dict(config_overrides)

    hash_input = (
        f"{operation_name}|{step_number}|{input_str}|{params_json}|{config_json}"
    )
    return digest_utf8(hash_input)


def _serialize_input_spec(input_spec: dict[str, tuple[str, str]]) -> str:
    """Serialize an input_spec dict to a deterministic string for hashing."""
    parts = []
    for role in sorted(input_spec.keys()):
        upstream_spec_id, upstream_role = input_spec[role]
        parts.append(f"{role}:{upstream_spec_id}:{upstream_role}")
    return ",".join(parts)


class _CanonicalEncoder(json.JSONEncoder):
    """JSON encoder that handles sets and Paths for deterministic output."""

    def default(self, o: Any) -> Any:
        return artisan_json_default(o)


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
