"""Hashing utilities for content-addressed artifact and execution IDs.

Provides xxh3_128-based hash functions for artifact content addressing,
execution spec deduplication, and step-level cache keys.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from typing import Any, BinaryIO

import xxhash

from artisan.utils.json import artisan_json_default

STREAM_CHUNK_BYTES = 1024 * 1024


@dataclass(frozen=True, slots=True)
class CacheInputIdentity:
    """One ordered, typed input occurrence in a cache preimage."""

    role: str
    group_id: str | None
    position: int
    artifact_type: str
    artifact_id: str


class _CanonicalEncoder(json.JSONEncoder):
    """JSON encoder that handles sets, Paths, and Enums for deterministic output."""

    def default(self, o: Any) -> Any:
        return artisan_json_default(o)


def canonical_json_bytes(value: Any) -> bytes:
    """Encode a value as deterministic compact UTF-8 JSON."""
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        cls=_CanonicalEncoder,
    ).encode("utf-8")


def compute_content_digest(content: bytes) -> str:
    """Compute the xxh3_128 digest of raw bytes.

    Args:
        content: Raw bytes to hash.

    Returns:
        32-character hexadecimal hash string.
    """
    return xxhash.xxh3_128(content).hexdigest()


def compute_stream_digest(stream: BinaryIO) -> tuple[str, int]:
    """Hash a binary stream using bounded reads.

    Args:
        stream: Binary stream positioned at the first byte to hash.

    Returns:
        Tuple of the 32-character digest and total byte count.
    """
    hasher = xxhash.xxh3_128()
    size_bytes = 0
    while chunk := stream.read(STREAM_CHUNK_BYTES):
        hasher.update(chunk)
        size_bytes += len(chunk)
    return hasher.hexdigest(), size_bytes


def compute_artifact_id(
    artifact_type: str,
    canonical_content: bytes,
    identity_metadata: dict[str, object],
) -> str:
    """Compute a versioned, type-domain artifact ID.

    Args:
        artifact_type: Registered concrete artifact type key.
        canonical_content: Type-owned canonical identity bytes.
        identity_metadata: Framework-owned semantic metadata.

    Returns:
        32-character lowercase hexadecimal artifact identifier.
    """
    preimage = bytearray(b"artifact-id-v1")
    for component in (
        artifact_type.encode("utf-8"),
        canonical_content,
        canonical_json_bytes(identity_metadata),
    ):
        preimage.extend(len(component).to_bytes(8, "big"))
        preimage.extend(component)
    return compute_content_digest(bytes(preimage))


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
    the caller typed at the call site. Every cache-affecting key is
    always present.

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
    inputs: dict[str, list[CacheInputIdentity]],
    params: dict[str, Any] | None = None,
    config_overrides: dict[str, Any] | None = None,
) -> str:
    """Compute a v2 execution cache ID from concrete ordered inputs.

    The spec_id uniquely identifies an execution based on:
    - operation_name: The operation's name attribute
    - inputs: All artifact IDs being processed, keyed by role
    - params: Merged parameters (defaults + overrides)
    - config_overrides: Runtime config overrides (environment, tool, etc.)

    Args:
        operation_name: The operation's name attribute.
        inputs: Concrete cache identities keyed by role. Role mapping order is
            ignored; item order and multiplicity within each role are retained.
        params: Merged parameters dict (defaults + runtime overrides).
            Will be JSON-canonicalized for deterministic hashing.
        config_overrides: Optional config overrides that affect execution
            behavior (merged environment + tool overrides).

    Returns:
        32-character xxh3_128 hex string.
    """
    payload = {
        "domain": "execution-spec-v2",
        "operation": operation_name,
        "params": params or {},
        "config": config_overrides or {},
        "inputs": _serialize_cache_inputs(inputs),
    }
    return compute_content_digest(canonical_json_bytes(payload))


def compute_step_spec_id(
    operation_name: str,
    step_number: int,
    params: dict[str, Any] | None,
    inputs: dict[str, list[CacheInputIdentity]],
    config_overrides: dict[str, Any] | None = None,
) -> str:
    """Compute a v2 step cache ID from concrete ordered inputs.

    Args:
        operation_name: The operation's name attribute.
        step_number: Position in the pipeline (0-based).
        params: Merged parameters dict.
        inputs: Full prepared concrete cache identities keyed by role.
        config_overrides: Optional config overrides that affect execution
            behavior (merged environment + tool overrides).

    Returns:
        32-character xxh3_128 hex string.
    """
    payload = {
        "domain": "step-spec-v2",
        "operation": operation_name,
        "step_number": step_number,
        "params": params or {},
        "config": config_overrides or {},
        "inputs": _serialize_cache_inputs(inputs),
    }
    return compute_content_digest(canonical_json_bytes(payload))


def _serialize_cache_inputs(
    inputs: dict[str, list[CacheInputIdentity]],
) -> list[dict[str, object]]:
    """Serialize role-keyed inputs without erasing occurrence order."""
    serialized: list[dict[str, object]] = []
    for role in sorted(inputs):
        entries = inputs[role]
        for entry in entries:
            if entry.role != role:
                msg = (
                    f"Cache input role mismatch: mapping key {role!r}, "
                    f"entry role {entry.role!r}"
                )
                raise ValueError(msg)
            serialized.append(asdict(entry))
    return serialized
