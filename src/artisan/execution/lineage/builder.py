"""Resolve declared output references into exact artifact derivation pairs."""

from __future__ import annotations

from artisan.execution.exceptions import LineageIntegrityError
from artisan.execution.inputs._validation import is_hex_id
from artisan.execution.lineage.enrich import require_artifact_type
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.provenance.lineage_mapping import LineageMapping
from artisan.schemas.provenance.source_target_pair import SourceTargetPair
from artisan.utils.hashing import canonical_json_bytes, compute_content_digest


def _output_id(artifacts: dict[str, list[Artifact]], role: str, index: int) -> str:
    """Resolve one explicit occurrence, rejecting missing or unfinished outputs."""
    if not 0 <= index < len(artifacts.get(role, [])):
        msg = f"Lineage references non-existent output {role!r}[{index}]"
        raise LineageIntegrityError(msg)
    artifact_id = artifacts[role][index].artifact_id
    if not is_hex_id(artifact_id):
        msg = f"Output {role!r}[{index}] has no valid finalized artifact ID"
        raise LineageIntegrityError(msg)
    return artifact_id


def _parent_group_id(parents: set[tuple[str, str, str]]) -> str | None:
    """Label the exact resolved parent set without adding any parents."""
    if len(parents) < 2:
        return None
    return compute_content_digest(
        canonical_json_bytes(
            {"domain": "lineage-parents-v1", "parents": sorted(parents)}
        )
    )


def build_edges(
    lineage: dict[str, list[LineageMapping]],
    finalized_artifacts: dict[str, list[Artifact]],
    artifact_types: dict[str, str],
) -> list[SourceTargetPair]:
    """Resolve role-local indices and label each occurrence's declared parents.

    Equivalent resolved parents collapse before grouping. Separate occurrences
    retain their own parent sets even when they finalize to an identical ID.

    Args:
        lineage: Operation-authored mappings keyed by target role.
        finalized_artifacts: Output lists in their original declaration order.
        artifact_types: Concrete source and target types keyed by artifact ID.

    Raises:
        LineageIntegrityError: If a reference cannot be resolved exactly.
    """
    parents_by_occurrence: dict[tuple[str, int], set[tuple[str, str, str]]] = {}
    for role, mappings in lineage.items():
        for mapping in mappings:
            target = _output_id(finalized_artifacts, role, mapping.draft_index)
            require_artifact_type(target, artifact_types)
            source = mapping.source_artifact_id
            if source is None:
                if mapping.source_output_index is None:
                    msg = "Lineage mapping has no source reference"
                    raise LineageIntegrityError(msg)
                source = _output_id(
                    finalized_artifacts,
                    mapping.source_role,
                    mapping.source_output_index,
                )
            if not is_hex_id(source):
                msg = f"Malformed lineage source ID: {source!r}"
                raise LineageIntegrityError(msg)
            source_type = require_artifact_type(source, artifact_types)
            parents_by_occurrence.setdefault((role, mapping.draft_index), set()).add(
                (mapping.source_role, source_type, source)
            )

    edges: list[SourceTargetPair] = []
    for (role, index), parents in parents_by_occurrence.items():
        group_id = _parent_group_id(parents)
        target = _output_id(finalized_artifacts, role, index)
        edges.extend(
            SourceTargetPair(
                source=source,
                target=target,
                source_role=source_role,
                target_role=role,
                group_id=group_id,
            )
            for source_role, _source_type, source in sorted(parents)
        )
    return edges
