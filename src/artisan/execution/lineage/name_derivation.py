"""Derive human-readable names for output artifacts after lineage.

After lineage edges are established via the filesystem match map,
this module overwrites transient artifact_id-based original_names
with human-readable names derived from input artifact names.
"""

from __future__ import annotations

from artisan.schemas.artifact.base import Artifact
from artisan.schemas.provenance.source_target_pair import SourceTargetPair


def derive_human_names(
    output_artifacts: dict[str, list[Artifact]],
    lineage_edges: list[SourceTargetPair],
    input_artifacts: dict[str, list[Artifact]],
    match_map: dict[str, str],
) -> None:
    """Overwrite original_name on outputs with derived human names.

    For each output matched via the filesystem match map:
    - Find the matched input's original_name (human-readable)
    - Extract the operation-applied suffix from the output name
    - Apply the suffix to the input's human name
    - Overwrite original_name on the output artifact

    Modifies artifacts in place. Must be called before staging.

    Args:
        output_artifacts: Role-keyed dict of finalized output artifacts.
        lineage_edges: Provenance edges (unused currently, reserved for
            future derivation strategies).
        input_artifacts: Role-keyed dict of input artifacts with human names.
        match_map: Dict mapping output filename stem to input artifact_id.
    """
    input_names: dict[str, str] = {}
    for role_artifacts in input_artifacts.values():
        for art in role_artifacts:
            if art.artifact_id and hasattr(art, "original_name") and art.original_name:
                input_names[art.artifact_id] = art.original_name

    for role_artifacts in output_artifacts.values():
        for art in role_artifacts:
            output_name = getattr(art, "original_name", None)
            if output_name is None:
                continue
            input_id = match_map.get(output_name)
            if input_id is None:
                continue
            input_name = input_names.get(input_id)
            if input_name is None:
                continue

            # Extract suffix: "abc123_scored" - "abc123" = "_scored"
            suffix = output_name[len(input_id):]
            art.original_name = f"{input_name}{suffix}"
