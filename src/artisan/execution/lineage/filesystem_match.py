"""Build a match map from output names to input artifacts by artifact_id prefix.

When inputs are materialized as {artifact_id}{extension}, output files
and artifacts that preserve the artifact_id prefix can be matched back
to their source input without relying on original_name stem matching.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

from artisan.utils.filename import strip_extensions

if TYPE_CHECKING:
    from artisan.schemas.artifact.base import Artifact


def build_filesystem_match_map(
    materialized_artifact_ids: set[str],
    output_files: list[str],
) -> dict[str, str]:
    """Match output files to input artifacts by artifact_id prefix.

    For each output file, checks whether its stem starts with any
    materialized input artifact_id. This works because inputs are
    materialized as {artifact_id}{extension}, so operations that
    derive output filenames from input filenames naturally preserve
    the artifact_id prefix.

    Args:
        materialized_artifact_ids: Set of input artifact_ids that were
            materialized to disk.
        output_files: Output files produced by the operation.

    Returns:
        Dict mapping output filename stem to input artifact_id.
    """
    match_map: dict[str, str] = {}
    for output_file in output_files:
        stem = strip_extensions(os.path.basename(output_file))
        for input_id in materialized_artifact_ids:
            if stem.startswith(input_id):
                match_map[stem] = input_id
                break
    return match_map


def augment_match_map_from_artifacts(
    match_map: dict[str, str],
    materialized_artifact_ids: set[str],
    output_artifacts: dict[str, list[Artifact]],
) -> None:
    """Add entries to the match map from output artifact original_names.

    Operations that use memory_outputs (not filesystem) still derive
    output names from input file stems. This function catches those
    cases by checking artifact original_name values against the
    materialized input artifact_ids.

    Modifies match_map in place.

    Args:
        match_map: Existing match map to augment.
        materialized_artifact_ids: Set of input artifact_ids that were
            materialized to disk.
        output_artifacts: Role-keyed finalized output artifacts.
    """
    for role_artifacts in output_artifacts.values():
        for art in role_artifacts:
            name = getattr(art, "original_name", None)
            if name is None or name in match_map:
                continue
            for input_id in materialized_artifact_ids:
                if name.startswith(input_id):
                    match_map[name] = input_id
                    break
