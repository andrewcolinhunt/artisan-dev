"""Build a match map from output files to input artifacts by artifact_id prefix.

When inputs are materialized as {artifact_id}{extension}, output files
that preserve the artifact_id prefix can be matched back to their source
input without relying on original_name stem matching.
"""

from __future__ import annotations

from pathlib import Path

from artisan.utils.filename import strip_extensions


def build_filesystem_match_map(
    materialized_artifact_ids: set[str],
    output_files: list[Path],
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
        stem = strip_extensions(output_file.name)
        for input_id in materialized_artifact_ids:
            if stem.startswith(input_id):
                match_map[stem] = input_id
                break
    return match_map
