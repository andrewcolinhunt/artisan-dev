"""Shared styling constants for provenance graph visualization.

Both micro (artifact-level) and macro (step-level) graphs import from here
to ensure consistent visual language.
"""

from __future__ import annotations

from artisan.schemas.artifact.registry import ArtifactTypeDef

# Shape and fill color for execution nodes (grey boxes)
EXECUTION_STYLE = ("box", "lightgray")

# Style for passthrough data nodes (null artifact type in output_types)
PASSTHROUGH_STYLE = ("box", "#E0F0FF")  # Very light blue

# Blue palette assigned by registration order
_BLUE_PALETTE = [
    "#87CEEB",  # Sky blue
    "#B0E0E6",  # Powder blue
    "#ADD8E6",  # Light blue
    "#B0C4DE",  # Light steel blue
    "#A8D0E6",
    "#90C8E8",
]

_DEFAULT_ARTIFACT_COLOR = "#A0C8E0"


def get_artifact_style(artifact_type: str) -> tuple[str, str]:
    """Return (shape, fill_color) for an artifact type.

    Colors are assigned from a blue palette by registration order.
    Unknown types get a default blue.

    Args:
        artifact_type: Artifact type key string.

    Returns:
        Tuple of (shape, fill_color_hex).
    """
    all_types = list(ArtifactTypeDef.get_all().keys())
    try:
        idx = all_types.index(artifact_type)
        color = _BLUE_PALETTE[idx % len(_BLUE_PALETTE)]
    except ValueError:
        color = _DEFAULT_ARTIFACT_COLOR
    return ("box", color)
