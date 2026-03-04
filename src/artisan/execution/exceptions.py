"""Execution-layer exceptions for artifact, lineage, and passthrough validation."""

from __future__ import annotations


class ArtifactValidationError(Exception):
    """Raised when artifacts don't match output specs.

    This includes:
    - Missing required output roles
    - Empty artifact lists for required roles
    - Artifact type mismatches (e.g., DataArtifact for METRIC spec)
    - Unexpected output roles not declared in specs
    """


class LineageCompletenessError(Exception):
    """Raised when artifacts are missing lineage mappings.

    Non-orphan outputs (those without infer_lineage_from={"inputs": []})
    must have lineage mappings declaring their source artifacts.
    """


class LineageIntegrityError(Exception):
    """Raised when lineage references are invalid.

    This includes:
    - Source artifact_id references a non-existent input or output artifact
    - Draft original_name references a non-existent output artifact
    - Duplicate lineage mappings for the same draft artifact
    """


class PassthroughValidationError(Exception):
    """Raised when passthrough result validation fails.

    This includes:
    - Missing output role in passthrough dict
    - Empty artifact ID list for a required output role
    - Invalid artifact ID references
    """
