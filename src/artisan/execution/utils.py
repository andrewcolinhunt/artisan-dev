"""Shared utility helpers for execution flows.

Provides execution-run ID generation, artifact finalization, and
passthrough result validation.
"""

from __future__ import annotations

from datetime import datetime

from artisan.execution.exceptions import PassthroughValidationError
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.execution.curator_result import PassthroughResult
from artisan.schemas.specs.output_spec import OutputSpec


def generate_execution_run_id(
    spec_id: str,
    timestamp: datetime,
    worker_id: int = 0,
) -> str:
    """Generate a deterministic execution run ID from spec, timestamp, and worker.

    Returns:
        32-character xxh3_128 hex digest.
    """
    from artisan.utils.hashing import digest_utf8

    return digest_utf8(f"{spec_id}:{timestamp.isoformat()}:{worker_id}")


def finalize_artifacts(
    draft_artifacts: dict[str, list[Artifact]],
) -> dict[str, list[Artifact]]:
    """Finalize all draft artifacts, computing content hashes and IDs."""
    return {
        role: [draft.finalize() for draft in drafts]
        for role, drafts in draft_artifacts.items()
    }


def validate_passthrough_result(
    result: PassthroughResult,
    output_specs: dict[str, OutputSpec],
) -> None:
    """Validate passthrough result before staging.

    Ensures all required output roles are present in the passthrough result
    and that they contain valid artifact IDs.

    Args:
        result: PassthroughResult from a passthrough curator operation.
        output_specs: Dict of role -> OutputSpec from operation.outputs.

    Raises:
        PassthroughValidationError: If validation fails.

    Example:
        >>> result = PassthroughResult(passthrough={"filtered": ["abc123..."]})
        >>> specs = {"filtered": OutputSpec(artifact_type=ArtifactTypes.ANY)}
        >>> validate_passthrough_result(result, specs)  # No error
    """
    for role, spec in output_specs.items():
        # Only validate required outputs
        if not spec.required:
            continue

        if role not in result.passthrough:
            message = f"Missing required output role: {role}"
            raise PassthroughValidationError(message)

        artifact_ids = result.passthrough[role]
        if not artifact_ids:
            message = f"Empty artifact ID list for required output role: {role}"
            raise PassthroughValidationError(message)
