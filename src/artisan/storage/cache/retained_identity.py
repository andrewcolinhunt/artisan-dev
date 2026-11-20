"""Scientific cache identity independent of fresh consuming occurrence owners."""

from __future__ import annotations

from artisan.schemas.composites.plan import ExecutionRecipe
from artisan.schemas.execution.assignment import AssignmentAttempt
from artisan.schemas.provenance.occurrence import OccurrenceAddress
from artisan.schemas.provenance.projection import BoundaryProjection
from artisan.utils.hashing import canonical_json_bytes, compute_content_digest


def retained_cache_key(
    recipe: ExecutionRecipe,
    attempt: AssignmentAttempt,
    inputs: BoundaryProjection,
) -> str | None:
    """Bind admitted science/software/input semantics, or disable ordinary reuse.

    Input admission must precede cache lookup. Fresh consuming addresses are
    normalized, while selected accepted producers remain exact. This computes
    identity only: a matching result still requires the durable winner and
    completed logical commit before either cache may return it.
    """
    attempt = AssignmentAttempt.model_validate(attempt)
    inputs = BoundaryProjection.model_validate(inputs)
    if (
        recipe.digest != attempt.recipe_digest
        or inputs.input_context_digest != attempt.input_context_digest
    ):
        msg = "Cache identity requires the exact admitted recipe and input context"
        raise ValueError(msg)
    if (
        not recipe.cacheable
        or not attempt.software_cacheable
        or attempt.replay_of_execution_run_id is not None
    ):
        return None
    descriptor = {
        "recipe": recipe.digest,
        "software": attempt.software_digest,
        "inputs": _input_semantics(inputs),
    }
    return compute_content_digest(
        b"artisan.retained-cache-v1\0" + canonical_json_bytes(descriptor)
    )


def _input_semantics(inputs: BoundaryProjection) -> dict[str, object]:
    slots: dict[OccurrenceAddress, int] = {}

    def slot(address: OccurrenceAddress) -> int:
        return slots.setdefault(address, len(slots))

    roles = [
        {
            "role": group.role,
            "members": [slot(address) for address in group.occurrences],
        }
        for group in inputs.boundary_inputs
    ]
    associations = [
        {
            "primary": slot(capture.primary),
            "artifact_type": capture.artifact_type,
            "members": [slot(address) for address in capture.members],
        }
        for capture in inputs.frozen_context.associations
    ]
    references = [
        {
            "source": slot(capture.source),
            "members": [slot(address) for address in capture.members],
            "direct_parents": [slot(address) for address in capture.direct_parents],
        }
        for capture in inputs.frozen_context.references
    ]
    constants = [
        {
            "call_path": list(capture.call_path),
            "role": capture.role,
            "members": [slot(address) for address in capture.occurrences],
        }
        for capture in inputs.frozen_context.constants
    ]
    occurrences = {item.address: item for item in inputs.occurrences}
    attribution = {
        item.boundary: item.producer.model_dump(mode="json")
        if item.producer is not None
        else None
        for item in inputs.input_attribution
    }
    content = [
        {
            "artifact_id": occurrences[address].artifact_id,
            "artifact_type": occurrences[address].artifact_type,
            "producer": attribution[address],
        }
        for address in slots
    ]
    descriptor = {
        "roles": roles,
        "content": content,
        "associations": associations,
        "references": references,
    }
    if constants:
        descriptor["constants"] = constants
    return descriptor
