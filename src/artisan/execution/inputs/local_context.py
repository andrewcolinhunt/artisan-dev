"""Capture explicit dependencies for one local definition call."""

from __future__ import annotations

from collections.abc import Mapping

from artisan.composites.base.compiled import CompiledPlan
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.execution_config import ExecutionConfigArtifact
from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.schemas.provenance.occurrence import OccurrenceAddress, ReferenceContext
from artisan.schemas.provenance.projection import (
    FrozenAssociation,
    FrozenBoundaryContext,
    FrozenConstant,
    FrozenReferences,
)
from artisan.schemas.specs.input_models import FrozenInputContext


def capture_local_context(
    plan: CompiledPlan,
    context: FrozenInputContext,
) -> tuple[FrozenBoundaryContext, dict[OccurrenceAddress, Artifact]]:
    """Bind each explicit membership to its supplied input occurrence."""
    requested = boundary_queries(plan)
    expected = {
        (role, index, kind)
        for role, values in context.artifacts.items()
        for index in range(len(values))
        for kind in requested.get(role, ())
    }
    if set(context.associations) != expected:
        msg = "Associated input context must exactly match requested input positions and types"
        raise ValueError(msg)
    inventory = {
        context.ref(role, index=index).address: artifact
        for role, values in context.artifacts.items()
        for index, artifact in enumerate(values)
    }
    captured: dict[OccurrenceAddress, Artifact] = {}

    def add(artifact: Artifact) -> OccurrenceAddress:
        address = OccurrenceAddress(
            invocation_id=f"{context.invocation_id}/context",
            role="captured",
            index=len(captured),
        )
        captured[address] = artifact
        return address

    associations = tuple(
        FrozenAssociation(
            primary=context.ref(role, index=index).address,
            artifact_type=kind,
            members=tuple(add(artifact) for artifact in members),
        )
        for (role, index, kind), members in context.associations.items()
    )
    references = [
        FrozenReferences(
            source=context.ref(role, index=index).address,
            members=tuple(add(artifact) for artifact in members.values()),
        )
        for (role, index), members in context.references.items()
    ]
    constants = tuple(
        FrozenConstant(
            call_path=binding.call_path,
            role=binding.role,
            occurrences=tuple(add(artifact) for artifact in binding.artifacts),
        )
        for binding in plan.constants
    )
    constant_addresses = {
        address for binding in constants for address in binding.occurrences
    }
    supplied = {item.source for item in references}
    for source, artifact in {**inventory, **captured}.items():
        if ArtifactTypeDef.get(artifact.artifact_type).reference_policy() is None:
            continue
        if source in supplied:
            continue
        if source in constant_addresses:
            required = ArtifactTypeDef.get(
                artifact.artifact_type
            ).resolve_required_references(
                artifact, ReferenceContext({**inventory, **captured})
            )
            references.append(
                FrozenReferences(
                    source=source, members=tuple(item.address for item in required)
                )
            )
            continue
        if (
            isinstance(artifact, ExecutionConfigArtifact)
            and artifact.get_artifact_references()
        ):
            msg = "Configuration references must be supplied explicitly for this input"
            raise ValueError(msg)
        references.append(FrozenReferences(source=source, members=()))
    return FrozenBoundaryContext(
        schema_version=2 if constants else 1,
        constants=constants,
        associations=associations,
        references=tuple(references),
    ), captured


def boundary_queries(plan: CompiledPlan) -> dict[str, set[str]]:
    """Follow only static boundary wiring; produced values get graph-local queries."""
    queries: dict[str, set[str]] = {}

    def visit(
        node: CompiledPlan, origins: Mapping[str, set[str]]
    ) -> dict[str, set[str]]:
        for spec in node.recipe.inputs:
            for role in origins.get(spec.role, ()):
                queries.setdefault(role, set()).update(spec.with_associated)
        calls: dict[str, dict[str, set[str]]] = {}
        for compiled, recorded in zip(node.calls, node.recipe.calls, strict=True):
            child = {
                binding.role: set().union(
                    *(
                        origins.get(source.role, set())
                        if source.call is None
                        else calls[source.call].get(source.role, set())
                        for source in binding.sources
                    )
                )
                for binding in recorded.inputs
            }
            calls[compiled.name] = visit(compiled.plan, child)
        return {
            binding.role: set().union(
                *(
                    origins.get(source.role, set())
                    if source.call is None
                    else calls[source.call].get(source.role, set())
                    for source in binding.sources
                )
            )
            for binding in node.recipe.exports
        }

    visit(plan, {spec.role: {spec.role} for spec in plan.recipe.inputs})
    return queries
