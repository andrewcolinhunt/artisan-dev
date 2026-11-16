"""Portable scientific-shape fixtures shared with durable storage probes.

CSV artifacts stand in for structures so the proof needs no application package
or GPU. Metrics remain ordinary MetricArtifact values with authored parents.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from artisan.schemas import (
    Artifact,
    ArtifactResult,
    CSVArtifact,
    ExecutionConfigArtifact,
    MetricArtifact,
    OutputSpec,
)

from .graph import Node, OccurrenceGraph, PreciseLineageMapping, RequiredReference
from .projection import BoundaryProjection, FrozenContext, FrozenReferences


@dataclass(frozen=True)
class ProjectionCase:
    """A closed projection and the real finalized artifacts it references."""

    name: str
    projection: BoundaryProjection
    artifacts: Mapping[str, Artifact]

    def payloads(self) -> dict[str, bytes]:
        """Encode complete artifact values, including protected identity metadata."""
        return {
            artifact_id: artifact.model_dump_json().encode()
            for artifact_id, artifact in self.artifacts.items()
        }

    def boundary_payloads(self) -> dict[str, tuple[str, bytes]]:
        """Return trusted admitted content to seed before worker publication."""
        boundary = self.projection.boundary_keys()
        payloads = self.payloads()
        return {
            item.artifact_id: (item.artifact_type, payloads[item.artifact_id])
            for item in self.projection.occurrences
            if item.key in boundary
        }

    def staged_payloads(self) -> dict[str, bytes]:
        """Return only worker-created content absent from trusted boundary inputs."""
        boundary_ids = self.boundary_payloads().keys()
        return {
            artifact_id: payload
            for artifact_id, payload in self.payloads().items()
            if artifact_id not in boundary_ids
        }


def structure(value: int, *, name: str = "structure", step: int = 1) -> Artifact:
    """Create real finalized content with deterministic synthetic geometry."""
    return CSVArtifact.draft(
        content=f"x,y,z\n{value},0,0\n".encode(),
        original_name=f"{name}.csv",
        step_number=step,
    ).finalize()


def metric(values: dict[str, object], *, name: str = "metrics") -> Artifact:
    """Create an ordinary finalized metric without any retention/category tag."""
    return MetricArtifact.draft(
        content=values, original_name=name, step_number=1
    ).finalize()


def derive(
    graph: OccurrenceGraph,
    invocation: str,
    artifact: Artifact,
    inputs: Mapping[str, Sequence[str]],
    *,
    role: str = "result",
) -> str:
    """Record exact input occurrence parents through the lineage extension."""
    mappings = [
        PreciseLineageMapping(
            draft_index=0,
            source_role=input_role,
            source_artifact_id=graph.nodes[key].artifact.artifact_id,
            source_input_index=index,
            source_invocation=invocation,
        )
        for input_role, keys in inputs.items()
        for index, key in enumerate(keys)
    ]
    result = ArtifactResult(artifacts={role: [artifact]}, lineage={role: mappings})
    return graph.record_result(
        invocation,
        inputs,
        result,
        {
            role: OutputSpec(
                artifact_type=artifact.artifact_type,
                derives_from={"inputs": list(inputs)},
            )
        },
    )[role][0]


def make_case(
    name: str,
    graph: OccurrenceGraph,
    exports: Mapping[str, Sequence[str]],
    *,
    frozen_context: FrozenContext | None = None,
) -> ProjectionCase:
    """Select only boundary and retained content for durable transport."""
    projection = graph.project(
        exports,
        {
            role: OutputSpec(
                artifact_type=graph.nodes[keys[0]].artifact.artifact_type,
            )
            for role, keys in exports.items()
        },
        frozen_context=frozen_context,
    )
    kept = {item.key for item in projection.occurrences}
    artifacts: dict[str, Artifact] = {}
    for key, node in graph.nodes.items():
        if key in kept:
            artifacts.setdefault(node.artifact.artifact_id, node.artifact)
    return ProjectionCase(name, projection, artifacts)


def prediction_measurement_case() -> ProjectionCase:
    """Retain prediction and its direct measurement under one boundary."""
    graph = OccurrenceGraph({"csv": None, "metric": None})
    source = graph.add_input("source", structure(0, name="source", step=0))
    predicted = derive(graph, "predict", structure(1), {"source": [source]})
    measured = derive(
        graph, "measure", metric({"score": 0.8}), {"structure": [predicted]}
    )
    return make_case(
        "prediction_measurement",
        graph,
        {"structure": [predicted], "metrics": [measured]},
    )


def alternating_reducer_case() -> ProjectionCase:
    """Discard internal geometry and measurements after ordinary metric reduction."""
    graph = OccurrenceGraph({"csv": None, "metric": None})
    current = graph.add_input("structure", structure(0, name="source", step=0))
    history: list[str] = []
    for iteration in range(3):
        current = derive(
            graph,
            f"a_{iteration}",
            structure(2 * iteration + 1),
            {"structure": [current]},
        )
        current = derive(
            graph,
            f"b_{iteration}",
            structure(2 * iteration + 2),
            {"structure": [current]},
        )
        history.append(
            derive(
                graph,
                f"measure_{iteration}",
                metric({"score": iteration + 1}),
                {"structure": [current]},
            )
        )
    summary = derive(
        graph,
        "summarize",
        metric({"scores": [1, 2, 3], "maximum": 3}),
        {"final_structure": [current], "history": history},
    )
    return make_case(
        "alternating_reducer",
        graph,
        {"structure": [current], "metrics": [summary]},
    )


def equal_content_branches_case() -> ProjectionCase:
    """Export B/A/B occurrences without merging independent parent branches."""
    graph = OccurrenceGraph({"csv": None, "metric": None})
    first = graph.add_input("source", structure(1, name="source_a", step=0))
    second = graph.add_input("source", structure(2, name="source_b", step=0))
    branch_a = derive(graph, "branch_a", metric({"score": 1}), {"source": [first]})
    branch_b = derive(graph, "branch_b", metric({"score": 1}), {"source": [second]})
    return make_case(
        "equal_content_branches", graph, {"metrics": [branch_b, branch_a, branch_b]}
    )


def scientific_cases() -> tuple[ProjectionCase, ...]:
    """Return the independent deterministic cases used by storage qualification."""
    return (
        prediction_measurement_case(),
        alternating_reducer_case(),
        equal_content_branches_case(),
    )


def alias_identity_case() -> ProjectionCase:
    """Reuse one content row while preserving the selected producer occurrences."""
    graph = OccurrenceGraph({"csv": None})
    source = graph.add_input("structure", structure(1, step=0))
    original = derive(graph, "original", structure(1, step=2), {"structure": [source]})
    alias = graph.add_alias("route", "structure", original)
    copied = derive(graph, "copy", structure(1, step=3), {"structure": [alias]})
    return make_case("alias_identity", graph, {"structure": [alias, copied, alias]})


def configuration_references(
    node: Node, nodes: Mapping[str, Node]
) -> tuple[RequiredReference, ...]:
    """Resolve explicit config markers through original direct input parents."""
    assert isinstance(node.artifact, ExecutionConfigArtifact)
    references: list[RequiredReference] = []
    for artifact_id in node.artifact.get_artifact_references():
        matches = [
            key
            for key in node.parents
            if nodes[key].artifact.artifact_id == artifact_id
        ]
        if len(matches) != 1:
            msg = "Configuration reference has no unique admitted direct parent"
            raise ValueError(msg)
        target = nodes[matches[0]]
        references.append(
            RequiredReference(target.key, artifact_id, target.artifact.artifact_type)
        )
    return tuple(references)


def required_support_case() -> ProjectionCase:
    """Retain a configuration's transitive dependencies without exporting them."""
    graph = OccurrenceGraph({"csv": None, "config": configuration_references})
    source = graph.add_input("source", structure(0, name="source", step=0))
    hidden = derive(graph, "predict", structure(1), {"source": [source]})
    first = ExecutionConfigArtifact.draft(
        content={"geometry": {"$artifact": graph.nodes[hidden].artifact.artifact_id}},
        original_name="first.json",
        step_number=1,
    ).finalize()
    first_key = derive(graph, "first_config", first, {"geometry": [hidden]})
    final = ExecutionConfigArtifact.draft(
        content={"configuration": {"$artifact": first.artifact_id}},
        original_name="final.json",
        step_number=1,
    ).finalize()
    final_key = derive(graph, "final_config", final, {"configuration": [first_key]})
    return make_case("required_support", graph, {"configuration": [final_key]})


def frozen_configuration_case() -> ProjectionCase:
    """Carry an input config's captured external referent through a native result."""
    graph = OccurrenceGraph({"csv": None})
    geometry = structure(1, step=0)
    config = ExecutionConfigArtifact.draft(
        content={"input": {"$artifact": geometry.artifact_id}},
        original_name="input.json",
        step_number=0,
    ).finalize()
    config_key = graph.add_input("config", config)
    geometry_key = graph.add_context_input("captured_structure", geometry)
    result = derive(graph, "native", structure(2), {"config": [config_key]})
    return make_case(
        "frozen_configuration",
        graph,
        {"structure": [result]},
        frozen_context=FrozenContext(
            references=(FrozenReferences(config=config_key, members=(geometry_key,)),)
        ),
    )
