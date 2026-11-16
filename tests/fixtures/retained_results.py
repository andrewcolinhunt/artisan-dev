"""Small real-artifact retained results for storage and process-boundary tests."""

from __future__ import annotations

import json
import os
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

from artisan.execution.lineage.graph import OccurrenceGraph
from artisan.schemas import (
    AppendableArtifact,
    Artifact,
    ArtifactResult,
    CSVArtifact,
    ExecutionConfigArtifact,
    FileRefArtifact,
    OutputSpec,
)
from artisan.schemas.composites.plan import (
    DefinitionIdentity,
    ExecutionContract,
    ExecutionRecipe,
    InputContract,
    InvocationPolicy,
    OutputContract,
)
from artisan.schemas.execution.assignment import (
    AssignmentAttempt,
    AssignmentIdentity,
    DeliveryIdentity,
    SuccessfulDelivery,
)
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.schemas.provenance.occurrence import InputReference, OccurrenceAddress
from artisan.schemas.provenance.projection import BoundaryProjection
from artisan.storage.io.retained_seal import stage_retained_result
from artisan.utils.hashing import compute_content_digest


@dataclass(frozen=True)
class RetainedCase:
    projection: BoundaryProjection
    boundary: Mapping[OccurrenceAddress, Artifact]
    artifacts: Mapping[OccurrenceAddress, Artifact]

    @property
    def recipe(self) -> ExecutionRecipe:
        kinds = {
            item.address: item.artifact_type for item in self.projection.occurrences
        }
        return ExecutionRecipe(
            definition=DefinitionIdentity(
                kind="creator",
                name="RetainedFixture",
                version="1",
                module=__name__,
                qualname="RetainedFixture",
            ),
            params_json="{}",
            inputs=tuple(
                InputContract(
                    role=group.role,
                    artifact_type="any",
                    required=True,
                    materialize=False,
                    hydrate=True,
                    materialize_as=None,
                    resolve_references=False,
                    with_associated=(),
                )
                for group in self.projection.boundary_inputs
            ),
            outputs=tuple(
                OutputContract(
                    role=group.role,
                    artifact_type=kinds[group.occurrences[0]]
                    if group.occurrences
                    else "any",
                    required=bool(group.occurrences),
                    parent_kind=None,
                    parent_roles=(),
                )
                for group in self.projection.exports
            ),
            policy=InvocationPolicy(),
            execution=ExecutionContract(mode="native"),
            cacheable=True,
        )

    def completion(self) -> SuccessfulDelivery:
        attempt = AssignmentAttempt(
            assignment=AssignmentIdentity(step_run_id="a" * 32, unit_index=0),
            attempt=0,
            generation=0,
            recipe_digest=self.recipe.digest,
            software_digest="c" * 64,
            input_context_digest=self.projection.input_context_digest,
        )
        return SuccessfulDelivery(
            delivery=DeliveryIdentity.new(attempt),
            computation_complete=True,
            cleanup_complete=True,
        )


def boundary_artifact() -> Artifact:
    return CSVArtifact.draft(b"x\n1\n", "input.csv", 1).finalize()


def _derive(
    graph: OccurrenceGraph,
    owner: str,
    artifact: Artifact,
    parents: Sequence[OccurrenceAddress],
) -> OccurrenceAddress:
    result = ArtifactResult()
    result.add_artifact(
        "output",
        artifact,
        sources={
            "data": [
                InputReference(
                    invocation_id=owner,
                    role="data",
                    index=index,
                    artifact_id=graph.nodes[parent].artifact.artifact_id,
                )
                for index, parent in enumerate(parents)
            ]
        },
    )
    return graph.record_result(
        owner,
        {"data": parents},
        result,
        {"output": OutputSpec(derives_from={"inputs": ["data"]})},
    )["output"][0]


def retained_case(scratch: Path, *, kind: str = "file") -> RetainedCase:
    graph = OccurrenceGraph()
    if kind == "empty":
        return RetainedCase(
            graph.project({"optional": []}, {"optional": OutputSpec(required=False)}),
            {},
            {},
        )
    source = graph.add_input("data", boundary_artifact())
    if kind == "alias":
        return RetainedCase(
            graph.project({"data": [source, source]}, {"data": OutputSpec()}),
            {source: graph.nodes[source].artifact},
            {},
        )
    scratch.mkdir(parents=True, exist_ok=True)
    if kind == "appendable":
        path = scratch / "records.jsonl"
        records = [
            json.dumps({"record_id": name, "value": index}).encode()
            for index, name in enumerate(("first", "second"))
        ]
        path.write_bytes(b"\n".join(records) + b"\n")
        outputs = [
            AppendableArtifact.draft(
                name, compute_content_digest(record), len(record), 2, str(path)
            ).finalize()
            for name, record in zip(("first", "second"), records, strict=True)
        ]
    else:
        outputs = _file_outputs(scratch)
    files = [
        _derive(graph, name, artifact, [source])
        for name, artifact in zip(("first", "second"), outputs, strict=True)
    ]
    config = ExecutionConfigArtifact.draft(
        {
            str(index): {"$artifact": graph.nodes[address].artifact.artifact_id}
            for index, address in enumerate(files)
        },
        "config.json",
        2,
    ).finalize()
    exported = _derive(graph, "configure", config, files)
    projection = graph.project(
        {"config": [exported], "optional": []},
        {"config": OutputSpec(), "optional": OutputSpec(required=False)},
    )
    return RetainedCase(
        projection,
        {source: graph.nodes[source].artifact},
        {
            item.address: graph.nodes[item.address].artifact
            for item in projection.occurrences
            if item.address != source
        },
    )


def _file_outputs(scratch: Path) -> list[Artifact]:
    content = b"selected structure bytes\n"
    path = scratch / "structure.bin"
    path.write_bytes(content)
    return [
        FileRefArtifact.draft(
            str(path),
            compute_content_digest(content),
            len(content),
            2,
            original_name=name,
        ).finalize()
        for name in ("first", "second")
    ]


def main() -> None:
    request = json.load(sys.stdin)
    case = retained_case(Path(request["scratch"]))
    completion = SuccessfulDelivery.model_validate(request["completion"])
    storage = StorageConfig.model_validate(request["storage"])
    if request.get("interrupt"):
        import artisan.storage.io.retained_seal as module

        module.publish_immutable_bytes = lambda *_args, **_kwargs: os._exit(29)
    receipt = stage_retained_result(
        request["staging_root"],
        completion,
        case.projection,
        case.artifacts,
        fs=storage.filesystem(),
    )
    Path(request["receipt"]).write_text(receipt.model_dump_json())
    os._exit(23)


if __name__ == "__main__":
    main()
