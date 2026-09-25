"""Local endpoint-worker transport preserves built-in operation declarations.

This exercises real file packing and worker commands locally. It is not live
Modal deployment or network evidence; those checks live in test_tool_endpoint.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from artisan.execution.compute.invoke import invoke_op_work
from artisan.execution.lineage.builder import build_edges
from artisan.execution.lineage.enrich import build_artifact_edges_from_types
from artisan.execution.lineage.validation import (
    validate_lineage_completeness,
    validate_lineage_integrity,
)
from artisan.execution.tool_endpoint.protocol import ToolRequest
from artisan.execution.tool_endpoint.server import run_tool_request
from artisan.execution.tool_endpoint.transport import InlineTransport
from artisan.operations.base import OperationDefinition
from artisan.operations.examples import CsvHead, WaitTool
from artisan.schemas import DataArtifact, ExecuteInput, PostprocessInput

pytestmark = pytest.mark.integration


@pytest.mark.parametrize(
    ("operation_cls", "params", "suffix"),
    [(WaitTool, {"seconds": 1}, "_waited"), (CsvHead, {"rows": 1}, "_head")],
)
def test_builtin_exact_parents_and_names_survive_worker_transport(
    tmp_path: Path, operation_cls: type[OperationDefinition], params: dict, suffix: str
) -> None:
    sources = [
        DataArtifact.draft(
            content=f"id,value\n1,{value}\n2,9\n".encode(),
            original_name=f"sample_{value}.csv",
            step_number=0,
        ).finalize()
        for value in (1, 10)
    ]
    materialized = tmp_path / "inputs"
    materialized.mkdir()
    paths = [source.materialize_to(str(materialized)) for source in sources]
    operation = operation_cls(params=params)
    transport = InlineTransport()
    signatures = []
    for route in ("local", "worker_transport"):
        output_files = []
        for index, path in enumerate(paths):
            output_dir = tmp_path / route / str(index)
            output_dir.mkdir(parents=True)
            if route == "local":
                assert (
                    invoke_op_work(
                        operation,
                        ExecuteInput(
                            inputs={"dataset": [path]}, execute_dir=str(output_dir)
                        ),
                    )
                    is None
                )
            else:
                refs = transport.pack_inputs({"dataset": path})
                assert refs[0].filename == Path(path).name
                response = run_tool_request(
                    operation_cls, ToolRequest(params=params, inputs=refs)
                )
                assert response.manifest.error is None
                assert response.output_tar is not None
                assert response.manifest.output_names == [
                    f"{Path(path).stem}{suffix}.csv"
                ]
                transport.unpack_outputs(response.output_tar, str(output_dir))
            output_files.extend(
                str(file) for file in output_dir.rglob("*") if file.is_file()
            )
        result = operation.postprocess(
            PostprocessInput(
                file_outputs=output_files,
                memory_outputs=None,
                input_artifacts={"dataset": sources},
                step_number=1,
                postprocess_dir=str(tmp_path),
            )
        )
        input_ids = {"dataset": [source.artifact_id for source in sources]}
        validate_lineage_integrity(
            result.lineage, input_ids, result.artifacts, operation.outputs
        )
        validate_lineage_completeness(
            result.artifacts, operation.outputs, result.lineage
        )
        finalized = {
            role: [artifact.finalize() for artifact in artifacts]
            for role, artifacts in result.artifacts.items()
        }
        all_outputs = [
            artifact for artifacts in finalized.values() for artifact in artifacts
        ]
        types = {
            artifact.artifact_id: artifact.artifact_type
            for artifact in [*sources, *all_outputs]
        }
        edges = build_artifact_edges_from_types(
            build_edges(result.lineage, finalized, types), "e" * 32, types
        )
        names = {
            artifact.artifact_id: artifact.original_name for artifact in all_outputs
        }
        assert len(edges) == len(all_outputs) == 2
        signatures.append(
            {
                (
                    names[edge.target_artifact_id],
                    edge.source_artifact_id,
                    edge.source_role,
                    edge.target_role,
                    edge.source_artifact_type,
                    edge.target_artifact_type,
                    edge.group_id,
                )
                for edge in edges
            }
        )
    target_role = "output" if operation_cls is WaitTool else "dataset"
    expected = {
        (
            f"{source.original_name}{suffix}",
            source.artifact_id,
            "dataset",
            target_role,
            "data",
            "data",
            None,
        )
        for source in sources
    }
    assert signatures == [expected, expected]
