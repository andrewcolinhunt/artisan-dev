"""Exact declared pairs survive native/command phases and retained publication."""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest
from fixtures.commands import command_router
from fixtures.declared_pair_ops import (
    DeclaredPairCommand,
    DeclaredPairs,
    MonolithicDeclaredPairCommand,
    MonolithicDeclaredPairs,
)
from fixtures.retained_acceptance import admit_prepared_case, open_committer
from fixtures.retained_results import RetainedCase

from artisan.composites.base.compiler import compile_definition
from artisan.execution.composition import execute_compiled_plan
from artisan.execution.executor import Executor
from artisan.operations.base import OperationDefinition
from artisan.schemas import CSVArtifact, ExecuteInput
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.storage.core.retained_reader import RetainedReader

pytestmark = pytest.mark.integration


@pytest.mark.parametrize(
    ("operation", "params"),
    [
        (DeclaredPairs, {"file_outputs": False}),
        (DeclaredPairs, {"file_outputs": True}),
        (MonolithicDeclaredPairs, {"file_outputs": False}),
        (MonolithicDeclaredPairs, {"file_outputs": True}),
        (DeclaredPairCommand, {}),
        (MonolithicDeclaredPairCommand, {}),
    ],
)
def test_pairs_keep_exact_declared_parents_after_acceptance(
    tmp_path: Path, operation: type[OperationDefinition], params: dict
) -> None:
    artifacts = [
        CSVArtifact.draft(f"value\n{i}\n".encode(), f"source-{i}.csv", 0).finalize()
        for i in range(4)
    ]
    inputs = {
        "primary": [artifacts[0], artifacts[0]],
        "partner": artifacts[1:3],
        "reference": [artifacts[3], artifacts[3]],
    }
    storage = StorageConfig()
    committer = open_committer(
        str(tmp_path / "store"), storage.filesystem(), storage, initialize=True
    )
    with Executor(working_root=tmp_path / "session") as executor:
        executed = execute_compiled_plan(
            compile_definition(operation, params=params),
            inputs,
            executor,
            invocation_id="pairs",
            execute_adapter=lambda _operation, _path: command_router(),
        )
        boundary = {
            address: inputs[role][index]
            for role, addresses in executed.projection.input_roles().items()
            for index, address in enumerate(addresses)
        }
        case = RetainedCase(executed.projection, boundary, executed.retained_artifacts)
        receipt = admit_prepared_case(committer, case)
        committer.accept(receipt)
    selected = RetainedReader(committer).read(receipt)
    assert selected.projection == executed.projection
    outputs = selected.output("prediction")
    assert len(outputs) == 2
    by_id = {item.address: item.artifact_id for item in selected.projection.occurrences}
    expected = {
        (artifacts[0].artifact_id, artifacts[1].artifact_id),
        (artifacts[0].artifact_id, artifacts[2].artifact_id),
    }
    observed = set()
    for output in outputs:
        parents = [
            edge.source
            for edge in selected.projection.parents
            if edge.target == output.reference.address
        ]
        assert {parent.role for parent in parents} == {"primary", "partner"}
        content = output.artifact.values
        assert {by_id[parent] for parent in parents} == {
            content["primary"],
            content["partner"],
        }
        observed.add((content["primary"], content["partner"]))
    assert observed == expected
    assert all(edge.source.role != "reference" for edge in selected.projection.parents)


def test_relative_output_manifest_survives_relocation_and_source_removal(
    tmp_path: Path,
) -> None:
    operation = DeclaredPairCommand()
    received = []
    for index, partner in enumerate(["b" * 32, "c" * 32]):
        source = tmp_path / f"source-{index}.json"
        source.write_text(
            json.dumps(
                {
                    "primary": "a" * 32,
                    "partner": partner,
                    "reference": "d" * 32,
                    "input_index": index,
                }
            )
        )
        remote = tmp_path / f"remote-{index}"
        remote.mkdir()
        operation.execute_function(
            ExecuteInput(inputs={"pair_file": str(source)}, execute_dir=str(remote))
        )
        local = tmp_path / f"received-{index}"
        shutil.copytree(remote, local)
        shutil.rmtree(remote)
        received.extend(str(path) for path in local.rglob("*") if path.is_file())
    declarations = []
    for filename in received:
        path = Path(filename)
        if path.name == "declared-outputs.json":
            for record in json.loads(path.read_text()):
                assert (
                    json.loads((path.parent / record["path"]).read_text())
                    == record["parents"]
                )
                declarations.append(record)
    assert [record["input_index"] for record in declarations] == [0, 1]
    assert [record["parents"] for record in declarations] == [
        {"primary": "a" * 32, "partner": "b" * 32},
        {"primary": "a" * 32, "partner": "c" * 32},
    ]
