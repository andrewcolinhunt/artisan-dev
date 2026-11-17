"""Built-in command phases preserve names and explicit parent declarations.

The adapter uses the test interpreter to exercise commands and the ordinary
kernel. Matched installed worker transport has a separate qualification driver.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from fixtures.commands import command_router

from artisan.composites.base.compiler import compile_definition
from artisan.execution.composition import execute_compiled_plan
from artisan.execution.executor import Executor
from artisan.operations.base import OperationDefinition
from artisan.operations.examples import CsvHead, WaitTool
from artisan.schemas import CSVArtifact

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("repeated", [False, True], ids=["distinct", "repeated"])
@pytest.mark.parametrize(
    ("operation", "params", "role", "suffix"),
    [
        (WaitTool, {"seconds": 1}, "output", "_waited"),
        (CsvHead, {"rows": 1}, "dataset", "_head"),
    ],
)
def test_builtin_command_preserves_exact_parents_and_names(
    tmp_path: Path,
    operation: type[OperationDefinition],
    params: dict,
    role: str,
    suffix: str,
    repeated: bool,
) -> None:
    sources = [
        CSVArtifact.draft(
            f"id,value\n1,{value}\n2,9\n".encode(), f"sample_{value}.csv", 0
        ).finalize()
        for value in (1, 10)
    ]
    if repeated:
        sources.append(sources[0])
    with Executor(working_root=tmp_path) as executor:
        result = execute_compiled_plan(
            compile_definition(operation, params=params),
            {"dataset": sources},
            executor,
            invocation_id="builtin-commands",
            execute_adapter=lambda _operation, _path: command_router(),
        )
        assert [item.original_name for item in result.result.artifacts[role]] == [
            item.original_name + suffix for item in sources
        ]
        assert [item.source_artifact_id for item in result.result.lineage[role]] == [
            item.artifact_id for item in sources
        ]
        assert [
            item.source_input_index for item in result.result.lineage[role]
        ] == list(range(len(sources)))
