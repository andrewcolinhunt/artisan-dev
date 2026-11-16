"""Operations declare science and command software independently of providers."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from artisan.operations.examples import (
    CsvHead,
    DataGenerator,
    DataTransformerScript,
    StreamingEcho,
    WaitTool,
)
from artisan.schemas.operation_config.environment import Environment


@pytest.mark.parametrize(
    "removed",
    ["environments", "execute_environment", "compute_provider", "compute_resources"],
)
def test_removed_execution_configuration_rejects_before_science(removed: str) -> None:
    assert removed not in DataGenerator.model_fields
    with pytest.raises(ValidationError, match="Extra inputs"):
        DataGenerator.model_validate({removed: {}})


@pytest.mark.parametrize(
    "definition", [CsvHead, DataTransformerScript, StreamingEcho, WaitTool]
)
def test_command_examples_use_selected_worker_software(definition: type) -> None:
    operation = definition()
    assert operation.is_command_op()
    assert operation.environment is None


def test_environment_declaration_remains_explicit_and_nonprovisioning() -> None:
    from pathlib import Path

    selected = Environment.from_pixi(Path("/not-installed/example/pixi.toml"))
    operation = WaitTool(environment=selected)
    assert operation.environment is selected
    assert selected.manifest == Path("/not-installed/example/pixi.toml")
