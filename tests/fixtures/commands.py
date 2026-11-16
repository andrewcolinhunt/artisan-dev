"""Unwrapped command adapter for tests of supervision and recording alone."""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, ClassVar, Literal

from artisan.execution.compute.prepared import PreparedExecuteRouter
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.schemas.specs.input_models import ExecuteInput


@dataclass
class IdentityCommandEnvironment:
    """Leave argv unchanged without claiming prepared scientific software."""

    env: dict[str, str] = field(default_factory=dict)
    public_env: tuple[str, ...] = ()
    cancellation_scope: ClassVar[Literal["process_group", "external"]] = "process_group"

    def wrap_command(self, command: list[str], cwd: str | None = None) -> list[str]:
        return list(command)

    def prepare_env(self) -> dict[str, str]:
        return {
            "PATH": os.defpath,
            "PYTHONPATH": str(Path(__file__).resolve().parents[1]),
            **self.env,
        }


@dataclass
class ExternalCommandEnvironment(IdentityCommandEnvironment):
    """Expose an external ownership obligation without a provider implementation."""

    cancellation_scope: ClassVar[Literal["process_group", "external"]] = "external"


def command_router() -> PreparedExecuteRouter:
    """Use the test interpreter for phase/ownership checks without qualification."""
    environment = IdentityCommandEnvironment(
        env={"PATH": f"{Path(sys.executable).parent}{os.pathsep}{os.defpath}"}
    )
    return PreparedExecuteRouter(
        None,
        pixi=Path("unused-test-launcher"),
        system_env=Path("/usr/bin/env"),
        runtime_env={},
        command_environment=lambda _root, _witness: environment,
    )


def execute_command(operation: OperationDefinition, inputs: ExecuteInput) -> Any:
    """Exercise the actual router using this test interpreter, without qualification."""
    return command_router().route_execute(operation, [inputs], inputs.execute_dir)[0]
