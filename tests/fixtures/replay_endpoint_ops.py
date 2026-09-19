"""Importable Python tool operation for endpoint diagnostic subprocess tests."""

from __future__ import annotations

from pathlib import Path
from typing import ClassVar

from pydantic import BaseModel, Field

from artisan.schemas.specs.input_models import ExecuteInput
from fixtures.endpoint_ops import FlagTool


class PythonDiagnosticTool(FlagTool):
    """Write an intermediate before a controlled Python execution failure."""

    name: ClassVar[str] = "python_diagnostic_tool"
    execute_as_tool: ClassVar[bool] = True

    class Params(BaseModel):
        """Control whether Python work fails after producing evidence."""

        fail: bool = Field(default=False, description="Fail after writing output.")

    params: Params = Params()

    def execute_function(self, inputs: ExecuteInput) -> None:
        Path(inputs.execute_dir, "partial.txt").write_text("Python intermediate")
        print("Python diagnostic log")
        if self.params.fail:
            msg = "Python tool failed after output"
            raise RuntimeError(msg)
