"""Tool specification for external binary/script invocation.

Declares what binary or script an operation invokes, separate from
the execution environment that wraps it.
"""

from __future__ import annotations

import os
import shutil
from typing import Annotated

from pydantic import BaseModel, BeforeValidator


def _coerce_to_str(v: object) -> str:
    """Accept Path objects from operations, store as str."""
    return str(v)


class ToolSpec(BaseModel):
    """Declares the binary or script an operation invokes.

    Attributes:
        executable: Name or path of the binary/script. Resolved via PATH
            if not an absolute path. Accepts Path objects for convenience
            but stores as str.
        interpreter: Optional interpreter prefix (e.g. "python", "python -u").
        subcommand: Optional subcommand inserted after the executable.
    """

    executable: Annotated[str, BeforeValidator(_coerce_to_str)]
    interpreter: str | None = None
    subcommand: str | None = None

    def parts(self) -> list[str]:
        """Build command prefix: [interpreter...] executable [subcommand]."""
        if self.interpreter is None:
            prefix = [str(self.executable)]
        else:
            prefix = [*self.interpreter.split(), str(self.executable)]
        if self.subcommand:
            prefix.append(self.subcommand)
        return prefix

    def validate_tool(self) -> None:
        """Check that the executable exists on PATH or as a file.

        Raises:
            FileNotFoundError: If the executable cannot be found.
        """
        exe = str(self.executable)
        if not os.path.exists(exe) and not shutil.which(exe):
            raise FileNotFoundError(f"Executable not found: {self.executable}")
