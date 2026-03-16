"""Tool specification for external binary/script invocation.

Declares what binary or script an operation invokes, separate from
the execution environment that wraps it.
"""

from __future__ import annotations

import shutil
from pathlib import Path

from pydantic import BaseModel


class ToolSpec(BaseModel):
    """Declares the binary or script an operation invokes.

    Attributes:
        executable: Path or name of the binary/script. Resolved via PATH
            if not an absolute path.
        interpreter: Optional interpreter prefix (e.g. "python", "python -u").
        subcommand: Optional subcommand inserted after the executable.
    """

    executable: str | Path
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
        if not Path(exe).exists() and not shutil.which(exe):
            raise FileNotFoundError(f"Executable not found: {self.executable}")
