"""Command specification hierarchy for external tool execution.

Provides ``CommandSpec`` and its subclasses (``ApptainerCommandSpec``,
``DockerCommandSpec``, ``LocalCommandSpec``, ``PixiCommandSpec``) for
declaring how an external tool should be invoked.
"""

from __future__ import annotations

from pathlib import Path

from pydantic import BaseModel, Field

from artisan.utils.external_tools import ArgStyle


class CommandSpec(BaseModel):
    """Base command specification for external tool invocation.

    Attributes:
        script: Path to the script or executable.
        arg_style: How arguments are formatted on the command line.
        subcommand: Optional subcommand inserted after the script.
        interpreter: Optional interpreter prefix (e.g. "python3").
    """

    script: Path
    arg_style: ArgStyle
    subcommand: str | None = None
    interpreter: str | None = None

    def script_parts(self) -> list[str]:
        """Build the command prefix for script invocation."""
        if self.interpreter is None:
            return [str(self.script)]
        return [*self.interpreter.split(), str(self.script)]


class ApptainerCommandSpec(CommandSpec):
    """Command specification for Apptainer (Singularity) containers."""

    image: Path
    gpu: bool = False
    binds: list[tuple[Path, Path]] = Field(default_factory=list)
    env: dict[str, str] = Field(default_factory=dict)


class DockerCommandSpec(CommandSpec):
    """Command specification for Docker containers."""

    image: Path
    gpu: bool = False
    binds: list[tuple[Path, Path]] = Field(default_factory=list)
    env: dict[str, str] = Field(default_factory=dict)


class LocalCommandSpec(CommandSpec):
    """Command specification for local execution, optionally in a venv."""

    venv_path: Path | None = None
    env: dict[str, str] = Field(default_factory=dict)


class PixiCommandSpec(CommandSpec):
    """Command specification for execution inside a Pixi environment."""

    environment: str = "default"
    manifest_path: Path | None = None
    env: dict[str, str] = Field(default_factory=dict)
