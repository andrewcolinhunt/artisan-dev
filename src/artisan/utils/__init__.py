"""Artisan utility modules."""

from __future__ import annotations

from artisan.schemas.enums import FailurePolicy
from artisan.utils.dataframes import pivot_metrics_wide, to_float
from artisan.utils.dicts import flatten_dict
from artisan.utils.errors import format_error
from artisan.utils.external_tools import (
    ArgStyle,
    Command,
    ExternalToolError,
    build_command_from_spec,
    format_args,
    run_command,
    run_external_command,
    to_cli_value,
)
from artisan.utils.filename import strip_extensions
from artisan.utils.hashing import compute_artifact_id
from artisan.utils.logging import configure_logging
from artisan.utils.path import find_project_root, get_caller_dir, shard_path
from artisan.utils.tutorial import TutorialEnv, tutorial_setup

__all__ = [
    # DataFrame utilities
    "pivot_metrics_wide",
    "to_float",
    # Dict utilities
    "flatten_dict",
    # Error formatting
    "format_error",
    # External tools
    "ArgStyle",
    "Command",
    "ExternalToolError",
    "build_command_from_spec",
    "format_args",
    "run_command",
    "run_external_command",
    "to_cli_value",
    # Filename utilities
    "strip_extensions",
    # Logging utilities
    "configure_logging",
    # Hashing utilities
    "compute_artifact_id",
    # Path utilities
    "find_project_root",
    "get_caller_dir",
    "shard_path",
    # Tutorial utilities
    "TutorialEnv",
    "tutorial_setup",
    # Type utilities
    "FailurePolicy",
]
