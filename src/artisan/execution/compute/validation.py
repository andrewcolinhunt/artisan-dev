"""Pre-flight validation for remote compute routing."""

from __future__ import annotations

import shutil
import warnings
from typing import Any


def validate_remote_execute(operation: Any) -> bool:
    """Check whether an operation is safe for remote routing.

    Performs two checks:

    - **Cloudpickle round-trip:** serializes and deserializes the operation
      instance, raises if it fails (file handles, lambdas, unpicklable
      attributes).
    - **ToolSpec path check:** warns if ``tool.executable`` points to a
      local-only binary that won't exist on the remote container.

    Intended for use in tests (``assert validate_remote_execute(MyOp())``)
    and as a pre-flight check before dispatching to remote providers.

    Args:
        operation: An OperationDefinition instance to validate.

    Returns:
        True if the operation passes all checks.

    Raises:
        RuntimeError: If cloudpickle round-trip fails.
        ImportError: If cloudpickle is not installed.
    """
    _check_pickle_roundtrip(operation)
    _check_tool_paths(operation)
    return True


def _check_pickle_roundtrip(operation: Any) -> None:
    """Verify the operation survives a cloudpickle serialize/deserialize cycle.

    Raises:
        RuntimeError: If serialization or deserialization fails.
        ImportError: If cloudpickle is not installed.
    """
    try:
        import cloudpickle
    except ImportError:
        raise ImportError(
            "cloudpickle is required for remote compute validation. "
            "Install it with: pip install cloudpickle"
        ) from None

    try:
        data = cloudpickle.dumps(operation)
        cloudpickle.loads(data)
    except Exception as exc:
        msg = (
            f"Operation {type(operation).__name__} failed cloudpickle "
            f"round-trip (not safe for remote execution): {exc}"
        )
        raise RuntimeError(msg) from exc


def _check_tool_paths(operation: Any) -> None:
    """Warn if tool.executable points to a local-only path.

    A local-only binary is one that exists as an absolute path on the
    local filesystem but wouldn't be available on a remote container
    (i.e., it's not on PATH).
    """
    tool = getattr(operation, "tool", None)
    if tool is None:
        return

    executable = getattr(tool, "executable", None)
    if executable is None:
        return

    # If the executable is an absolute path, it likely won't exist on the remote
    if executable.startswith("/") and shutil.which(executable) is None:
        warnings.warn(
            f"Operation {type(operation).__name__} has tool.executable="
            f"'{executable}' which is an absolute path not found on PATH. "
            "This binary may not exist on the remote compute target.",
            UserWarning,
            stacklevel=3,
        )
