"""Tests for validate_remote_execute."""

from __future__ import annotations

import warnings
from unittest.mock import MagicMock, patch

import pytest

from artisan.execution.compute.validation import validate_remote_execute


class TestValidateRemoteExecute:
    def test_picklable_operation_passes(self):
        """A simple picklable object passes validation."""
        operation = MagicMock(spec=[])
        operation.tool = None
        assert validate_remote_execute(operation) is True

    def test_unpicklable_operation_raises(self):
        """An operation with an unpicklable attribute raises RuntimeError."""

        class UnpicklableOp:
            tool = None

            def __init__(self):
                self.file_handle = open("/dev/null")  # noqa: SIM115

            def __del__(self):
                self.file_handle.close()

        op = UnpicklableOp()
        try:
            # cloudpickle can actually pickle file handles on some platforms,
            # so we mock the failure to ensure the error path is tested
            mock_cp = MagicMock()
            mock_cp.dumps.side_effect = TypeError("cannot pickle file")
            with patch.dict("sys.modules", {"cloudpickle": mock_cp}):
                with pytest.raises(RuntimeError, match="failed cloudpickle round-trip"):
                    validate_remote_execute(op)
        finally:
            op.file_handle.close()

    def test_tool_with_local_path_warns(self):
        """An operation with a local-only tool executable emits a warning."""
        operation = MagicMock(spec=[])
        operation.tool = MagicMock()
        operation.tool.executable = "/opt/local-only-binary"

        with patch("shutil.which", return_value=None):
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                validate_remote_execute(operation)
                assert len(w) == 1
                assert "absolute path" in str(w[0].message)
                assert "/opt/local-only-binary" in str(w[0].message)

    def test_tool_on_path_no_warning(self):
        """An operation whose tool is on PATH does not warn."""
        operation = MagicMock(spec=[])
        operation.tool = MagicMock()
        operation.tool.executable = "/usr/bin/python"

        with patch("shutil.which", return_value="/usr/bin/python"):
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                validate_remote_execute(operation)
                assert len(w) == 0

    def test_no_tool_no_warning(self):
        """An operation without a tool does not warn."""
        operation = MagicMock(spec=[])
        operation.tool = None

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            validate_remote_execute(operation)
            assert len(w) == 0

    def test_relative_tool_path_no_warning(self):
        """An operation with a relative tool path does not warn."""
        operation = MagicMock(spec=[])
        operation.tool = MagicMock()
        operation.tool.executable = "python"

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            validate_remote_execute(operation)
            assert len(w) == 0
