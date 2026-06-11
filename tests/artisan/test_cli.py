"""Tests for the artisan CLI entry point."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from artisan.cli import main


class TestModalDeploy:
    @patch("artisan.registry.discovery.discover")
    @patch("artisan.execution.tool_endpoint.deploy.build_app")
    def test_deploys_registered_op(self, mock_build_app, mock_discover, capsys):
        from artisan.operations.examples import WaitTool

        mock_app = MagicMock()
        mock_build_app.return_value = mock_app

        rc = main(["modal", "deploy", "wait_tool"])

        assert rc == 0
        mock_discover.assert_called_once()
        mock_build_app.assert_called_once_with(WaitTool)
        mock_app.deploy.assert_called_once()
        assert "artisan-tool-wait_tool" in capsys.readouterr().out

    @patch("artisan.registry.discovery.discover")
    @patch("artisan.execution.tool_endpoint.deploy.build_app")
    def test_unknown_op_returns_error(self, mock_build_app, mock_discover, capsys):
        rc = main(["modal", "deploy", "no_such_op_anywhere"])

        assert rc == 1
        mock_build_app.assert_not_called()
        assert "no_such_op_anywhere" in capsys.readouterr().err

    def test_missing_subcommand_exits(self):
        with pytest.raises(SystemExit):
            main([])
