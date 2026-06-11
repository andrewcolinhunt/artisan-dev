"""Tests for the artisan CLI entry point."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from artisan.cli import _CONTAINER_VIEW_FIELDS, main
from artisan.schemas.operation_config.compute import ARTISAN_WORKER_IMAGE


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
        mock_build_app.assert_called_once_with(WaitTool, overlay=None)
        mock_app.deploy.assert_called_once()
        assert "artisan-tool-wait_tool" in capsys.readouterr().out

    @patch("artisan.registry.discovery.discover")
    @patch("artisan.execution.tool_endpoint.deploy.build_app")
    def test_overlay_forwarded_to_build_app(self, mock_build_app, mock_discover):
        from artisan.operations.examples import WaitTool

        mock_build_app.return_value = MagicMock()

        rc = main(
            ["modal", "deploy", "wait_tool", "--overlay", "artisan", "--overlay", "pkg"]
        )

        assert rc == 0
        mock_build_app.assert_called_once_with(WaitTool, overlay=["artisan", "pkg"])

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


class TestOpImage:
    @patch("artisan.registry.discovery.discover")
    def test_prints_bare_ref(self, mock_discover, capsys):
        """Bare ref on stdout — shell substitution is the contract."""
        rc = main(["op", "image", "wait_tool"])

        assert rc == 0
        assert capsys.readouterr().out == f"{ARTISAN_WORKER_IMAGE}\n"

    @patch("artisan.registry.discovery.discover")
    def test_json_emits_container_view_only(self, mock_discover, capsys):
        """The --json view carries run-anywhere fields, not deploy concerns."""
        rc = main(["op", "image", "wait_tool", "--json"])

        assert rc == 0
        view = json.loads(capsys.readouterr().out)
        assert view["image"] == ARTISAN_WORKER_IMAGE
        assert set(view) == set(_CONTAINER_VIEW_FIELDS)
        # endpoint-deploy concerns stay out of the container view
        assert "params_schema" not in view
        assert "op_module" not in view
        assert "description" not in view
        assert "input_roles" not in view

    @patch("artisan.registry.discovery.discover")
    def test_unknown_op_returns_error(self, mock_discover, capsys):
        rc = main(["op", "image", "no_such_op_anywhere"])

        assert rc == 1
        assert "no_such_op_anywhere" in capsys.readouterr().err

    @patch("artisan.registry.discovery.discover")
    def test_non_tool_op_returns_error(self, mock_discover, capsys):
        from artisan.operations.examples import DataGenerator  # noqa: F401 — registers

        rc = main(["op", "image", "data_generator"])

        assert rc == 1
        assert "not a tool op" in capsys.readouterr().err


class TestDockerBuild:
    @patch("artisan.registry.discovery.discover")
    @patch("artisan.execution.tool_endpoint.docker.build_image")
    def test_dispatches_to_build_image(self, mock_build, mock_discover, capsys):
        from artisan.operations.examples import WaitTool

        mock_build.return_value = "ghcr.io/org/img:tag"

        rc = main(["docker", "build", "wait_tool"])

        assert rc == 0
        mock_build.assert_called_once()
        assert mock_build.call_args.args[0] is WaitTool
        assert capsys.readouterr().out == "ghcr.io/org/img:tag\n"

    @patch("artisan.registry.discovery.discover")
    @patch(
        "artisan.execution.tool_endpoint.docker.build_image",
        side_effect=FileNotFoundError("No Dockerfile for image 'x'"),
    )
    def test_missing_dockerfile_returns_error(self, mock_build, mock_discover, capsys):
        rc = main(["docker", "build", "wait_tool"])

        assert rc == 1
        assert "No Dockerfile" in capsys.readouterr().err
