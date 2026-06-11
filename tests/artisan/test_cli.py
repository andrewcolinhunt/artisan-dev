"""Tests for the artisan CLI entry point."""

from __future__ import annotations

import json
from enum import StrEnum, auto
from pathlib import Path
from typing import Any, ClassVar
from unittest.mock import MagicMock, patch

import pytest
from pydantic import BaseModel, Field

from artisan.cli import _CONTAINER_VIEW_FIELDS, main
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.schemas.operation_config.compute import ARTISAN_WORKER_IMAGE
from artisan.schemas.specs.input_models import ExecuteInput
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec

_RUNNER_OUTPUTS: dict[str, OutputSpec] = {
    "result": OutputSpec(
        artifact_type=ArtifactTypes.DATA,
        infer_lineage_from={"inputs": []},
    ),
}


class RunnerOp(OperationDefinition):
    """op-run fixture: records its inputs and writes a marker file."""

    class OutputRole(StrEnum):
        result = auto()

    name: ClassVar[str] = "cli_runner_op_test"
    description: ClassVar[str] = "Writes marker.txt and records its inputs"
    execute_as_tool: ClassVar[bool] = True
    inputs: ClassVar[dict[str, InputSpec]] = {}
    outputs: ClassVar[dict[str, OutputSpec]] = _RUNNER_OUTPUTS

    seen: ClassVar[list[dict[str, Any]]] = []

    class Params(BaseModel):
        text: str = Field(default="hi", description="Marker file content.")

    params: Params = Params()

    def execute_function(self, inputs: ExecuteInput) -> None:
        type(self).seen.append(dict(inputs.inputs))
        Path(inputs.execute_dir, "marker.txt").write_text(self.params.text)


class ReturningOp(OperationDefinition):
    """op-run fixture violating the None-return contract."""

    class OutputRole(StrEnum):
        result = auto()

    name: ClassVar[str] = "cli_returning_op_test"
    description: ClassVar[str] = "Returns a value — contract violation"
    execute_as_tool: ClassVar[bool] = True
    inputs: ClassVar[dict[str, InputSpec]] = {}
    outputs: ClassVar[dict[str, OutputSpec]] = _RUNNER_OUTPUTS

    def execute_function(self, inputs: ExecuteInput) -> Any:
        return {"oops": 1}


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
        assert "not a command op" in capsys.readouterr().err


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


class TestOpRun:
    """The execute_as_tool runner — module:Qualname, no registry discovery."""

    def test_happy_path_writes_to_execute_dir(self, tmp_path):
        rc = main(
            [
                "op",
                "run",
                f"{__name__}:RunnerOp",
                "--params",
                '{"text": "from-params"}',
                "--inputs",
                '{"source": ["/a.csv"]}',
                "--execute-dir",
                str(tmp_path),
            ]
        )

        assert rc == 0
        assert (tmp_path / "marker.txt").read_text() == "from-params"
        # the runner's log tempfile lives outside execute_dir
        assert [p.name for p in tmp_path.iterdir()] == ["marker.txt"]

    def test_bare_str_input_delivered_as_one_element_list(self, tmp_path):
        """The wire's one-file-per-role shape is re-wrapped: str -> [str]."""
        RunnerOp.seen.clear()

        rc = main(
            [
                "op",
                "run",
                f"{__name__}:RunnerOp",
                "--inputs",
                '{"source": "/a.csv"}',
                "--execute-dir",
                str(tmp_path),
            ]
        )

        assert rc == 0
        assert RunnerOp.seen == [{"source": ["/a.csv"]}]

    def test_list_input_passes_through_unchanged(self, tmp_path):
        RunnerOp.seen.clear()

        rc = main(
            [
                "op",
                "run",
                f"{__name__}:RunnerOp",
                "--inputs",
                '{"source": ["/a.csv", "/b.csv"]}',
                "--execute-dir",
                str(tmp_path),
            ]
        )

        assert rc == 0
        assert RunnerOp.seen == [{"source": ["/a.csv", "/b.csv"]}]

    def test_non_path_input_value_rejected(self, tmp_path, capsys):
        rc = main(
            [
                "op",
                "run",
                f"{__name__}:RunnerOp",
                "--inputs",
                '{"source": 5}',
                "--execute-dir",
                str(tmp_path),
            ]
        )

        assert rc == 1
        err = capsys.readouterr().err
        assert "source" in err
        assert "file path" in err

    def test_non_none_return_rejected(self, tmp_path, capsys):
        rc = main(
            ["op", "run", f"{__name__}:ReturningOp", "--execute-dir", str(tmp_path)]
        )

        assert rc == 1
        assert "return None" in capsys.readouterr().err

    def test_bad_target_exits_nonzero(self, capsys):
        rc = main(["op", "run", "no.such.module:Nope"])

        assert rc == 1
        assert capsys.readouterr().err

    def test_target_without_colon_exits_nonzero(self, capsys):
        rc = main(["op", "run", "not-a-target"])

        assert rc == 1
        assert "module:Qualname" in capsys.readouterr().err
