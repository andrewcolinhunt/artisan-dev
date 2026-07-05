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


def _seed_steps(root: Path, run_ids: list[str]) -> None:
    """Write a steps table with a running+completed row pair per run."""
    from datetime import UTC, datetime, timedelta

    import polars as pl

    from artisan.schemas.enums import TablePath
    from artisan.storage.core.table_schemas import STEPS_SCHEMA

    rows = []
    t0 = datetime(2026, 7, 1, tzinfo=UTC)
    for i, run_id in enumerate(run_ids):
        for j, status in enumerate(["running", "completed"]):
            rows.append(
                {
                    "step_run_id": f"{run_id}-step-1",
                    "step_spec_id": "spec-1",
                    "pipeline_run_id": run_id,
                    "step_number": 1,
                    "step_name": "generate",
                    "status": status,
                    "operation_class": "DataGenerator",
                    "params_json": "{}",
                    "input_refs_json": "{}",
                    "compute_backend": "local",
                    "compute_options_json": "{}",
                    "output_roles_json": "[]",
                    "output_types_json": "[]",
                    "total_count": 1,
                    "succeeded_count": 1,
                    "failed_count": 0,
                    "timestamp": t0 + timedelta(minutes=10 * i + j),
                    "duration_seconds": 1.0,
                    "error": None,
                    "dispatch_error": None,
                    "commit_error": None,
                    "metadata": "{}",
                }
            )
    df = pl.DataFrame(rows, schema=STEPS_SCHEMA)
    df.write_delta(str(root / TablePath.STEPS))


class TestOpList:
    """artisan op list."""

    def test_json_lists_registered_ops(self, capsys):
        rc = main(["op", "list", "--json"])

        assert rc == 0
        payload = json.loads(capsys.readouterr().out)
        names = [item["name"] for item in payload["items"]]
        assert "filter" in names  # builtin curator
        assert "cli_runner_op_test" in names  # registered by this module

    def test_kind_filter(self, capsys):
        rc = main(["op", "list", "--kind", "curator", "--json"])

        assert rc == 0
        payload = json.loads(capsys.readouterr().out)
        assert payload["items"]
        assert all(item["kind"] == "curator" for item in payload["items"])

    def test_query_filter(self, capsys):
        rc = main(["op", "list", "--query", "cli_runner_op", "--json"])

        assert rc == 0
        payload = json.loads(capsys.readouterr().out)
        assert [item["name"] for item in payload["items"]] == ["cli_runner_op_test"]

    def test_human_mode(self, capsys):
        rc = main(["op", "list"])

        assert rc == 0
        out = capsys.readouterr().out
        assert "cli_runner_op_test" in out


class TestOpDescribe:
    """artisan op describe."""

    def test_json_metadata(self, capsys):
        rc = main(["op", "describe", "cli_runner_op_test", "--json"])

        assert rc == 0
        payload = json.loads(capsys.readouterr().out)
        assert payload["name"] == "cli_runner_op_test"
        assert "params_schema" in payload
        assert "examples" in payload

    def test_unknown_op_emits_envelope(self, capsys):
        rc = main(["op", "describe", "cli_runner_op_tset", "--json"])

        assert rc == 1
        envelope = json.loads(capsys.readouterr().out)
        assert envelope["code"] == "unknown_operation"
        assert "cli_runner_op_test" in envelope["suggestions"]

    def test_unknown_op_human_mode_uses_stderr(self, capsys):
        rc = main(["op", "describe", "no_such_op_anywhere"])

        assert rc == 1
        captured = capsys.readouterr()
        assert captured.out == ""
        assert "no_such_op_anywhere" in captured.err


class TestRuns:
    """artisan runs."""

    def test_missing_delta_root_emits_envelope(self, capsys, monkeypatch):
        monkeypatch.delenv("ARTISAN_DELTA_ROOT", raising=False)
        rc = main(["runs", "--json"])

        assert rc == 1
        envelope = json.loads(capsys.readouterr().out)
        assert envelope["code"] == "delta_root_unset"
        assert envelope["recovery_hint"] == "CHECK_INPUT"

    def test_lists_seeded_runs(self, tmp_path, capsys):
        _seed_steps(tmp_path, ["run-a", "run-b"])
        rc = main(["runs", "--delta-root", str(tmp_path), "--json"])

        assert rc == 0
        payload = json.loads(capsys.readouterr().out)
        by_id = {item["pipeline_run_id"]: item for item in payload["items"]}
        assert set(by_id) == {"run-a", "run-b"}
        assert by_id["run-a"]["last_status"] == "completed"
        assert by_id["run-a"]["started_at"]  # datetime serialized via default=str

    def test_env_var_fallback(self, tmp_path, capsys, monkeypatch):
        _seed_steps(tmp_path, ["run-a"])
        monkeypatch.setenv("ARTISAN_DELTA_ROOT", str(tmp_path))
        rc = main(["runs", "--json"])

        assert rc == 0
        payload = json.loads(capsys.readouterr().out)
        assert len(payload["items"]) == 1


class TestFailures:
    """artisan failures."""

    def test_empty_root_emits_store_not_found(self, tmp_path, capsys):
        rc = main(["failures", "--delta-root", str(tmp_path), "--json"])

        assert rc == 1
        envelope = json.loads(capsys.readouterr().out)
        assert envelope["code"] == "store_not_found"
        assert envelope["recovery_hint"] == "CHECK_INPUT"
        assert envelope["cause"]["type"] == "FileNotFoundError"


class TestProvenance:
    """artisan provenance."""

    A = "a" * 32
    B = "b" * 32
    C = "c" * 32

    def test_backward_edges(self, tmp_path, capsys, seed_artifact_edges):
        seed_artifact_edges(tmp_path, [(self.A, self.B), (self.B, self.C)])
        rc = main(["provenance", self.C, "--delta-root", str(tmp_path), "--json"])

        assert rc == 0
        payload = json.loads(capsys.readouterr().out)
        assert payload["edges"] == [
            {"source_artifact_id": self.B, "target_artifact_id": self.C},
            {"source_artifact_id": self.A, "target_artifact_id": self.B},
        ]
        assert payload["truncated"] is False

    def test_forward_depth_truncation(self, tmp_path, capsys, seed_artifact_edges):
        seed_artifact_edges(tmp_path, [(self.A, self.B), (self.B, self.C)])
        rc = main(
            [
                "provenance",
                self.A,
                "--delta-root",
                str(tmp_path),
                "--direction",
                "forward",
                "--depth",
                "1",
                "--json",
            ]
        )

        assert rc == 0
        payload = json.loads(capsys.readouterr().out)
        assert payload["edges"] == [
            {"source_artifact_id": self.A, "target_artifact_id": self.B}
        ]
        assert payload["truncated"] is True

    def test_missing_table_degrades_to_empty(self, tmp_path, capsys):
        rc = main(["provenance", self.A, "--delta-root", str(tmp_path), "--json"])

        assert rc == 0
        payload = json.loads(capsys.readouterr().out)
        assert payload["edges"] == []
