"""Tests for the artisan CLI entry point."""

from __future__ import annotations

import json
import os
import tempfile
from enum import StrEnum, auto
from pathlib import Path
from typing import Any, ClassVar
from unittest.mock import MagicMock

import pytest
from fsspec.implementations.local import LocalFileSystem
from pydantic import BaseModel, Field

from artisan.cli import main
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.schemas.specs.input_models import ExecuteInput
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec

_RUNNER_OUTPUTS: dict[str, OutputSpec] = {
    "result": OutputSpec(
        artifact_type=ArtifactTypes.CSV,
        derives_from={"inputs": []},
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

    def test_removes_log_tempfile(self, tmp_path, monkeypatch):
        output_dir = tmp_path / "output"
        output_dir.mkdir()
        log_paths: list[Path] = []
        real_mkstemp = tempfile.mkstemp

        def tracked_mkstemp(*, prefix: str, suffix: str) -> tuple[int, str]:
            fd, path = real_mkstemp(prefix=prefix, suffix=suffix, dir=tmp_path)
            log_paths.append(Path(path))
            return fd, path

        monkeypatch.setattr("artisan.cli.tempfile.mkstemp", tracked_mkstemp)

        rc = main(
            [
                "op",
                "run",
                f"{__name__}:RunnerOp",
                "--execute-dir",
                str(output_dir),
            ]
        )

        assert rc == 0
        assert len(log_paths) == 1
        assert not log_paths[0].exists()

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

    def test_non_none_return_rejected(self, tmp_path, capsys, monkeypatch):
        fd, log_path = tempfile.mkstemp(dir=tmp_path)
        os.close(fd)
        monkeypatch.setattr(
            "artisan.cli.tempfile.mkstemp",
            lambda **_kwargs: (os.open(log_path, os.O_RDWR), log_path),
        )
        rc = main(
            ["op", "run", f"{__name__}:ReturningOp", "--execute-dir", str(tmp_path)]
        )

        assert rc == 1
        assert "return None" in capsys.readouterr().err
        assert not Path(log_path).exists()

    def test_bad_target_exits_nonzero(self, capsys):
        rc = main(["op", "run", "no.such.module:Nope"])

        assert rc == 1
        assert capsys.readouterr().err

    def test_target_without_colon_exits_nonzero(self, capsys):
        rc = main(["op", "run", "not-a-target"])

        assert rc == 1
        assert "module:Qualname" in capsys.readouterr().err


def _seed_steps(root: Path, run_ids: list[str]) -> None:
    from uuid import uuid4

    from artisan.orchestration.engine.retained_step_tracker import RetainedStepTracker
    from artisan.schemas.orchestration.step_start_record import StepStartRecord
    from artisan.storage.io.retained_store import create_retained_store

    committer = create_retained_store(
        str(root),
        str(root.parent / "files"),
        fs=LocalFileSystem(),
    )
    for run_id in run_ids:
        tracker = RetainedStepTracker(committer, run_id)
        record = StepStartRecord(
            step_run_id=uuid4().hex,
            step_number=1,
            step_name="empty-step",
            operation_class="synthetic.Empty",
            params_json="{}",
            input_refs_json="{}",
            compute_backend="local",
            compute_options_json="{}",
            output_roles_json="[]",
            output_types_json="{}",
        )
        tracker.create_attempt(record)
        tracker.skip_pending(record.step_run_id, reason="empty_inputs")


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
        assert by_id["run-a"]["last_status"] == "skipped"
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

    def test_empty_store_returns_empty_report(self, tmp_path, capsys):
        _seed_steps(tmp_path, [])
        rc = main(["failures", "--delta-root", str(tmp_path), "--json"])

        assert rc == 0
        assert json.loads(capsys.readouterr().out) == {"items": []}


class TestProvenance:
    """CLI formatting and routing; retained-reader traversal is tested separately."""

    @pytest.mark.parametrize(
        ("direction", "depth", "truncated"),
        [("backward", 3, False), ("forward", 1, True)],
    )
    def test_edge_response(
        self, tmp_path, capsys, monkeypatch, direction, depth, truncated
    ):
        from artisan.provenance.reader import ProvenanceEdges

        edges = [{"source_artifact_id": "a" * 32, "target_artifact_id": "b" * 32}]
        read = MagicMock(
            return_value=ProvenanceEdges(
                artifact_id="b" * 32,
                direction=direction,
                depth=depth,
                edges=edges,
                truncated=truncated,
            )
        )
        monkeypatch.setattr("artisan.provenance.provenance_edges", read)
        assert (
            main(
                [
                    "provenance",
                    "b" * 32,
                    "--delta-root",
                    str(tmp_path),
                    "--direction",
                    direction,
                    "--depth",
                    str(depth),
                    "--json",
                ]
            )
            == 0
        )
        payload = json.loads(capsys.readouterr().out)
        assert payload["edges"] == edges
        assert payload["truncated"] is truncated
        assert read.call_args.kwargs["direction"] == direction
        assert read.call_args.kwargs["depth"] == depth

    def test_empty_supported_store_has_no_edges(self, tmp_path, capsys):
        _seed_steps(tmp_path, [])
        assert (
            main(["provenance", "a" * 32, "--delta-root", str(tmp_path), "--json"]) == 0
        )
        assert json.loads(capsys.readouterr().out)["edges"] == []


@pytest.mark.parametrize("apply", [False, True])
def test_store_repair_passes_independent_staging_controls(tmp_path, monkeypatch, apply):
    from artisan.storage.io import repair
    from artisan.storage.io.repair import RepairReport

    repair_mock = MagicMock(return_value=RepairReport(items=()))
    monkeypatch.setattr(repair, "repair_store", repair_mock)
    arguments = [
        "store",
        "repair",
        "--delta-root",
        str(tmp_path / "delta"),
        "--staging-root",
        str(tmp_path / "staging"),
        "--preserve-staging",
        "--files-root",
        str(tmp_path / "files"),
        "--json",
    ]
    if apply:
        arguments.append("--apply")
    assert main(arguments) == 0
    kwargs = repair_mock.call_args.kwargs
    assert kwargs["apply"] is apply
    assert "recover_staging" not in kwargs
    assert kwargs["preserve_staging"] is True
    assert kwargs["files_root"] == str(tmp_path / "files")


@pytest.mark.parametrize("flag", ["--abandon", "--reason", "--recover-staging"])
def test_store_repair_rejects_removed_actions_before_io(
    tmp_path, monkeypatch, capsys, flag
):
    from artisan.storage.io import repair

    repair_mock = MagicMock()
    monkeypatch.setattr(repair, "repair_store", repair_mock)
    with pytest.raises(SystemExit) as caught:
        main(
            [
                "store",
                "repair",
                "--delta-root",
                str(tmp_path / "delta"),
                "--staging-root",
                str(tmp_path / "staging"),
                flag,
            ]
        )
    assert caught.value.code == 2
    assert "unrecognized arguments" in capsys.readouterr().err
    repair_mock.assert_not_called()


def test_store_repair_rejected_application_performs_no_repair(
    tmp_path, monkeypatch, capsys
):
    repair = MagicMock(side_effect=AssertionError("repair before bootstrap"))
    monkeypatch.setattr("artisan.storage.io.repair.repair_store", repair)
    code = main(
        [
            "store",
            "repair",
            "--delta-root",
            str(tmp_path / "delta"),
            "--staging-root",
            str(tmp_path / "staging"),
            "--load-module",
            "missing_cli_repair_application",
            "--apply",
            "--json",
        ]
    )
    assert code == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["error_type"] == "config"
    assert payload["code"] == "application_registration_failed"
    assert "corrupt" not in json.dumps(payload)
    repair.assert_not_called()


@pytest.mark.parametrize(
    ("command", "expected_code", "expected_name"),
    [
        (["op", "list", "--query", "cli_runner_op_test"], 0, "cli_runner_op_test"),
        (["op", "describe", "cli_runner_op_test"], 0, "cli_runner_op_test"),
        (["op", "describe", "no_such_op_anywhere"], 1, None),
    ],
)
def test_operation_diagnostics_leave_one_json_response(
    command: list[str],
    expected_code: int,
    expected_name: str | None,
    monkeypatch: pytest.MonkeyPatch,
    capfd: pytest.CaptureFixture[str],
) -> None:
    from artisan import registry

    discover = registry.discover

    def noisy_discover(*args: Any, **kwargs: Any) -> Any:
        print("python discovery diagnostic", end="")
        os.write(1, b"native discovery diagnostic\n")
        return discover(*args, **kwargs)

    monkeypatch.setattr(registry, "discover", noisy_discover)
    assert main([*command, "--json"]) == expected_code
    output = capfd.readouterr()
    assert "python discovery diagnostic" in output.err
    assert "native discovery diagnostic" in output.err
    assert output.out.count("\n") == 1
    payload = json.loads(output.out)
    if expected_code:
        assert payload["code"] == "unknown_operation"
    elif command[1] == "list":
        assert [item["name"] for item in payload["items"]] == [expected_name]
    else:
        assert payload["name"] == expected_name


@pytest.mark.parametrize("failure", [False, True])
def test_store_bootstrap_diagnostics_leave_one_json_response(
    failure: bool,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capfd: pytest.CaptureFixture[str],
) -> None:
    from artisan.registry import discovery

    _seed_steps(tmp_path, ["bootstrap-run"])
    bootstrap = discovery.bootstrap_applications

    def noisy_bootstrap(*args: Any, **kwargs: Any) -> Any:
        print("python bootstrap diagnostic", end="")
        os.write(1, b"native bootstrap diagnostic\n")
        if failure:
            message = "missing test store"
            raise FileNotFoundError(message)
        return bootstrap(*args, **kwargs)

    monkeypatch.setattr(discovery, "bootstrap_applications", noisy_bootstrap)
    assert main(["runs", "--delta-root", str(tmp_path), "--json"]) == int(failure)
    output = capfd.readouterr()
    assert "python bootstrap diagnostic" in output.err
    assert "native bootstrap diagnostic" in output.err
    assert output.out.count("\n") == 1
    payload = json.loads(output.out)
    if failure:
        assert payload["code"] == "store_not_found"
        assert payload["recovery_hint"] == "CHECK_INPUT"
    else:
        assert [item["pipeline_run_id"] for item in payload["items"]] == [
            "bootstrap-run"
        ]


def test_serialization_diagnostics_stay_on_stderr(
    monkeypatch: pytest.MonkeyPatch, capfd: pytest.CaptureFixture[str]
) -> None:
    from artisan import cli

    render = cli._render

    def noisy_render(*args: Any, **kwargs: Any) -> Any:
        print("python serializer diagnostic")
        os.write(1, b"native serializer diagnostic\n")
        return render(*args, **kwargs)

    monkeypatch.setattr(cli, "_render", noisy_render)
    assert main(["op", "describe", "cli_runner_op_test", "--json"]) == 0
    output = capfd.readouterr()
    assert "python serializer diagnostic" in output.err
    assert "native serializer diagnostic" in output.err
    assert json.loads(output.out)["name"] == "cli_runner_op_test"


def test_unexpected_cli_failure_restores_python_and_native_stdout(
    monkeypatch: pytest.MonkeyPatch, capfd: pytest.CaptureFixture[str]
) -> None:
    from artisan import registry

    failure = RuntimeError("reader bug")

    def broken_describe(*args: Any, **kwargs: Any) -> Any:
        print("python failure diagnostic", end="")
        os.write(1, b"native failure diagnostic\n")
        raise failure

    monkeypatch.setattr(registry, "describe", broken_describe)
    with pytest.raises(RuntimeError) as exc_info:
        main(["op", "describe", "cli_runner_op_test", "--json"])
    assert exc_info.value is failure
    output = capfd.readouterr()
    assert output.out == ""
    assert "python failure diagnostic" in output.err
    assert "native failure diagnostic" in output.err
    print("restored python stdout", flush=True)
    os.write(1, b"restored native stdout\n")
    restored = capfd.readouterr()
    assert restored.out == "restored python stdout\nrestored native stdout\n"
    assert restored.err == ""


@pytest.mark.parametrize(
    "command",
    [
        ["modal", "deploy"],
        ["docker", "build"],
        ["environment", "prepare"],
        ["op", "image"],
    ],
)
def test_removed_operation_deployment_commands_reject_during_parsing(command, capsys):
    with pytest.raises(SystemExit) as caught:
        main(command)
    assert caught.value.code == 2
    assert "invalid choice" in capsys.readouterr().err
