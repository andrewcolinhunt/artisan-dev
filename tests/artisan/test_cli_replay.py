"""CLI replay argument and structured preflight errors."""

from __future__ import annotations

import json

import pytest

from artisan.cli import main


def test_replay_help_exposes_unit_and_diagnostic_options(capsys):
    with pytest.raises(SystemExit) as caught:
        main(["execution", "replay", "--help"])
    assert caught.value.code == 0
    output = capsys.readouterr().out
    for flag in ("--debug-root", "--supply", "--local-runner", "--allow-code-change"):
        assert flag in output


def test_replay_cloud_requires_explicit_remote_roots(tmp_path, capsys):
    code = main(
        [
            "execution",
            "replay",
            "id",
            "--operation",
            "artisan.operations.examples.data_generator:DataGenerator",
            "--delta-root",
            "s3://bucket/delta",
            "--debug-root",
            str(tmp_path),
            "--json",
        ]
    )
    assert code == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["code"] == "replay_configuration_invalid"
    assert "--files-root" in payload["message"]


@pytest.mark.parametrize("supply", ["token=ENV", "/params/token", "/params/token="])
def test_replay_supply_rejects_values_without_slot_and_name(tmp_path, capsys, supply):
    code = main(
        [
            "execution",
            "replay",
            "id",
            "--operation",
            "artisan.operations.examples.data_generator:DataGenerator",
            "--delta-root",
            str(tmp_path / "delta"),
            "--debug-root",
            str(tmp_path / "debug"),
            "--supply",
            supply,
            "--json",
        ]
    )
    assert code == 1
    assert json.loads(capsys.readouterr().out)["code"] == "replay_configuration_invalid"


def test_replay_rejected_application_prevents_store_reads(
    tmp_path, capsys, monkeypatch
):
    import importlib
    from unittest.mock import Mock

    replay = importlib.import_module("artisan.orchestration.replay")
    read = Mock(side_effect=AssertionError("read before bootstrap"))
    monkeypatch.setattr(replay, "open_configured_store", read)
    code = main(
        [
            "execution",
            "replay",
            "id",
            "--operation",
            "artisan.operations.examples.data_generator:DataGenerator",
            "--delta-root",
            str(tmp_path / "delta"),
            "--debug-root",
            str(tmp_path / "debug"),
            "--load-module",
            "missing_cli_replay_application",
            "--json",
        ]
    )
    assert code == 1
    assert (
        json.loads(capsys.readouterr().out)["code"] == "application_registration_failed"
    )
    read.assert_not_called()


def test_replay_resolves_explicit_definitions_and_replacement_values(
    tmp_path,
    monkeypatch,
    capsys,
):
    from unittest.mock import Mock

    from artisan import orchestration
    from artisan.operations.examples.data_generator import DataGenerator
    from artisan.orchestration.runners import LocalRunner
    from artisan.schemas.orchestration.replay_result import ReplayResult
    from artisan.schemas.orchestration.step_lifecycle import StepDisposition, StepStatus
    from artisan.schemas.orchestration.step_result import StepResult

    outcome = ReplayResult(
        source_execution_run_id="source",
        pipeline_run_id="diagnostic",
        step_run_id="new-step",
        execution_run_id="new-execution",
        step_result=StepResult(
            step_name="example",
            step_number=0,
            status=StepStatus.SUCCEEDED,
            disposition=StepDisposition.EXECUTED,
        ),
        diagnostic_roots={},
        diagnostic_status="complete",
        diagnostic_errors=[],
        reproducibility_notes=[],
    )
    run = Mock(return_value=outcome)
    monkeypatch.setattr(orchestration, "replay_execution", run)
    monkeypatch.setenv("TEST_RUNTIME_VALUE", '"keep-runtime-quotes"')
    monkeypatch.setenv("TEST_CONFIG_VALUE", '{"token": "private-value"}')
    code = main(
        [
            "execution",
            "replay",
            "source",
            "--operation",
            "artisan.operations.examples.data_generator:DataGenerator",
            "--delta-root",
            str(tmp_path / "delta"),
            "--files-root",
            str(tmp_path / "carriers"),
            "--debug-root",
            str(tmp_path / "debug"),
            "--local-runner",
            "--supply",
            "/params/credentials=TEST_CONFIG_VALUE",
            "--runtime",
            "TOKEN=TEST_RUNTIME_VALUE",
            "--json",
        ]
    )
    assert code == 0
    values = run.call_args.kwargs
    assert values["operation"] is DataGenerator
    assert isinstance(values["step_runner"], LocalRunner)
    assert values["runtime_replacements"] == {"TOKEN": '"keep-runtime-quotes"'}
    assert values["configuration_replacements"] == {
        "/params/credentials": {"token": "private-value"},
    }
    assert values["files_root"] == str(tmp_path / "carriers")
    assert "private-value" not in capsys.readouterr().out
