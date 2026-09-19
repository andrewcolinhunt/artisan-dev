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
    assert "--staging-root" in payload["message"]


@pytest.mark.parametrize("supply", ["token=ENV", "/params/token", "/params/token="])
def test_replay_supply_rejects_values_without_slot_and_name(tmp_path, capsys, supply):
    code = main(
        [
            "execution",
            "replay",
            "id",
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
