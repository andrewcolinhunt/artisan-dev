"""Local inspection boundaries preserve contracts and isolate diagnostics."""

from __future__ import annotations

import io
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any
from unittest.mock import Mock

import pytest
from fixtures.outcome_store import append_metric_attempt

from artisan.cli import main
from artisan.errors import ArtisanError, ErrorCode
from artisan.inspection import inspect_evidence, list_inspections, read_operating_guide
from artisan.inspection._evidence import MAX_PAYLOAD_BYTES, json_ready
from artisan.registry import discovery
from artisan.schemas import MetricArtifact


@pytest.fixture
def store(tmp_path: Path) -> str:
    root = tmp_path / "store"
    for name in ("first", "second"):
        append_metric_attempt(
            root,
            [MetricArtifact.draft({"value": 1}, name, 0).finalize()],
            pipeline_run_id=name,
        )
    return str(root)


def test_catalog_matches_python_without_discovery(
    monkeypatch: pytest.MonkeyPatch, capfd: pytest.CaptureFixture[str]
) -> None:
    discover = Mock(side_effect=AssertionError("discovery must not run"))
    monkeypatch.setattr(discovery, "discover", discover)
    assert main(["inspect", "list", "--json"]) == 0
    assert json.loads(capfd.readouterr().out) == list_inspections().model_dump(
        mode="json"
    )
    discover.assert_not_called()


def test_cli_and_python_continue_same_cursor(
    store: str, tmp_path: Path, capfd: pytest.CaptureFixture[str]
) -> None:
    first = inspect_evidence("list_runs", {"limit": 1}, delta_root=store)
    follow_up = first.model_dump(mode="json")["follow_up"]
    request = tmp_path / "request.json"
    request.write_text(json.dumps(follow_up["arguments"]))
    assert (
        main(
            [
                "inspect",
                "run",
                follow_up["capability"],
                "--request",
                str(request),
                "--delta-root",
                store,
                "--json",
            ]
        )
        == 0
    )
    expected = inspect_evidence(
        follow_up["capability"], follow_up["arguments"], delta_root=store
    )
    payload = json.loads(capfd.readouterr().out)
    assert payload == json_ready(expected)
    assert payload["has_more"] is False


@pytest.mark.parametrize(
    "data",
    [
        b"",
        b"[]",
        b"null",
        b'{"limit":1,"limit":2}',
        b'{"limit":NaN}',
        b'{"limit":Infinity}',
        b'{"limit":1e999}',
        b"{} {}",
        b"\xff",
        b'{"limit":true}',
        b'{"limit":"1"}',
        b'{"limit":0}',
        b'{"unknown":"private"}',
    ],
)
def test_bad_requests_fail_before_bootstrap(
    data: bytes,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capfd: pytest.CaptureFixture[str],
) -> None:
    request = tmp_path / "request.json"
    request.write_bytes(data)
    bootstrap = Mock(side_effect=AssertionError("bootstrap must not run"))
    monkeypatch.setattr(discovery, "bootstrap_applications", bootstrap)
    assert (
        main(
            [
                "inspect",
                "run",
                "list_runs",
                "--request",
                str(request),
                "--json",
            ]
        )
        == 1
    )
    payload = json.loads(capfd.readouterr().out)
    assert payload["code"] == ErrorCode.PARAM_TYPE_MISMATCH
    assert "private" not in json.dumps(payload)
    bootstrap.assert_not_called()


@pytest.mark.parametrize("request_path", ["missing.json", "/"])
def test_request_io_is_not_a_store_error(
    request_path: str, capfd: pytest.CaptureFixture[str]
) -> None:
    assert (
        main(["inspect", "run", "list_runs", "--request", request_path, "--json"]) == 1
    )
    assert json.loads(capfd.readouterr().out)["code"] == ErrorCode.PARAM_TYPE_MISMATCH


def test_stdin_and_environment_root(
    store: str, monkeypatch: pytest.MonkeyPatch, capfd: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(sys, "stdin", io.TextIOWrapper(io.BytesIO(b'{"limit":1}')))
    monkeypatch.setenv("ARTISAN_DELTA_ROOT", store)
    assert main(["inspect", "run", "list_runs", "--request", "-", "--json"]) == 0
    assert json.loads(capfd.readouterr().out)["returned_items"] == 1


def test_python_and_native_diagnostics_are_stderr(
    store: str, monkeypatch: pytest.MonkeyPatch, capfd: pytest.CaptureFixture[str]
) -> None:
    bootstrap = discovery.bootstrap_applications

    def noisy(*args: Any, **kwargs: Any) -> Any:
        print("python diagnostic")
        os.write(1, b"native diagnostic\n")
        return bootstrap(*args, **kwargs)

    monkeypatch.setattr(discovery, "bootstrap_applications", noisy)
    assert main(["inspect", "run", "list_runs", "--delta-root", store, "--json"]) == 0
    output = capfd.readouterr()
    assert "python diagnostic" in output.err
    assert "native diagnostic" in output.err
    assert json.loads(output.out)["total_items"] == 2
    assert output.out.count("\n") == 1


def test_store_bootstrap_failure_is_fatal_and_bounded(
    store: str, monkeypatch: pytest.MonkeyPatch, capfd: pytest.CaptureFixture[str]
) -> None:
    failure = ArtisanError(
        "test_error", "x" * (MAX_PAYLOAD_BYTES + 1), error_type="config"
    )
    monkeypatch.setattr(discovery, "bootstrap_applications", Mock(side_effect=failure))
    assert main(["inspect", "run", "list_runs", "--delta-root", store, "--json"]) == 1
    output = capfd.readouterr().out
    assert len(output.encode()) <= MAX_PAYLOAD_BYTES + 1
    assert json.loads(output)["code"] == ErrorCode.EVIDENCE_RESPONSE_TOO_LARGE


def test_discovery_failure_is_inspectable(capfd: pytest.CaptureFixture[str]) -> None:
    assert (
        main(
            [
                "inspect",
                "run",
                "capabilities",
                "--load-module",
                "missing_test_application",
                "--json",
            ]
        )
        == 0
    )
    assert json.loads(capfd.readouterr().out)["error_count"] == 1


def test_unexpected_defect_propagates_without_json(
    store: str, monkeypatch: pytest.MonkeyPatch, capfd: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(
        discovery,
        "bootstrap_applications",
        Mock(side_effect=RuntimeError("reader bug")),
    )
    with pytest.raises(RuntimeError, match="reader bug"):
        main(["inspect", "run", "list_runs", "--delta-root", store, "--json"])
    assert capfd.readouterr().out == ""


@pytest.mark.parametrize(
    ("command", "code"),
    [
        (["inspect", "describe", "unknown"], ErrorCode.UNKNOWN_INSPECTION),
        (["inspect", "run", "unknown"], ErrorCode.UNKNOWN_INSPECTION),
        (
            ["guide", "show", "pipeline-operate", "--topic", "../private"],
            ErrorCode.UNKNOWN_OPERATING_GUIDE_TOPIC,
        ),
    ],
)
def test_known_errors_have_single_envelope(
    command: list[str], code: str, capfd: pytest.CaptureFixture[str]
) -> None:
    assert main([*command, "--json"]) == 1
    output = capfd.readouterr().out
    assert json.loads(output)["code"] == code
    assert output.count("\n") == 1
    assert "private" not in output


def test_guide_human_output_preserves_markdown(
    capfd: pytest.CaptureFixture[str],
) -> None:
    assert main(["guide", "show", "pipeline-operate"]) == 0
    output = capfd.readouterr().out
    assert output.endswith(read_operating_guide().markdown)
    assert read_operating_guide().content_digest in output


@pytest.mark.parametrize(
    "command",
    [
        ["inspect"],
        ["inspect", "run"],
        ["guide", "show", "unknown"],
        ["evidence", "export", "list_runs"],
    ],
)
def test_argument_errors_exit_two(command: list[str]) -> None:
    with pytest.raises(SystemExit) as error:
        main(command)
    assert error.value.code == 2


def test_help_avoids_heavy_imports() -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            """
import sys
from artisan.cli import main
try:
    main(['inspect', '--help'])
except SystemExit as exc:
    assert exc.code == 0
assert not any(name.startswith(('artisan.inspection', 'artisan.registry', 'fastmcp', 'polars')) for name in sys.modules)
""",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
