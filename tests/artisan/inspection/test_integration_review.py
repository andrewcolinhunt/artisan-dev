"""Cross-boundary regressions from the independent inspection integration review."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest.mock import Mock

import pytest

from artisan.cli import main
from artisan.errors import ArtisanError, ErrorCode
from artisan.inspection import inspect_evidence, load_evidence_export
from artisan.inspection._boundary import error_payload
from artisan.inspection._evidence import decode_json_object, encode_json

_PRIVATE = "PRIVATE_REQUEST_VALUE"


def _request(capability: str) -> dict[str, Any]:
    selection = {"pipeline_run_id": "run", "step_name": "score", "role": "metrics"}
    assessment = {
        "label": "quality",
        "checks": [
            {
                "check_id": "score",
                "metric_path": ["score"],
                "operator": "ge",
                "threshold": 1,
            }
        ],
    }
    if capability == "assess_outcomes":
        return {**selection, "spec": assessment}
    return {
        "baseline": dict(selection),
        "candidate": dict(selection),
        "spec": {
            "label": "comparison",
            "matching": {"kind": "metadata_key", "key": "case"},
            "metrics": [
                {
                    "metric_id": "score",
                    "metric_path": ["score"],
                    "direction": "higher",
                    "measurement_id": "score",
                    "measurement_version": "1",
                    "unit": "unitless",
                }
            ],
            "assessment": assessment,
        },
    }


@pytest.mark.parametrize(
    ("capability", "location", "value", "code"),
    [
        (
            "assess_outcomes",
            ("spec", "checks", 0, "threshold"),
            _PRIVATE,
            ErrorCode.ASSESSMENT_SPEC_INVALID,
        ),
        (
            "assess_outcomes",
            ("spec", "checks", 0, "threshold"),
            True,
            ErrorCode.ASSESSMENT_SPEC_INVALID,
        ),
        (
            "assess_outcomes",
            ("spec", _PRIVATE),
            "extra",
            ErrorCode.ASSESSMENT_SPEC_INVALID,
        ),
        (
            "assess_outcomes",
            ("step_number",),
            True,
            ErrorCode.OUTCOME_SELECTION_INVALID,
        ),
        (
            "compare_runs",
            ("baseline", "step_number"),
            _PRIVATE,
            ErrorCode.OUTCOME_SELECTION_INVALID,
        ),
        (
            "compare_runs",
            ("candidate", _PRIVATE),
            "extra",
            ErrorCode.OUTCOME_SELECTION_INVALID,
        ),
        (
            "compare_runs",
            ("spec", "metrics", 0, "absolute_tolerance"),
            True,
            ErrorCode.COMPARISON_SPEC_INVALID,
        ),
        (
            "compare_runs",
            ("spec", "assessment", "checks", 0, "threshold"),
            _PRIVATE,
            ErrorCode.COMPARISON_SPEC_INVALID,
        ),
    ],
)
def test_nested_request_errors_match_python_and_cli_before_bootstrap(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capfd: pytest.CaptureFixture[str],
    capability: str,
    location: tuple[str | int, ...],
    value: Any,
    code: str,
) -> None:
    arguments = _request(capability)
    target = arguments
    for component in location[:-1]:
        target = target[component]
    target[location[-1]] = value
    with pytest.raises(ArtisanError) as caught:
        inspect_evidence(capability, arguments, delta_root="/unopened")
    expected = error_payload(caught.value)
    assert expected["code"] == code
    assert _PRIVATE not in encode_json(expected).decode()
    assert "cause" not in expected

    def no_source(*args: Any, **kwargs: Any) -> None:
        pytest.fail("Invalid nested request reached bootstrap or the store")

    monkeypatch.setattr("artisan._inspection_cli._store_context", no_source)
    request = tmp_path / "request.json"
    request.write_bytes(encode_json(arguments))
    assert (
        main(["inspect", "run", capability, "--request", str(request), "--json"]) == 1
    )
    assert capfd.readouterr().out == encode_json(expected).decode() + "\n"


@pytest.mark.parametrize(
    "target", ["json.loads", "artisan.inspection._evidence.json_ready"]
)
def test_decode_json_object_handles_recursion_error(
    monkeypatch: pytest.MonkeyPatch, target: str
) -> None:
    # Process-wide recursion limits can change when other tests import dependencies.
    failure = RecursionError(_PRIVATE)
    recurse = Mock(side_effect=failure)
    with monkeypatch.context() as patch:
        patch.setattr(target, recurse)
        with pytest.raises(ValueError) as caught:
            decode_json_object("{}")
    recurse.assert_called_once()
    assert str(caught.value) == "Expected one valid UTF-8 JSON object"
    assert caught.value.__cause__ is failure


def test_cli_request_recursion_returns_sanitized_request_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capfd: pytest.CaptureFixture[str],
) -> None:
    request = tmp_path / "request.json"
    request.write_text("{}")
    recurse = Mock(side_effect=RecursionError(_PRIVATE))
    store = Mock(
        side_effect=AssertionError("Invalid request reached bootstrap or the store")
    )
    with monkeypatch.context() as patch:
        patch.setattr("json.loads", recurse)
        patch.setattr("artisan._inspection_cli._store_context", store)
        assert (
            main(["inspect", "run", "list_runs", "--request", str(request), "--json"])
            == 1
        )
    recurse.assert_called_once()
    store.assert_not_called()
    output = capfd.readouterr()
    payload = json.loads(output.out)
    assert payload["code"] == ErrorCode.PARAM_TYPE_MISMATCH
    assert payload["field"] == "request"
    assert "cause" not in payload
    assert _PRIVATE not in output.out + output.err
    assert "Traceback" not in output.err


def test_export_manifest_recursion_returns_integrity_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "export"
    path.mkdir()
    (path / "report.json").write_text("{}")
    (path / "schema.json").write_text("{}")
    (path / "manifest.json").write_text("{}")
    failure = RecursionError(_PRIVATE)
    recurse = Mock(side_effect=failure)
    with monkeypatch.context() as patch:
        patch.setattr("json.loads", recurse)
        with pytest.raises(ArtisanError) as caught:
            load_evidence_export(path)
    recurse.assert_called_once()
    assert caught.value.code == ErrorCode.EVIDENCE_EXPORT_INTEGRITY_FAILED
    assert isinstance(caught.value.__cause__, ValueError)
    assert caught.value.__cause__.__cause__ is failure
    payload = error_payload(caught.value)
    assert "cause" not in payload
    assert _PRIVATE not in encode_json(payload).decode()
