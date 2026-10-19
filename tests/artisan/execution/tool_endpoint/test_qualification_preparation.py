"""Check cleanup across deployment and test initialization failures locally."""

from __future__ import annotations

import importlib
import json
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import AsyncMock, Mock

import modal
import pytest
from modal._utils.async_utils import synchronizer


@pytest.fixture
def qualification_modules(monkeypatch) -> tuple[ModuleType, ModuleType, ModuleType]:
    root = Path(__file__).resolve().parents[3] / "qualification" / "modal"
    monkeypatch.syspath_prepend(str(root))
    return (
        importlib.import_module("deploy_probe"),
        importlib.import_module("run_qualification"),
        importlib.import_module("test_modal_cancellation"),
    )


def test_failed_deployment_stops_exact_id_before_app_hydrates(
    monkeypatch, tmp_path: Path, qualification_modules
) -> None:
    deploy_probe, _, _ = qualification_modules
    app = modal.App("unit-test-owned-deployment")
    raw = synchronizer._translate_in(app)

    def fail_after_creation() -> None:
        raw._root_load_context._app_id = "ap-owned-test"
        msg = "synthetic image build failure"
        raise RuntimeError(msg)

    monkeypatch.setattr(app, "deploy", fail_after_creation)

    manifest = tmp_path / "deployment.json"
    monkeypatch.setattr(
        sys, "argv", ["deploy", "--deploy-reviewed-probe", "--manifest", str(manifest)]
    )
    monkeypatch.setattr(
        modal.App, "lookup", Mock(side_effect=modal.exception.NotFoundError("missing"))
    )
    monkeypatch.setattr(deploy_probe, "build_app", lambda _op: app)
    monkeypatch.setattr(
        deploy_probe.subprocess,
        "check_output",
        lambda *_args, **_kwargs: "local-test-head\n",
    )
    stop = Mock(return_value=SimpleNamespace(returncode=0))
    monkeypatch.setattr(deploy_probe.subprocess, "run", stop)
    with pytest.raises(RuntimeError, match="synthetic image build failure"):
        deploy_probe.main()
    assert stop.call_args.args[0][-1] == "ap-owned-test"
    recorded = json.loads(manifest.read_text())
    assert recorded["app_id"] == "ap-owned-test"
    assert recorded["failed_deployment_cleanup_returncode"] == 0


def test_test_process_failure_stops_exact_owned_app(
    monkeypatch, tmp_path: Path, qualification_modules
) -> None:
    _, run_qualification, _ = qualification_modules
    manifest = tmp_path / "deployment.json"
    manifest.write_text(
        json.dumps(
            {
                "app_name": "artisan-tool-managed_cancellation_probe",
                "app_id": "ap-owned-test",
            }
        )
    )
    monkeypatch.setattr(sys, "argv", ["run", "--manifest", str(manifest)])
    monkeypatch.setattr(
        run_qualification, "__file__", str(tmp_path / "run_qualification.py")
    )
    run = Mock(
        side_effect=[SimpleNamespace(returncode=1), SimpleNamespace(returncode=0)]
    )
    monkeypatch.setattr(run_qualification.subprocess, "run", run)
    assert run_qualification.main() == 1
    assert run.call_args_list[-1].args[0][-1] == "ap-owned-test"


def test_schema_failure_closes_client(
    monkeypatch, tmp_path: Path, qualification_modules
) -> None:
    _, _, qualification = qualification_modules
    monkeypatch.setattr(
        modal.App, "lookup", Mock(return_value=SimpleNamespace(app_id="ap-owned-test"))
    )
    endpoint = Mock()
    endpoint.get_web_url.return_value = "https://example.invalid"
    monkeypatch.setattr(modal.Function, "from_name", Mock(return_value=endpoint))
    monkeypatch.setattr(qualification, "source_digest", lambda: "same-source")
    monkeypatch.setattr(
        qualification.endpoint_client, "_auth_headers", lambda *_args: {}
    )
    client = Mock()
    client.get.return_value.json.return_value = {}
    monkeypatch.setattr(qualification.httpx, "Client", Mock(return_value=client))
    with pytest.raises(ValueError):
        qualification.Harness(
            {"app_id": "ap-owned-test", "source_digest": "same-source"},
            tmp_path / "evidence.json",
        )
    client.close.assert_called_once_with()


def test_provider_observation_uses_required_retained_output_cursor(
    monkeypatch, qualification_modules
) -> None:
    _, _, qualification = qualification_modules

    async def outputs(request):
        assert request.last_entry_id == "0-0"
        assert request.clear_on_success is False
        return qualification.api_pb2.FunctionGetOutputsResponse()

    client = SimpleNamespace(stub=SimpleNamespace(FunctionGetOutputs=outputs))
    monkeypatch.setattr(
        qualification._Client, "from_env", AsyncMock(return_value=client)
    )
    observed = qualification.provider_observation("fc-owned-test")
    assert observed["call_statuses"] == []


def test_snapshot_copies_only_framework_sources_and_reviewed_harness(
    monkeypatch, tmp_path: Path, qualification_modules
) -> None:
    prepare = importlib.import_module("prepare_snapshot")
    package = tmp_path / "source"
    package.mkdir()
    (package / "__init__.py").write_text("")
    (package / "module.py").write_text("VALUE = 1\n")
    (package / "py.typed").write_text("")
    (package / ".env").write_text("DO_NOT_COPY=synthetic\n")
    (package / "output.bin").write_bytes(b"not source")
    monkeypatch.setattr(prepare.artisan, "__file__", str(package / "__init__.py"))
    snapshot = tmp_path / "snapshot"
    monkeypatch.setattr(sys, "argv", ["prepare", "--snapshot", str(snapshot)])
    monkeypatch.setattr(
        prepare.subprocess,
        "check_output",
        lambda *_args, **kwargs: "local-test-head\n" if kwargs.get("text") else b"",
    )
    prepare.main()
    copied = {path.name for path in (snapshot / "src" / "artisan").iterdir()}
    assert copied == {"__init__.py", "module.py", "py.typed"}
    assert {path.name for path in (snapshot / "harness").iterdir()} == {
        "README.md",
        "managed_cancellation_probe.py",
        "prepare_snapshot.py",
        "deploy_probe.py",
        "test_modal_cancellation.py",
        "run_qualification.py",
        "local_smoke.py",
    }
