"""Qualification checkpoints never replace the observed transport outcome."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from unittest.mock import Mock

import pytest
from artisan_modal import pack_worker
from artisan_modal.pack_owner import ModalPackOwner
from test_pack_owner import Function, finish, outcome


def progress_class():
    path = Path(__file__).parent / "qualification/progress.py"
    spec = importlib.util.spec_from_file_location("modal_test_progress", path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.Progress


@pytest.mark.usefixtures("control_fs", "pack_runtime")
def test_checkpoint_contains_sanitized_sdk_phase_before_interaction(
    tmp_path: Path,
    pack_request,
    monkeypatch,
) -> None:
    path = tmp_path / "progress.json"
    progress = progress_class()(path)
    progress.update(app_id="ap-example", phase="remote_transform")
    function = Function()
    original_spawn = function.spawn
    monkeypatch.setattr(pack_worker, "execute_call", lambda *_: outcome())

    def spawn(encoded: bytes):
        saved = json.loads(path.read_text())
        assert saved["app_id"] == "ap-example"
        events = next(iter(saved["owners"].values()))
        assert any(event["phase"] == "submit" for event in events.values())
        return original_spawn(encoded)

    monkeypatch.setattr(function, "spawn", spawn)
    owner = ModalPackOwner(function, lambda: pack_request, count=3)
    try:
        owner.start()
        assert owner.result(0, 2)
        finish(owner, function)
        encoded = path.read_text()
        assert "request-secret" not in encoded
        assert "fc-example" in encoded
        assert path.stat().st_mode & 0o777 == 0o600
    finally:
        progress.close()


def test_checkpoint_write_failure_is_separate_and_redacted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    progress = progress_class()(tmp_path / "progress.json")
    progress.learn(("synthetic-key",))
    try:
        monkeypatch.setattr("os.replace", Mock(side_effect=OSError("synthetic-key")))
        progress.update(phase="submit")
        assert progress.write_error == "Qualification checkpoint persistence failed"
        progress.failure(
            "body_failure",
            ValueError("https://user:capability@example.test/?token=secret"),
        )
        assert "capability" not in json.dumps(progress._data)
    finally:
        progress.close()


def test_checkpoint_preserves_existing_receipt(tmp_path: Path) -> None:
    path = tmp_path / "progress.json"
    path.write_text("original")
    with pytest.raises(FileExistsError):
        progress_class()(path)
    assert path.read_text() == "original"


def test_pipeline_body_and_cleanup_failures_remain_separate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from types import SimpleNamespace

    from artisan.orchestration.runner_api import CommandOwnershipError, OwnershipState

    directory = Path(__file__).parent / "qualification"
    monkeypatch.syspath_prepend(str(directory))
    spec = importlib.util.spec_from_file_location(
        "modal_assignment_test", directory / "assignment.py"
    )
    assert spec is not None
    assert spec.loader is not None
    driver = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(driver)
    fs = Mock()
    storage = SimpleNamespace(
        filesystem=lambda: fs,
        delta_storage_options=dict,
        options={"secret": "selected-password"},
        delta_options={},
    )
    monkeypatch.setattr(driver, "StorageConfig", lambda **_kwargs: storage)
    pipeline = Mock()
    pipeline.dispatch = SimpleNamespace(runtime_env={}, storage=storage)
    pipeline.config.pipeline_run_id = "test-diagnostic-run"
    pipeline.run.side_effect = ValueError("body selected-password")
    pipeline.cancel.side_effect = OSError("cancel selected-password")
    pipeline.close.side_effect = CommandOwnershipError("close selected-password")
    pipeline.ownership_state.return_value = OwnershipState.UNKNOWN
    handle = Mock()
    handle.done.return_value = True
    handle.ownership_state.return_value = OwnershipState.UNKNOWN
    handle.worker_owner.observations = ()
    handle.worker_owner.diagnostic_write_error = None
    pipeline.executions = (SimpleNamespace(handles=(handle,)),)
    monkeypatch.setattr(driver.PipelineManager, "create", Mock(return_value=pipeline))
    report = {}
    scratch = tmp_path / "working"
    scratch.mkdir()
    progress = progress_class()(tmp_path / "checkpoint.json")
    try:
        with pytest.raises(ValueError, match="body"):
            driver._pipeline(
                "https://example.test",
                {
                    "MINIO_ROOT_USER": "selected-key",
                    "MINIO_ROOT_PASSWORD": "selected-password",
                },
                Mock(),
                scratch,
                report,
                progress,
            )
        assert report["pipeline_error"]["type"] == "ValueError"
        assert report["pipeline_cleanup_error"]["type"] == "CommandOwnershipError"
        assert report["pipeline_cancel_error"]["type"] == "OSError"
        assert report["handle_outcomes"][0]["ownership"] == "unknown"
        assert report["unresolved_scratch_retained"] is True
        assert "selected-password" not in json.dumps(report)
        assert "selected-password" not in progress.path.read_text()
        assert scratch.exists()
        fs.rm.assert_not_called()
    finally:
        progress.close()


def test_checkpoint_error_formatter_cannot_replace_original_failure(
    tmp_path: Path,
) -> None:
    class BrokenMessageError(RuntimeError):
        def __str__(self) -> str:
            message = "selected-token"
            raise RuntimeError(message)

    progress = progress_class()(tmp_path / "checkpoint.json")
    try:
        progress.failure("body_failure", BrokenMessageError())
        assert progress.write_error == "Qualification diagnostic formatting failed"
        progress.update(phase="cleanup")
        assert "selected-token" not in progress.path.read_text()
    finally:
        progress.close()


@pytest.mark.parametrize("formatter_failure", [False, True])
def test_gate_error_and_store_cleanup_formatting_is_safe(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, formatter_failure: bool
) -> None:
    directory = Path(__file__).parent / "qualification"
    monkeypatch.syspath_prepend(str(directory))
    spec = importlib.util.spec_from_file_location(
        "modal_guard_test", directory / "assignment.py"
    )
    assert spec is not None
    assert spec.loader is not None
    driver = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(driver)

    class BrokenMessageError(RuntimeError):
        def __str__(self) -> str:
            message = "selected-token"
            raise RuntimeError(message)

    error = BrokenMessageError() if formatter_failure else ValueError("selected-token")
    detail = driver._safe_error(error, ("selected-token",))
    assert "selected-token" not in json.dumps(detail)
    if formatter_failure:
        assert detail["type"] == "diagnostic_failure"
    control, call = Mock(), Mock()
    control.put.side_effect = error
    call.get.side_effect = TimeoutError("selected-token")
    call.cancel.side_effect = OSError("selected-token")
    report = {}
    progress = progress_class()(tmp_path / "checkpoint.json")
    progress.learn(("selected-token",))
    try:
        assert driver._stop_store(control, call, report, progress) is error
        assert report["server_cleanup"] == {"ownership": "unknown"}
        assert len(report["server_cleanup_errors"]) == 3
        assert "selected-token" not in json.dumps(report)
        assert "selected-token" not in progress.path.read_text()
        call.cancel.assert_called_once_with(terminate_containers=True)
    finally:
        progress.close()
