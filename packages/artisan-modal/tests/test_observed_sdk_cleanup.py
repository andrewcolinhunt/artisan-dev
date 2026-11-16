"""No-network regressions for cleanup inside the observed pinned SDK scope."""

from __future__ import annotations

import asyncio
import importlib
import inspect
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from modal import runner
from modal.client import _Client
from modal_proto import api_pb2


@pytest.fixture
def observer(monkeypatch: pytest.MonkeyPatch) -> ModuleType:
    monkeypatch.syspath_prepend(str(Path(__file__).parent / "qualification"))
    return importlib.import_module("final_apps")


def test_observer_preserves_actual_sdk_signatures_and_exact_originals(
    observer: ModuleType,
    tmp_path: Path,
) -> None:
    originals = {name: getattr(runner, name) for name in observer._SIGNATURES}
    with observer.observe_apps(tmp_path / "apps.json"):
        observer.check_sdk_hooks()
        for name, original in originals.items():
            observed = getattr(runner, name)
            assert observed is not original
            assert inspect.unwrap(observed) is original
            assert inspect.signature(observed) == inspect.signature(original)
            assert inspect.iscoroutinefunction(observed)
    assert all(
        getattr(runner, name) is original for name, original in originals.items()
    )


@pytest.mark.parametrize("action", ["list", "stop"])
def test_cleanup_inside_observed_scope_keeps_exact_id_rpc_bounds(
    observer: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, action: str
) -> None:
    registry = tmp_path / "apps.json"
    listing = AsyncMock(
        return_value=api_pb2.AppListResponse(
            apps=[
                {
                    "app_id": "ap-Owned",
                    "state": api_pb2.APP_STATE_STOPPED,
                    "n_running_tasks": 0,
                },
                {
                    "app_id": "ap-Unrelated",
                    "state": api_pb2.APP_STATE_STOPPED,
                    "n_running_tasks": 0,
                },
            ]
        )
    )
    stopping = AsyncMock()
    client = SimpleNamespace(stub=SimpleNamespace(AppList=listing, AppStop=stopping))
    construct = AsyncMock(return_value=client)
    monkeypatch.setattr(_Client, "from_env", construct)
    with observer.observe_apps(registry):
        observer.write_json(registry, {"app_ids": ["ap-Owned"], "pending_creations": 0})
        result = asyncio.run(observer.app_control(registry, action, "ap-Owned"))
    assert result == (
        [{"App ID": "ap-Owned", "State": "stopped", "Tasks": 0}]
        if action == "list"
        else []
    )
    called = listing if action == "list" else stopping
    assert called.await_count == 1
    assert called.await_args.kwargs == {"timeout": 5, "retry": None}
    if action == "stop":
        assert called.await_args.args[0].app_id == "ap-Owned"
    construct.assert_awaited_once()


def test_signature_drift_inside_scope_still_rejects_before_client(
    observer: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    construct = AsyncMock()
    monkeypatch.setattr(_Client, "from_env", construct)
    registry = tmp_path / "apps.json"

    async def incompatible(client: object) -> None:
        del client
        message = "Must never call incompatible SDK"
        raise AssertionError(message)

    with observer.observe_apps(registry):
        observer.write_json(registry, {"app_ids": ["ap-Owned"], "pending_creations": 0})
        with monkeypatch.context() as temporary:
            temporary.setattr(runner, "_init_local_app_new", incompatible)
            with pytest.raises(ValueError, match="signature changed"):
                asyncio.run(observer.app_control(registry, "list", "ap-Owned"))
    construct.assert_not_called()
