"""No-network checks of the frozen qualification launcher and its cleanup gates."""

from __future__ import annotations

import asyncio
import importlib
import json
import tomllib
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest


@pytest.fixture
def launcher(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.syspath_prepend(str(Path(__file__).parent / "qualification"))
    return importlib.import_module("final_launch")


def closed(**changes):
    return {
        "ownership": "closed",
        "collector_stopped": True,
        "collection_complete": True,
        "owner_exited": True,
        "exit_code": 0,
        **changes,
    }


@pytest.mark.parametrize("existing", [False, True])
def test_pinned_sdk_hook_records_before_return_without_changing_value(
    launcher, monkeypatch: pytest.MonkeyPatch, tmp_path: Path, existing: bool
) -> None:
    assert launcher is not None
    from modal import runner

    observer = importlib.import_module("final_apps")
    registry = tmp_path / "apps.json"
    returned = SimpleNamespace(app_id="ap-ExactReturned123")

    async def new(
        client, description, tags, app_state, environment_name="", interactive=False
    ):
        del client, description, tags, app_state, environment_name, interactive
        assert json.loads(registry.read_bytes()) == {
            "app_ids": [],
            "pending_creations": 1,
        }
        return returned

    async def named(client, name, tags, environment_name=""):
        return await new(client, name, tags, None, environment_name)

    monkeypatch.setattr(runner, "_init_local_app_new", new)
    monkeypatch.setattr(runner, "_init_local_app_from_name", named)
    with observer.observe_apps(registry):
        assert observer.recorded_apps() == registry
        result = asyncio.run(
            runner._init_local_app_from_name(None, "unused", {})
            if existing
            else runner._init_local_app_new(None, "unused", {}, None)
        )
        assert result is returned
        assert json.loads(registry.read_bytes()) == {
            "app_ids": [returned.app_id],
            "pending_creations": 0,
        }
    assert runner._init_local_app_new is new
    assert runner._init_local_app_from_name is named
    with pytest.raises(ValueError, match="active recorded ownership"):
        observer.recorded_apps()


def test_signature_change_rejects_before_any_sdk_or_registry_work(
    launcher, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    assert launcher is not None
    from modal import runner

    observer = importlib.import_module("final_apps")
    called = False

    async def incompatible(client):
        del client
        nonlocal called
        called = True

    monkeypatch.setattr(runner, "_init_local_app_new", incompatible)
    with (
        pytest.raises(ValueError, match="signature changed"),
        observer.observe_apps(tmp_path / "apps.json"),
    ):
        pytest.fail("An unsupported SDK must fail before launch")
    assert not called
    assert not (tmp_path / "apps.json").exists()


def test_ambiguous_creation_preserves_pending_and_restores_hook(
    launcher, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from modal import runner

    observer = importlib.import_module("final_apps")
    registry = tmp_path / "apps.json"

    async def new(
        client, description, tags, app_state, environment_name="", interactive=False
    ):
        del client, description, tags, app_state, environment_name, interactive
        message = "selected-secret"
        raise OSError(message)

    monkeypatch.setattr(runner, "_init_local_app_new", new)
    with (
        pytest.raises(OSError, match="selected-secret"),
        observer.observe_apps(registry),
    ):
        asyncio.run(runner._init_local_app_new(None, "unused", {}, None))
    assert runner._init_local_app_new is new
    assert json.loads(registry.read_bytes()) == {"app_ids": [], "pending_creations": 1}
    assert not launcher.apps_closed(registry, {})
    assert "selected-secret" not in registry.read_text()


def test_named_sdk_creation_nested_new_hook_keeps_one_owned_id(
    launcher, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    assert launcher is not None
    from modal import runner

    observer = importlib.import_module("final_apps")
    registry = tmp_path / "apps.json"
    returned = SimpleNamespace(app_id="ap-Nested")

    async def new(
        client, description, tags, app_state, environment_name="", interactive=False
    ):
        del client, description, tags, app_state, environment_name, interactive
        assert json.loads(registry.read_bytes())["pending_creations"] == 2
        return returned

    async def named(client, name, tags, environment_name=""):
        return await runner._init_local_app_new(
            client, name, tags, None, environment_name
        )

    monkeypatch.setattr(runner, "_init_local_app_new", new)
    monkeypatch.setattr(runner, "_init_local_app_from_name", named)
    with observer.observe_apps(registry):
        assert (
            asyncio.run(runner._init_local_app_from_name(None, "unused", {}))
            is returned
        )
    assert json.loads(registry.read_bytes()) == {
        "app_ids": [returned.app_id],
        "pending_creations": 0,
    }


def test_app_creation_bound_rejects_before_fifth_sdk_call(
    launcher, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    assert launcher is not None
    from modal import runner

    observer = importlib.import_module("final_apps")
    registry = tmp_path / "apps.json"
    entered = []

    async def new(
        client, description, tags, app_state, environment_name="", interactive=False
    ):
        del client, description, tags, app_state, environment_name, interactive
        entered.append(len(entered))
        return SimpleNamespace(app_id=f"ap-Bounded{len(entered)}")

    monkeypatch.setattr(runner, "_init_local_app_new", new)
    with observer.observe_apps(registry):
        for _ in range(4):
            asyncio.run(runner._init_local_app_new(None, "unused", {}, None))
        with pytest.raises(ValueError, match="four-App"):
            asyncio.run(runner._init_local_app_new(None, "unused", {}, None))
    assert len(entered) == 4
    assert json.loads(registry.read_bytes())["pending_creations"] == 0


def test_independent_cleanup_targets_only_recorded_app(
    launcher, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    registry = tmp_path / "apps.json"
    launcher.write_json(registry, {"app_ids": ["ap-Owned"], "pending_creations": 0})
    commands = []

    def run(argv, _runtime, **_kwargs):
        commands.append(argv)
        stopped = any("stop" in command for command in commands)
        return closed(), json.dumps(
            [
                {"App ID": "ap-Unrelated", "State": "running", "Tasks": 1},
                {
                    "App ID": "ap-Owned",
                    "State": "stopped" if stopped else "running",
                    "Tasks": 0 if stopped else 1,
                },
            ]
        )

    monkeypatch.setattr(launcher, "supervised", run)
    monkeypatch.setattr(launcher.time, "sleep", lambda _seconds: None)
    result = launcher.stop_apps(registry, tmp_path, launcher.time.monotonic() + 5)
    assert result == {"ap-Owned": {"state": "stopped", "tasks": 0}}
    assert launcher.apps_closed(registry, result)
    assert [command[-1] for command in commands if "stop" in command] == ["ap-Owned"]
    assert "ap-Unrelated" not in json.dumps(result)


@pytest.mark.parametrize(
    "changes",
    [
        {"ownership": "unknown"},
        {"collector_stopped": False},
        {"collection_complete": False},
        {"owner_exited": False},
        {"dispose_error": "OSError"},
    ],
)
def test_local_closure_requires_every_owner_and_collector_fact(
    launcher, changes
) -> None:
    assert not launcher.command_closed(closed(**changes))


def configure_launch(
    launcher,
    monkeypatch,
    tmp_path,
    *,
    outcome=None,
    unknown=False,
    inventory_error=False,
    cleanup_error=False,
):
    inputs = launcher.LaunchInputs(
        owned_root=tmp_path,
        profile="profile/pixi.toml",
        python="prefix/bin/python",
        harness="harness",
        wheels=("a.whl", "b.whl", "c.whl"),
        files={"a": "0"},
    )
    verify = Mock(
        side_effect=[None, OSError("selected-secret")] if inventory_error else None
    )
    monkeypatch.setattr(launcher.LaunchInputs, "verify", lambda _self: verify())
    monkeypatch.setattr(launcher, "guard_resources", Mock())
    watch = Mock(pid=123)

    def finish_watchdog(**_kwargs):
        runtime = next((tmp_path / "runs").iterdir())
        launcher.write_json(runtime / "watchdog.json", {"stopped_by_parent": True})
        return 0

    watch.wait.side_effect = finish_watchdog
    monkeypatch.setattr(launcher.subprocess, "Popen", Mock(return_value=watch))
    events = []

    def run(_argv, runtime, **_kwargs):
        launcher.write_json(
            runtime / "apps.json", {"app_ids": ["ap-Owned"], "pending_creations": 0}
        )
        launcher.write_json(
            runtime / "driver.json",
            {
                "passed": True,
                "expected_unknown_retained": unknown,
            },
        )
        (runtime / "working" / "retained").write_text("owned evidence")
        events.append("command")
        return outcome or closed(), "discarded sensitive output"

    def stop(*_args):
        events.append("cleanup")
        if cleanup_error:
            message = "selected-secret"
            raise OSError(message)
        return {"ap-Owned": {"state": "stopped", "tasks": 0}}

    monkeypatch.setattr(launcher, "supervised", run)
    monkeypatch.setattr(launcher, "stop_apps", stop)
    return inputs, events, watch


@pytest.mark.parametrize("unknown", [False, True])
def test_success_removes_only_closed_nonretained_working_data(
    launcher, monkeypatch, tmp_path, unknown
) -> None:
    inputs, events, watch = configure_launch(
        launcher, monkeypatch, tmp_path, unknown=unknown
    )
    assert launcher.launch(inputs, "cpu") == 0
    runtime = next((tmp_path / "runs").iterdir())
    report = json.loads((runtime / "outer.json").read_bytes())
    assert events == ["command", "cleanup"]
    assert (runtime / "working").exists() is unknown
    assert report["scratch_removed"] is not unknown
    assert (runtime / "watchdog-stop").exists()
    watch.wait.assert_called_once()
    assert "sensitive" not in json.dumps(report)


@pytest.mark.parametrize("case", ["inventory", "cleanup", "owner", "collector"])
def test_each_failed_postflight_preserves_scratch_and_original_outcome(
    launcher, monkeypatch, tmp_path, case
) -> None:
    outcome = closed(exit_code=7)
    if case == "owner":
        outcome["ownership"] = "unknown"
    if case == "collector":
        outcome["collector_stopped"] = False
    inputs, events, _watch = configure_launch(
        launcher,
        monkeypatch,
        tmp_path,
        outcome=outcome,
        inventory_error=case == "inventory",
        cleanup_error=case == "cleanup",
    )
    assert launcher.launch(inputs, "cpu") == 1
    runtime = next((tmp_path / "runs").iterdir())
    report = json.loads((runtime / "outer.json").read_bytes())
    assert events == ["command", "cleanup"]
    assert report["command"] == outcome
    assert (runtime / "working" / "retained").exists()
    assert not report["scratch_removed"]
    assert (runtime / "watchdog-stop").exists() is (case == "inventory")
    assert "selected-secret" not in json.dumps(report)


@pytest.mark.parametrize("reason", ["deadline", "RuntimeError"])
def test_guard_rejection_cannot_pass_when_child_finishes_successfully(
    launcher, monkeypatch, tmp_path, reason
) -> None:
    inputs, _events, _watch = configure_launch(
        launcher, monkeypatch, tmp_path, outcome=closed(stop_reason=reason)
    )
    assert launcher.launch(inputs, "cpu") == 1
    runtime = next((tmp_path / "runs").iterdir())
    report = json.loads((runtime / "outer.json").read_bytes())
    assert report["driver_passed"]
    assert report["command"]["stop_reason"] == reason
    assert report["apps_closed"]
    assert (runtime / "watchdog-stop").exists()
    assert (runtime / "working" / "retained").exists()


@pytest.mark.parametrize("record", [None, {"closed": False}])
def test_watchdog_exit_without_cleanup_evidence_retains_scratch(
    launcher, monkeypatch, tmp_path, record
) -> None:
    inputs, _events, watch = configure_launch(launcher, monkeypatch, tmp_path)

    def finish(**_kwargs):
        if record is not None:
            runtime = next((tmp_path / "runs").iterdir())
            launcher.write_json(runtime / "watchdog.json", record)
        return 0

    watch.wait.side_effect = finish
    assert launcher.launch(inputs, "cpu") == 1
    runtime = next((tmp_path / "runs").iterdir())
    report = json.loads((runtime / "outer.json").read_bytes())
    assert report["watchdog_stopped"]
    assert (runtime / "working" / "retained").exists()


def test_watchdog_wait_error_keeps_original_command_and_scratch(
    launcher, monkeypatch, tmp_path
) -> None:
    inputs, _events, watch = configure_launch(launcher, monkeypatch, tmp_path)
    watch.wait.side_effect = OSError("selected-secret")
    assert launcher.launch(inputs, "cpu") == 1
    runtime = next((tmp_path / "runs").iterdir())
    report = json.loads((runtime / "outer.json").read_bytes())
    assert report["command"] == closed()
    assert report["apps_closed"]
    assert report["watchdog_wait_error"] == "OSError"
    assert "selected-secret" not in json.dumps(report)
    assert (runtime / "working" / "retained").exists()


@pytest.mark.parametrize(
    "phase", ["image", "activation", "cpu", "redelivery", "gpu", "unknown"]
)
def test_fixed_phase_argv_stays_inside_owned_profile(launcher, tmp_path, phase) -> None:
    inputs = launcher.LaunchInputs(
        owned_root=tmp_path,
        profile="profile/pixi.toml",
        python="prefix/bin/python",
        harness="harness",
        wheels=("a.whl", "b.whl", "c.whl"),
        files={"a": "0"},
    )
    argv = launcher.child_argv(inputs, phase, tmp_path / "runs/current")
    assert argv[1:3] == [str(tmp_path / "harness/final_launch.py"), "child"]
    assert argv[3] == str(tmp_path / "runs/current/apps.json")
    assert argv[4] == str(
        tmp_path
        / "harness"
        / {
            "image": "final_prepare.py",
            "activation": "activate_tools.py",
        }.get(phase, "final_named.py")
    )


def test_input_path_cannot_escape_through_parent_or_symlink(launcher, tmp_path) -> None:
    root = tmp_path / "owned"
    root.mkdir()
    (root / "escaped").symlink_to(tmp_path, target_is_directory=True)
    inputs = launcher.LaunchInputs(
        owned_root=root,
        profile="profile/pixi.toml",
        python="prefix/bin/python",
        harness="harness",
        wheels=("a.whl", "b.whl", "c.whl"),
        files={"a": "0"},
    )
    for path in ("../source", "/source", "escaped/source"):
        with pytest.raises(ValueError, match="owned root"):
            inputs.path(path)


def test_command_manifest_matches_fixed_launcher_bounds_and_phase_order(
    launcher,
) -> None:
    path = Path(__file__).parent / "qualification/final_commands.json"
    manifest = json.loads(path.read_bytes())
    assert manifest["preparation"][0]["id"] == "builder-prepare"
    for step in manifest["preparation"][1:4]:
        assert step["argv"][:3] == ["${BUILDER_PYTHON}", "${BACKEND_SCRIPT}", "build"]
        assert step["timeout_seconds"] == 120
    assert [item["id"] for item in manifest["launches"]] == list(launcher.PHASES)
    assert manifest["outer_limits"]["work_seconds"] == launcher.WORK_SECONDS
    assert (
        manifest["outer_limits"]["local_owner_cleanup_seconds"]
        == launcher.LOCAL_CLOSE_SECONDS
    )
    assert (
        manifest["outer_limits"]["independent_watchdog_at_seconds"]
        == launcher.WATCHDOG_SECONDS
    )
    assert (
        manifest["outer_limits"]["watchdog_remote_cleanup_seconds"]
        == launcher.REMOTE_CLOSE_SECONDS
    )
    for item in manifest["launches"]:
        assert item["argv"] == [
            "${PYTHON}",
            "${HARNESS}/final_launch.py",
            "--inputs",
            "${INPUTS}",
            "--phase",
            item["id"],
        ]
    assert manifest["remote_limits"]["retries"] == 0
    assert manifest["remote_limits"]["science_input_seconds"] == 180
    profile = tomllib.loads(path.with_name("final_profile.toml").read_text())
    assert profile["environments"] == {
        "controller": ["framework"],
        "worker": ["framework", "worker"],
        "tool-a": ["tool-a"],
        "tool-b": ["tool-b"],
    }
    assert profile["dependencies"]["python"] == "==3.12.12"
    for name, marker, version in (
        ("tool-a", "alpha", "==24.2"),
        ("tool-b", "beta", "==25.0"),
    ):
        selected = profile["feature"][name]
        assert selected["pypi-dependencies"]["packaging"] == version
        assert selected["activation"]["env"] == {
            "ARTISAN_MODAL_TOOL": marker,
            "PROBE_ACTIVATION": marker,
        }


def test_supervised_command_keeps_caches_owned_and_records_collector_closure(
    launcher, monkeypatch, tmp_path
) -> None:
    owner = Mock()
    owner.process.pid = 321
    owner.process.poll.return_value = 0
    owner.finish.return_value.state = "closed"
    monkeypatch.setattr(launcher.CommandSupervisor, "start", Mock(return_value=owner))
    command = Mock(returncode=0, collection_complete=True)
    command.communicate.return_value = ("bounded reply", "")
    constructor = Mock(return_value=command)
    monkeypatch.setattr(launcher, "ManagedCommand", constructor)
    monkeypatch.setenv("PIXI_CACHE_DIR", "/outside-cache")
    monkeypatch.setenv("PYTHONPATH", "/unqualified-source")
    monkeypatch.setenv("MODAL_TOKEN_SECRET", "transient-selected-value")
    result, output = launcher.supervised(
        ["fixed-program"], tmp_path / "runtime", seconds=1, guard_root=tmp_path
    )
    environment = constructor.call_args.args[2]
    assert environment["PIXI_CACHE_DIR"] == str(tmp_path / "cache/pixi")
    assert environment["XDG_CACHE_HOME"] == str(tmp_path / "cache/xdg")
    assert environment["MODAL_TOKEN_SECRET"] == "transient-selected-value"
    assert "PYTHONPATH" not in environment
    assert launcher.command_closed(result)
    assert output == "bounded reply"
    assert "transient-selected-value" not in json.dumps(result)
    command.dispose.assert_called_once()


@pytest.mark.parametrize(("state", "tasks"), [(3, 0), (3, 1), (1, 1)])
def test_sdk_app_control_filters_exact_id_and_uses_bounded_rpc(
    launcher, monkeypatch: pytest.MonkeyPatch, tmp_path: Path, state: int, tasks: int
) -> None:
    from modal.client import _Client
    from modal_proto import api_pb2

    observer = importlib.import_module("final_apps")
    registry = tmp_path / "apps.json"
    launcher.write_json(registry, {"app_ids": ["ap-Owned"], "pending_creations": 0})
    response = SimpleNamespace(
        apps=[
            SimpleNamespace(
                app_id="ap-Other",
                state=state,
                n_running_tasks=99,
                description="private",
            ),
            SimpleNamespace(
                app_id="ap-Owned",
                state=state,
                n_running_tasks=tasks,
                description="private",
            ),
        ]
    )
    app_list = AsyncMock(return_value=response)
    client = SimpleNamespace(stub=SimpleNamespace(AppList=app_list))
    monkeypatch.setattr(_Client, "from_env", AsyncMock(return_value=client))
    result = asyncio.run(observer.app_control(registry, "list", "ap-Owned"))
    assert result == [
        {
            "App ID": "ap-Owned",
            "State": "stopped" if state == api_pb2.APP_STATE_STOPPED else "other",
            "Tasks": tasks,
        }
    ]
    args, kwargs = app_list.call_args
    assert isinstance(args[0], api_pb2.AppListRequest)
    assert kwargs == {"timeout": 5, "retry": None}
    assert "private" not in json.dumps(result)


def test_sdk_app_control_stops_only_recorded_id_without_claiming_closure(
    launcher, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from modal.client import _Client
    from modal_proto import api_pb2

    observer = importlib.import_module("final_apps")
    registry = tmp_path / "apps.json"
    launcher.write_json(registry, {"app_ids": ["ap-Owned"], "pending_creations": 0})
    stop = AsyncMock()
    client = AsyncMock(return_value=SimpleNamespace(stub=SimpleNamespace(AppStop=stop)))
    monkeypatch.setattr(_Client, "from_env", client)
    with pytest.raises(ValueError, match="exact recorded"):
        asyncio.run(observer.app_control(registry, "stop", "ap-Other"))
    client.assert_not_called()
    assert asyncio.run(observer.app_control(registry, "stop", "ap-Owned")) == []
    args, kwargs = stop.call_args
    assert args[0] == api_pb2.AppStopRequest(
        app_id="ap-Owned", source=api_pb2.APP_STOP_SOURCE_CLI
    )
    assert kwargs == {"timeout": 5, "retry": None}
    stop.assert_awaited_once()


def test_sdk_app_control_absent_record_is_not_closure(
    launcher, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from modal.client import _Client

    observer = importlib.import_module("final_apps")
    registry = tmp_path / "apps.json"
    launcher.write_json(registry, {"app_ids": ["ap-Owned"], "pending_creations": 0})
    client = SimpleNamespace(
        stub=SimpleNamespace(AppList=AsyncMock(return_value=SimpleNamespace(apps=[])))
    )
    monkeypatch.setattr(_Client, "from_env", AsyncMock(return_value=client))
    assert asyncio.run(observer.app_control(registry, "list", "ap-Owned")) == []
    assert not launcher.apps_closed(registry, {})


@pytest.mark.parametrize("phase", ["image", "activation", "cpu"])
def test_image_binding_isolated_from_previous_revision_and_canonical_profile(
    launcher, tmp_path: Path, phase: str
) -> None:
    original = tmp_path / "image.json"
    original.write_bytes(b"immutable original")
    bindings = []
    for revision in ("revision-v2", "revision-v3"):
        inputs = launcher.LaunchInputs(
            owned_root=tmp_path,
            profile=revision + "/profile/pixi.toml",
            python=revision + "/profile/.pixi/envs/controller/bin/python",
            harness="harness",
            wheels=("a.whl", "b.whl", "c.whl"),
            files={"a": "0"},
        )
        argv = launcher.child_argv(inputs, phase, tmp_path / "runtime")
        binding = Path(argv[argv.index("--binding") + 1])
        assert binding == tmp_path / revision / "image.json"
        assert not binding.is_relative_to(tmp_path / revision / "profile")
        bindings.append(binding)
    assert len(set(bindings)) == 2
    assert original.read_bytes() == b"immutable original"


@pytest.mark.parametrize("current_uid", [1000, 1001, 22001])
@pytest.mark.parametrize("same_owner", [False, True])
def test_input_owner_uses_current_user_and_preserves_next_identity_guard(
    launcher, monkeypatch: pytest.MonkeyPatch, current_uid: int, same_owner: bool
) -> None:
    root = Path("/dev/shm/artisan-modal-final-portability")
    original_stat = Path.stat

    def selected_stat(path: Path, *args, **kwargs):
        if path == root:
            return SimpleNamespace(
                st_uid=current_uid if same_owner else current_uid + 1
            )
        return original_stat(path, *args, **kwargs)

    monkeypatch.setattr(Path, "stat", selected_stat)
    monkeypatch.setattr(launcher.os, "getuid", lambda: current_uid)
    monkeypatch.setattr(launcher.sys, "executable", str(root / "prefix/bin/python"))
    inputs = launcher.LaunchInputs(
        owned_root=root,
        profile="profile/pixi.toml",
        python="prefix/bin/python",
        harness="harness",
        wheels=("a.whl", "b.whl", "c.whl"),
        files={"a": "0"},
    )
    expected = (
        "exact frozen qualification wrapper"
        if same_owner
        else "owned by the current user"
    )
    with pytest.raises(ValueError, match=expected):
        inputs.verify()
