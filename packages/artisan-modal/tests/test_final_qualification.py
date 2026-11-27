"""Local contract checks for the portable installed-provider qualification driver."""

from __future__ import annotations

import contextlib
import importlib
import json
import os
import subprocess
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from artisan.orchestration.runner_api import (
    AssignmentLaunchError,
    CancellationAcknowledgement,
    CancellationStatus,
    CommandOwnershipError,
    OwnershipState,
)


@pytest.fixture
def harness(monkeypatch: pytest.MonkeyPatch):
    directory = Path(__file__).parent / "qualification"
    monkeypatch.syspath_prepend(str(directory))
    monkeypatch.syspath_prepend(str(directory / "application/src"))
    return importlib.import_module("final_named")


def test_qualification_native_rows_preserve_exact_batch_occurrences(
    harness,
    tmp_path: Path,
) -> None:
    assert harness is not None
    from modal_qualification_app import NativeProbe

    from artisan.execution.composition import execute_compiled_plan
    from artisan.execution.compute.local import LocalExecuteRouter
    from artisan.execution.executor import Executor
    from artisan.orchestration.runner_api import compile_definition
    from artisan.schemas import MetricArtifact

    artifacts = [
        MetricArtifact.draft({"input": n, "value": n}, f"{n}.json") for n in (0, 1)
    ]
    for artifact in artifacts:
        artifact.finalize()
    with Executor(working_root=tmp_path) as executor:
        result = execute_compiled_plan(
            compile_definition(NativeProbe),
            {"source": artifacts},
            executor,
            invocation_id="qualification-native",
            execute_adapter=lambda *_: LocalExecuteRouter(),
        ).result
        assert [item.values["input"] for item in result.artifacts["values"]] == [0, 1]
        assert [item.source_input_index for item in result.lineage["values"]] == [0, 1]
        assert all(
            isinstance(item.values["value"], int) for item in result.artifacts["values"]
        )


def test_qualification_two_tools_compile_exact_declared_features(
    harness, tmp_path: Path
) -> None:
    assert harness is not None
    from modal_qualification_app import TwoTools

    from artisan.orchestration.runner_api import compile_definition

    manifest = tmp_path / "pixi.toml"
    manifest.write_text('[workspace]\nname="fixture"\nplatforms=["linux-64"]\n')
    plan = compile_definition(TwoTools, params={"manifest": str(manifest)})
    assert [call.name for call in plan.calls][-2:] == ["tool_a", "tool_b"]
    assert [call.plan.instantiate().environment.name for call in plan.calls[1:]] == [
        "tool-a",
        "tool-b",
    ]


@pytest.mark.parametrize(
    ("mode", "original", "code"), [("normal", 0, 0), ("fail", 0, 0), ("fail", 1, 7)]
)
def test_exact_installed_tool_program_preserves_nonzero_and_json_shape(
    harness,
    tmp_path: Path,
    mode: str,
    original: int,
    code: int,
) -> None:
    assert harness is not None
    from modal_qualification_app import _PROGRAM

    source = tmp_path / "input.json"
    source.write_text(json.dumps({"input": original, "value": original}))
    output = tmp_path / "output.json"
    result = subprocess.run(
        [sys.executable, "-c", _PROGRAM, str(source), str(output), "a", mode, "false"],
        env={**os.environ, "PROBE_ACTIVATION": "alpha"},
        check=False,
        capture_output=True,
        timeout=10,
    )
    assert result.returncode == code, result.stderr.decode()
    if code == 0:
        value = json.loads(output.read_text())
        assert value["value"] == original + 1
        assert value["tools"][0]["activation"] == "alpha"
        assert value["tools"][0]["python"] == sys.executable
    else:
        assert not output.exists()


@pytest.mark.parametrize("failed", [False, True])
def test_pipeline_closes_before_sdk_worker_stop(
    harness,
    prepared,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failed: bool,
) -> None:
    events = []
    report = {}
    progress = Mock()
    store = Mock()
    store.root.side_effect = lambda name: "s3://fixture/" + name
    pipeline = Mock()
    pipeline.config.pipeline_run_id = "run"
    pipeline.ownership_state.return_value = OwnershipState.CLOSED
    pipeline.cancel.side_effect = lambda: events.append("cancel")
    pipeline.close.side_effect = lambda: events.append("close")

    @contextlib.contextmanager
    def shared(*_args):
        yield store
        events.append("store-stop")

    @contextlib.contextmanager
    def named(*_args):
        try:
            yield Mock()
        finally:
            events.append("worker-stop")

    def exercise(*_args):
        if failed:
            message = "original body"
            raise ValueError(message)

    monkeypatch.setattr(harness, "shared_store", shared)
    monkeypatch.setattr(harness, "named_runner", named)
    monkeypatch.setattr(harness.PipelineManager, "create", Mock(return_value=pipeline))
    monkeypatch.setattr(harness, "_exercise", exercise)
    monkeypatch.setattr(
        harness,
        "_finish_store",
        lambda *_args, **_kwargs: events.append("store-retain"),
    )
    if failed:
        with pytest.raises(ValueError, match="original body"):
            harness.run_group(
                "cpu", prepared[0], tmp_path, tmp_path, tmp_path, report, progress
            )
        assert events == ["cancel", "close", "worker-stop", "store-retain"]
    else:
        harness.run_group(
            "cpu", prepared[0], tmp_path, tmp_path, tmp_path, report, progress
        )
        assert events == ["close", "worker-stop", "store-retain", "store-stop"]


def test_expected_unknown_does_not_hide_store_preservation_failure(
    harness, tmp_path: Path
) -> None:
    report = {"expected_unknown_retained": True, "pipeline_ownership": "unknown"}
    store = Mock()
    store.storage.filesystem.side_effect = OSError("sensitive credential")
    with pytest.raises(OSError, match="sensitive credential"):
        harness._finish_store(store, tmp_path, report, Mock(), failed=False)
    assert report["store_retention_error"] == "OSError"
    assert "sensitive credential" not in json.dumps(report)


def test_pipeline_body_error_survives_cleanup_error(harness) -> None:
    pipeline = Mock()
    pipeline.cancel.side_effect = OSError("cancel")
    pipeline.close.side_effect = CommandOwnershipError("unresolved")
    pipeline.ownership_state.return_value = OwnershipState.UNKNOWN
    report = {}
    harness._close_pipeline(pipeline, report, Mock(), failed=True)
    assert report["pipeline_cleanup_errors"] == ["OSError", "CommandOwnershipError"]
    assert report["pipeline_ownership"] == "unknown"


@pytest.mark.parametrize("unknown", [False, True])
def test_inventory_failure_cannot_skip_cleanup_or_remove_unknown_scratch(
    harness,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    unknown: bool,
) -> None:
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    (scratch / "owned").write_text("evidence")
    monkeypatch.setattr(
        harness, "installed_inventory", Mock(side_effect=OSError("read failed"))
    )
    report = {
        "passed": True,
        "pipeline_ownership": "unknown" if unknown else "closed",
        "worker_app_cleanup": {"tasks": 0},
        "store_app_cleanup": {"tasks": 0},
        "bucket_removed": True,
        "tunnel_closed": True,
        "queue_context_closed": True,
        "server_cleanup": {"server_closed": True, "tunnel_context_closed": True},
    }
    harness.finish_report(scratch, {}, report, Mock())
    assert report["passed"] is False
    assert scratch.exists() is unknown
    assert report["inventory_error"] == "OSError"


def test_scratch_cleanup_failure_is_recorded_separately(
    harness, tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setattr(harness, "installed_inventory", dict)
    monkeypatch.setattr(
        harness.shutil, "rmtree", Mock(side_effect=OSError("remove failed"))
    )
    report = {
        "passed": True,
        "body_error": "ValueError",
        "pipeline_ownership": "closed",
        "worker_app_cleanup": {"tasks": 0},
        "store_app_cleanup": {"tasks": 0},
        "bucket_removed": True,
        "tunnel_closed": True,
        "queue_context_closed": True,
        "server_cleanup": {"server_closed": True, "tunnel_context_closed": True},
    }
    harness.finish_report(tmp_path, {}, report, Mock())
    assert report["body_error"] == "ValueError"
    assert report["scratch_cleanup_error"] == "OSError"
    assert report["passed"] is False


def test_quarantine_rejects_path_escape_before_local_write(
    harness, tmp_path: Path
) -> None:
    fs = Mock()
    fs.find.return_value = {"bucket/../escape": {"size": 1}}
    store = SimpleNamespace(
        bucket="bucket", storage=SimpleNamespace(filesystem=lambda: fs)
    )
    with pytest.raises(AssertionError):
        harness._quarantine(store, tmp_path, {})
    fs.open.assert_not_called()


@pytest.mark.parametrize("mismatch", ["nonce", "attempt"])
def test_readiness_mismatch_retains_original_handles(
    harness, tmp_path: Path, monkeypatch, mismatch
) -> None:
    assert harness is not None
    control = importlib.import_module("final_control")
    from artisan.orchestration.runner_api import AssignmentDispatch, StorageConfig

    first = SimpleNamespace(attempt=Mock())
    second = SimpleNamespace(attempt=Mock())
    first.attempt.model_dump.return_value = {"unit": 0}
    second.attempt.model_dump.return_value = {"unit": 1}
    owner = Mock()
    handles = tuple(Mock(worker_owner=owner) for _ in range(2))
    fs = Mock()
    fs.exists.return_value = True
    dispatch = AssignmentDispatch(
        delta_root=str(tmp_path / "delta"),
        files_root=str(tmp_path / "files"),
        staging_root=str(tmp_path / "staging"),
        working_root=tmp_path,
        storage=StorageConfig(),
    )
    monkeypatch.setattr(StorageConfig, "filesystem", lambda _self: fs)
    delegate = Mock()

    def launch(_assignments, *, software, dispatch):
        assert software is not None
        channel = json.loads(dispatch.runtime_env["ARTISAN_QUAL_CHANNEL"])
        marker = {"nonce": channel["nonce"], "attempt": {"unit": 0}}
        marker[mismatch] = "incorrect"
        fs.open.return_value.__enter__ = Mock(
            return_value=SimpleNamespace(
                read=lambda _limit: json.dumps(marker).encode()
            )
        )
        fs.open.return_value.__exit__ = Mock(return_value=False)
        return handles

    delegate.launch.side_effect = launch
    runner = control.ControlledRunner(delegate, "sibling")
    with pytest.raises(AssignmentLaunchError) as caught:
        runner.launch((first, second), software=Mock(), dispatch=dispatch)
    assert caught.value.handles == handles
    assert caught.value.worker_owners == (owner,)
    assert caught.value.cancellation.status == CancellationStatus.UNKNOWN
    handles[0].cancel.assert_not_called()


def test_stop_application_observes_only_exact_id(
    harness, monkeypatch, tmp_path
) -> None:
    assert harness is not None
    environment = importlib.import_module("final_environment")
    registry = tmp_path / "apps.json"
    registry.write_text(json.dumps({"app_ids": ["ap-owned"], "pending_creations": 0}))
    monkeypatch.setattr(environment, "recorded_apps", lambda: registry)
    control = AsyncMock(
        side_effect=[[], [{"App ID": "ap-owned", "State": "stopped", "Tasks": 0}]]
    )
    monkeypatch.setattr(environment, "app_control", control)
    assert environment.stop_application("ap-owned") == {
        "app_id": "ap-owned",
        "state": "stopped",
        "tasks": 0,
    }
    assert [call.args for call in control.await_args_list] == [
        (registry, "stop", "ap-owned"),
        (registry, "list", "ap-owned"),
    ]


def test_installed_inventory_rejects_editable_application(harness, monkeypatch) -> None:
    owner = Mock()
    owner.read_text.return_value = '{"dir_info":{"editable":true}}'
    monkeypatch.setattr(harness.importlib.metadata, "distribution", lambda _name: owner)
    with pytest.raises(AssertionError):
        harness.installed_inventory()


def test_named_runner_uses_deployed_exact_normal_lookup(
    harness, prepared, tmp_path, monkeypatch
) -> None:
    assert harness is not None
    environment = importlib.import_module("final_environment")
    app = Mock(app_id="ap-owned")
    report = {}
    monkeypatch.setattr(environment.modal, "App", lambda: app)
    monkeypatch.setattr(environment, "_function", Mock())
    monkeypatch.setattr(environment, "publish_image", Mock())
    monkeypatch.setattr(
        environment,
        "stop_application",
        lambda identity: {"app_id": identity, "tasks": 0},
    )
    lookup = Mock()
    monkeypatch.setattr(environment.modal.Function, "from_name", lookup)
    from artisan.orchestration.runner_api import RunnerResources

    resources = RunnerResources(cpus=1, memory_gb=1, gpus=0, time_limit="00:03:00")
    with environment.named_runner(
        prepared[0], tmp_path / "pixi.toml", tmp_path, resources, report, Mock()
    ) as runner:
        profile = environment.function_profile(prepared[0].image_id, resources)
        runner._lookup_profile(prepared[0], profile)
        lookup.assert_called_once_with(
            app.deploy.call_args.kwargs["name"], "assignment"
        )
    assert report["worker_app_cleanup"] == {"app_id": "ap-owned", "tasks": 0}


def test_preparation_failure_writes_bounded_receipt(
    harness, tmp_path, monkeypatch
) -> None:
    preparation = importlib.import_module("final_prepare")
    report = tmp_path / "report.json"
    binding = tmp_path / "binding.json"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "prepare",
            "--manifest",
            str(tmp_path / "pixi.toml"),
            "--cache-root",
            str(tmp_path),
            "--binding",
            str(binding),
            "--report",
            str(report),
        ],
    )
    monkeypatch.setattr(
        harness, "installed_inventory", lambda: {"qualified": "snapshot"}
    )
    monkeypatch.setattr(
        preparation, "requirements", Mock(side_effect=ValueError("selected credential"))
    )
    assert preparation.main() == 1
    value = json.loads(report.read_text())
    assert value["body_error"] == "ValueError"
    assert value["source_unchanged"] is True
    assert "selected credential" not in report.read_text()
    assert not binding.exists()


@pytest.mark.parametrize("second_fails", [False, True])
def test_redelivery_retains_both_real_owners_before_controller_acceptance(
    harness,
    pack_request,
    monkeypatch,
    second_fails: bool,
) -> None:
    assert harness is not None
    from uuid import uuid4

    redelivery = importlib.import_module("final_redelivery")
    actual_owner_type = redelivery.ModalPackOwner
    first = Mock(spec=actual_owner_type)
    second = Mock(spec=actual_owner_type)
    first._request = pack_request
    first._function = Mock()
    original_progress = SimpleNamespace(
        pack_id=pack_request.pack_id,
        request_sha256=pack_request.digest,
        entry_id=uuid4(),
    )
    repeated_progress = SimpleNamespace(
        pack_id=pack_request.pack_id,
        request_sha256=pack_request.digest,
        entry_id=uuid4(),
    )
    first_entries = [uuid4(), uuid4()]
    second_entries = [uuid4(), uuid4()]
    first.outcome.side_effect = lambda index: SimpleNamespace(
        **vars(original_progress),
        outcome=SimpleNamespace(entry_id=first_entries[index]),
    )
    second.outcome.side_effect = lambda index: SimpleNamespace(
        **vars(repeated_progress),
        outcome=SimpleNamespace(entry_id=second_entries[index]),
    )
    first.observations = [SimpleNamespace(owner_id=uuid4())]
    second.observations = [SimpleNamespace(owner_id=uuid4())]
    handles = tuple(Mock(worker_owner=first) for _ in range(2))
    repeated = tuple(Mock(worker_owner=second) for _ in range(2))
    events = []
    second.start.side_effect = lambda: events.append("second-start")
    for handle in handles:
        handle.result.side_effect = lambda **_kwargs: events.append("first-result")
    for handle in repeated:
        handle.result.side_effect = (
            RuntimeError("second failed")
            if second_fails
            else lambda **_kwargs: events.append("second-result")
        )
    monkeypatch.setattr(
        redelivery.ControlledRunner, "launch", lambda *_args, **_kwargs: handles
    )
    monkeypatch.setattr(
        redelivery,
        "wait_closed",
        lambda values, _deadline: events.append(
            "first-closed" if values == handles else "second-closed"
        ),
    )

    class FactoryMeta(type):
        def __instancecheck__(cls, value):
            return isinstance(value, actual_owner_type)

    class OwnerFactory(metaclass=FactoryMeta):
        def __new__(cls, function, prepare, **_kwargs):
            assert function is first._function
            assert prepare() is pack_request
            return second

    monkeypatch.setattr(redelivery, "ModalPackOwner", OwnerFactory)
    monkeypatch.setattr(
        redelivery,
        "ModalAssignmentHandle",
        lambda _attempt, _owner, index, _values: repeated[index],
    )
    runner = redelivery.RedeliveryRunner(Mock())
    assignments = tuple(SimpleNamespace(attempt=Mock()) for _ in range(2))
    if second_fails:
        with pytest.raises(AssignmentLaunchError) as caught:
            runner.launch(assignments, software=Mock(), dispatch=Mock())
        assert caught.value.handles == (*handles, *repeated)
        assert caught.value.worker_owners == (first, second)
    else:
        assert runner.launch(assignments, software=Mock(), dispatch=Mock()) == handles
        assert runner.evidence["pack_entries"] == [
            str(original_progress.entry_id),
            str(repeated_progress.entry_id),
        ]
    assert events[:4] == [
        "first-result",
        "first-result",
        "first-closed",
        "second-start",
    ]


def test_application_composite_runs_native_then_both_real_commands(
    harness,
    tmp_path: Path,
) -> None:
    assert harness is not None
    from fixtures.commands import IdentityCommandEnvironment
    from modal_qualification_app import CommandProbe, Seeds, TwoTools

    from artisan.execution.composition import execute_compiled_plan
    from artisan.execution.compute.local import LocalExecuteRouter
    from artisan.execution.compute.prepared import PreparedExecuteRouter
    from artisan.execution.executor import Executor
    from artisan.orchestration.runner_api import compile_definition

    def adapter(operation, _path):
        if not isinstance(operation, CommandProbe):
            return LocalExecuteRouter()
        environment = IdentityCommandEnvironment(
            env={
                "PATH": str(Path(sys.executable).parent) + os.pathsep + os.defpath,
                "PROBE_ACTIVATION": "alpha" if operation.params.role == "a" else "beta",
            }
        )
        return PreparedExecuteRouter(
            None,
            pixi=Path("unused"),
            system_env=Path("/usr/bin/env"),
            runtime_env={},
            command_environment=lambda *_: environment,
        )

    manifest = tmp_path / "pixi.toml"
    manifest.write_text('[workspace]\nname="fixture"\nplatforms=["linux-64"]\n')
    with Executor(working_root=tmp_path / "work") as executor:
        seeds = execute_compiled_plan(
            compile_definition(Seeds),
            {},
            executor,
            invocation_id="seeds",
            execute_adapter=adapter,
        ).result.artifacts["values"]
        values = []
        for index, seed in enumerate(seeds):
            result = execute_compiled_plan(
                compile_definition(TwoTools, params={"manifest": str(manifest)}),
                {"source": [seed]},
                executor,
                invocation_id=f"compose-{index}",
                execute_adapter=adapter,
            ).result
            value = result.artifacts["values"][0].values
            assert [tool["activation"] for tool in value["tools"]] == ["alpha", "beta"]
            assert [tool["role"] for tool in value["tools"]] == ["a", "b"]
            values.append(value["value"])
        assert values == [2, 4]


@pytest.mark.parametrize("body_fails", [False, True])
def test_shared_store_cleanup_validation_preserves_body_error(
    harness,
    monkeypatch,
    body_fails: bool,
) -> None:
    assert harness is not None
    import deltalake

    environment = importlib.import_module("final_environment")
    control = Mock()
    control.get.return_value = {
        "url": "https://owned.example.invalid",
        "region": "us-east",
        "cloud": "test-cloud",
    }
    call = Mock()
    call.get.return_value = {"server_closed": False, "tunnel_context_closed": True}
    function = Mock()
    function.spawn.return_value = call
    app = Mock(app_id="ap-store")
    app.function.return_value = lambda _function: function
    app.run.return_value = contextlib.nullcontext()
    monkeypatch.setattr(environment.modal, "App", lambda: app)
    monkeypatch.setattr(environment.modal.Secret, "from_dict", Mock())
    monkeypatch.setattr(environment.modal.Image, "from_registry", Mock())
    monkeypatch.setattr(
        environment.modal.Queue, "ephemeral", lambda: contextlib.nullcontext(control)
    )
    monkeypatch.setattr(environment, "_check_tunnel_closed", Mock())
    monkeypatch.setattr(
        environment, "stop_application", lambda app_id: {"app_id": app_id, "tasks": 0}
    )
    content = {}
    fs = Mock()

    def pipe(path, value, *, mode="overwrite"):
        if mode == "create" and path in content:
            from botocore.exceptions import ClientError
            from s3fs.errors import translate_boto_error

            source = ClientError(
                {
                    "Error": {"Code": "PreconditionFailed"},
                    "ResponseMetadata": {"HTTPStatusCode": 412},
                },
                "PutObject",
            )
            raise translate_boto_error(source)
        content[path] = value

    fs.pipe_file.side_effect = pipe
    fs.cat_file.side_effect = content.__getitem__
    storage = SimpleNamespace(filesystem=lambda: fs, delta_storage_options=dict)
    monkeypatch.setattr(environment, "StorageConfig", lambda **_kwargs: storage)
    monkeypatch.setattr(deltalake, "write_deltalake", Mock())
    table = Mock()
    table.to_pyarrow_table.return_value.to_pydict.return_value = {"value": [42]}
    monkeypatch.setattr(deltalake, "DeltaTable", Mock(return_value=table))
    report = {}
    error = ValueError if body_fails else RuntimeError
    match = "original body" if body_fails else "Store did not confirm"

    def execute():
        with environment.shared_store(report, Mock()):
            if body_fails:
                message = "original body"
                raise ValueError(message)

    with pytest.raises(error, match=match):
        execute()
    assert report["server_cleanup_validation_error"] == "RuntimeError"
    assert report["store_app_cleanup"]["tasks"] == 0
    assert report["tunnel_closed"] is True
    assert report["conditional_create_verified"] is True


@pytest.mark.parametrize(
    "case",
    [
        "sibling",
        "sibling_delayed",
        "sibling_timeout",
        "sibling_closure_timeout",
        "never_ready",
        "unknown",
        "unknown_delayed",
        "unknown_skipped_sibling",
        "unknown_entered_sibling",
        "unknown_not_skipped",
        "unknown_marker_present",
    ],
)
def test_actual_handle_interception_requires_matching_closure_evidence(
    harness: ModuleType,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    case: str,
) -> None:
    assert harness is not None
    import io
    from uuid import uuid4

    control = importlib.import_module("final_control")

    from artisan_modal.pack_control import UnitProgress
    from artisan_modal.pack_owner import ModalPackOwner

    from artisan.execution.inert_worker import InertCallResult
    from artisan.orchestration.runner_api import AssignmentDispatch, StorageConfig

    clock = SimpleNamespace(now=0.0)

    def sleep(seconds: float) -> None:
        clock.now = round(clock.now + seconds, 6)

    monkeypatch.setattr(
        control, "time", SimpleNamespace(monotonic=lambda: clock.now, sleep=sleep)
    )
    delayed = case not in {"sibling", "unknown", "unknown_entered_sibling"}
    ready_at = 70 if delayed else 0
    attempts = tuple(Mock() for _ in range(2))
    for index, attempt in enumerate(attempts):
        attempt.model_dump.return_value = {"unit": index}
    assignments = tuple(SimpleNamespace(attempt=value) for value in attempts)
    owner = Mock(spec=ModalPackOwner)
    native = UnitProgress(
        pack_id=uuid4(),
        request_sha256="1" * 64,
        entry_id=uuid4(),
        index=0,
        payload_sha256="2" * 64,
        outcome=InertCallResult(
            entry_id=uuid4(),
            ownership=OwnershipState.UNKNOWN,
            error="Synthetic native cleanup remains unresolved",
            retained_working=True,
        ),
    )
    skipped = UnitProgress(
        **native.model_dump(exclude={"index", "skipped", "outcome"}),
        index=1,
        skipped=True,
    )
    sibling = None
    if case in {"unknown_skipped_sibling", "unknown_marker_present"}:
        sibling = skipped
    elif case == "unknown_entered_sibling":
        sibling = native.model_copy(update={"index": 1})
    elif case == "unknown_not_skipped":
        sibling = skipped.model_copy(update={"skipped": False})
    owner.outcome.side_effect = lambda index: native if index == 0 else sibling
    handles = tuple(Mock(worker_owner=owner) for _ in range(2))
    unknown = case.startswith("unknown")
    for handle in handles:
        handle.done.return_value = False
        handle.ownership_state.side_effect = lambda: (
            OwnershipState.UNKNOWN
            if unknown
            else OwnershipState.PENDING
            if case == "sibling_closure_timeout" and clock.now < 194
            else OwnershipState.CLOSED
        )

    def cancel() -> CancellationAcknowledgement:
        sleep(2)
        return CancellationAcknowledgement(
            CancellationStatus.UNKNOWN if unknown else CancellationStatus.CONFIRMED
        )

    handles[0].cancel.side_effect = cancel
    receipt = Mock(delivery=SimpleNamespace(attempt=attempts[1]))
    receipt.model_dump.return_value = {"sealed": "sibling"}

    def sibling_result(*, timeout: float) -> Mock:
        duration = {
            "sibling_delayed": 49,
            "sibling_timeout": 121,
            "sibling_closure_timeout": 119,
        }.get(case, 0)
        if duration > timeout:
            sleep(timeout)
            message = "Synthetic sibling exceeded its wait"
            raise TimeoutError(message)
        sleep(duration)
        return receipt

    handles[1].result.side_effect = sibling_result
    fs = Mock()
    fs.exists.side_effect = lambda path: (
        case == "unknown_marker_present"
        if path.endswith("/1.json")
        else case != "never_ready" and clock.now >= ready_at
    )
    monkeypatch.setattr(StorageConfig, "filesystem", lambda _self: fs)
    delegate = Mock()

    def launch(
        _assignments: object, *, dispatch: AssignmentDispatch, software: object
    ) -> tuple[Mock, ...]:
        assert software is not None
        channel = json.loads(dispatch.runtime_env["ARTISAN_QUAL_CHANNEL"])
        encoded = json.dumps(
            {"nonce": channel["nonce"], "attempt": {"unit": 0}}
        ).encode()
        fs.open.side_effect = lambda *_args: io.BytesIO(encoded)
        return handles

    delegate.launch.side_effect = launch
    runner = control.ControlledRunner(delegate, "unknown" if unknown else "sibling")
    dispatch = AssignmentDispatch(
        delta_root="s3://fixture/delta",
        files_root="s3://fixture/files",
        staging_root="s3://fixture/staging",
        storage=StorageConfig(),
        working_root=tmp_path,
    )
    failures = {
        "unknown_entered_sibling",
        "unknown_not_skipped",
        "unknown_marker_present",
        "sibling_timeout",
        "sibling_closure_timeout",
        "never_ready",
    }
    if case in failures:
        with pytest.raises(AssignmentLaunchError) as caught:
            runner.launch(assignments, software=Mock(), dispatch=dispatch)
        assert caught.value.handles == handles
        expected = TimeoutError if case == "sibling_timeout" else AssertionError
        assert isinstance(caught.value.__cause__, expected)
        assert "owners_closed" not in runner.evidence
    else:
        assert runner.launch(assignments, software=Mock(), dispatch=dispatch) is handles
        assert (
            runner.evidence["native_returned_unknown" if unknown else "owners_closed"]
            is True
        )
        if unknown:
            assert runner.evidence["queued_sibling_not_entered"] is True
    if unknown or case == "never_ready":
        handles[1].result.assert_not_called()
    else:
        handles[1].result.assert_called_once_with(timeout=120)
        assert runner.evidence["cancellation"] == "confirmed"
    if case == "never_ready":
        handles[0].cancel.assert_not_called()
        assert clock.now == 120
        assert (
            str(caught.value.__cause__)
            == "Exact first unit did not publish its readiness"
        )
    elif case in {"sibling_timeout", "sibling_closure_timeout"}:
        assert clock.now == 192
    elif case == "sibling_delayed":
        assert clock.now == 121
    elif case == "unknown_delayed":
        assert clock.now == 72


def test_nominal_pack_rejects_different_native_worker_pids(
    harness, tmp_path, monkeypatch
) -> None:
    assert harness is not None
    cases = importlib.import_module("final_cases")
    from artisan.orchestration.runner_api import RunnerResources
    from artisan.schemas.orchestration.step_lifecycle import StepStatus

    pipeline = Mock()
    pipeline.run.return_value = SimpleNamespace(
        status=StepStatus.SUCCEEDED, step_number=1
    )
    monkeypatch.setattr(cases, "seed", Mock())
    monkeypatch.setattr(
        cases,
        "metrics",
        lambda *_: [
            {"input": 0, "value": 2, "worker_pid": 101},
            {"input": 1, "value": 4, "worker_pid": 102},
        ],
    )
    with pytest.raises(AssertionError):
        cases.nominal(pipeline, tmp_path, RunnerResources(gpus=0))


def test_pack_evidence_rejects_missing_core_outcome(harness) -> None:
    assert harness is not None
    cases = importlib.import_module("final_cases")
    from artisan_modal.pack_owner import ModalPackOwner

    owner = Mock(spec=ModalPackOwner)
    owner.outcome.return_value = SimpleNamespace(entry_id="observed-pack", outcome=None)
    with pytest.raises(AssertionError):
        cases.pack_evidence((Mock(worker_owner=owner),))


def test_stop_application_requires_recorded_scope_before_rpc(
    harness, monkeypatch
) -> None:
    assert harness is not None
    environment = importlib.import_module("final_environment")
    control = AsyncMock()
    monkeypatch.setattr(environment, "app_control", control)
    with pytest.raises(ValueError, match="active recorded ownership"):
        environment.stop_application("ap-unrecorded")
    control.assert_not_called()


def test_shared_store_uses_actual_sdk_us_east_placement(harness, monkeypatch) -> None:
    assert harness is not None
    environment = importlib.import_module("final_environment")
    app = environment.modal.App()
    original = app.function
    captured = []

    def decorate(**kwargs):
        def apply(function):
            selected = original(**kwargs)(function)
            captured.append(selected)
            return selected

        return apply

    @contextlib.contextmanager
    def stop_before_launch():
        message = "offline definition boundary"
        raise RuntimeError(message)
        yield

    monkeypatch.setattr(app, "function", decorate)
    monkeypatch.setattr(app, "run", stop_before_launch)
    monkeypatch.setattr(environment.modal, "App", lambda: app)
    monkeypatch.setattr(
        environment.modal.Queue,
        "ephemeral",
        lambda: contextlib.nullcontext(object()),
    )
    with (
        pytest.raises(RuntimeError, match="offline definition boundary"),
        environment.shared_store({}, Mock()),
    ):
        pytest.fail("No store should be entered")
    assert len(captured) == 1
    assert list(captured[0].spec.scheduler_placement.regions) == ["us-east"]
    assert captured[0].spec.cpu == 1
    assert captured[0].spec.memory == 1024
    assert app.app_id is None


@pytest.mark.parametrize("region", ["us-east", "eu-west", None])
def test_store_placement_is_recorded_before_storage_and_cleanup_survives_mismatch(
    harness, tmp_path, monkeypatch, region
) -> None:
    assert harness is not None
    environment = importlib.import_module("final_environment")
    progress_type = importlib.import_module("progress").Progress
    ready = {
        "url": "https://synthetic.invalid",
        "region": region,
        "cloud": "test-cloud",
    }
    control = SimpleNamespace(get=lambda **_kwargs: ready)
    function = SimpleNamespace(spawn=lambda: SimpleNamespace(object_id="fc-synthetic"))
    app = SimpleNamespace(
        app_id="ap-synthetic",
        run=lambda: contextlib.nullcontext(),
        function=lambda **_kwargs: lambda _fn: function,
    )
    image = SimpleNamespace(entrypoint=lambda _args: object())
    monkeypatch.setattr(environment.modal, "App", lambda: app)
    monkeypatch.setattr(
        environment.modal.Image, "from_registry", lambda *_a, **_k: image
    )
    monkeypatch.setattr(environment.modal.Secret, "from_dict", lambda _values: object())
    monkeypatch.setattr(
        environment.modal.Queue, "ephemeral", lambda: contextlib.nullcontext(control)
    )
    storage = Mock(side_effect=RuntimeError("storage initialization failed"))
    monkeypatch.setattr(environment, "StorageConfig", storage)

    def stop(_control, _call, row, _progress):
        row["server_cleanup"] = {"server_closed": True, "tunnel_context_closed": True}

    monkeypatch.setattr(environment, "_stop_store", stop)
    tunnel_closed = Mock()
    monkeypatch.setattr(environment, "_check_tunnel_closed", tunnel_closed)
    monkeypatch.setattr(
        environment,
        "stop_application",
        lambda app_id: {"app_id": app_id, "state": "stopped", "tasks": 0},
    )
    progress = progress_type(tmp_path / "placement.progress.json")
    row = {}
    try:
        error = RuntimeError if region == "us-east" else AssertionError
        with pytest.raises(error), environment.shared_store(row, progress):
            pytest.fail("No store should be entered")
    finally:
        progress.close()
    saved = json.loads(progress.path.read_bytes())
    assert saved["requested_region"] == "us-east"
    assert saved["store_placement"] == {"region": region, "cloud": "test-cloud"}
    assert saved["store_app_id"] == "ap-synthetic"
    assert "synthetic.invalid" not in progress.path.read_text()
    if region == "us-east":
        options = storage.call_args.kwargs["options"]
        assert options["client_kwargs"] == {
            "endpoint_url": ready["url"],
            "region_name": "us-east-1",
        }
        assert options["config_kwargs"] == {
            "s3": {"addressing_style": "path"},
            "retries": {"max_attempts": 2},
            "connect_timeout": 10,
            "read_timeout": 30,
        }
    else:
        storage.assert_not_called()
    tunnel_closed.assert_called_once_with(ready["url"])
    assert row["server_cleanup"] == {
        "server_closed": True,
        "tunnel_context_closed": True,
    }
    assert row["tunnel_closed"] is True
    assert row["store_app_cleanup"] == {
        "app_id": "ap-synthetic",
        "state": "stopped",
        "tasks": 0,
    }
