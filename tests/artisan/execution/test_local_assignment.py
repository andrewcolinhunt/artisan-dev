"""Private local admission, command isolation, and successful sealing order."""

from __future__ import annotations

import os
import threading
from concurrent.futures import CancelledError
from dataclasses import replace
from enum import StrEnum
from pathlib import Path
from typing import Any, ClassVar

import pytest

from artisan.composites import CompositeContext
from artisan.composites.base.compiler import compile_definition
from artisan.composites.base.composite_definition import CompositeDefinition
from artisan.execution.assignment import Assignment, execute_assignment
from artisan.execution.composition import PlanExecution
from artisan.execution.compute.prepared import PreparedCommandEnvironment
from artisan.execution.lineage.graph import OccurrenceGraph
from artisan.execution.local_assignment import (
    PreparedLocalBindings,
    bind_local_software,
)
from artisan.execution.local_profile import LocalLauncherIdentity
from artisan.execution.local_software import LOCAL_SOFTWARE_ENV, LocalSoftware
from artisan.execution.local_worker import launch_local_assignment
from artisan.operations.base import OperationDefinition, PerArtifact
from artisan.schemas import ArtifactResult, InputSpec, OutputSpec
from artisan.schemas.artifact.file_ref import FileRefArtifact
from artisan.schemas.artifact.metric import MetricArtifact
from artisan.schemas.execution.assignment import (
    AssignmentAttempt,
    AssignmentIdentity,
    DeliveryIdentity,
    SealedDelivery,
    SuccessfulDelivery,
)
from artisan.schemas.execution.prepared_environment import (
    EnvironmentFile,
    EnvironmentSource,
    PreparedNativeEnvironment,
)
from artisan.schemas.execution.worker_transport import (
    TRANSPORT_PACKAGES,
    DistributionMetadata,
    RuntimeCompatibility,
)
from artisan.schemas.orchestration.step_lifecycle import CancellationStatus
from artisan.schemas.specs.input_models import (
    ExecuteInput,
    PostprocessInput,
    PreprocessInput,
)
from artisan.utils.command_supervisor import CommandOwnershipError, check_command_scope
from artisan.utils.hashing import compute_content_digest


class _NativeFile(OperationDefinition):
    name = "local_assignment_native_file"

    class InputRole(StrEnum):
        source = "source"

    class OutputRole(StrEnum):
        files = "files"

    inputs: ClassVar = {"source": InputSpec(artifact_type="metric", materialize=False)}
    outputs: ClassVar = {
        "files": OutputSpec(
            artifact_type="file_ref", derives_from={"inputs": ["source"]}
        )
    }
    calls: ClassVar[list[str]] = []

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        self.calls.append("preprocess")
        return {
            "values": PerArtifact(
                [
                    artifact.values["value"]
                    for artifact in inputs.input_artifacts["source"]
                ]
            )
        }

    def execute_function(self, inputs: ExecuteInput) -> dict[str, Any]:
        self.calls.append("execute")
        assert inputs.files_dir is not None
        path = Path(inputs.files_dir) / "result.txt"
        path.write_text(str(inputs.inputs["values"][0] + 1))
        return {"paths": [str(path)]}

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        self.calls.append("postprocess")
        path = Path(inputs.memory_outputs["paths"][0])
        content = path.read_bytes()
        result = ArtifactResult()
        result.add_artifact(
            "files",
            FileRefArtifact.draft(
                str(path),
                compute_content_digest(content),
                len(content),
                inputs.step_number,
            ),
            sources={"source": [inputs.ref("source", index=0)]},
        )
        return result


@pytest.fixture
def bindings(tmp_path: Path) -> PreparedLocalBindings:
    """Supply structural metadata only; execution tests explicitly stub verification."""
    compatibility = RuntimeCompatibility(
        python_implementation="CPython",
        python_version="3.12.12",
        cache_tag="cpython-312",
        packages=tuple(
            DistributionMetadata(
                name=name,
                version="1",
            )
            for name in sorted(TRANSPORT_PACKAGES)
        ),
    )
    workspace = tmp_path / "prepared"
    target = PreparedNativeEnvironment(
        source=EnvironmentSource(
            manifest_name="pixi.toml",
            name="worker",
            files=tuple(
                EnvironmentFile(path=name, sha256="0" * 64)
                for name in ("pixi.lock", "pixi.toml")
            ),
        ),
        platform="linux-64",
        compatibility=compatibility,
        workspace=workspace,
        prefix=workspace / ".pixi/envs/worker",
    )
    return PreparedLocalBindings(
        target,
        (),
        tmp_path / "pixi",
        Path("/usr/bin/env"),
        LocalLauncherIdentity("pixi 0.83.0", "1" * 64, "2" * 64, ("glibc", "2.35")),
    )


def _assignment(
    bindings: PreparedLocalBindings,
    definition: type[OperationDefinition] | type[CompositeDefinition] = _NativeFile,
) -> Assignment:
    plan = compile_definition(definition)
    identity = AssignmentIdentity(step_run_id="1" * 32, unit_index=0)
    artifact = MetricArtifact.draft({"value": 7}, "source.json").finalize()
    graph = OccurrenceGraph(boundary_invocation=identity.assignment_id)
    graph.add_input("source", artifact)
    snapshot = graph.project({}, {})
    attempt = AssignmentAttempt(
        assignment=identity,
        attempt=0,
        generation=0,
        recipe_digest=plan.recipe.digest,
        software_digest=bindings.software_digest,
        software_cacheable=bindings.software_cacheable,
        input_context_digest=snapshot.input_context_digest,
    )
    return Assignment(
        attempt,
        plan,
        {"source": [artifact]},
        input_attribution=snapshot.input_attribution,
    )


def _receipt(completion: SuccessfulDelivery, _result: PlanExecution) -> SealedDelivery:
    return SealedDelivery(
        delivery=completion.delivery,
        seal_uri="memory://sealed-result",
        seal_digest="a" * 32,
    )


def test_worker_entry_reuses_admission_but_creates_fresh_delivery_and_seals_after_cleanup(
    tmp_path: Path,
    bindings: PreparedLocalBindings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(PreparedLocalBindings, "verify", lambda *_: None)
    assignment = _assignment(bindings)
    paths: list[Path] = []
    snapshots: list[str] = []

    def seal(completion: SuccessfulDelivery, result: PlanExecution) -> SealedDelivery:
        with pytest.raises(CommandOwnershipError, match="closed"):
            check_command_scope()
        path = Path(result.result.artifacts["files"][0].path)
        assert path.read_bytes() == b"8"
        paths.append(path)
        snapshots.append(result.projection.input_context_digest)
        return _receipt(completion, result)

    receipts = [
        execute_assignment(
            assignment, bindings, seal, working_root=tmp_path / "work", runtime_env={}
        )
        for _ in range(2)
    ]
    assert receipts[0].delivery.delivery_id != receipts[1].delivery.delivery_id
    assert all(receipt.delivery.attempt == assignment.attempt for receipt in receipts)
    assert snapshots == [assignment.attempt.input_context_digest] * 2
    assert all(not path.exists() for path in paths)


@pytest.mark.parametrize(
    "field", ["software_digest", "software_cacheable", "input_context_digest"]
)
def test_changed_admission_never_reaches_science_or_sealing(
    tmp_path: Path,
    bindings: PreparedLocalBindings,
    monkeypatch: pytest.MonkeyPatch,
    field: str,
) -> None:
    monkeypatch.setattr(PreparedLocalBindings, "verify", lambda *_: None)
    assignment = _assignment(bindings)
    assignment = replace(
        assignment,
        attempt=assignment.attempt.model_copy(
            update={
                field: False
                if field == "software_cacheable"
                else "0" * (64 if field == "software_digest" else 32)
            }
        ),
    )
    _NativeFile.calls.clear()

    def forbidden(*args: object) -> SealedDelivery:
        pytest.fail("Invalid admission reached sealing")

    with pytest.raises(ValueError, match="admitted"):
        execute_assignment(
            assignment,
            bindings,
            forbidden,
            working_root=tmp_path / "work",
            runtime_env={},
        )
    assert _NativeFile.calls == []


def test_uncertain_command_cleanup_never_reaches_sealer(
    tmp_path: Path,
    bindings: PreparedLocalBindings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(PreparedLocalBindings, "verify", lambda *_: None)
    monkeypatch.setattr(
        "artisan.execution.assignment.close_command_scope", lambda: False
    )

    def forbidden(*args: object) -> SealedDelivery:
        pytest.fail("Unconfirmed cleanup reached sealing")

    with pytest.raises(CommandOwnershipError):
        execute_assignment(
            _assignment(bindings),
            bindings,
            forbidden,
            working_root=tmp_path / "work",
            runtime_env={},
        )


@pytest.mark.parametrize(
    "key",
    [
        "PATH",
        "HOME",
        "TMPDIR",
        "PYTHONPATH",
        "PYTHONHOME",
        "PYTHONUSERBASE",
        "LD_LIBRARY_PATH",
        "LD_PRELOAD",
        "LD_AUDIT",
        "CONDA_PREFIX",
        "PIXI_PROJECT_ROOT",
        "VIRTUAL_ENV",
    ],
)
def test_runtime_values_cannot_override_software_or_ownership(
    tmp_path: Path,
    bindings: PreparedLocalBindings,
    key: str,
) -> None:
    with pytest.raises(ValueError, match="controls"):
        PreparedCommandEnvironment(
            bindings.worker,
            bindings.pixi,
            bindings.system_env,
            tmp_path,
            {key: "/ambient"},
        )


def test_clean_command_environment_preserves_gpu_absence_and_selected_literals_only(
    tmp_path: Path,
    bindings: PreparedLocalBindings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PYTHONPATH", "/worker-python")
    monkeypatch.setenv("LD_LIBRARY_PATH", "/worker-loader")
    monkeypatch.setenv("AMBIENT_CREDENTIAL", "should-not-leak")
    monkeypatch.delenv("CUDA_VISIBLE_DEVICES", raising=False)
    monkeypatch.setattr(
        "artisan.execution.compute.prepared.native_executable",
        lambda target, name: target.prefix / "bin" / name,
    )
    selected = {
        "SELECTED_CREDENTIAL": "synthetic",
        "LITERAL": "a literal $value with spaces",
    }
    command = PreparedCommandEnvironment(
        bindings.worker,
        bindings.pixi,
        bindings.system_env,
        tmp_path / "runtime",
        selected,
    )
    selected["LITERAL"] = "changed"
    environment = command.prepare_env()
    assert environment["PATH"] == os.defpath
    assert (
        not {
            "PYTHONPATH",
            "LD_LIBRARY_PATH",
            "AMBIENT_CREDENTIAL",
            "CUDA_VISIBLE_DEVICES",
        }
        & environment.keys()
    )
    argv = command.wrap_command(["python", "--version"])
    assert "LITERAL=a literal $value with spaces" in argv
    assert "SELECTED_CREDENTIAL=synthetic" in argv
    assert argv[-2] == str(bindings.worker.prefix / "bin/python")
    assert {"--no-config", "--offline", "--as-is", "--executable"} <= set(argv)
    monkeypatch.setenv("CUDA_VISIBLE_DEVICES", "0")
    assert command.prepare_env()["CUDA_VISIBLE_DEVICES"] == "0"
    assert "CUDA_VISIBLE_DEVICES=0" in command.wrap_command(["python"])


def test_prepared_command_passes_local_locations_through_pixi_activation(
    tmp_path: Path,
    bindings: PreparedLocalBindings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "artisan.execution.compute.prepared.native_executable",
        lambda target, name: target.prefix / "bin" / name,
    )
    software = LocalSoftware(
        tmp_path / "cache",
        bindings.pixi,
        bindings.system_env,
        {"SERVICE_TOKEN": "synthetic"},
    )
    command = PreparedCommandEnvironment(
        bindings.worker,
        bindings.pixi,
        bindings.system_env,
        tmp_path / "runtime",
        software.runtime_env,
        local_software=software,
    )
    encoded = software.environment()[LOCAL_SOFTWARE_ENV]
    assert command.prepare_env()[LOCAL_SOFTWARE_ENV] == encoded
    assert f"{LOCAL_SOFTWARE_ENV}={encoded}" in command.wrap_command(["python"])
    assert "SERVICE_TOKEN=synthetic" in command.wrap_command(["python"])


def test_unsupported_process_packing_rejects_before_owner_start(
    tmp_path: Path,
    bindings: PreparedLocalBindings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "artisan.execution.local_call.CommandSupervisor.start",
        lambda: pytest.fail("Packing started a worker owner"),
    )
    with pytest.raises(ValueError, match="units_per_worker=1"):
        launch_local_assignment(
            _assignment(bindings),
            bindings,
            _receipt,
            working_root=tmp_path,
            units_per_worker=2,
        )


def test_cancel_during_serialization_prevents_late_worker_launch(
    tmp_path: Path,
    bindings: PreparedLocalBindings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(PreparedLocalBindings, "verify", lambda *_: None)
    entered, release = threading.Event(), threading.Event()

    def serialize(*args: object, **kwargs: object) -> object:
        entered.set()
        assert release.wait(10)
        return object()

    monkeypatch.setattr(
        "artisan.execution.local_worker.serialize_worker_call", serialize
    )
    monkeypatch.setattr(
        "artisan.execution.local_call.ManagedCommand",
        lambda *args, **kwargs: pytest.fail(
            "Cancelled serialization launched a worker"
        ),
    )
    handle = launch_local_assignment(
        _assignment(bindings), bindings, _receipt, working_root=tmp_path
    )
    try:
        assert entered.wait(10)
        assert handle.cancel().status == CancellationStatus.REQUESTED
    finally:
        release.set()
    with pytest.raises(CancelledError):
        handle.result(timeout=10)
    assert handle.cancel().status == CancellationStatus.CONFIRMED


def test_conflicting_target_identities_reject_before_payload_verification(
    bindings: PreparedLocalBindings, monkeypatch: pytest.MonkeyPatch
) -> None:
    from artisan.execution.local_assignment import PreparedToolBinding

    changed = bindings.worker.model_copy(update={"platform": "linux-aarch64"})
    selected = replace(bindings, tools=(PreparedToolBinding(("command",), changed),))
    monkeypatch.setattr(
        "artisan.execution.local_assignment._command_paths", lambda _: {("command",)}
    )
    monkeypatch.setattr(LocalLauncherIdentity, "capture", lambda *_: bindings.launcher)
    monkeypatch.setattr(
        "artisan.execution.local_assignment.verify_native_environment",
        lambda *_: pytest.fail("Conflicting identities reached payload verification"),
    )
    with pytest.raises(ValueError, match="conflicting prepared identities"):
        selected.verify(_assignment(bindings).plan)


@pytest.mark.parametrize("shares_worker", [False, True])
def test_shared_commands_resolve_one_target_and_keep_distinct_bindings(
    tmp_path: Path,
    bindings: PreparedLocalBindings,
    monkeypatch: pytest.MonkeyPatch,
    shares_worker: bool,
) -> None:
    from artisan.execution.environments.requirements import ToolEnvironmentSelection
    from artisan.schemas.operation_config.environment import Environment

    shared = Environment.from_pixi(tmp_path / "shared/pixi.toml")
    other = Environment.from_pixi(tmp_path / "other/pixi.toml")
    selections = tuple(
        ToolEnvironmentSelection(path, "command", declaration)
        for path, declaration in (
            (("first",), shared),
            (("second",), shared),
            (("nested", "third"), shared),
            (("other",), other),
        )
    )
    resolved: list[Environment] = []

    def lookup(declaration: Environment, *_: object) -> PreparedNativeEnvironment:
        resolved.append(declaration)
        return bindings.worker.model_copy()

    module = "artisan.execution.local_assignment"
    monkeypatch.setattr(f"{module}.select_tool_environments", lambda _: selections)
    monkeypatch.setattr(f"{module}._lookup_tool", lookup)
    monkeypatch.setattr(f"{module}.lookup_command_witness", lambda _: None)
    monkeypatch.setattr(LocalLauncherIdentity, "capture", lambda *_: bindings.launcher)
    monkeypatch.setattr(PreparedLocalBindings, "verify", lambda *_: None)
    selected = bind_local_software(
        _assignment(bindings).plan,
        bindings.worker,
        cache_root=tmp_path,
        pixi=bindings.pixi,
        system_env=bindings.system_env,
        worker_environment=shared if shares_worker else None,
    )
    assert resolved == ([other] if shares_worker else [shared, other])
    targets = {tool.call_path: tool.target for tool in selected.tools}
    assert set(targets) == {selection.call_path for selection in selections}
    assert targets[("first",)] is targets[("second",)] is targets[("nested", "third")]
    assert targets[("first",)] is not targets[("other",)]
    if shares_worker:
        assert targets[("first",)] is bindings.worker


def test_thread_start_failure_closes_created_owner(
    tmp_path: Path, bindings: PreparedLocalBindings, monkeypatch: pytest.MonkeyPatch
) -> None:
    from artisan.execution.local_call import LocalCallStartupError
    from artisan.execution.local_worker import LocalAssignmentHandle
    from artisan.schemas.execution.worker_ownership import OwnershipState
    from artisan.utils.command_supervisor import CommandSupervisor

    owner = CommandSupervisor.start()
    monkeypatch.setattr(CommandSupervisor, "start", lambda: owner)

    def fail_start(_thread: threading.Thread) -> None:
        msg = "thread creation failed"
        raise RuntimeError(msg)

    monkeypatch.setattr(threading.Thread, "start", fail_start)
    with pytest.raises(LocalCallStartupError) as caught:
        LocalAssignmentHandle(
            _assignment(bindings),
            bindings,
            _receipt,
            working_root=tmp_path,
            runtime_env={},
        )
    assert caught.value.handle.ownership_state() == OwnershipState.CLOSED
    assert caught.value.cleanup_outcome.state == "closed"
    with pytest.raises(RuntimeError, match="thread creation failed"):
        caught.value.handle.result(timeout=0)
    assert owner.poll().state == "closed"
    assert owner.process.poll() is not None


def test_sealer_cannot_return_different_delivery(
    tmp_path: Path, bindings: PreparedLocalBindings, monkeypatch: pytest.MonkeyPatch
) -> None:
    from artisan.schemas.execution.assignment import DeliveryIdentity

    monkeypatch.setattr(PreparedLocalBindings, "verify", lambda *_: None)

    def wrong(completion: SuccessfulDelivery, result: PlanExecution) -> SealedDelivery:
        return _receipt(completion, result).model_copy(
            update={"delivery": DeliveryIdentity.new(completion.delivery.attempt)}
        )

    with pytest.raises(ValueError, match="different delivery"):
        execute_assignment(
            _assignment(bindings),
            bindings,
            wrong,
            working_root=tmp_path / "work",
            runtime_env={},
        )


def test_unsettled_serialization_cancellation_becomes_unknown(
    tmp_path: Path, bindings: PreparedLocalBindings, monkeypatch: pytest.MonkeyPatch
) -> None:
    import time

    monkeypatch.setattr(PreparedLocalBindings, "verify", lambda *_: None)
    monkeypatch.setattr("artisan.execution.local_call.CLEANUP_SECONDS", 0.1)
    entered, release = threading.Event(), threading.Event()

    def serialize(*args: object, **kwargs: object) -> object:
        entered.set()
        assert release.wait(10)
        return object()

    monkeypatch.setattr(
        "artisan.execution.local_worker.serialize_worker_call", serialize
    )
    handle = launch_local_assignment(
        _assignment(bindings), bindings, _receipt, working_root=tmp_path
    )
    try:
        assert entered.wait(10)
        assert handle.cancel().status == CancellationStatus.REQUESTED
        time.sleep(0.15)
        assert handle.cancel().status == CancellationStatus.UNKNOWN
        assert not handle.done()
    finally:
        release.set()
    with pytest.raises((CancelledError, CommandOwnershipError)):
        handle.result(timeout=10)
    assert handle.cancel().status == CancellationStatus.UNKNOWN


def test_native_runtime_values_restore_previous_value_and_absence(
    tmp_path: Path, bindings: PreparedLocalBindings, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(PreparedLocalBindings, "verify", lambda *_: None)
    monkeypatch.setenv("SELECTED_VALUE", "previous")
    monkeypatch.delenv("NEW_SELECTED_VALUE", raising=False)
    original = _NativeFile.execute_function
    observed = []

    def execute(operation: _NativeFile, inputs: ExecuteInput) -> dict[str, Any]:
        observed.append(
            (os.environ["SELECTED_VALUE"], os.environ["NEW_SELECTED_VALUE"])
        )
        return original(operation, inputs)

    monkeypatch.setattr(_NativeFile, "execute_function", execute)
    for value in ("first literal $value", "second literal $value"):
        execute_assignment(
            _assignment(bindings),
            bindings,
            _receipt,
            working_root=tmp_path / "work",
            runtime_env={"SELECTED_VALUE": value, "NEW_SELECTED_VALUE": value},
        )
        assert os.environ["SELECTED_VALUE"] == "previous"
        assert "NEW_SELECTED_VALUE" not in os.environ
    assert observed == [
        (value, value) for value in ("first literal $value", "second literal $value")
    ]


def test_incompatible_packed_workers_reject_before_owner_creation(
    tmp_path: Path, bindings: PreparedLocalBindings, monkeypatch: pytest.MonkeyPatch
) -> None:
    from artisan.execution.local_packing import (
        LocalAssignmentRequest,
        launch_local_assignments,
    )

    changed = replace(
        bindings,
        worker=bindings.worker.model_copy(update={"platform": "linux-aarch64"}),
    )
    monkeypatch.setattr(
        "artisan.execution.local_packing.CommandSupervisor.start",
        lambda: pytest.fail("Incompatible pack created an owner"),
    )
    with pytest.raises(ValueError, match="one worker"):
        launch_local_assignments(
            [
                LocalAssignmentRequest(_assignment(binding), binding, _receipt)
                for binding in (bindings, changed)
            ],
            working_root=tmp_path,
            units_per_worker=2,
        )


@pytest.mark.parametrize("failed_pack", [1, 2])
def test_pack_startup_failure_retains_handles_and_stops_started_work(
    tmp_path: Path,
    bindings: PreparedLocalBindings,
    monkeypatch: pytest.MonkeyPatch,
    failed_pack: int,
) -> None:
    import json
    import sys
    import time

    from artisan.execution.local_packing import (
        LocalAssignmentRequest,
        LocalPackStartupError,
        launch_local_assignments,
    )
    from artisan.execution.local_worker import LocalAssignmentHandle

    monkeypatch.setattr(PreparedLocalBindings, "verify", lambda *_: None)
    monkeypatch.setattr(LocalAssignmentHandle, "_write_payload", lambda *_: None)
    marker = tmp_path / "started.json"
    program = (
        "import json,os,subprocess,sys,time; from pathlib import Path; "
        "child=subprocess.Popen([sys.executable,'-c','import time; time.sleep(60)'],"
        "stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL); "
        f"Path({str(marker)!r}).write_text(json.dumps([os.getpid(),child.pid])); "
        "time.sleep(60)"
    )
    monkeypatch.setattr(
        PreparedCommandEnvironment,
        "wrap_command",
        lambda *_: [sys.executable, "-c", program],
    )
    original_start = threading.Thread.start
    starts = 0

    def start(thread: threading.Thread) -> None:
        nonlocal starts
        if thread.name == "artisan-local-pack":
            starts += 1
            if starts == failed_pack:
                if failed_pack > 1:
                    deadline = time.monotonic() + 10
                    while not marker.exists() and time.monotonic() < deadline:
                        time.sleep(0.01)
                    assert marker.exists()
                msg = "injected pack thread failure"
                raise RuntimeError(msg)
        original_start(thread)

    monkeypatch.setattr(threading.Thread, "start", start)
    request = LocalAssignmentRequest(_assignment(bindings), bindings, _receipt)
    working_root = tmp_path / "workers"
    with pytest.raises(LocalPackStartupError) as caught:
        launch_local_assignments(
            [request, request], working_root=working_root, units_per_worker=1
        )
    error = caught.value
    assert len(error.handles) == failed_pack
    assert len(error.worker_owners) == failed_pack
    assert all(handle.worker_owner in error.worker_owners for handle in error.handles)
    assert all(handle.done() for handle in error.handles)
    assert all(
        handle.cancel().status == CancellationStatus.CONFIRMED
        for handle in error.handles
    )
    assert all(outcome.state == "closed" for outcome in error.transport_outcomes)
    assert not list(working_root.iterdir())
    if marker.exists():
        for pid in json.loads(marker.read_text()):
            status = Path(f"/proc/{pid}/status")
            assert not status.exists() or "State:\tZ" in status.read_text()


@pytest.mark.parametrize(
    ("method", "phase"),
    [
        ("preprocess", "preprocess"),
        ("execute_function", "execute"),
        ("postprocess", "postprocess"),
    ],
)
def test_authored_failure_preserves_phase_and_redacts_selected_values(
    tmp_path: Path,
    bindings: PreparedLocalBindings,
    monkeypatch: pytest.MonkeyPatch,
    method: str,
    phase: str,
) -> None:
    from artisan.execution.exceptions import AssignmentExecutionError

    def fail(*_args: object) -> Any:
        message = "scientific failure with selected-secret-value"
        raise ValueError(message)

    monkeypatch.setattr(PreparedLocalBindings, "verify", lambda *_: None)
    monkeypatch.setattr(_NativeFile, method, fail)
    with pytest.raises(AssignmentExecutionError) as raised:
        execute_assignment(
            _assignment(bindings),
            bindings,
            _receipt,
            working_root=tmp_path / "work",
            runtime_env={"SELECTED_TOKEN": "selected-secret-value"},
        )
    assert raised.value.phase == phase
    assert raised.value.call_path == ()
    assert raised.value.delivery is not None
    assert raised.value.delivery.attempt == _assignment(bindings).attempt
    assert "scientific failure" in str(raised.value)
    assert "selected-secret-value" not in str(raised.value)
    assert not list((tmp_path / "work").glob("*/artisan-executor-*"))


@pytest.mark.parametrize("failure_location", ["adapter", "materialization", "sealing"])
def test_assignment_infrastructure_errors_never_gain_scientific_authority(
    tmp_path: Path,
    bindings: PreparedLocalBindings,
    monkeypatch: pytest.MonkeyPatch,
    failure_location: str,
) -> None:
    from artisan.execution.exceptions import AssignmentExecutionError

    def fail(*_args: object, **_kwargs: object) -> Any:
        message = "infrastructure failed"
        raise OSError(message)

    monkeypatch.setattr(PreparedLocalBindings, "verify", lambda *_: None)
    sealer = _receipt
    if failure_location == "adapter":
        monkeypatch.setattr(PreparedLocalBindings, "create_adapter", fail)
    elif failure_location == "materialization":
        monkeypatch.setattr(
            "artisan.execution.invocation.materialize_captured_inputs", fail
        )
    else:
        sealer = fail
    with pytest.raises(OSError, match="infrastructure") as raised:
        execute_assignment(
            _assignment(bindings),
            bindings,
            sealer,
            working_root=tmp_path / "work",
            runtime_env={},
        )
    assert not isinstance(raised.value, AssignmentExecutionError)


def test_scientific_failure_with_unknown_cleanup_is_ownership_failure(
    tmp_path: Path,
    bindings: PreparedLocalBindings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail(*_args: object) -> Any:
        message = "science failed"
        raise ValueError(message)

    monkeypatch.setattr(PreparedLocalBindings, "verify", lambda *_: None)
    monkeypatch.setattr(_NativeFile, "execute_function", fail)
    monkeypatch.setattr(
        "artisan.execution.assignment.close_command_scope", lambda *_: False
    )
    from artisan.execution.assignment_reply import AssignmentReply

    assignment = _assignment(bindings)
    with pytest.raises(CommandOwnershipError) as raised:
        execute_assignment(
            assignment,
            bindings,
            _receipt,
            working_root=tmp_path / "work",
            runtime_env={"SELECTED_TOKEN": "science failed"},
        )
    assert (
        "science failed"
        not in AssignmentReply.failed(
            assignment.attempt, raised.value
        ).model_dump_json()
    )


def test_assignment_reply_preserves_failure_kind_location_and_attempt(
    bindings: PreparedLocalBindings,
) -> None:
    from artisan.execution.assignment_reply import AssignmentReply
    from artisan.execution.exceptions import AssignmentExecutionError

    attempt = _assignment(bindings).attempt
    delivery = DeliveryIdentity.new(attempt)
    reply = AssignmentReply.failed(
        attempt,
        AssignmentExecutionError(
            "native error: credential",
            phase="execute",
            call_path=("first", "child"),
            delivery=delivery,
        ),
    )
    restored = AssignmentReply.model_validate_json(reply.model_dump_json())
    with pytest.raises(AssignmentExecutionError) as raised:
        restored.result(attempt, sensitive_values=("credential",))
    assert raised.value.phase == "execute"
    assert raised.value.call_path == ("first", "child")
    assert raised.value.delivery == delivery
    assert "credential" not in str(raised.value)
    with pytest.raises(ValueError, match="different admitted"):
        restored.result(attempt.model_copy(update={"attempt": 1}))
    for error, kind in [
        (ValueError("decode"), "infrastructure"),
        (CommandOwnershipError(), "ownership"),
    ]:
        reply = AssignmentReply.failed(attempt, error)
        assert reply.kind == kind
        with pytest.raises(
            CommandOwnershipError if kind == "ownership" else RuntimeError
        ) as failure:
            reply.result(attempt)
        assert not isinstance(failure.value, AssignmentExecutionError)
    with pytest.raises(ValueError, match="exact receipt"):
        AssignmentReply(attempt=attempt, kind="success")
    with pytest.raises(ValueError, match="diagnostic"):
        AssignmentReply(attempt=attempt, kind="science")
    with pytest.raises(ValueError, match="exact closed delivery"):
        AssignmentReply(attempt=attempt, kind="science", message="failed")
    with pytest.raises(ValueError, match="exact closed delivery"):
        AssignmentReply(
            attempt=attempt,
            kind="science",
            message="failed",
            failed_delivery=DeliveryIdentity.new(
                attempt.model_copy(update={"attempt": 1})
            ),
        )
    for kind in ("infrastructure", "ownership"):
        with pytest.raises(ValueError, match="cannot claim"):
            AssignmentReply(
                attempt=attempt,
                kind=kind,
                message="failed",
                failed_delivery=delivery,
            )


@pytest.mark.parametrize("failure", ["unknown", "exception"])
def test_failed_delivery_is_not_exposed_before_session_exit(
    tmp_path: Path,
    bindings: PreparedLocalBindings,
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
) -> None:
    from artisan.execution.exceptions import AssignmentExecutionError
    from artisan.execution.executor import Executor
    from artisan.utils.command_supervisor import close_command_scope

    def science(*_args: object) -> Any:
        message = "authored failure"
        raise ValueError(message)

    def uncertain_close(error: BaseException | None = None) -> bool:
        close_command_scope(error)
        return False

    original_exit = Executor.__exit__

    def failed_exit(self: Executor, *args: Any) -> None:
        original_exit(self, *args)
        message = "session exit failed"
        raise OSError(message)

    monkeypatch.setattr(PreparedLocalBindings, "verify", lambda *_: None)
    monkeypatch.setattr(_NativeFile, "execute_function", science)
    if failure == "unknown":
        monkeypatch.setattr(
            "artisan.execution.executor.close_command_scope", uncertain_close
        )
    else:
        monkeypatch.setattr(Executor, "__exit__", failed_exit)
    with pytest.raises(
        CommandOwnershipError if failure == "unknown" else OSError
    ) as caught:
        execute_assignment(
            _assignment(bindings),
            bindings,
            _receipt,
            working_root=tmp_path / "work",
            runtime_env={},
        )
    assert not isinstance(caught.value, AssignmentExecutionError)
    cause = caught.value.__cause__ or caught.value.__context__
    assert isinstance(cause, AssignmentExecutionError)
    assert cause.delivery is None


def test_failed_reentry_gets_a_fresh_delivery_without_optional_diagnostics(
    tmp_path: Path,
    bindings: PreparedLocalBindings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from artisan.execution.exceptions import AssignmentExecutionError

    def fail(*_args: object) -> Any:
        message = "authored failure"
        raise ValueError(message)

    monkeypatch.setattr(PreparedLocalBindings, "verify", lambda *_: None)
    monkeypatch.setattr(_NativeFile, "execute_function", fail)
    assignment = _assignment(bindings)
    deliveries = []
    for _ in range(2):
        with pytest.raises(AssignmentExecutionError) as caught:
            execute_assignment(
                assignment,
                bindings,
                _receipt,
                working_root=tmp_path / "work",
                runtime_env={},
            )
        assert caught.value.delivery is not None
        assert caught.value.delivery.attempt == assignment.attempt
        deliveries.append(caught.value.delivery)
    assert deliveries[0] != deliveries[1]


@pytest.mark.parametrize("location", ["sealer", "session"])
def test_infrastructure_marker_cannot_acquire_a_failed_delivery(
    tmp_path: Path,
    bindings: PreparedLocalBindings,
    monkeypatch: pytest.MonkeyPatch,
    location: str,
) -> None:
    from artisan.execution.assignment_reply import AssignmentReply
    from artisan.execution.exceptions import AssignmentExecutionError
    from artisan.execution.executor import Executor
    from artisan.schemas.execution.delivery_diagnostics import DeliveryDiagnostics

    def fail(*_args: Any) -> Any:
        message = "misplaced scientific marker"
        raise AssignmentExecutionError(message, phase="execute")

    original_exit = Executor.__exit__

    def failed_exit(self: Executor, *args: Any) -> None:
        original_exit(self, *args)
        fail()

    monkeypatch.setattr(PreparedLocalBindings, "verify", lambda *_: None)
    if location == "session":
        monkeypatch.setattr(Executor, "__exit__", failed_exit)
    assignment = _assignment(bindings)
    captured: list[DeliveryDiagnostics] = []
    with pytest.raises(RuntimeError, match="outside scientific") as caught:
        execute_assignment(
            assignment,
            bindings,
            fail if location == "sealer" else _receipt,
            working_root=tmp_path / "work",
            runtime_env={},
            diagnostic_sink=captured.append,
        )
    assert not isinstance(caught.value, AssignmentExecutionError)
    reply = AssignmentReply.failed(assignment.attempt, caught.value)
    assert reply.kind == "infrastructure"
    assert reply.failed_delivery is None
    assert captured[0].outcome == "infrastructure"
    assert captured[0].phase is None
    assert captured[0].call_path == ()


@pytest.mark.parametrize("kind", ["science", "infrastructure", "ownership"])
def test_worker_reply_redacts_every_failure_before_json(
    bindings: PreparedLocalBindings, kind: str
) -> None:
    from artisan.execution.assignment_reply import AssignmentReply
    from artisan.execution.exceptions import AssignmentExecutionError

    secret = "selected-transport-secret"
    attempt = _assignment(bindings).attempt
    try:
        message = f"original failure: {secret}"
        raise ValueError(message)
    except ValueError as cause:
        try:
            if kind == "science":
                raise AssignmentExecutionError(
                    str(cause), phase="execute", delivery=DeliveryIdentity.new(attempt)
                ) from cause
            if kind == "ownership":
                raise CommandOwnershipError() from cause
            message = "bootstrap failed"
            raise RuntimeError(message) from cause
        except Exception as error:
            reply = AssignmentReply.failed(attempt, error, sensitive_values=(secret,))
    assert reply.kind == kind
    assert secret not in reply.model_dump_json()
    assert "<redacted>" in reply.model_dump_json()


def test_observed_credentials_reach_reply_redaction_before_json(
    bindings: PreparedLocalBindings, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import hashlib
    import sys

    from artisan.execution.assignment_reply import (
        AssignmentReply,
        AssignmentReplyContext,
    )
    from artisan.execution.environments.observed_worker import ObservedWorker
    from artisan.execution.local_worker import LocalAssignmentHandle
    from artisan.execution.worker_bootstrap import PreparedProcessCall
    from artisan.schemas.execution.worker_transport import WorkerTransportHeader

    secret = "synthetic-observed-credential"
    worker = ObservedWorker(
        Path(sys.executable),
        Path(sys.prefix),
        Path(sys.base_prefix),
        "0" * 64,
        bindings.compatibility,
        {"APPLICATION_API_KEY": secret, "ORDINARY_SETTING": "visible"},
    )
    bindings = replace(bindings, worker=worker, launcher=None)
    monkeypatch.setattr(ObservedWorker, "verify", lambda self: None)
    assert bindings.diagnostic_sensitive_values == (secret,)
    payload = b"trusted-call-placeholder"
    call = PreparedProcessCall(
        WorkerTransportHeader(
            compatibility=bindings.compatibility,
            payload_sha256=hashlib.sha256(payload).hexdigest(),
        ),
        payload,
    )
    monkeypatch.setattr(
        "artisan.execution.local_worker.serialize_worker_call", lambda *a, **kw: call
    )

    def fail(self: LocalAssignmentHandle, directory: Path) -> SealedDelivery:
        context = AssignmentReplyContext.model_validate_json(
            (directory / "reply-context.json").read_bytes()
        )
        assert secret in context.sensitive_values
        try:
            raise ValueError(secret)
        except ValueError as cause:
            try:
                message = "bootstrap failed"
                raise RuntimeError(message) from cause
            except RuntimeError as error:
                reply = AssignmentReply.failed(
                    self.attempt, error, sensitive_values=context.sensitive_values
                )
        assert secret not in reply.model_dump_json()
        assert "<redacted>" in reply.model_dump_json()
        return reply.result(self.attempt)

    monkeypatch.setattr(LocalAssignmentHandle, "_launch_worker", fail)
    work = tmp_path / "worker-scratch"
    handle = launch_local_assignment(
        _assignment(bindings), bindings, _receipt, working_root=work
    )
    with pytest.raises(RuntimeError, match="bootstrap failed") as raised:
        handle.result(timeout=10)
    assert secret not in str(raised.value)
    assert not any(work.iterdir())


@pytest.mark.parametrize("packed", [False, True])
def test_swapped_payload_pair_rejects_before_worker_call(
    tmp_path: Path,
    bindings: PreparedLocalBindings,
    monkeypatch: pytest.MonkeyPatch,
    packed: bool,
) -> None:
    import contextlib
    import json
    import sys

    from artisan.execution import local_call, local_packing, worker_reply
    from artisan.execution.assignment_reply import (
        AssignmentReply,
        AssignmentReplyContext,
    )
    from artisan.schemas.execution.worker_transport import WorkerTransportHeader
    from artisan.utils.configuration_secrets import COMMAND_ATTACHMENT_ENV

    attempt = _assignment(bindings).attempt
    context = AssignmentReplyContext(
        attempt=attempt, payload_sha256="a" * 64, sensitive_values=("selected-secret",)
    )
    header = WorkerTransportHeader(
        compatibility=bindings.compatibility, payload_sha256="b" * 64
    )
    (tmp_path / "reply-context.json").write_text(context.model_dump_json())
    (tmp_path / "header.json").write_text(header.model_dump_json())
    (tmp_path / "payload.bin").write_bytes(b"another admitted unit")
    for key in COMMAND_ATTACHMENT_ENV:
        monkeypatch.setenv(key, "synthetic")
    module = local_packing if packed else local_call
    monkeypatch.setattr(
        worker_reply,
        "execute_worker_call",
        lambda *_: pytest.fail("Swapped payload reached unpickle/science"),
    )
    if packed:
        monkeypatch.setattr(module, "validate_worker_attachment", lambda: None)
        monkeypatch.setattr(
            worker_reply, "attached_worker_unit", lambda *_: contextlib.nullcontext()
        )
        index = tmp_path / "units.json"
        index.write_text(json.dumps([{"directory": str(tmp_path), "attachment": {}}]))
        monkeypatch.setattr(sys, "argv", ["worker", str(index)])
        output = tmp_path / "reply.json"
    else:
        output = tmp_path / "result.json"
        monkeypatch.setattr(
            sys,
            "argv",
            [
                "worker",
                str(tmp_path / "header.json"),
                str(tmp_path / "payload.bin"),
                str(output),
            ],
        )
    module.main()
    reply = AssignmentReply.model_validate_json(output.read_bytes())
    assert reply.kind == "infrastructure"
    with pytest.raises(RuntimeError, match="exact worker payload"):
        reply.result(attempt)


def test_invalid_authored_output_is_scientific_contract_failure(
    tmp_path: Path, bindings: PreparedLocalBindings, monkeypatch: pytest.MonkeyPatch
) -> None:
    from artisan.execution.exceptions import AssignmentExecutionError

    monkeypatch.setattr(PreparedLocalBindings, "verify", lambda *_: None)
    monkeypatch.setattr(_NativeFile, "postprocess", lambda *_: ArtifactResult())
    with pytest.raises(AssignmentExecutionError) as raised:
        execute_assignment(
            _assignment(bindings),
            bindings,
            _receipt,
            working_root=tmp_path / "work",
            runtime_env={},
        )
    assert raised.value.phase == "output_validation"
    assert raised.value.delivery is not None
    assert raised.value.delivery.attempt == _assignment(bindings).attempt


@pytest.mark.parametrize("as_result", [False, True, "cancel-first"])
def test_route_ownership_uncertainty_preserves_authority(
    tmp_path: Path,
    bindings: PreparedLocalBindings,
    monkeypatch: pytest.MonkeyPatch,
    as_result: bool | str,
) -> None:
    from artisan.execution.compute.prepared import PreparedExecuteRouter

    def unknown(*_args: object) -> list[object]:
        error = CommandOwnershipError("synthetic unknown owner")
        if as_result:
            return [CancelledError(), error] if as_result == "cancel-first" else [error]
        raise error

    monkeypatch.setattr(PreparedLocalBindings, "verify", lambda *_: None)
    monkeypatch.setattr(PreparedExecuteRouter, "route_execute", unknown)
    with pytest.raises(CommandOwnershipError, match="synthetic unknown owner"):
        execute_assignment(
            _assignment(bindings),
            bindings,
            _receipt,
            working_root=tmp_path / "work",
            runtime_env={},
        )


class _MissingReference(_NativeFile):
    name = "missing_assignment_reference"
    outputs: ClassVar = {
        "files": OutputSpec(artifact_type="config", derives_from={"inputs": ["source"]})
    }

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        from artisan.schemas.artifact.execution_config import ExecutionConfigArtifact

        result = ArtifactResult()
        result.add_artifact(
            "files",
            ExecutionConfigArtifact.draft(
                {"missing": {"$artifact": "f" * 32}}, "config.json"
            ),
            sources={"source": [inputs.ref("source", index=0)]},
        )
        return result


class _MissingReferenceBoundary(CompositeDefinition):
    name = "missing_reference_boundary"

    class InputRole(StrEnum):
        source = "source"

    class OutputRole(StrEnum):
        files = "files"

    inputs: ClassVar = _NativeFile.inputs
    outputs: ClassVar = {"files": OutputSpec(artifact_type="config")}

    def compose(self, ctx: CompositeContext) -> None:
        child = ctx.call(
            _MissingReference, inputs={"source": ctx.input("source")}, name="child"
        )
        ctx.output("files", child.output("files"))


def test_missing_required_output_reference_is_scientific_boundary_failure(
    tmp_path: Path,
    bindings: PreparedLocalBindings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from artisan.execution.exceptions import AssignmentExecutionError

    monkeypatch.setattr(PreparedLocalBindings, "verify", lambda *_: None)
    with pytest.raises(AssignmentExecutionError, match="reference") as raised:
        execute_assignment(
            _assignment(bindings, _MissingReferenceBoundary),
            bindings,
            _receipt,
            working_root=tmp_path / "work",
            runtime_env={},
        )
    assert raised.value.phase == "output_projection"
    assert raised.value.call_path == ()
