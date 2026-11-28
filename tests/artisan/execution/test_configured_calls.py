"""Configured calls execute real phases without provisioning or publication."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest
from fixtures.configured_operations import CommandScale, ControllerScale, Scale

from artisan.composites.base.called_operations import capture_called_operations
from artisan.execution import CalledOperation, run_operation
from artisan.execution.configured_calls import configured_calls
from artisan.execution.executor_runtime import ExecutorRuntime
from artisan.execution.runtime_compatibility import capture_runtime_compatibility
from artisan.operations.base import OperationDefinition
from artisan.schemas.execution.called_operations import (
    ConfiguredCallScope,
    PreparedCalledOperation,
)
from artisan.schemas.execution.worker_transport import TRANSPORT_PACKAGES


def scope_for(selections: dict[str, CalledOperation]) -> ConfiguredCallScope:
    return ConfiguredCallScope(
        operations=tuple(
            PreparedCalledOperation(contract=item.contract, environment_key=None)
            for item in capture_called_operations(selections)
        ),
        caller_prefix=Path(sys.prefix),
        caller_compatibility=capture_runtime_compatibility(TRANSPORT_PACKAGES),
        pixi=Path.home() / ".pixi/bin/pixi",
        system_env=Path("/usr/bin/env"),
        runtime_env={},
    )


def test_fileless_calls_use_fixed_settings_and_current_process(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scope = scope_for(
        {
            "small": CalledOperation(Scale, {"factor": 2}),
            "large": CalledOperation(Scale, {"factor": 4}),
        }
    )
    monkeypatch.setattr(
        ExecutorRuntime, "_software", lambda *args: pytest.fail("software lookup")
    )
    with configured_calls(scope):
        for key, expected in [("small", 6), ("large", 12), ("small", 6)]:
            result = run_operation(Scale, selection_key=key, params={"value": 3})
            metric = result.artifacts["metrics"][0]
            assert metric.values["value"] == expected
            assert metric.values["pid"] == os.getpid()
            assert metric.is_finalized


@pytest.mark.parametrize(
    ("key", "definition", "params", "message"),
    [
        ("missing", Scale, {"value": 1}, "Unknown configured"),
        ("score", CommandScale, {"value": 1}, "definition or software"),
        ("score", Scale, {"factor": 5, "value": 1}, "override"),
    ],
)
def test_invalid_selection_fails_before_execution(
    key: str,
    definition: type[OperationDefinition],
    params: dict[str, int],
    message: str,
) -> None:
    with (
        configured_calls(scope_for({"score": CalledOperation(Scale, {"factor": 2})})),
        pytest.raises(ValueError, match=message),
    ):
        run_operation(definition, selection_key=key, params=params)


def test_command_call_runs_subprocess_and_leaves_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        ExecutorRuntime, "_software", lambda *args: pytest.fail("software lookup")
    )
    with configured_calls(
        scope_for({"score": CalledOperation(CommandScale, {"factor": 3})})
    ):
        result = run_operation(
            CommandScale, selection_key="score", params={"value": 4}, work_dir=tmp_path
        )
    assert result.artifacts["metrics"][0].values["value"] == 12
    assert result.artifacts["metrics"][0].values["pid"] != os.getpid()
    assert list(tmp_path.rglob("metric.json"))


def test_running_command_reuses_selection_for_native_and_command_calls(
    tmp_path: Path,
) -> None:
    import json

    from artisan.execution.configured_calls import ConfiguredCommandEnvironment
    from artisan.execution.environments.observed_worker import (
        ObservedCommandEnvironment,
        observe_current_worker,
    )
    from artisan.utils.external_tools import run_command

    scope = scope_for(
        {
            "native": CalledOperation(Scale, {"factor": 2}),
            "command": CalledOperation(CommandScale, {"factor": 3}),
            "controller": CalledOperation(ControllerScale),
            **{
                f"candidate_{index}": CalledOperation(Scale, {"factor": index})
                for index in range(100)
            },
        }
    )
    assert len(scope.model_dump_json()) > 65_536
    worker = observe_current_worker(TRANSPORT_PACKAGES)
    environment = ConfiguredCommandEnvironment(
        ObservedCommandEnvironment(worker, tmp_path / "parent", {}),
        scope.model_dump(mode="json"),
        tmp_path / "parent",
    )
    fixture_root = Path(__file__).resolve().parents[2]
    program = """
import json, os, sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
from fixtures.configured_operations import Scale, CommandScale, ControllerScale
from artisan.execution import run_operation
from artisan.execution.executor_runtime import ExecutorRuntime

def reject_lookup(*args: object) -> None:
    raise AssertionError("Configured call performed software lookup")
ExecutorRuntime._software = reject_lookup
values = []
for value in (2, 4):
    native = run_operation(Scale, selection_key="native", params={"value": value})
    command = run_operation(CommandScale, selection_key="command", params={"value": value}, work_dir=Path.cwd())
    values.append([native.artifacts["metrics"][0].values, command.artifacts["metrics"][0].values])
controller = run_operation(ControllerScale, selection_key="controller", params={"value": 7}, work_dir=Path.cwd())
Path("report.json").write_text(json.dumps({"pid": os.getpid(), "values": values, "controller": controller.artifacts["metrics"][0].values}))
"""
    run_command(
        environment,
        ["python", "-I", "-c", program, str(fixture_root)],
        cwd=str(tmp_path),
    )
    report = json.loads((tmp_path / "report.json").read_text())
    assert report["controller"]["value"] == 14
    assert report["controller"]["pid"] != report["pid"]
    assert [pair[0]["value"] for pair in report["values"]] == [4, 8]
    assert [pair[1]["value"] for pair in report["values"]] == [6, 12]
    assert all(pair[0]["pid"] == report["pid"] for pair in report["values"])
    assert all(pair[1]["pid"] != report["pid"] for pair in report["values"])


def test_configured_command_uses_separate_prepared_software(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fixtures.lightweight_commands import PreparedCommandProbe

    from artisan.composites.base.called_operations import environment_key
    from artisan.composites.base.compiler import compile_definition
    from artisan.execution.environments.command_witness import lookup_command_witness
    from artisan.execution.environments.observed_worker import observe_current_worker
    from artisan.execution.local_assignment import bind_local_software
    from artisan.operations.examples import DataGenerator

    cache = os.environ.get("ARTISAN_TEST_PREPARED_CACHE")
    if cache is None:
        pytest.skip("Explicit existing command-witness qualification cache required")
    captured = capture_called_operations(
        {"compiler": CalledOperation(PreparedCommandProbe)}
    )[0]
    assert captured.environment is not None
    plan = compile_definition(
        DataGenerator,
        called_operations={"compiler": CalledOperation(PreparedCommandProbe)},
    )
    bound = bind_local_software(
        plan,
        observe_current_worker(TRANSPORT_PACKAGES),
        cache_root=Path(cache),
        pixi=Path.home() / ".pixi/bin/pixi",
        system_env=Path("/usr/bin/env"),
    )
    target = bound.called_operations[0].target
    assert target is not None
    scope = scope_for({}).model_copy(
        update={
            "operations": (
                PreparedCalledOperation(
                    contract=captured.contract,
                    environment_key=environment_key(
                        captured.definition, captured.environment
                    ),
                    target=target,
                ),
            ),
            "witness": lookup_command_witness(Path(cache)),
        }
    )
    monkeypatch.setattr(
        ExecutorRuntime, "_software", lambda *args: pytest.fail("software lookup")
    )
    with configured_calls(scope):
        for _ in range(2):
            result = run_operation(
                PreparedCommandProbe, selection_key="compiler", work_dir=tmp_path
            )
            assert result.artifacts["metrics"][0].values == {"compiler_ran": True}
    assert target.prefix != Path(sys.prefix)
    assert len(list(tmp_path.rglob("macros.txt"))) == 2


@pytest.mark.parametrize("cancelled", [False, True])
def test_configured_failure_propagates_and_releases_call_guard(cancelled: bool) -> None:
    from concurrent.futures import CancelledError

    from artisan.execution.invocation import ExecuteFailure
    from artisan.schemas.specs.input_models import ExecuteInput

    class Failure(Scale):
        name = "configured_failure_scale"

        def execute_function(self, inputs: ExecuteInput) -> dict[str, list[int]]:
            error = CancelledError if cancelled else ValueError
            message = "configured failure"
            raise error(message)

    scope = scope_for(
        {"failure": CalledOperation(Failure), "good": CalledOperation(Scale)}
    )
    with configured_calls(scope):
        with pytest.raises(
            CancelledError if cancelled else ExecuteFailure, match="configured failure"
        ):
            run_operation(Failure, selection_key="failure", params={"value": 1})
        assert run_operation(Scale, selection_key="good", params={"value": 1}).success
