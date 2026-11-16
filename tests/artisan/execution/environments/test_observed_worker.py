"""Observed interpreter qualification never invents or provisions software."""

from __future__ import annotations

import hashlib
import json
import sys
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest
from fixtures.composition_operations import Transform

from artisan.composites.base.compiler import compile_definition
from artisan.execution.environments.observed_worker import (
    ObservedCommandEnvironment,
    ObservedWorker,
    qualify_observed_worker,
)
from artisan.execution.local_assignment import (
    bind_local_software,
)
from artisan.execution.recording.commands import capture_commands
from artisan.schemas.execution.worker_transport import (
    TRANSPORT_PACKAGES,
    DistributionMetadata,
    RuntimeCompatibility,
)
from artisan.utils.command_supervisor import managed_invocation
from artisan.utils.external_tools import run_command

MODULE = "artisan.execution.environments.observed_worker"


@pytest.fixture
def worker() -> ObservedWorker:
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
    executable = Path(sys.executable).absolute()
    return ObservedWorker(
        executable,
        Path(sys.prefix),
        Path(sys.base_prefix),
        hashlib.sha256(executable.read_bytes()).hexdigest(),
        compatibility,
        {"PATH": "/observed/bin", "ACTIVATED": "literal $value"},
    )


def test_qualification_uses_current_python_and_detects_worker_mismatch(
    worker: ObservedWorker, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        f"{MODULE}.capture_runtime_compatibility",
        lambda *args, **kwargs: worker.compatibility,
    )
    monkeypatch.setattr(ObservedWorker, "verify", lambda self: None)
    measured = {
        "executable": str(worker.executable),
        "prefix": str(worker.prefix),
        "base_prefix": str(worker.base_prefix),
        "executable_sha256": worker.executable_sha256,
        "compatibility": worker.compatibility.model_dump(mode="json"),
    }
    calls = []

    def launch(command: list[str], **kwargs: object) -> SimpleNamespace:
        calls.append(command)
        assert command[:3] == [str(worker.executable), "-I", "-m"]
        return SimpleNamespace(stdout=json.dumps(measured))

    monkeypatch.setattr(f"{MODULE}.subprocess.run", launch)
    observed = qualify_observed_worker(TRANSPORT_PACKAGES)
    assert observed.executable == worker.executable
    assert observed.compatibility == worker.compatibility
    assert len(calls) == 1
    measured["executable_sha256"] = "f" * 64
    with pytest.raises(ValueError, match="reproduce"):
        qualify_observed_worker(TRANSPORT_PACKAGES)


def test_verification_is_read_only_and_rejects_interpreter_changes(
    worker: ObservedWorker, monkeypatch: pytest.MonkeyPatch
) -> None:
    verified = []
    monkeypatch.setattr(
        f"{MODULE}.verify_runtime_compatibility",
        lambda value, **kwargs: verified.append(value),
    )
    monkeypatch.setattr(
        f"{MODULE}.subprocess.run",
        lambda *args, **kwargs: pytest.fail("Verification launched a process"),
    )
    worker.verify()
    assert verified == [worker.compatibility]
    with pytest.raises(ValueError, match="interpreter changed"):
        replace(worker, executable_sha256="f" * 64).verify()


def test_inherited_command_preserves_activation_without_fresh_path_fallback(
    worker: ObservedWorker, tmp_path: Path
) -> None:
    environment = ObservedCommandEnvironment(
        worker, tmp_path, {"SELECTED_CREDENTIAL": "synthetic"}
    )
    assert environment.wrap_command(["python", "-I"]) == [str(worker.executable), "-I"]
    values = environment.prepare_env()
    assert values["ACTIVATED"] == "literal $value"
    assert values["SELECTED_CREDENTIAL"] == "synthetic"
    assert values["TMPDIR"] == str(tmp_path / "tmp")
    with pytest.raises(ValueError, match="captured worker PATH"):
        environment.wrap_command(["unavailable-command"])
    with pytest.raises(ValueError, match="cannot replace"):
        ObservedCommandEnvironment(worker, tmp_path, {"PATH": "/host"})


@pytest.mark.parametrize("relative", [False, True])
def test_inherited_command_uses_captured_path_and_literal_environment(
    worker: ObservedWorker,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    relative: bool,
) -> None:
    selected, ambient = tmp_path / "selected", tmp_path / "ambient"
    for directory, marker in ((selected, "selected"), (ambient, "ambient")):
        directory.mkdir()
        executable = directory / "probe-command"
        executable.write_text(
            f'#!/bin/sh\nprintf "%s:%s:%s" "{marker}" "$1" "$SELECTED_CREDENTIAL"\n'
        )
        executable.chmod(0o700)
    target = replace(
        worker, environment={"PATH": "selected" if relative else str(selected)}
    )
    monkeypatch.setenv("PATH", str(ambient))
    environment = ObservedCommandEnvironment(
        target, tmp_path / "scratch", {"SELECTED_CREDENTIAL": "literal $VALUE"}
    )
    command = environment.wrap_command(
        ["probe-command", "literal `argv`"], str(tmp_path)
    )
    assert command[0] == str(selected / "probe-command")
    with managed_invocation(), capture_commands() as recorder:
        result = run_command(
            environment, ["probe-command", "literal `argv`"], cwd=str(tmp_path)
        )
    assert result.stdout == "selected:literal `argv`:literal $VALUE"
    assert recorder.snapshot().commands[0].argv[0] == command[0]
    assert environment.wrap_command(["python"])[0] == str(worker.executable)


def test_unavailable_current_interpreter_fails_before_qualification(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(sys, "executable", str(tmp_path / "missing-python"))
    monkeypatch.setattr(
        f"{MODULE}.subprocess.run",
        lambda *args, **kwargs: pytest.fail("Missing interpreter launched a process"),
    )
    with pytest.raises(ValueError, match="unavailable"):
        qualify_observed_worker(TRANSPORT_PACKAGES)


def test_observed_binding_is_cache_ineligible_without_requiring_pixi(
    worker: ObservedWorker, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(ObservedWorker, "verify", lambda self: None)
    monkeypatch.setattr(
        "artisan.execution.local_profile.LocalLauncherIdentity.capture",
        lambda *args: pytest.fail("Observed native worker inspected Pixi"),
    )
    bindings = bind_local_software(
        compile_definition(Transform),
        worker,
        cache_root=tmp_path,
        pixi=tmp_path / "missing-pixi",
        system_env=tmp_path / "missing-env",
    )
    assert not bindings.software_cacheable
    assert bindings.launcher is None
    assert len(bindings.software_digest) == 64
    assert isinstance(
        bindings.worker_environment(tmp_path, {}), ObservedCommandEnvironment
    )
