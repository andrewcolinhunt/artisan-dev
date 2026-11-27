"""Bound one frozen installed Modal qualification command and its recorded Apps.

The inputs JSON contains owned_root, profile, python, harness and a relative-path
SHA256 map named files. Populate it only after the exact installed profile exists.
This script never solves, installs, builds wheels or changes the supplied profile.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import importlib
import importlib.metadata
import json
import os
import runpy
import shutil
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any, Literal
from uuid import uuid4

from final_apps import app_control, app_ids, check_sdk_hooks, observe_apps, write_json
from pydantic import BaseModel, ConfigDict, Field

from artisan.utils.command_supervisor import (
    CommandSupervisor,
    ManagedCommand,
    strip_internal_attachment,
)
from artisan.utils.configuration_secrets import COMMAND_ATTACHMENT_ENV

MIB = 1024**2
WORK_SECONDS = 900
LOCAL_CLOSE_SECONDS = 10
WATCHDOG_SECONDS = 1030
REMOTE_CLOSE_SECONDS = 90
PHASES = ("image", "activation", "cpu", "redelivery", "gpu", "unknown")
type Phase = Literal["image", "activation", "cpu", "redelivery", "gpu", "unknown"]


class LaunchInputs(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    owned_root: Path
    profile: str
    python: str
    harness: str
    wheels: tuple[str, ...] = Field(min_length=3, max_length=3)
    files: dict[str, str] = Field(min_length=1)

    def path(self, relative: str) -> Path:
        path = Path(relative)
        if path.is_absolute() or ".." in path.parts:
            message = "Qualification input must remain under its owned root"
            raise ValueError(message)
        result = self.owned_root / path
        if not result.resolve().is_relative_to(self.owned_root):
            message = "Qualification path escaped its owned root"
            raise ValueError(message)
        return result

    def verify(self) -> None:
        root = self.owned_root
        if (
            root.parent != Path("/dev/shm")
            or root.resolve() != root
            or not root.name.startswith("artisan-modal-final-")
            or root.stat().st_uid != os.getuid()
        ):
            message = "Use an explicit host tmpfs root owned by the current user"
            raise ValueError(message)
        executable = self.path(self.python)
        if Path(
            sys.executable
        ) != executable or not executable.resolve().is_relative_to(root):
            message = "Use this profile's own installed controller interpreter"
            raise ValueError(message)
        if Path(__file__) != self.path(self.harness) / "final_launch.py":
            message = "Run the exact frozen qualification wrapper"
            raise ValueError(message)
        required = {
            self.profile,
            self.python,
            str(Path(self.profile).with_name("pixi.lock")),
            *self.wheels,
        }
        for module_name, name in (
            ("artisan", "dexterity-artisan"),
            ("artisan_modal", "artisan-modal"),
            ("modal_qualification_app", "artisan-modal-qualification"),
            ("modal", "modal"),
        ):
            package = importlib.import_module(module_name)
            owner = importlib.metadata.distribution(name)
            direct = json.loads(owner.read_text("direct_url.json") or "{}")
            if direct.get("dir_info", {}).get("editable", False) or owner.files is None:
                message = "Qualification requires ordinary installed wheels"
                raise ValueError(message)
            if package.__file__ is None or not Path(package.__file__).is_relative_to(
                executable.parent.parent
            ):
                message = "Qualification imported a package outside its own prefix"
                raise ValueError(message)
            for item in owner.files:
                if "__pycache__" not in item.parts and item.suffix != ".pyc":
                    location = Path(str(owner.locate_file(item))).resolve()
                    required.add(str(location.relative_to(root)))
        required.update(
            str(path.relative_to(root)) for path in self.path(self.harness).glob("*.py")
        )
        if not required <= self.files.keys():
            message = "Frozen inputs omit a canonical or harness file"
            raise ValueError(message)
        for relative, expected in self.files.items():
            with self.path(relative).open("rb") as stream:
                if hashlib.file_digest(stream, "sha256").hexdigest() != expected:
                    message = (
                        "Installed qualification input differs from its frozen hash"
                    )
                    raise ValueError(message)
        check_sdk_hooks()


def allocated_bytes(root: Path) -> int:
    seen = set()
    total = 0
    for directory, _, names in os.walk(root, followlinks=False):
        for name in names:
            try:
                info = (Path(directory) / name).lstat()
            except FileNotFoundError:
                continue
            identity = (info.st_dev, info.st_ino)
            if identity not in seen:
                total += info.st_blocks * 512
                seen.add(identity)
    return total


def guard_resources(root: Path, *, initial: bool = False) -> None:
    if shutil.disk_usage("/").free < 500 * MIB:
        message = "Host root reserve fell below 500 MiB"
        raise RuntimeError(message)
    if shutil.disk_usage("/dev/shm").free < (8192 if initial else 2048) * MIB:
        message = "Owned tmpfs headroom is insufficient"
        raise RuntimeError(message)
    if allocated_bytes(root) >= 5632 * MIB:
        message = "Owned data reached the 5.5 GiB guard before its 6 GiB budget"
        raise RuntimeError(message)


def supervised(
    argv: list[str],
    runtime: Path,
    *,
    seconds: float,
    guard_root: Path | None = None,
) -> tuple[dict[str, Any], str]:
    """Use the existing exact-subtree owner; retain uncertainty and collector state."""
    environment = strip_internal_attachment(os.environ)
    environment.update(
        PYTHONDONTWRITEBYTECODE="1",
        TMPDIR=str(runtime / "tmp"),
        XDG_CACHE_HOME=str((guard_root or runtime) / "cache/xdg"),
        PIXI_CACHE_DIR=str((guard_root or runtime) / "cache/pixi"),
        UV_CACHE_DIR=str((guard_root or runtime) / "cache/uv"),
        PIP_CACHE_DIR=str((guard_root or runtime) / "cache/pip"),
        NO_COLOR="1",
    )
    environment.pop("PYTHONPATH", None)
    owner = CommandSupervisor.start()
    result: dict[str, Any] = {"owner_pid": owner.process.pid, "ownership": "unknown"}
    command: ManagedCommand | None = None
    collector: threading.Thread | None = None
    stopped = threading.Event()
    captured: list[str] = []
    output_bytes = 0
    stop_reason: str | None = None

    def collect() -> None:
        nonlocal output_bytes
        assert command is not None
        try:

            def count(line: str) -> None:
                nonlocal output_bytes
                output_bytes += len(line.encode())
                if output_bytes > 8 * MIB:
                    message = "Qualification output exceeded 8 MiB"
                    raise ValueError(message)

            stdout, _stderr = command.communicate(on_line=count)
            captured.append(stdout)
            result["exit_code"] = command.returncode
        except BaseException as error:
            result["collector_error"] = type(error).__name__
        finally:
            stopped.set()

    started = time.monotonic()
    try:
        command = ManagedCommand(
            owner.attachment,
            argv,
            environment,
            str(runtime),
            merged=False,
            external=False,
        )
        collector = threading.Thread(target=collect, daemon=True)
        collector.start()
        next_guard = started
        while not stopped.wait(0.25):
            now = time.monotonic()
            if guard_root is not None and now >= next_guard:
                guard_resources(guard_root)
                next_guard = now + 5
            if now >= started + seconds:
                stop_reason = "deadline"
                break
        if "collector_error" in result:
            stop_reason = "collector_failure"
    except BaseException as error:
        stop_reason = type(error).__name__
    finally:
        deadline = time.monotonic() + LOCAL_CLOSE_SECONDS
        try:
            if stop_reason is not None:
                owner.request_close(deadline)
            result["ownership"] = owner.finish(deadline).state
        except BaseException as error:
            result["owner_error"] = type(error).__name__
        if collector is not None and collector.ident is not None:
            collector.join(max(0, deadline - time.monotonic()))
        result.update(
            collector_stopped=collector is None or not collector.is_alive(),
            collection_complete=command is not None and command.collection_complete,
            owner_exited=owner.process.poll() is not None,
            elapsed_seconds=time.monotonic() - started,
            stop_reason=stop_reason,
            discarded_output_bytes=output_bytes,
        )
        if command is not None and result["collector_stopped"]:
            try:
                command.dispose()
            except BaseException as error:
                result["dispose_error"] = type(error).__name__
    return result, captured[0] if captured else ""


def command_closed(result: dict[str, Any]) -> bool:
    return all(
        (
            result.get("ownership") == "closed",
            result.get("collector_stopped"),
            result.get("collection_complete"),
            result.get("owner_exited"),
            not result.get("dispose_error"),
        )
    )


def stop_apps(registry: Path, runtime: Path, deadline: float) -> dict[str, Any]:
    """Inspect/stop only returned SDK IDs, independent of the child driver's result."""
    identifiers = app_ids(registry)
    results: dict[str, Any] = {}
    for app_id in identifiers:
        requested = False
        while time.monotonic() < deadline:
            outcome, output = supervised(
                [
                    sys.executable,
                    __file__,
                    "app-control",
                    str(registry),
                    "list",
                    app_id,
                ],
                runtime,
                seconds=min(10, max(0, deadline - time.monotonic())),
            )
            if not command_closed(outcome) or outcome.get("exit_code") != 0:
                results[app_id] = {"state": "unknown", "inspection": outcome}
                break
            records = [item for item in json.loads(output) if item["App ID"] == app_id]
            if (
                len(records) == 1
                and records[0]["State"] == "stopped"
                and int(records[0]["Tasks"]) == 0
            ):
                results[app_id] = {"state": "stopped", "tasks": 0}
                break
            if not requested:
                outcome, _output = supervised(
                    [
                        sys.executable,
                        __file__,
                        "app-control",
                        str(registry),
                        "stop",
                        app_id,
                    ],
                    runtime,
                    seconds=min(30, max(0, deadline - time.monotonic())),
                )
                requested = True
                if not command_closed(outcome) or outcome.get("exit_code") != 0:
                    results[app_id] = {"state": "unknown", "stop": outcome}
                    break
            time.sleep(0.2)
        results.setdefault(app_id, {"state": "unknown", "reason": "cleanup_deadline"})
    return results


def apps_closed(registry: Path, results: dict[str, Any]) -> bool:
    if not registry.exists():
        return False
    identifiers = app_ids(registry)
    recorded = json.loads(registry.read_bytes())
    return (
        recorded["pending_creations"] == 0
        and set(results) == set(identifiers)
        and all(
            value.get("state") == "stopped" and value.get("tasks") == 0
            for value in results.values()
        )
    )


def child_argv(inputs: LaunchInputs, phase: Phase, runtime: Path) -> list[str]:
    harness = inputs.path(inputs.harness)
    binding = inputs.path(str(Path(inputs.profile).parent.parent / "image.json"))
    common = ["--binding", str(binding), "--report", str(runtime / "driver.json")]
    if phase == "image":
        target = harness / "final_prepare.py"
        args = [
            "--manifest",
            str(inputs.path(inputs.profile)),
            "--cache-root",
            str(inputs.owned_root / "cache"),
            *common,
        ]
    elif phase == "activation":
        target, args = harness / "activate_tools.py", common
    else:
        target = harness / "final_named.py"
        args = [
            "--case",
            phase,
            "--manifest",
            str(inputs.path(inputs.profile)),
            "--cache-root",
            str(inputs.owned_root / "cache"),
            "--working-root",
            str(runtime / "working"),
            *common,
        ]
    return [
        sys.executable,
        str(harness / "final_launch.py"),
        "child",
        str(runtime / "apps.json"),
        str(target),
        *args,
    ]


def watchdog(runtime: Path, deadline: float) -> int:
    while not (runtime / "watchdog-stop").exists() and time.monotonic() < deadline:
        time.sleep(0.5)
    if (runtime / "watchdog-stop").exists():
        write_json(runtime / "watchdog.json", {"stopped_by_parent": True})
        return 0
    try:
        results = stop_apps(
            runtime / "apps.json", runtime, time.monotonic() + REMOTE_CLOSE_SECONDS
        )
        write_json(
            runtime / "watchdog.json",
            {"apps": results, "closed": apps_closed(runtime / "apps.json", results)},
        )
    except BaseException as error:
        write_json(
            runtime / "watchdog.json", {"error": type(error).__name__, "closed": False}
        )
    return 0


def launch(inputs: LaunchInputs, phase: Phase) -> int:
    inputs.verify()
    guard_resources(inputs.owned_root, initial=True)
    runtime = inputs.owned_root / "runs" / f"{phase}-{uuid4().hex}"
    for name in ("tmp", "working"):
        (runtime / name).mkdir(parents=True)
    registry = runtime / "apps.json"
    report: dict[str, Any] = {"phase": phase, "passed": False, "runtime": str(runtime)}
    report_path = runtime / "outer.json"
    started = time.monotonic()
    environment = strip_internal_attachment(os.environ)
    environment.update(PYTHONDONTWRITEBYTECODE="1", TMPDIR=str(runtime / "tmp"))
    environment.pop("PYTHONPATH", None)
    watch = subprocess.Popen(
        [
            sys.executable,
            __file__,
            "watchdog",
            str(runtime),
            str(started + WATCHDOG_SECONDS),
        ],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        env=environment,
        start_new_session=True,
    )
    report["watchdog_pid"] = watch.pid
    try:
        write_json(report_path, report)
        outcome, _discarded = supervised(
            child_argv(inputs, phase, runtime),
            runtime,
            seconds=WORK_SECONDS,
            guard_root=inputs.owned_root,
        )
        report["command"] = outcome
    except BaseException as error:
        report["outer_error"] = type(error).__name__
    finally:
        try:
            report["apps"] = stop_apps(
                registry, runtime, time.monotonic() + REMOTE_CLOSE_SECONDS
            )
            report["apps_closed"] = apps_closed(registry, report["apps"])
            if not registry.exists() and command_closed(report.get("command", {})):
                report["app_creation_never_started"] = True
                report["apps_closed"] = True
        except BaseException as error:
            report["app_cleanup_error"] = type(error).__name__
        try:
            inputs.verify()
            report["source_unchanged"] = True
        except BaseException as error:
            report["inventory_error"] = type(error).__name__
        if command_closed(report.get("command", {})) and report.get("apps_closed"):
            try:
                (runtime / "watchdog-stop").touch(exist_ok=False)
            except OSError as error:
                report["watchdog_signal_error"] = type(error).__name__
        try:
            watch_returncode = watch.wait(
                timeout=max(
                    0,
                    started
                    + WATCHDOG_SECONDS
                    + REMOTE_CLOSE_SECONDS
                    + 20
                    - time.monotonic(),
                )
            )
            report["watchdog_stopped"] = True
            report["watchdog_exit_code"] = watch_returncode
        except subprocess.TimeoutExpired:
            report["watchdog_stopped"] = False
        except BaseException as error:
            report["watchdog_stopped"] = False
            report["watchdog_wait_error"] = type(error).__name__
        if (runtime / "watchdog.json").exists():
            try:
                report["watchdog"] = json.loads(
                    (runtime / "watchdog.json").read_bytes()
                )
                report["watchdog_closed"] = (
                    report["watchdog"].get("stopped_by_parent") is True
                    or report["watchdog"].get("closed") is True
                )
            except Exception as error:
                report["watchdog_report_error"] = type(error).__name__
        try:
            driver = json.loads((runtime / "driver.json").read_bytes())
            report["driver_passed"] = driver.get("passed") is True
            report["driver_body_error"] = driver.get("body_error") or driver.get(
                "error_type"
            )
            report["expected_unknown_retained"] = bool(
                driver.get("expected_unknown_retained")
            )
        except Exception as error:
            report["driver_report_error"] = type(error).__name__
        report["passed"] = all(
            (
                command_closed(report.get("command", {})),
                report.get("command", {}).get("exit_code") == 0,
                report.get("command", {}).get("stop_reason") is None,
                report.get("driver_passed"),
                report.get("apps_closed"),
                report.get("source_unchanged"),
                report.get("watchdog_stopped"),
                report.get("watchdog_exit_code") == 0,
                report.get("watchdog_closed"),
            )
        )
        report["scratch_removed"] = False
        if report["passed"] and not report.get("expected_unknown_retained"):
            try:
                for name in ("tmp", "working"):
                    shutil.rmtree(runtime / name)
                report["scratch_removed"] = True
            except OSError as error:
                report["passed"] = False
                report["scratch_cleanup_error"] = type(error).__name__
        report["elapsed_seconds"] = time.monotonic() - started
        write_json(report_path, report)
    return 0 if report["passed"] else 1


def main() -> int:
    if len(sys.argv) == 5 and sys.argv[1] == "app-control":
        sys.stdout.write(
            json.dumps(
                asyncio.run(app_control(Path(sys.argv[2]), sys.argv[3], sys.argv[4]))
            )
            + "\n"
        )
        return 0
    if len(sys.argv) > 1 and sys.argv[1] == "child":
        for key in COMMAND_ATTACHMENT_ENV:
            os.environ.pop(key, None)
        registry, target, *arguments = sys.argv[2:]
        sys.argv = [target, *arguments]
        with observe_apps(Path(registry)):
            runpy.run_path(target, run_name="__main__")
        return 0
    if len(sys.argv) > 1 and sys.argv[1] == "watchdog":
        return watchdog(Path(sys.argv[2]), float(sys.argv[3]))
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inputs", type=Path, required=True)
    parser.add_argument("--phase", choices=PHASES, required=True)
    args = parser.parse_args()
    return launch(
        LaunchInputs.model_validate_json(args.inputs.read_bytes()), args.phase
    )


if __name__ == "__main__":
    raise SystemExit(main())
