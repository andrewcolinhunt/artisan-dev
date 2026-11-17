"""File lifetime and command ownership across local scientific invocations."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
from fixtures.commands import IdentityCommandEnvironment

from artisan.execution.executor import Executor
from artisan.utils.command_supervisor import check_command_scope, close_command_scope
from artisan.utils.external_tools import run_command


def test_child_scope_closure_retains_files_and_parent_accepts_next_call(
    tmp_path: Path,
) -> None:
    with Executor(working_root=tmp_path / "sessions") as executor:
        with executor.invocation() as first:
            assert first.files is not None
            output = Path(first.files) / "intermediate.txt"
            run_command(
                IdentityCommandEnvironment(),
                [
                    sys.executable,
                    "-c",
                    "import pathlib,sys; pathlib.Path(sys.argv[1]).write_text('ready')",
                    str(output),
                ],
            )
            close_command_scope()
        check_command_scope()
        with executor.invocation() as second:
            assert second.sandbox != first.sandbox
            assert output.read_text() == "ready"
            run_command(IdentityCommandEnvironment(), [sys.executable, "-c", "pass"])
        assert output.exists()
    assert not output.exists()
    assert not list((tmp_path / "sessions").iterdir())


def test_session_rejects_invocation_outside_its_lifetime(tmp_path: Path) -> None:
    executor = Executor(working_root=tmp_path / "sessions")
    assert not executor.cleanup_complete
    with pytest.raises(RuntimeError, match="must be entered"), executor.invocation():
        pytest.fail("An inactive session admitted an invocation")
    with executor, executor.invocation() as paths:
        assert not executor.cleanup_complete
        assert Path(paths.sandbox).is_dir()
    assert executor.cleanup_complete
    with executor:
        assert not executor.cleanup_complete
    assert executor.cleanup_complete
    with pytest.raises(RuntimeError, match="must be entered"), executor.invocation():
        pytest.fail("A closed session admitted an invocation")


@pytest.mark.parametrize("fail", [False, True])
def test_preserved_session_files_outlive_success_or_closed_failure(
    tmp_path: Path, fail: bool
) -> None:
    executor = Executor(working_root=tmp_path, preserve_working=True)

    def run() -> Path:
        with executor, executor.invocation() as paths:
            output = Path(paths.execute) / "debug.txt"
            output.write_text("intermediate output")
            if fail:
                message = "scientific failure"
                raise ValueError(message)
        return output

    if fail:
        with pytest.raises(ValueError, match="scientific failure"):
            run()
        output = next(tmp_path.rglob("debug.txt"))
    else:
        output = run()
    assert output.read_text() == "intermediate output"
    with pytest.raises(RuntimeError, match="must be entered"), executor.invocation():
        pytest.fail("Preserved files kept the session active")


@pytest.mark.skipif(sys.platform != "linux", reason="Requires Linux process tracking")
def test_session_reaps_command_descendants_before_next_invocation(
    tmp_path: Path,
) -> None:
    child_program = (
        "import os,pathlib,signal,sys,time; "
        "signal.signal(signal.SIGTERM,signal.SIG_IGN); signal.alarm(20); "
        "pathlib.Path(sys.argv[1]).write_text(str(os.getpid())); time.sleep(30)"
    )
    leader_program = (
        "import pathlib,subprocess,sys,time; "
        "subprocess.Popen([sys.executable,'-c',sys.argv[1],sys.argv[2]], "
        "stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL); "
        "p=pathlib.Path(sys.argv[2]); "
        "exec('while not p.exists(): time.sleep(0.01)')"
    )
    with Executor(working_root=tmp_path / "session") as executor:
        with executor.invocation() as first:
            pid_path = Path(first.execute) / "descendant.pid"
            run_command(
                IdentityCommandEnvironment(),
                [sys.executable, "-c", leader_program, child_program, str(pid_path)],
            )
        pid = int(pid_path.read_text())
        assert not Path(f"/proc/{pid}").exists()
        with executor.invocation():
            run_command(IdentityCommandEnvironment(), [sys.executable, "-c", "pass"])
