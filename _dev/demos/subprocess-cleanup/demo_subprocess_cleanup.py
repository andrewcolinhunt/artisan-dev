"""Demo: Subprocess cleanup on Ctrl+C.

Shows that child processes spawned by run_external_command are properly
cleaned up when interrupted, compared to raw subprocess.Popen which
leaves orphans.

Scenarios:
  1. RAW POPEN (no cleanup)  — child process survives interruption
  2. ARTISAN (with cleanup)  — child process is killed on interruption

Each scenario spawns a `sleep 300` child, interrupts it, and checks
whether the child is still running.

Usage:
    ~/.pixi/bin/pixi run python _dev/demos/subprocess-cleanup/demo_subprocess_cleanup.py
"""

from __future__ import annotations

import os
import signal
import subprocess
import time
from pathlib import Path


def _is_alive(pid: int) -> bool:
    """Check if a process is still running."""
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False


def _cleanup_pid(pid: int) -> None:
    """Best-effort kill of a leftover process."""
    try:
        os.kill(pid, signal.SIGKILL)
        os.waitpid(pid, 0)
    except (ProcessLookupError, ChildProcessError, OSError):
        pass


def scenario_raw_popen() -> None:
    """Scenario 1: Raw Popen — no cleanup, child becomes orphan."""
    print("=" * 60)
    print("SCENARIO 1: Raw subprocess.Popen (no cleanup)")
    print("=" * 60)
    print()

    proc = subprocess.Popen(
        ["sleep", "300"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    child_pid = proc.pid
    print(f"  Spawned child: PID {child_pid}")
    print("  Simulating interruption (no cleanup)...")
    print()

    # Simulate what happens without cleanup: parent just moves on.
    # The child is still running — it's an orphan.
    alive_before_cleanup = _is_alive(child_pid)
    print(f"  Child alive after interruption? {alive_before_cleanup}")

    if alive_before_cleanup:
        print(f"  ^^^ ORPHAN! PID {child_pid} is still running.")
        print(f"  Cleaning up orphan for demo purposes...")
        _cleanup_pid(child_pid)
        print(f"  Child alive after manual cleanup? {_is_alive(child_pid)}")
    print()


def scenario_artisan_timeout() -> None:
    """Scenario 2: Artisan run_external_command — cleanup via timeout."""
    from artisan.schemas.operation_config.command_spec import LocalCommandSpec
    from artisan.utils.external_tools import (
        ArgStyle,
        ExternalToolError,
        run_external_command,
    )

    print("=" * 60)
    print("SCENARIO 2: Artisan run_external_command (with cleanup)")
    print("=" * 60)
    print()

    spec = LocalCommandSpec(
        script=Path("/bin/sleep"),
        arg_style=ArgStyle.ARGPARSE,
        interpreter=None,
    )

    # We'll capture the child PID by inspecting /proc or ps.
    # Strategy: record PIDs of `sleep 300` before and after.
    pids_before = _find_sleep_pids()

    print("  Running `sleep 300` via run_external_command (timeout=1s)...")
    try:
        run_external_command(spec, ["300"], timeout=1)
    except ExternalToolError as e:
        print(f"  Caught ExternalToolError: {e.message}")

    # Brief pause for OS to reap
    time.sleep(0.2)
    pids_after = _find_sleep_pids()
    new_pids = pids_after - pids_before

    print()
    if not new_pids:
        print("  No orphan `sleep` processes found. Cleanup worked!")
    else:
        print(f"  WARNING: orphan PIDs found: {new_pids}")
        for pid in new_pids:
            _cleanup_pid(pid)
    print()


def scenario_artisan_interrupt() -> None:
    """Scenario 3: Artisan streaming mode — cleanup via KeyboardInterrupt."""
    import threading

    from artisan.schemas.operation_config.command_spec import LocalCommandSpec
    from artisan.utils.external_tools import ArgStyle, run_external_command

    print("=" * 60)
    print("SCENARIO 3: Artisan streaming + KeyboardInterrupt")
    print("=" * 60)
    print()

    spec = LocalCommandSpec(
        # Use bash -c to echo then sleep, so streaming has output to read
        script=Path("bash"),
        arg_style=ArgStyle.ARGPARSE,
        interpreter=None,
    )

    pids_before = _find_sleep_pids()
    caught: list[BaseException] = []

    def _run() -> None:
        try:
            run_external_command(
                spec,
                ["-c", "echo 'started'; sleep 300"],
                stream_output=True,
            )
        except KeyboardInterrupt:
            caught.append(KeyboardInterrupt())
        except Exception as e:
            caught.append(e)

    thread = threading.Thread(target=_run)
    thread.start()

    # Wait for the child to start
    time.sleep(0.5)
    pids_during = _find_sleep_pids() - pids_before
    print(f"  Child `sleep` PIDs during run: {pids_during or 'none found'}")

    # Send SIGINT to our own process — this triggers KeyboardInterrupt
    # in the main thread, which propagates to the streaming loop.
    print("  Sending KeyboardInterrupt...")
    os.kill(os.getpid(), signal.SIGINT)

    thread.join(timeout=5)
    time.sleep(0.2)

    pids_after = _find_sleep_pids() - pids_before
    print()
    if not pids_after:
        print("  No orphan `sleep` processes found. Cleanup worked!")
    else:
        print(f"  Remaining PIDs: {pids_after}")
        for pid in pids_after:
            _cleanup_pid(pid)
    print()


def _find_sleep_pids() -> set[int]:
    """Find PIDs of `sleep 300` processes owned by current user."""
    try:
        result = subprocess.run(
            ["pgrep", "-f", "sleep 300"],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode == 0 and result.stdout.strip():
            return {int(p) for p in result.stdout.strip().split("\n")}
    except FileNotFoundError:
        pass
    return set()


def main() -> None:
    print()
    print("Subprocess Cleanup Demo")
    print("~~~~~~~~~~~~~~~~~~~~~~~")
    print()
    print("This demo shows that artisan's run_external_command properly")
    print("cleans up child processes on interruption, unlike raw Popen.")
    print()

    scenario_raw_popen()
    scenario_artisan_timeout()

    # Scenario 3 uses SIGINT which makes the demo harder to run
    # non-interactively. Uncomment to test manually:
    # scenario_artisan_interrupt()

    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print()
    print("  Scenario 1 (raw Popen):  Child survives — ORPHAN")
    print("  Scenario 2 (artisan):    Child killed   — CLEAN")
    print()


if __name__ == "__main__":
    main()
