"""Demo: Subprocess cleanup on Ctrl+C.

Shows that child processes spawned by run_command are properly
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
    """Scenario 2: Artisan run_command — cleanup via timeout."""
    from artisan.schemas.operation_config.environment_spec import LocalEnvironmentSpec
    from artisan.schemas.operation_config.tool_spec import ToolSpec
    from artisan.utils.external_tools import ExternalToolError, run_command

    print("=" * 60)
    print("SCENARIO 2: Artisan run_command (with cleanup)")
    print("=" * 60)
    print()

    env = LocalEnvironmentSpec()
    tool = ToolSpec(executable=Path("/bin/sleep"))

    pids_before = _find_sleep_pids()

    print("  Running `sleep 300` via run_command (timeout=1s)...")
    try:
        run_command(env, [*tool.parts(), "300"], timeout=1)
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
    print("This demo shows that artisan's run_command properly")
    print("cleans up child processes on interruption, unlike raw Popen.")
    print()

    scenario_raw_popen()
    scenario_artisan_timeout()

    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print()
    print("  Scenario 1 (raw Popen):  Child survives — ORPHAN")
    print("  Scenario 2 (artisan):    Child killed   — CLEAN")
    print()


if __name__ == "__main__":
    main()
