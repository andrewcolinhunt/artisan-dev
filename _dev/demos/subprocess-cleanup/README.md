# Demo: Subprocess Cleanup on Ctrl+C

Shows that `run_external_command` properly kills child processes on
interruption, unlike raw `subprocess.Popen` which leaves orphans.

## Run

```bash
~/.pixi/bin/pixi run python _dev/demos/subprocess-cleanup/demo_subprocess_cleanup.py
```

## Scenarios

**Scenario 1 — Raw Popen (no cleanup):** Spawns `sleep 300`, simulates
interruption by abandoning the process. Confirms the child is still alive
(orphan), then manually cleans it up.

**Scenario 2 — Artisan with timeout:** Runs `sleep 300` via
`run_external_command` with a 1-second timeout. The timeout triggers
`_kill_process_group`, which sends SIGTERM to the child's process group.
Confirms no orphan remains.

**Scenario 3 — Artisan with KeyboardInterrupt (commented out):** Sends
`SIGINT` to the process during streaming mode. Useful for manual testing
but tricky to run non-interactively.
