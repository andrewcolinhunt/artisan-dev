# Logging Redesign

## Status: Draft

## Problem

Artisan's logging has several issues:

- **Prefect logs leak in child processes.** `configure_logging()` suppresses
  Prefect by setting the Python logger level to CRITICAL, but this only affects
  the current process. When `Backend.LOCAL` uses `ProcessPoolTaskRunner`
  (spawn context), child processes get fresh loggers with Prefect's defaults,
  so Prefect task lifecycle logs appear on stdout.

- **No auto-configuration.** Users must call `configure_logging()` themselves.
  `PipelineManager` doesn't call it. Forgetting means either no artisan logs
  (no handler) or noisy Prefect logs (no suppression).

- **Default verbosity is wrong.** At INFO level, external tool stdout is
  streamed to console via `print()` (not the logger), and many artisan INFO
  messages are step-internal details users don't need on every run.

- **SLURM backend logs nothing to console.** Job IDs, node allocations, queue
  waits — none of this is logged. Worker stdout/stderr is silently written to
  parquet. Users get no visibility into what SLURM is doing.

- **Missing information.** Some useful signals (dispatch confirmation, artifact
  counts per role, backend resource allocation) are never logged at any level.

## Current State

### `configure_logging()` (`utils/logging.py`)

```python
def configure_logging(level="INFO", suppress_noise=True, loggers=("artisan",)):
    for logger_name in loggers:
        logger = logging.getLogger(logger_name)
        logger.setLevel(level)
        if not logger.handlers:
            handler = _ConsoleHandler(stream=sys.stdout)  # Rich-based
            handler.setFormatter(...)
            logger.addHandler(handler)
        logger.propagate = False

    if suppress_noise:
        for name in ("prefect", "httpx", "httpcore", "asyncio"):
            logging.getLogger(name).setLevel(logging.CRITICAL)
```

### Who calls it

| Caller | Context |
|--------|---------|
| `tutorial_setup()` | Notebook convenience |
| Demo scripts | Manual |
| **PipelineManager** | **Never** |

### Logger usage (src/artisan/)

| Level | ~Count | What |
|-------|--------|------|
| INFO | ~32 | Step lifecycle, cache hits, input resolution, commits |
| DEBUG | ~9 | Timings, NFS verification, failure log diagnostics |
| WARNING | ~15 | Missing inputs, timeouts, filter diagnostics |
| ERROR | ~16 | Dispatch/commit/setup failures |

### External tool output

`run_external_command(stream_output=True)` uses `print()` directly to stdout.
This bypasses the logger entirely — no level control, no formatting, no way to
suppress. Tool output is also persisted to `tool_output` column in execution
parquet (truncated to 500K chars).

### SLURM logging gaps

| Available info | Logged? |
|----------------|---------|
| SLURM job ID | Stored in result dict, not logged |
| Worker stdout/stderr | Written to parquet only |
| Job submission event | Not logged |
| Node allocation | Not captured |
| Queue wait time | Not captured |
| Job duration | Not captured |

## Design

### Fix: Prefect log suppression via environment variable

Set `PREFECT_LOGGING_LEVEL` in `os.environ` so it propagates to child
processes. Prefect 3 uses pydantic-settings with `env_prefix =
PREFECT_LOGGING_`, so `PREFECT_LOGGING_LEVEL=CRITICAL` is respected in every
spawned process.

```python
if suppress_noise:
    os.environ.setdefault("PREFECT_LOGGING_LEVEL", "CRITICAL")
    for name in _NOISY_LOGGERS:
        logging.getLogger(name).setLevel(logging.CRITICAL)
```

Use `setdefault` so users can override with an explicit env var if they want
Prefect logs.

### Fix: Auto-configure in PipelineManager

`PipelineManager.__init__()` calls `configure_logging()` as a default. Since
`configure_logging()` is already idempotent (checks for existing handlers),
this is safe even if the user already called it.

```python
class PipelineManager:
    def __init__(self, ..., configure_logging: bool = True):
        if configure_logging:
            from artisan.utils.logging import configure_logging as _configure
            _configure()
```

The `configure_logging=False` escape hatch lets advanced users opt out.

### Redesign: Verbosity levels

Redefine what each level means for the user, then reassign existing log calls.

#### INFO — Pipeline progress (default)

What the user needs to follow pipeline execution. One or two lines per step.
A user running a pipeline should see a compact, scannable summary.

**Keep at INFO:**
- Pipeline initialized (run_id, delta_root, staging_root)
- Step N (name) starting... [backend=X]
- Step N (name) CACHED
- Step N (name) completed in Xs [M/N succeeded]
- Pipeline complete: N steps, status
- Final summary table
- Prefect server URL

**Demote to DEBUG:**
- `resolved %d input artifacts` — internal detail
- `promoted %d file paths to Delta Lake` — internal detail
- `all input roles are empty — skipping` — better as part of step summary
- Commit details (`Step N commit: X rows artifacts, Y rows executions`)
- Staging verification complete
- Staged recovery messages
- `%d artifacts -> %d execution units` — internal batching detail
- `%d units cached, %d to dispatch` — internal detail

#### DEBUG — Internal machinery

For diagnosing problems. Includes everything at INFO plus:

- Input resolution details (artifact counts per role, sources)
- Batching/dispatch details (unit counts, cache breakdown)
- Commit row counts per table
- Staging verification progress and timing
- Execution timings breakdown
- Staged recovery events
- External tool output (see below)

#### WARNING — Anomalies (unchanged)

Keep current usage. These are appropriate:
- Empty inputs causing pipeline stop
- Missing artifacts in type map
- Staging verification timeout
- Filter passthrough diagnostics
- Compaction failures
- Invalid input file paths

#### ERROR — Failures (unchanged)

Keep current usage:
- Dispatch failures
- Commit failures
- Setup failures (creator/curator/chain)
- Future collection errors

### New logging: SLURM visibility

Add INFO-level logs for SLURM dispatch events:

```
Step 2 (MyOp): dispatched 8 units to SLURM [job_id=12345, partition=gpu]
Step 2 (MyOp): SLURM jobs complete, capturing worker logs
```

Add DEBUG-level logs for SLURM details:

```
Step 2 (MyOp): SLURM job 12345 stdout: 2.3KB, stderr: 0B
```

Implementation: log job IDs in `_collect_results()` after futures resolve, and
in `_capture_slurm_logs()` when extracting logs. Both methods live in
`orchestration/engine/dispatch.py`.

### External tool output: route through logger

Replace `print(line.rstrip("\n"))` in `_run_with_streaming()` with a logger
call at DEBUG level. This gives users control: at INFO they see clean pipeline
progress; at DEBUG they see tool output inline.

```python
# Before
print(line.rstrip("\n"))

# After
_tool_logger.debug("%s", line.rstrip("\n"))
```

Use a dedicated logger (`artisan.tools`) so users can control tool output
independently:

```python
configure_logging(loggers=("artisan",))              # default: no tool output
configure_logging(loggers=("artisan", "artisan.tools"))  # opt-in to tool output
```

Since `artisan.tools` is a child of `artisan`, it inherits the handler via
`propagate=True` (its default). Its level starts at `NOTSET`, which means it
defers to its parent's effective level. When `artisan` is set to INFO,
`artisan.tools` DEBUG calls are filtered out by the parent. When `artisan` is
set to DEBUG, `artisan.tools` DEBUG calls pass through — tool output appears
automatically at DEBUG without explicitly including `artisan.tools` in the
loggers tuple.

For users who want tool output at INFO (without all other DEBUG messages),
explicitly including `artisan.tools` in the loggers tuple sets its level to
INFO, overriding the NOTSET inheritance:

```python
configure_logging(level="INFO", loggers=("artisan", "artisan.tools"))
```

**`stream_output` parameter:** `stream_output=True` still controls whether
output is streamed line-by-line (vs. captured and returned). The change is
the output channel: `print()` → `_tool_logger.debug()`. At DEBUG level, users
see tool output inline as before. At INFO, streaming still happens (for the
`log_path` file and `tool_output` capture) but console output is suppressed
by the logger level.

**Tool file logging is unchanged** — `_run_with_streaming()`'s `log_path`
parameter still writes raw tool output to disk regardless of console level.

### Log storage strategy

Logs are stored in two tiers: Delta Lake (always) and files on disk
(selectively).

#### Delta Lake: primary log store

The `executions` table already captures logs per execution record:

| Column | Content | Granularity | Truncation |
|--------|---------|-------------|------------|
| `tool_output` | External command stdout+stderr | Per execution unit | 500K chars |
| `worker_log` | SLURM job stdout+stderr | Per SLURM job, duplicated onto each execution unit in that batch | 100K chars per stream (stdout/stderr truncated independently) |
| `error` | Python traceback on failure | Per execution unit | None |

This is unchanged. Logs belong with the execution record — that's where users
access them. There is no worker-level or step-level table, so `worker_log`
duplication across execution units in a batch is the cost of keeping everything
queryable from one place.

#### Disk: `logs/` directory

File-based logs live under `{delta_root.parent}/logs/`. This path is derived,
not user-configurable.

```
{pipeline_root}/
├── delta/
├── staging/
└── logs/
    ├── failures/                             ← moved from failure_logs/
    │   └── step_2_MyOp/
    │       └── {run_id}.log
    └── pipeline.log                          ← only when level="DEBUG"
```

**What gets written to disk:**

- **`logs/failures/`** — Human-readable failure logs, one per failed execution.
  Written unconditionally (current behavior, moved from `failure_logs/`).
- **`logs/pipeline.log`** — Full console logger output at DEBUG level. Only
  created when `configure_logging(level="DEBUG")` is called. Adds a file
  handler to the `artisan` logger that captures all levels to disk.
- **No `logs/slurm/` or `logs/tools/`** — SLURM worker logs and tool output
  are already persisted in the Delta Lake `executions` table. Writing them to
  disk as well would be duplication.

**Implementation:**

`configure_logging()` gains awareness of `logs_root` when called from
`PipelineManager` (which knows `delta_root`). When `level="DEBUG"`:

```python
def configure_logging(
    level: str = "INFO",
    suppress_noise: bool = True,
    loggers: tuple[str, ...] = ("artisan",),
    logs_root: Path | None = None,
) -> None:
    ...
    if logs_root and level.upper() == "DEBUG":
        logs_root.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(logs_root / "pipeline.log")
        file_handler.setFormatter(logging.Formatter(_LOG_FORMAT, datefmt=_LOG_DATEFMT))
        file_handler.setLevel(logging.DEBUG)
        logger.addHandler(file_handler)
```

`PipelineManager.__init__()` passes `logs_root`:

```python
_configure(logs_root=config.delta_root.parent / "logs")
```

**`failure_logs_root` migration:** Changes from `delta_root.parent /
"failure_logs"` to `delta_root.parent / "logs" / "failures"`
(`step_executor.py:190`). Existing failure logs in the old location are left
in place — failure logs are ephemeral debugging aids, not precious data.

## Migration

All changes are backwards-compatible:

- Users who already call `configure_logging()` see no change (idempotent)
- Users who don't call it now get auto-configured at INFO
- Demoted log lines move to DEBUG — visible with `configure_logging(level="DEBUG")`
- External tool output moves from `print()` to DEBUG logger — visible with
  `configure_logging(level="DEBUG")` or by including `artisan.tools` in loggers
- SLURM logs are additive
- `failure_logs/` moves to `logs/failures/` — old logs left in place
- `pipeline.log` only appears when DEBUG level is explicitly set

## Implementation Plan

- Add `os.environ.setdefault("PREFECT_LOGGING_LEVEL", "CRITICAL")` to
  `configure_logging()` when `suppress_noise=True`
- Add `logs_root` parameter to `configure_logging()`; add file handler when
  `level="DEBUG"`
- Add `configure_logging()` call in `PipelineManager.__init__()`, passing
  `logs_root=config.delta_root.parent / "logs"`
- Change `failure_logs_root` from `delta_root.parent / "failure_logs"` to
  `delta_root.parent / "logs" / "failures"` in `step_executor.py`
- Demote verbose INFO calls to DEBUG (list above)
- Add SLURM dispatch/completion logging
- Route external tool streaming through `artisan.tools` logger
- Update tests
