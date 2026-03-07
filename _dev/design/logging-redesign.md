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

| Level | Count | What |
|-------|-------|------|
| INFO | 34 | Step lifecycle, cache hits, input resolution, commits |
| DEBUG | 8 | Timings, NFS verification, failure log diagnostics |
| WARNING | 12 | Missing inputs, timeouts, filter diagnostics |
| ERROR | 13 | Dispatch/commit/setup failures |

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
in `_capture_slurm_logs()` when extracting logs.

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

Since `artisan.tools` is a child of `artisan`, it inherits the handler when
`propagate=True` (its default). But `configure_logging()` only sets the level
on loggers in the `loggers` tuple, so `artisan.tools` stays at WARNING unless
explicitly included. This gives fine-grained control without a new parameter.

**Alternative:** Add a `tool_output_level` parameter to `configure_logging()`.
Simpler API but less flexible.

**File logging is unchanged** — `log_path` still captures everything to disk
regardless of console level.

## Migration

All changes are backwards-compatible:

- Users who already call `configure_logging()` see no change (idempotent)
- Users who don't call it now get auto-configured at INFO
- Demoted log lines move to DEBUG — visible with `configure_logging(level="DEBUG")`
- External tool output moves from `print()` to DEBUG logger — visible with
  `configure_logging(level="DEBUG")` or by including `artisan.tools` in loggers
- SLURM logs are additive

## Implementation Plan

- Add `os.environ.setdefault("PREFECT_LOGGING_LEVEL", "CRITICAL")` to
  `configure_logging()` when `suppress_noise=True`
- Add `configure_logging()` call in `PipelineManager.__init__()`
- Demote verbose INFO calls to DEBUG (list above)
- Add SLURM dispatch/completion logging
- Route external tool streaming through `artisan.tools` logger
- Update tests
