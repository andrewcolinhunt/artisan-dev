# Logging Redesign Demo

Demonstrates all new logging behaviors introduced by the logging redesign.

## Run

```bash
pixi run python _dev/demos/logging-redesign/demo_logging.py
```

## What it shows

- **Prefect env var suppression** — `PREFECT_LOGGING_LEVEL=CRITICAL` set via `setdefault` so child processes inherit it
- **INFO vs DEBUG verbosity** — internal details (input resolution, batching, caching) demoted to DEBUG
- **File handler** — `RotatingFileHandler` created when `level="DEBUG"` + `logs_root` provided
- **Idempotent calls** — `configure_logging()` safe to call multiple times
- **artisan.tools logger** — external tool streaming output routed through logger hierarchy
- **Auto-configure** — `PipelineManager.__init__()` calls `configure_logging()` automatically
- **Failure logs path** — moved from `failure_logs/` to `logs/failures/`
