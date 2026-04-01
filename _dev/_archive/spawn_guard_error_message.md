# Design: Spawn Guard Error Message

**Date:** 2026-03-27
**Status:** Draft

---

## Summary

When users run an Artisan pipeline without the `if __name__ == "__main__":` guard,
Python's `spawn`-based multiprocessing raises a cryptic `RuntimeError` about
"bootstrapping phase" and `freeze_support()`. Replace this with a clear,
actionable error message that tells the user exactly what to fix.

---

## Problem

Artisan explicitly uses `multiprocessing.get_context("spawn")` in two places:

- `LocalBackend` / `SIGINTSafeProcessPoolTaskRunner` (`backends/local.py:42`)
- `_run_curator_in_subprocess` (`engine/step_executor.py:606`)

The `spawn` start method re-imports the user's main module in child processes.
Without the `if __name__ == "__main__":` guard, the child re-executes the pipeline,
which tries to spawn more children, triggering:

```
RuntimeError:
    An attempt has been made to start a new process before the
    current process has finished its bootstrapping phase.
```

This error is a Python stdlib message with no mention of Artisan. Users don't know
what to fix or that it's a one-line change.

---

## Approach

Catch the `RuntimeError` at the two spawn sites, pattern-match on the message text,
and re-raise with an Artisan-specific message that includes the fix.

### Error message

```
Multiprocessing failed: your script's pipeline code must be inside
an `if __name__ == "__main__":` block.

Artisan uses the "spawn" start method for child processes, which re-imports
your script. Without the guard, the import re-triggers pipeline execution.

Fix:

    if __name__ == "__main__":
        pipeline.run()

See: https://docs.python.org/3/library/multiprocessing.html#the-spawn-and-forkserver-start-methods
```

### Intercept points

**`_get_one` in `dispatch.py`** — This is where the `RuntimeError` currently
surfaces (line 117). The `future.result()` call propagates the child exception.
This is the primary intercept point since it handles all local backend futures.

```python
def _get_one(future: object) -> dict:
    try:
        return future.result()
    except RuntimeError as exc:
        if "bootstrapping phase" in str(exc):
            raise _spawn_guard_error() from exc
        raise
    except Exception as exc:
        ...
```

**`_run_curator_in_subprocess` in `step_executor.py`** — The curator subprocess
path. Same pattern: catch `RuntimeError` from `future.result()`, check message,
re-raise.

### Helper

Add a helper to `utils/errors.py`:

```python
_SPAWN_GUARD_MESSAGE = """\
Multiprocessing failed: your script's pipeline code must be inside \
an `if __name__ == "__main__":` block.

Artisan uses the "spawn" start method for child processes, which re-imports \
your script. Without the guard, the import re-triggers pipeline execution.

Fix:

    if __name__ == "__main__":
        pipeline.run()

See: https://docs.python.org/3/library/multiprocessing.html\
#the-spawn-and-forkserver-start-methods"""


def spawn_guard_error(cause: BaseException) -> RuntimeError:
    """Wrap a spawn bootstrapping error with an actionable message."""
    return RuntimeError(_SPAWN_GUARD_MESSAGE) from cause


def is_spawn_guard_error(exc: BaseException) -> bool:
    """Check if an exception is the multiprocessing spawn bootstrap error."""
    return isinstance(exc, RuntimeError) and "bootstrapping phase" in str(exc)
```

---

## Scope

- `src/artisan/utils/errors.py` — add `spawn_guard_error`, `is_spawn_guard_error`
- `src/artisan/orchestration/engine/dispatch.py` — intercept in `_get_one`
- `src/artisan/orchestration/engine/step_executor.py` — intercept in `_run_curator_in_subprocess`
- `tests/artisan/utils/test_errors.py` — test message detection and wrapping

---

## Alternatives considered

**Detect at import time / pipeline construction.** We could inspect the call
stack when `Pipeline.run()` is called and warn if not inside a `__main__` guard.
This is fragile (doesn't work in notebooks, test harnesses, or when the pipeline
is constructed in a function called from `__main__`). Rejected in favor of
catching the actual error.

**Change start method to `fork`.** Would eliminate the issue but `fork` is
deprecated on macOS since Python 3.12 and is unsafe with threads. Not viable.

---

## Testing

- Unit test: `is_spawn_guard_error` returns True for matching `RuntimeError`,
  False for other `RuntimeError`s and other exception types.
- Unit test: `spawn_guard_error` wraps with the correct message and preserves
  the cause chain.
- The actual spawn failure is hard to trigger in-process; rely on the message
  detection tests.
