# Design: Migrate Artisan to `prefect_submitit.server`

**Date:** 2026-03-08
**Status:** Draft

## Problem

Artisan maintains its own Prefect server management code:

- **`src/artisan/orchestration/prefect_server.py`** — 173 lines of discovery,
  health check, and URL resolution logic
- **`scripts/prefect/`** — Three shell scripts (start, stop, init-db) that
  hardcode pixi binary paths

This duplicates functionality now provided by `prefect_submitit.server`, which
ships a `prefect-server` CLI entry point and a Python API for discovery, health
checks, and server lifecycle. The duplication means:

- Bug fixes and improvements must be applied in two places
- The discovery file path differs (`~/.artisan/prefect/server.json` vs
  `~/.prefect-submitit/server.json`), so a server started by one tool is
  invisible to the other
- The env var name differs (`ARTISAN_PREFECT_SERVER` vs
  `PREFECT_SUBMITIT_SERVER`), confusing users who work with both repos

## Goal

Remove Artisan's bespoke server management code. Replace it with imports from
`prefect_submitit.server`, which Artisan already depends on via
`prefect-submitit`. After this change:

- One discovery file path: `~/.prefect-submitit/server.json`
- One env var: `PREFECT_SUBMITIT_SERVER` (replaces `ARTISAN_PREFECT_SERVER`)
- One CLI: `prefect-server` (replaces `scripts/prefect/*.sh`)
- `prefect_server.py` becomes a thin adapter (~50 lines) over `prefect_submitit.server`

## Scope

### In scope

- Rewrite `prefect_server.py` to delegate to `prefect_submitit.server.discovery`
- Delete `scripts/prefect/` directory (three shell scripts)
- Update pixi tasks to use `prefect-server` CLI
- Rename env var from `ARTISAN_PREFECT_SERVER` to `PREFECT_SUBMITIT_SERVER`
- Update tests and docs

### Out of scope

- Changing `PipelineManager.create()` / `resume()` signatures
- Changing `activate_server()` behavior (Prefect `SettingsContext` override)
- Modifying `prefect_submitit.server` itself

---

## Design

### Rewritten `prefect_server.py`

The module keeps its public API (`discover_server`, `activate_server`,
`PrefectServerInfo`, `PrefectServerNotFound`, `PrefectServerUnreachable`) so
that `pipeline_manager.py` and downstream code need no changes. Internally, it
delegates to `prefect_submitit.server`.

```python
from prefect_submitit.server.discovery import (
    health_check,
    resolve_api_url,
)

ENV_VAR = "PREFECT_SUBMITIT_SERVER"


def discover_server(prefect_server: str | None = None) -> PrefectServerInfo:
    """Discover and validate a Prefect server URL.

    Delegates URL resolution to prefect_submitit.server.discovery.resolve_api_url().
    """
    try:
        url = _normalize_url(resolve_api_url(prefect_server))
    except RuntimeError as exc:
        raise PrefectServerNotFound(...) from exc

    info = PrefectServerInfo(url=url, source=_source_label(url, prefect_server))
    _validate_health(info)
    return info
```

Key changes:

- **`discover_server()`**: Calls `resolve_api_url(prefect_server)` which handles
  the full resolution chain (explicit arg > `PREFECT_SUBMITIT_SERVER` env >
  `PREFECT_API_URL` env > discovery file). Wraps the `RuntimeError` from
  `resolve_api_url` in `PrefectServerNotFound` for backward compatibility.

- **`_validate_health()`** (renamed from `_health_check`): Calls
  `health_check(url)` from prefect_submitit and raises
  `PrefectServerUnreachable` on failure. This preserves Artisan's richer error
  messages with instructions.

- **`activate_server()`**: Unchanged. It handles Prefect's `SettingsContext`
  override, which is Artisan-specific.

- **`_normalize_url()`**: Unchanged. Ensures URL ends with `/api`.

- **`_source_label()`**: New helper. Determines the source label
  (`"argument"`, `"env:PREFECT_SUBMITIT_SERVER"`, `"env:PREFECT_API_URL"`,
  `"discovery_file"`) by checking which source matched, for logging.

- **Constants removed**: `DISCOVERY_FILE`, `ENV_VAR_ARTISAN`,
  `HEALTH_TIMEOUT_SECONDS` — all handled by `prefect_submitit.server` now.

### Delete `scripts/prefect/`

All three scripts are removed:

| Script | Replacement |
|---|---|
| `start_prefect_server.sh` | `prefect-server start [--bg] [--sqlite] [--restart]` |
| `stop_prefect_server.sh` | `prefect-server stop [--all] [-f]` |
| `init_postgres.sh` | `prefect-server init-db [--reset]` |

### Update `pyproject.toml`

Pixi tasks switch to the Python entry point:

```toml
# Before
prefect-start = "./scripts/prefect/start_prefect_server.sh --bg"
prefect-stop = "./scripts/prefect/stop_prefect_server.sh -f"

# After
prefect-start = "prefect-server start --bg"
prefect-stop = "prefect-server stop --force"
```

### Env var rename

| Before | After |
|---|---|
| `ARTISAN_PREFECT_SERVER` | `PREFECT_SUBMITIT_SERVER` |

The `ARTISAN_PREFECT_SERVER` name is dropped entirely — no fallback, no
deprecation shim. The var was only documented in two places (installation guide
and tutorial notebook) and has no external consumers beyond this repo.

### Discovery file path change

| Before | After |
|---|---|
| `~/.artisan/prefect/server.json` | `~/.prefect-submitit/server.json` |

The old path is no longer read. Users who had a running server started with the
old shell scripts will need to restart it with `prefect-server start` (which
writes the new discovery file). This is fine because the old scripts stored
PostgreSQL data in `$PROJECT_ROOT/.prefect-postgres`, while the new CLI uses
`~/.prefect-submitit/postgres/` — a fresh `init-db` is needed regardless.

---

## Files to change

| File | Action |
|---|---|
| `src/artisan/orchestration/prefect_server.py` | Rewrite: delegate to `prefect_submitit.server` |
| `scripts/prefect/start_prefect_server.sh` | Delete |
| `scripts/prefect/stop_prefect_server.sh` | Delete |
| `scripts/prefect/init_postgres.sh` | Delete |
| `pyproject.toml` | Update pixi tasks |
| `tests/artisan/orchestration/test_prefect_server.py` | Update: new env var name, new discovery path |
| `tests/artisan/orchestration/conftest.py` | No change (mocks `discover_server` / `activate_server` which keep same signatures) |
| `tests/integration/conftest.py` | No change (uses `prefect_test_harness`, unrelated) |
| `docs/getting-started/installation.md` | Update env var name |
| `docs/tutorials/getting-started/01-first-pipeline.ipynb` | Update env var name |
| `docs/getting-started/first-pipeline.md` | No change (only references `pixi run prefect-start`) |
| `README.md` | No change (only references `pixi run prefect-start`) |

## Files unchanged

- **`pipeline_manager.py`** — Calls `discover_server()` and `activate_server()`
  which keep their signatures.
- **`tests/artisan/orchestration/conftest.py`** — Mocks `discover_server` and
  `activate_server` by name; same function names, same module path.
- **`tests/integration/conftest.py`** — Uses `prefect_test_harness()`, no
  discovery involved.

---

## Test plan

### Unit tests: `tests/artisan/orchestration/test_prefect_server.py`

Update existing tests:

| Test class | Changes |
|---|---|
| `TestNormalizeUrl` | No changes |
| `TestDiscoverServerExplicit` | No changes (explicit arg path unchanged) |
| `TestDiscoverServerEnv` | Change `ARTISAN_PREFECT_SERVER` → `PREFECT_SUBMITIT_SERVER`, update expected source labels |
| `TestDiscoverServerFile` | Patch `prefect_submitit.server.discovery.DEFAULT_DATA_DIR` instead of `DISCOVERY_FILE` |
| `TestDiscoverServerNotFound` | Same approach, updated patch targets |
| `TestDiscoverServerPriority` | Change env var name |
| `TestHealthCheck` | Update patch target to wrap new internal health function |
| `TestActivateServer` | No changes |

### Manual validation

1. `pixi run prefect-start` → server starts, discovery file at `~/.prefect-submitit/server.json`
2. `PipelineManager.create(...)` discovers server automatically
3. `pixi run prefect-stop` → server stops, discovery file removed

---

## Risks

- **Breaking change for users with `ARTISAN_PREFECT_SERVER` in their
  environment.** Mitigation: the var was only documented in two files within
  this repo. Any user who has it set will get a clear `PrefectServerNotFound`
  error with instructions to use `PREFECT_SUBMITIT_SERVER` instead.

- **Users with running servers started by old shell scripts.** They need to
  restart with `prefect-server start`. The old PostgreSQL data lives at
  `$PROJECT_ROOT/.prefect-postgres` and is not auto-migrated. Users can manually
  move it to `~/.prefect-submitit/postgres/` or reinitialize.
