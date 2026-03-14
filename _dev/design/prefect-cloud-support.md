# Design: Support Prefect Cloud alongside Self-Hosted Server

**Date:** 2026-03-14
**Status:** Draft

## Problem

Artisan requires a self-hosted Prefect server for all pipeline execution. The
discovery module (`prefect_server.py`) delegates to
`prefect_submitit.server.discovery`, which only knows about self-hosted servers
(env vars, discovery files, health checks). This blocks users who want to use
Prefect Cloud for flow tracking and monitoring without running their own
infrastructure.

Three specific assumptions break Cloud compatibility:

- **`_normalize_url()` appends `/api`** — Cloud workspace URLs look like
  `https://api.prefect.cloud/api/accounts/{id}/workspaces/{id}`. They don't
  end with `/api`, so the normalizer appends it, producing an invalid URL.

- **`_validate_health()` hits `{url}/health`** — The `health_check()` from
  prefect_submitit does an unauthenticated `GET {url}/health`. Cloud workspace
  URLs don't expose this endpoint without authentication, causing spurious
  `PrefectServerUnreachable` errors.

- **`discover_server()` only uses `resolve_api_url()`** — Users who configure
  Cloud via `prefect cloud login` store their credentials in Prefect profiles
  (`~/.prefect/profiles.toml`), not shell env vars. `resolve_api_url()` from
  prefect_submitit doesn't read Prefect profiles, so discovery fails with
  `PrefectServerNotFound` even though Prefect is properly configured.

## Goal

Support both self-hosted and Prefect Cloud with zero friction. After this
change:

- Cloud users need only `prefect cloud login` (or env vars) — no
  Artisan-specific configuration
- Self-hosted users see no behavior change
- The `prefect_server` parameter on `create()`/`resume()` accepts Cloud
  workspace URLs directly
- Error messages guide users toward the right setup for their mode

## Key Insight

The Prefect SDK already handles Cloud vs self-hosted transparently. The
`@flow`/`@task` decorators and `TaskRunner` implementations (including
`SlurmTaskRunner`) work identically against either backend. We don't need
parallel code paths — just three small fixes to the discovery/validation layer.

## Scope

### In scope

- Fix `_normalize_url()` to handle Cloud URLs
- Fix `_validate_health()` to skip health check for Cloud
- Add Prefect profile fallback to `discover_server()`
- Update error messages and log output for mode awareness
- Add tests for Cloud URL handling

### Out of scope

- Changing `PipelineManager.create()` / `resume()` signatures
- Changing `PrefectServerInfo` dataclass fields
- Changing `activate_server()` behavior
- Adding Cloud-specific features (deployments, work pools, blocks)
- Modifying `prefect_submitit`
- Network/firewall configuration for SLURM + Cloud

---

## Design

### Cloud detection

A single helper determines whether a URL points to Prefect Cloud:

```python
def _is_cloud_url(url: str) -> bool:
    """Check if URL points to Prefect Cloud."""
    return "api.prefect.cloud" in url
```

This mirrors the Prefect SDK's own `Settings.connected_to_cloud` property,
which checks if `PREFECT_API_URL` starts with `PREFECT_CLOUD_API_URL`
(defaulting to `https://api.prefect.cloud/api`).

### Fix `_normalize_url()`

Cloud workspace URLs contain `/api/` in the middle of the path
(`/api/accounts/.../workspaces/...`). The current check `url.endswith("/api")`
misses this. Rather than a Cloud-specific branch, the fix handles any URL with
`/api/` already in the path:

```python
def _normalize_url(url: str) -> str:
    """Ensure URL ends with /api."""
    url = url.rstrip("/")
    if "/api/" in url or url.endswith("/api"):
        return url
    return f"{url}/api"
```

| Input | Output |
|-------|--------|
| `http://host:4200` | `http://host:4200/api` |
| `http://host:4200/api` | `http://host:4200/api` (unchanged) |
| `http://host:4200/api/` | `http://host:4200/api` (trailing slash stripped) |
| `https://api.prefect.cloud/api/accounts/x/workspaces/y` | unchanged |

### Fix `_validate_health()`

Skip the `prefect_submitit` health check for Cloud URLs. Cloud is managed
infrastructure — "is the server running?" is not a meaningful question. Auth
errors surface clearly at flow submission time with Prefect's own error
messages.

```python
def _validate_health(info: PrefectServerInfo) -> None:
    if _is_cloud_url(info.url):
        return  # Cloud is managed; auth errors surface at flow submission
    if not health_check(info.url):
        raise PrefectServerUnreachable(...)
```

### Add Prefect profile fallback to `discover_server()`

When `resolve_api_url()` fails (no explicit arg, no env vars, no discovery
file), check Prefect's own settings. This catches the `prefect cloud login`
case where config lives in `~/.prefect/profiles.toml`:

```python
def discover_server(prefect_server: str | None = None) -> PrefectServerInfo:
    _warn_old_env_var()
    try:
        url = _normalize_url(resolve_api_url(prefect_server))
        source = _source_label(url, prefect_server)
    except RuntimeError:
        url = _resolve_from_prefect_settings()
        if url is None:
            raise PrefectServerNotFound(
                "No Prefect server detected.\n"
                "\n"
                "For self-hosted:\n"
                "  pixi run prefect-start\n"
                "\n"
                "For Prefect Cloud:\n"
                "  prefect cloud login\n"
                "\n"
                "Or set the URL directly:\n"
                "  export PREFECT_API_URL=http://<host>:<port>/api\n"
            ) from None
        source = "prefect_profile"

    info = PrefectServerInfo(url=url, source=source)
    _validate_health(info)
    return info


def _resolve_from_prefect_settings() -> str | None:
    """Fallback: read API URL from Prefect's own settings (handles profiles)."""
    try:
        from prefect.settings import PREFECT_API_URL

        url = PREFECT_API_URL.value()
        if url:
            return _normalize_url(url)
    except Exception:
        pass
    return None
```

The fallback is lazy-imported so Prefect isn't loaded unless primary discovery
fails.

### Update `activate_server()` log message

Make the log output mode-aware:

```python
mode = "Cloud" if _is_cloud_url(info.url) else "self-hosted"
logger.info("Prefect %s: %s (source: %s)", mode, info.url, info.source)
```

### What does NOT change

- **`PrefectServerInfo`** — No new fields. API key handling is Prefect's
  responsibility (it reads from profiles and env vars natively).

- **`activate_server()` API key logic** — Not needed. `activate_server` sets
  `PREFECT_API_URL` in the SettingsContext. For Cloud, the API key is already
  in Prefect's settings from the profile or env var. The SettingsContext update
  preserves existing settings — it only overrides `PREFECT_API_URL`.

- **`pipeline_manager.py`** — No changes. `create()` and `resume()` call
  `discover_server()` then `activate_server()`, same as before.

- **Backends** — No changes. `@flow`/`@task`/`TaskRunner` work identically
  with Cloud. `SlurmTaskRunner` already propagates all Prefect settings
  (including `PREFECT_API_KEY`) to SLURM workers via
  `get_current_settings().to_environment_variables()`.

- **`prefect_submitit`** — No changes needed.

---

## Resolution chain (updated)

```
1. Explicit argument (prefect_server parameter)
2. PREFECT_SUBMITIT_SERVER env var
3. PREFECT_API_URL env var
4. Discovery file (~/.prefect-submitit/server.json)
   ── prefect_submitit.server.discovery handles 1-4 ──
5. Prefect profile (~/.prefect/profiles.toml)     ← NEW
6. PrefectServerNotFound with dual-mode instructions
```

---

## User experience by mode

### Cloud via `prefect cloud login`

```bash
$ prefect cloud login
# Stores API URL + key in ~/.prefect/profiles.toml

$ python my_pipeline.py
# discover_server() → resolve_api_url() fails (no env vars/discovery file)
# → _resolve_from_prefect_settings() finds URL in profile
# → _is_cloud_url() → True → skip health check
# → activate_server() sets PREFECT_API_URL in SettingsContext
# → log: "Prefect Cloud: https://api.prefect.cloud/api/accounts/.../workspaces/... (source: prefect_profile)"
```

### Cloud via env vars

```bash
$ export PREFECT_API_URL="https://api.prefect.cloud/api/accounts/.../workspaces/..."
$ export PREFECT_API_KEY="pnu_..."

$ python my_pipeline.py
# discover_server() → resolve_api_url() finds PREFECT_API_URL
# → _normalize_url() preserves Cloud URL (has /api/ in path)
# → _is_cloud_url() → True → skip health check
# → activate_server() sets PREFECT_API_URL in SettingsContext
# → log: "Prefect Cloud: https://... (source: env:PREFECT_API_URL)"
```

### Cloud via explicit argument

```python
pm = PipelineManager.create(
    name="my-pipeline",
    delta_root="/data/delta",
    staging_root="/data/staging",
    prefect_server="https://api.prefect.cloud/api/accounts/.../workspaces/...",
)
# PREFECT_API_KEY must be set separately via env var or profile
```

### Self-hosted (unchanged)

```bash
$ pixi run prefect-start
$ python my_pipeline.py
# Existing behavior: resolve_api_url() finds discovery file or env var
# → _normalize_url() appends /api if needed
# → _is_cloud_url() → False → health check runs
# → Everything works as before
```

---

## Files to change

| File | Action |
|------|--------|
| `src/artisan/orchestration/prefect_server.py` | Add `_is_cloud_url`, `_resolve_from_prefect_settings`; update `_normalize_url`, `_validate_health`, `discover_server`, `activate_server` log |
| `tests/artisan/orchestration/test_prefect_server.py` | Add Cloud URL tests |

## Files unchanged

| File | Why |
|------|-----|
| `src/artisan/orchestration/pipeline_manager.py` | Calls `discover_server()`/`activate_server()` which keep same signatures |
| `src/artisan/orchestration/backends/*.py` | `@flow`/`@task`/`TaskRunner` are server-mode-agnostic |
| `src/artisan/orchestration/engine/dispatch.py` | `execute_unit_task` is a `@task` — works with any backend |
| `tests/artisan/orchestration/conftest.py` | Mocks `discover_server`/`activate_server` by name; no changes needed |
| `tests/integration/conftest.py` | Uses `prefect_test_harness()`; unrelated to Cloud |

---

## Test plan

### New unit tests in `test_prefect_server.py`

**`TestNormalizeUrl`** — add:
- `test_cloud_url_unchanged`: Cloud workspace URL passes through untouched
- `test_cloud_url_trailing_slash`: trailing slash stripped, `/api` not appended
- `test_url_with_api_in_path`: any URL with `/api/` in path preserved

**`TestIsCloudUrl`** — new class:
- `test_cloud_url`: standard Cloud URL returns True
- `test_self_hosted_url`: `http://host:4200/api` returns False
- `test_localhost`: `http://localhost:4200/api` returns False

**`TestDiscoverServerCloud`** — new class:
- `test_cloud_via_env`: set `PREFECT_API_URL` to Cloud URL, verify discovery
  succeeds with `source="env:PREFECT_API_URL"` and health check skipped
- `test_cloud_via_prefect_profile`: mock `PREFECT_API_URL.value()` to return
  Cloud URL, verify fallback works with `source="prefect_profile"`
- `test_cloud_skips_health_check`: verify `health_check()` is never called
  for Cloud URLs (use `assert_not_called()`)

### Existing tests — no changes needed

All existing tests use `http://` URLs that don't match `api.prefect.cloud`,
so `_is_cloud_url()` returns False and existing behavior is preserved.

---

## SLURM + Cloud

`SlurmTaskRunner` works with Cloud out of the box:
- Propagates `PREFECT_API_URL` and `PREFECT_API_KEY` to SLURM workers via
  `get_current_settings().to_environment_variables()` (runner.py:216-218)
- Workers create task runs and report state changes via
  `get_client(sync_client=True)` (executors.py:131) — Cloud handles this
  identically to self-hosted

**Deployment requirement:** SLURM compute nodes need outbound HTTPS access
to `api.prefect.cloud`. Many HPC clusters restrict outbound internet. This is
a network/infra concern that should be documented but doesn't affect code.

## Risks

- **`prefect cloud login` profile format** — The fallback reads
  `PREFECT_API_URL.value()` from Prefect's settings system. If Prefect changes
  how profiles work, this could break. Low risk: `PREFECT_API_URL` is a core
  Prefect setting.

- **Non-standard Cloud domains** — `_is_cloud_url()` checks for
  `api.prefect.cloud`. Users with custom/EU Cloud domains would not be
  detected. Low risk: can be extended when needed.

- **Health check false sense of security** — Skipping health check for Cloud
  means auth errors only surface at flow submission time, not at pipeline
  creation. This is acceptable — the error is still clear, just slightly
  delayed.
