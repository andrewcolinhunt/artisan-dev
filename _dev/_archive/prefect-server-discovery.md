# Design: Prefect Server Discovery & Multi-Node Resilience

## Problem

Running a pipeline from a different node than where the Prefect server is
running should be seamless. Today it isn't — users hit opaque connectivity
failures that require manual debugging and environment variable exports.

## Context: What Happened

A user working across two nodes (g2117, jojo) encountered a cascade of failures:

1. **`pixi run prefect-start` on g2117 failed** — a stale Prefect server process
   (PID 3841055) was already bound to port 4296. The startup script reported
   "did not become healthy within 30s" with no guidance about the existing
   process.

2. **Existing server was reachable on localhost but not on hostname** — `curl
   http://localhost:4296/api/health` returned 200, but `curl
   http://g2117.ipd:4296/api/health` returned connection refused. The discovery
   file stored the hostname URL, so `PipelineManager.create()` failed with
   `PrefectServerUnreachable`.

3. **Setting `PREFECT_API_URL=http://localhost:4296/api` didn't help** — same
   `PrefectServerUnreachable` error. The env var was read (error showed
   `source: env:PREFECT_API_URL`) but the health check still failed from within
   the pixi-managed Python process.

4. **The server on g2117 turned out to be from a different environment** —
   started from `prefect-submitit-dev/.pixi/envs/default/bin/prefect`, not the
   `pipelines` pixi env. Different Prefect versions between client and server.

5. **Pointing to a healthy server on jojo (`PREFECT_API_URL=http://jojo.ipd:4296/api`)
   fixed discovery** — `PipelineManager.create()` succeeded. But the pipeline
   then failed at step dispatch with `500 Internal Server Error` on
   `POST /api/flows/`, again likely due to Prefect version mismatch between the
   client in `pipelines` and the server on jojo.

6. **Resolution required killing the jojo server and restarting from the correct
   environment** — manual, error-prone, and required understanding the
   architecture.

## Current Discovery Flow

`PipelineManager.create()` calls `discover_server()`, which resolves a URL
through two layers:

**`resolve_api_url()`** (in `prefect_submitit.server.discovery`) tries, in order:

1. Explicit argument to `PipelineManager.create(prefect_server=...)`
2. `PREFECT_SUBMITIT_SERVER` env var
3. `PREFECT_API_URL` env var
4. Discovery file `~/.prefect-submitit/server.json` (written by `prefect-start`)

If none resolve, **`discover_server()`** (in `artisan.orchestration.prefect_server`)
falls back to:

5. Prefect profile settings via `prefect.settings.get_current_settings()`

Once a URL is resolved, `_validate_health()` calls
`prefect_submitit.server.discovery.health_check(url)` — a simple HTTP GET to
`/api/health`. If it returns False, the user gets `PrefectServerUnreachable`.

## What's Missing

### 1. No version compatibility check

The health check only verifies HTTP connectivity. A server running Prefect 3.x
can return healthy to a 3.y client, but dispatch fails at runtime with 500
errors. The user discovers the version mismatch only after the pipeline starts
running and steps fail.

### 2. No stale process detection on startup

`prefect-start` tries to bind the port, fails, and reports a generic timeout.
It doesn't check whether an existing process owns the port, what environment
it's from, or whether it's healthy. The user has to manually run `lsof`,
`ps aux | grep prefect`, etc.

### 3. Discovery file is node-specific but stored in a shared filesystem

The discovery file is written by `prefect-start` with the hostname of the node
where the server was started (e.g., `g2117.ipd:4296`). On a shared NFS
filesystem, another node reads this file and tries to connect to the hostname —
which may be unreachable due to firewall rules, DNS issues, or the server
process having died. There is no fallback to try `localhost` or re-discover.

### 4. Limited diagnostic guidance when health check fails

The error message includes the URL and its resolution source (e.g.,
`source: discovery_file`) and suggests `pixi run prefect-start`. But it
doesn't:
- Check if the port is in use by a stale process
- Suggest checking whether the server is on a different node
- Recommend version alignment

### 5. No cross-environment version check

Nothing validates that the Prefect server was started from the same (or
compatible) environment as the client. A server from `prefect-submitit-dev` and
a client from `pipelines` can have different Prefect versions, and the user has
no way to know until runtime failures occur.

## Desired Behavior

| Scenario | Current | Desired |
|----------|---------|---------|
| Port already in use by compatible server | Timeout, generic error | Detect existing process, reuse if healthy + compatible |
| Port in use by incompatible server | Timeout, generic error | Detect version mismatch, offer to kill and restart |
| Discovery file points to unreachable host | `PrefectServerUnreachable` | Try localhost fallback, then suggest cross-node options |
| Server healthy but wrong Prefect version | Succeeds discovery, 500s at runtime | Fail fast at discovery with version mismatch error |
| User on different node than server | Manual `export PREFECT_API_URL=...` | Discovery file includes version; cross-node resolution works |

## Scope

### In scope

- Version compatibility check at discovery time (artisan-side)
- Localhost fallback when hostname URL is unreachable (artisan-side)
- Enhanced error diagnostics with actionable guidance (artisan-side)
- Stale process detection on server startup (upstream: `prefect_submitit`)
- ~~Version field in discovery file (upstream: `prefect_submitit`)~~ — already shipped

### Out of scope

- Multi-server registry or automatic failover
- Automatic server restart on failure
- Prefect Cloud changes (Cloud manages its own lifecycle)

## Proposed Solution

### Overview

Changes split into two layers that can ship independently:

| Layer | Package | Touches |
|-------|---------|---------|
| Client-side validation | `artisan.orchestration.prefect_server` | Version check, localhost fallback, error diagnostics |
| Server-side enrichment | `prefect_submitit.server` | Stale process detection (~version in discovery file~ — already shipped) |

### Change 1: Version compatibility check

**Where:** `artisan/orchestration/prefect_server.py`

After the health check passes, query the Prefect server's `/api/admin/version`
endpoint (which returns the Prefect package version, as opposed to
`/api/version` which returns the API protocol version) and compare with the
client's `prefect.__version__`. Fail fast if major.minor versions diverge.

```python
def _check_version_compatibility(info: PrefectServerInfo) -> None:
    """Fail fast on client/server Prefect version mismatch."""
    import prefect

    server_version = _get_server_version(info.url)  # GET {url}/admin/version
    if server_version is None:
        logger.warning("Could not determine server version at %s", info.url)
        return
    if not _versions_compatible(prefect.__version__, server_version):
        raise PrefectVersionMismatch(
            f"Prefect version mismatch: client={prefect.__version__}, "
            f"server={server_version} at {info.url}.\n"
            "\n"
            "Restart the server from this environment:\n"
            "  pixi run prefect-stop && pixi run prefect-start\n"
        )


def _versions_compatible(client: str, server: str) -> bool:
    """Major.minor must match."""
    return client.split(".")[:2] == server.split(".")[:2]


def _get_server_version(url: str) -> str | None:
    """GET {url}/admin/version — the Prefect *package* version.

    Note: /api/version returns SERVER_API_VERSION (the protocol version,
    e.g. "0.8.4"), not the package version. /api/admin/version returns
    prefect.__version__ (e.g. "3.6.13"), which is what we compare against.
    """
    try:
        with urllib.request.urlopen(
            f"{url.rstrip('/')}/admin/version", timeout=5
        ) as resp:
            return resp.read().decode().strip().strip('"')
    except Exception:
        return None
```

Call from `discover_server()` after `_validate_health()` returns (assumes the
updated `_validate_health` signature from Change 2; if shipping Change 1 alone,
use `_validate_health(info)` without assignment since the current signature
returns `None`):

```python
info = PrefectServerInfo(url=url, source=source)
info = _validate_health(info)
_check_version_compatibility(info)
return info
```

New exception: `PrefectVersionMismatch(RuntimeError)`.

**Why package version, not API protocol version?** Prefect exposes two version
values: the API protocol version at `/api/version` (e.g., `"0.8.4"`) and the
package version at `/api/admin/version` (e.g., `"3.6.13"`). We compare package
versions because: (a) the user sees and controls package versions via `pip`/`pixi`,
so mismatch messages are actionable; (b) the protocol version can remain unchanged
across package releases that still have incompatible behavior; (c) the incident
that motivated this design involved different package versions, not protocol
versions.

### Change 2: Localhost fallback

**Where:** `artisan/orchestration/prefect_server.py`

When the discovery file URL uses a hostname and the health check fails, try
replacing the host with `localhost` (same port). This handles the common case
where the server binds to `0.0.0.0` but the FQDN is unreachable due to
firewall rules.

```python
def _try_localhost_fallback(info: PrefectServerInfo) -> PrefectServerInfo | None:
    """If URL uses a hostname, try localhost with the same port."""
    from urllib.parse import urlparse, urlunparse

    parsed = urlparse(info.url)
    if parsed.hostname in ("localhost", "127.0.0.1"):
        return None
    if parsed.port is None:
        return None
    localhost_url = urlunparse(parsed._replace(netloc=f"localhost:{parsed.port}"))
    if health_check(localhost_url):
        logger.warning(
            "Server at %s unreachable; fell back to %s", info.url, localhost_url
        )
        return PrefectServerInfo(
            url=localhost_url, source=f"{info.source}->localhost"
        )
    return None
```

Update `_validate_health()` to return the (possibly updated) info and attempt
the fallback before raising:

```python
def _validate_health(info: PrefectServerInfo) -> PrefectServerInfo:
    """Check server health, try localhost fallback, raise on failure.

    Returns the validated info — which may be a localhost fallback if the
    original hostname was unreachable.
    """
    if _is_cloud_url(info.url):
        return info
    if health_check(info.url):
        return info
    fallback = _try_localhost_fallback(info)
    if fallback is not None:
        return fallback
    raise PrefectServerUnreachable(_build_unreachable_message(info))
```

### Change 3: Enhanced error diagnostics

**Where:** `artisan/orchestration/prefect_server.py`

Enrich `PrefectServerUnreachable` with contextual diagnostics gathered at the
point of failure:

```python
def _build_unreachable_message(info: PrefectServerInfo) -> str:
    """Build actionable error context when health check fails."""
    lines = [
        f"Prefect server at {info.url} is not reachable "
        f"(source: {info.source}).",
        "",
    ]
    disco = _read_discovery_file()
    if disco and all(k in disco for k in ("pid", "started", "host")):
        lines.append(
            f"Discovery file: PID {disco['pid']}, "
            f"started {disco['started']} on {disco['host']}"
        )
        if disco.get("host") != socket.getfqdn():
            lines.append(
                f"  Server was started on {disco.get('host', '?')} "
                f"(you are on {socket.getfqdn()})."
            )
            lines.append(
                "  Check that the server is still running on that node."
            )
        elif not _pid_alive(disco["pid"]):
            lines.append("  Process is no longer running (stale discovery file).")
            lines.append("  Start a new server: pixi run prefect-start")
        else:
            lines.append("  Process is alive but not responding on the expected URL.")
            lines.append("  It may have been started from a different environment.")
    else:
        lines.append("No discovery file found at ~/.prefect-submitit/server.json")
        lines.append("  Start a server: pixi run prefect-start")
    return "\n".join(lines)
```

Helper `_read_discovery_file()` reads `~/.prefect-submitit/server.json` (same
path used by `prefect_submitit`). Helper `_pid_alive(pid)` wraps
`os.kill(pid, 0)`.

### Change 4: Stale process detection on startup (upstream)

**Where:** `prefect_submitit.server.prefect_proc` (upstream PR)

Before attempting to bind the port, `prefect-server start` should:

- Read the existing discovery file
- If a PID is recorded, check if it's alive (`os.kill(pid, 0)`)
- If alive: report the process info (PID, environment, start time) and exit
  with a message suggesting `prefect-server stop` first
- If dead: delete the stale discovery file and proceed with startup

This eliminates the "did not become healthy within 30s" timeout when a stale
process holds the port.

### Change 5: Version field in discovery file (upstream) — ALREADY SHIPPED

**Where:** `prefect_submitit.server.discovery`

The `prefect_version` field is already present in `write_discovery()` in the
current version of `prefect_submitit`. The discovery file now contains:

```json
{
    "url": "http://g2117.ipd:4296/api",
    "host": "g2117.ipd",
    "port": 4296,
    "pid": 12345,
    "user": "ahunt",
    "backend": "sqlite",
    "started": "2026-03-29T10:00:00",
    "prefect_version": "3.2.1"
}
```

When present, the version check (change 1) can use this field instead of an
HTTP round-trip to `/api/admin/version`, making the check work even when the
server is only partially responsive.

## Implementation Order

| Priority | Change | Layer | Depends on |
|----------|--------|-------|------------|
| P0 | Version compatibility check | artisan | Localhost fallback (shared call site) |
| P0 | Localhost fallback | artisan | — |
| P1 | Enhanced error diagnostics | artisan | — |
| P2 | Stale process detection | prefect_submitit | — |
| ~~P2~~ | ~~Version in discovery file~~ | ~~prefect_submitit~~ | Already shipped upstream |

P0 changes share a call site in `discover_server()` and should ship together —
the version check's call site uses the updated `_validate_health` return type
from the localhost fallback change. Both only touch
`artisan/orchestration/prefect_server.py`. The stale process detection (P2)
requires an upstream PR to `prefect_submitit`.

## Testing Strategy

Tests in `tests/artisan/orchestration/test_prefect_server.py`:

- **Version check:** Mock `urllib.request.urlopen` to return version responses;
  test compatible, incompatible, and unavailable scenarios
- **Localhost fallback:** Mock `health_check()` to fail on hostname, succeed on
  localhost; verify fallback URL and warning log
- **Error diagnostics:** Mock discovery file reads and PID checks; verify
  message content for stale, alive, and missing discovery file scenarios
- **Integration** (`@pytest.mark.slow`): Start a real server and validate the
  full discovery flow including version check
