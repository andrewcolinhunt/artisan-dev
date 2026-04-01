"""Prefect server discovery and connection management.

Thin adapter over ``prefect_submitit.server``. Server lifecycle
(start/stop) is managed by the ``prefect-server`` CLI; this module
discovers and validates connectivity.
"""

from __future__ import annotations

import logging
import os
import socket
import urllib.request
from dataclasses import dataclass

from prefect_submitit.server.discovery import (
    health_check,
    read_discovery,
    resolve_api_url,
)

logger = logging.getLogger(__name__)

ENV_VAR = "PREFECT_SUBMITIT_SERVER"
_OLD_ENV_VAR = "ARTISAN_PREFECT_SERVER"


def _is_cloud_url(url: str) -> bool:
    """Check if URL points to Prefect Cloud."""
    return "api.prefect.cloud" in url


@dataclass(frozen=True)
class PrefectServerInfo:
    """Resolved Prefect server connection info."""

    url: str
    source: str


def discover_server(prefect_server: str | None = None) -> PrefectServerInfo:
    """Discover and validate a Prefect server URL.

    Delegates URL resolution to
    ``prefect_submitit.server.discovery.resolve_api_url()``.

    Args:
        prefect_server: Explicit server URL. If provided, used directly.

    Returns:
        PrefectServerInfo with validated URL and source.

    Raises:
        PrefectServerNotFound: If no server can be discovered.
        PrefectServerUnreachable: If a server URL is found but health check fails.
        PrefectVersionMismatch: If client/server Prefect versions diverge.
    """
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
    info = _validate_health(info)
    _check_version_compatibility(info)
    return info


def _normalize_url(url: str) -> str:
    """Ensure URL ends with /api."""
    url = url.rstrip("/")
    if "/api/" in url or url.endswith("/api"):
        return url
    return f"{url}/api"


def _pid_alive(pid: int) -> bool:
    """Check whether a process is running."""
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def _read_discovery_file() -> dict | None:
    """Read the prefect_submitit discovery file.

    Thin wrapper so tests can mock at the artisan module level.
    """
    return read_discovery()


def _build_unreachable_message(info: PrefectServerInfo) -> str:
    """Build actionable error context when health check fails."""
    lines = [
        f"Prefect server at {info.url} is not reachable " f"(source: {info.source}).",
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
            lines.append("  Check that the server is still running on that node.")
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
        return PrefectServerInfo(url=localhost_url, source=f"{info.source}->localhost")
    return None


def _validate_health(info: PrefectServerInfo) -> PrefectServerInfo:
    """Check server health, try localhost fallback, raise on failure.

    Returns the validated info — which may be a localhost fallback if the
    original hostname was unreachable.

    Cloud URLs are skipped — Cloud is managed infrastructure and the
    unauthenticated health endpoint is not exposed.
    """
    if _is_cloud_url(info.url):
        return info
    if health_check(info.url):
        return info
    fallback = _try_localhost_fallback(info)
    if fallback is not None:
        return fallback
    raise PrefectServerUnreachable(_build_unreachable_message(info))


def _get_server_version(url: str) -> str | None:
    """GET {url}/admin/version -- the Prefect *package* version.

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


def _versions_compatible(client: str, server: str) -> bool:
    """Major.minor must match."""
    return client.split(".")[:2] == server.split(".")[:2]


def _check_version_compatibility(info: PrefectServerInfo) -> None:
    """Fail fast on client/server Prefect version mismatch.

    Skips Cloud URLs. Fails open (warns) if server version is unavailable.
    """
    if _is_cloud_url(info.url):
        return
    import prefect

    server_version = _get_server_version(info.url)
    if server_version is None:
        logger.warning("Could not determine server version at %s", info.url)
        return
    if not _versions_compatible(prefect.__version__, server_version):
        msg = (
            f"Prefect version mismatch: client={prefect.__version__}, "
            f"server={server_version} at {info.url}.\n"
            "\n"
            "Restart the server from this environment:\n"
            "  pixi run prefect-stop && pixi run prefect-start\n"
        )
        raise PrefectVersionMismatch(msg)


def _source_label(resolved_url: str, explicit: str | None) -> str:
    """Derive which source resolve_api_url() used (best-effort, for logging).

    resolve_api_url() returns only a URL string with no provenance metadata,
    so this re-checks the same sources in priority order.  This is a
    display-only annotation — correctness of discovery is guaranteed by
    resolve_api_url() itself.
    """
    if explicit is not None:
        return "argument"
    if os.environ.get(ENV_VAR):
        return f"env:{ENV_VAR}"
    if os.environ.get("PREFECT_API_URL"):
        return "env:PREFECT_API_URL"
    return "discovery_file"


def _resolve_from_prefect_settings() -> str | None:
    """Fallback: read API URL from Prefect's own settings (handles profiles)."""
    try:
        from prefect.settings import get_current_settings

        url = str(get_current_settings().api.url)
        if url:
            return _normalize_url(url)
    except Exception:
        pass
    return None


def _warn_old_env_var() -> None:
    """Emit a warning if the deprecated ARTISAN_PREFECT_SERVER is set."""
    if os.environ.get(_OLD_ENV_VAR) and not os.environ.get(ENV_VAR):
        import warnings

        warnings.warn(
            f"{_OLD_ENV_VAR} is no longer recognized. " f"Use {ENV_VAR} instead.",
            DeprecationWarning,
            stacklevel=3,
        )


def activate_server(info: PrefectServerInfo) -> None:
    """Set PREFECT_API_URL in both os.environ and Prefect's settings context.

    Args:
        info: Validated server info to activate.
    """
    os.environ["PREFECT_API_URL"] = info.url

    try:
        from prefect.context import SettingsContext

        ctx = SettingsContext.get()
        if ctx is not None:
            new_api = ctx.settings.api.model_copy(update={"url": info.url})
            new_settings = ctx.settings.model_copy(update={"api": new_api})
            new_ctx = SettingsContext(profile=ctx.profile, settings=new_settings)
            new_ctx.__enter__()
    except Exception:
        pass  # Prefect not imported yet or API changed; env var still set

    mode = "Cloud" if _is_cloud_url(info.url) else "self-hosted"
    logger.info("Prefect %s: %s (source: %s)", mode, info.url, info.source)


class PrefectServerNotFound(RuntimeError):
    """No Prefect server could be discovered."""


class PrefectServerUnreachable(RuntimeError):
    """A Prefect server URL was found but the server is not responding."""


class PrefectVersionMismatch(RuntimeError):
    """Client and server Prefect versions are incompatible."""
