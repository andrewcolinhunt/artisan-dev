"""Prefect server discovery and connection management.

Discover a running Prefect server via environment variables or a
discovery file. Server lifecycle (start/stop) is managed by shell
scripts; this module only discovers and validates connectivity.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)

DISCOVERY_FILE = Path.home() / ".artisan" / "prefect" / "server.json"
ENV_VAR_ARTISAN = "ARTISAN_PREFECT_SERVER"
ENV_VAR_PREFECT = "PREFECT_API_URL"
HEALTH_TIMEOUT_SECONDS = 5


@dataclass(frozen=True)
class PrefectServerInfo:
    """Resolved Prefect server connection info."""

    url: str
    source: str  # "argument", "env:ARTISAN_PREFECT_SERVER", "env:PREFECT_API_URL", "discovery_file"


def discover_server(prefect_server: str | None = None) -> PrefectServerInfo:
    """Discover the Prefect server URL.

    Resolution order:
        1. Explicit argument
        2. ARTISAN_PREFECT_SERVER env var
        3. PREFECT_API_URL env var
        4. Discovery file (~/.artisan/prefect/server.json)
        5. Raise with instructions

    Args:
        prefect_server: Explicit server URL. If provided, used directly.

    Returns:
        PrefectServerInfo with validated URL and source.

    Raises:
        PrefectServerNotFound: If no server can be discovered.
        PrefectServerUnreachable: If a server URL is found but health check fails.
    """
    # 1. Explicit argument
    if prefect_server is not None:
        url = _normalize_url(prefect_server)
        info = PrefectServerInfo(url=url, source="argument")
        _health_check(info)
        return info

    # 2. ARTISAN_PREFECT_SERVER env var
    env_artisan = os.environ.get(ENV_VAR_ARTISAN)
    if env_artisan:
        url = _normalize_url(env_artisan)
        info = PrefectServerInfo(url=url, source=f"env:{ENV_VAR_ARTISAN}")
        _health_check(info)
        return info

    # 3. PREFECT_API_URL env var
    env_prefect = os.environ.get(ENV_VAR_PREFECT)
    if env_prefect:
        url = _normalize_url(env_prefect)
        info = PrefectServerInfo(url=url, source=f"env:{ENV_VAR_PREFECT}")
        _health_check(info)
        return info

    # 4. Discovery file
    if DISCOVERY_FILE.exists():
        data = json.loads(DISCOVERY_FILE.read_text())
        url = data["url"]
        info = PrefectServerInfo(url=url, source="discovery_file")
        _health_check(info)
        return info

    # 5. Nothing found
    msg = (
        "No Prefect server detected.\n"
        "\n"
        "Start a server:\n"
        "  pixi run prefect-start\n"
        "\n"
        "  OR\n"
        "\n"
        "  ./scripts/prefect/start_prefect_server.sh --bg\n"
        "\n"
        "Or point to an existing server:\n"
        f"  export {ENV_VAR_ARTISAN}=http://<host>:<port>/api\n"
    )
    raise PrefectServerNotFound(msg)


def _normalize_url(url: str) -> str:
    """Ensure URL ends with /api."""
    url = url.rstrip("/")
    if not url.endswith("/api"):
        url = f"{url}/api"
    return url


def _health_check(info: PrefectServerInfo) -> None:
    """Verify the server is reachable.

    Args:
        info: Server info to check.

    Raises:
        PrefectServerUnreachable: If the health check fails.
    """
    import urllib.error
    import urllib.request

    health_url = info.url.replace("/api", "/api/health")
    try:
        req = urllib.request.Request(health_url, method="GET")
        with urllib.request.urlopen(req, timeout=HEALTH_TIMEOUT_SECONDS):
            pass
    except (urllib.error.URLError, OSError, TimeoutError) as exc:
        msg = (
            f"Prefect server at {info.url} is not reachable "
            f"(source: {info.source}).\n"
            f"\n"
            f"Health check failed: {exc}\n"
            f"\n"
            f"If the server is not running, start it:\n"
            f"  ./scripts/prefect/start_prefect_server.sh --bg\n"
        )
        raise PrefectServerUnreachable(msg) from exc


def activate_server(info: PrefectServerInfo) -> None:
    """Set PREFECT_API_URL in both os.environ and Prefect's settings context.

    Args:
        info: Validated server info to activate.
    """
    os.environ[ENV_VAR_PREFECT] = info.url

    # Update Prefect's cached settings if already initialized.
    # Prefect profiles are loaded once at first use and cached in a
    # SettingsContext.  Changing os.environ after that has no effect on
    # the cached settings, so we must update the context directly.
    try:
        from prefect.context import SettingsContext
        from prefect.settings import PREFECT_API_URL

        ctx = SettingsContext.get()
        if ctx is not None:
            new_settings = ctx.settings.copy_with_update(
                updates={PREFECT_API_URL: info.url}
            )
            new_ctx = SettingsContext(profile=ctx.profile, settings=new_settings)
            new_ctx.__enter__()
    except Exception:
        pass  # Prefect not imported yet or API changed; env var still set

    logger.info("Prefect server: %s (source: %s)", info.url, info.source)


class PrefectServerNotFound(RuntimeError):
    """No Prefect server could be discovered."""


class PrefectServerUnreachable(RuntimeError):
    """A Prefect server URL was found but the server is not responding."""
