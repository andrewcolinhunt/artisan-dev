"""Prefect server discovery and connection management.

Thin adapter over ``prefect_submitit.server``. Server lifecycle
(start/stop) is managed by the ``prefect-server`` CLI; this module
discovers and validates connectivity.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass

from prefect_submitit.server.discovery import (
    health_check,
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
                f"  export PREFECT_API_URL=http://<host>:<port>/api\n"
            ) from None
        source = "prefect_profile"

    info = PrefectServerInfo(url=url, source=source)
    _validate_health(info)
    return info


def _normalize_url(url: str) -> str:
    """Ensure URL ends with /api."""
    url = url.rstrip("/")
    if "/api/" in url or url.endswith("/api"):
        return url
    return f"{url}/api"


def _validate_health(info: PrefectServerInfo) -> None:
    """Check server health; raise PrefectServerUnreachable on failure.

    ``health_check()`` from prefect_submitit returns ``bool`` (never raises),
    so this wrapper checks the return value and raises with a rich error
    message including remediation instructions.

    Cloud URLs are skipped — Cloud is managed infrastructure and the
    unauthenticated health endpoint is not exposed.
    """
    if _is_cloud_url(info.url):
        return
    if not health_check(info.url):
        raise PrefectServerUnreachable(
            f"Prefect server at {info.url} is not reachable "
            f"(source: {info.source}).\n"
            "\n"
            "If the server is not running, start it:\n"
            "  pixi run prefect-start\n"
        )


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
