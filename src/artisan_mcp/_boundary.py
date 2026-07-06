"""The single error boundary shared by every fallible MCP tool.

Mirrors ``cli.py``'s ``_emit`` verbatim: an ``ArtisanError`` serializes as
its ``to_dict()`` envelope; a ``FileNotFoundError`` from a store reader
(an agent pointing at an empty or wrong root) wraps into the shipped
``store_not_found`` envelope with a chained cause. Reuses the existing
``ErrorCode`` constants — no new codes are minted here.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable

    from artisan_mcp.config import ArtisanMCPConfig


def boundary(payload_fn: Callable[[], Any]) -> Any:
    """Run a tool body, returning its payload or an error envelope.

    Args:
        payload_fn: Thunk that computes the tool's success payload. Any
            ``ArtisanError`` it raises (including ``delta_root_unset`` from
            ``require_delta_root``) or ``FileNotFoundError`` from a store
            reader is caught and serialized.

    Returns:
        The thunk's return value on success, or ``ArtisanError.to_dict()``
        on a handled failure.
    """
    from artisan.errors import ArtisanError, ErrorCode

    try:
        return payload_fn()
    except FileNotFoundError as exc:
        err = ArtisanError(
            ErrorCode.STORE_NOT_FOUND,
            str(exc),
            error_type="io",
            hint="no Delta tables at this root — check ARTISAN_DELTA_ROOT",
            recovery_hint="CHECK_INPUT",
        )
        err.__cause__ = exc
        return err.to_dict()
    except ArtisanError as exc:
        return exc.to_dict()


def require_delta_root(config: ArtisanMCPConfig) -> str:
    """Resolve the configured Delta root or raise the ``delta_root_unset`` envelope.

    Args:
        config: The server configuration carrying ``delta_root``.

    Returns:
        The resolved Delta root path.

    Raises:
        ArtisanError: With ``code=delta_root_unset`` when no root is set.
    """
    from artisan.errors import ArtisanError, ErrorCode

    if not config.delta_root:
        raise ArtisanError(
            ErrorCode.DELTA_ROOT_UNSET,
            "no Delta root configured",
            error_type="config",
            hint="set ARTISAN_DELTA_ROOT or pass --delta-root",
            recovery_hint="CHECK_INPUT",
        )
    return config.delta_root
