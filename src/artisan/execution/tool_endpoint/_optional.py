"""Lazy optional-dependency loading for tool endpoints."""

from __future__ import annotations

from typing import Any

MODAL_EXTRA_MESSAGE = (
    "Modal support requires the 'modal' extra: pip install 'dexterity-artisan[modal]'"
)


def import_modal() -> Any:
    """Import Modal at its feature boundary with the install instruction."""
    try:
        import modal
    except ModuleNotFoundError as exc:
        if exc.name == "modal":
            raise ImportError(MODAL_EXTRA_MESSAGE) from exc
        raise
    return modal
