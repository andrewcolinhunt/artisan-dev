"""Fixtures for tests that ship work to real Modal."""

from __future__ import annotations

import os
from pathlib import Path

import pytest


@pytest.fixture(scope="session")
def modal_credentials() -> dict[str, str]:
    """Skip when no Modal credentials are configured.

    Modal SDK reads MODAL_TOKEN_ID/MODAL_TOKEN_SECRET from env (preferred for
    CI) or falls back to ~/.modal.toml (the file ``modal token new`` writes
    after the browser flow).
    """
    token_id = os.environ.get("MODAL_TOKEN_ID")
    token_secret = os.environ.get("MODAL_TOKEN_SECRET")
    if (
        not (token_id and token_secret)
        and not Path("~/.modal.toml").expanduser().exists()
    ):
        pytest.skip("MODAL_TOKEN_ID/MODAL_TOKEN_SECRET unset and no ~/.modal.toml")
    return {"token_id": token_id or "", "token_secret": token_secret or ""}
