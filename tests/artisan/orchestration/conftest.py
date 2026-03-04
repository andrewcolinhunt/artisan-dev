"""Shared fixtures for orchestration unit tests."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture(autouse=True)
def _mock_prefect_discovery():
    """Auto-mock Prefect server discovery for all orchestration unit tests.

    PipelineManager.create() and resume() call discover_server() which requires
    a running Prefect server. Unit tests don't need a real server, so we mock
    both discover_server and activate_server.
    """
    fake_info = MagicMock(url="http://test:4200/api", source="test")
    with (
        patch(
            "artisan.orchestration.prefect_server.discover_server",
            return_value=fake_info,
        ),
        patch("artisan.orchestration.prefect_server.activate_server"),
    ):
        yield
