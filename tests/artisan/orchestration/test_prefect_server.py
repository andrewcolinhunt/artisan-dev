"""Unit tests for prefect server discovery module."""

from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import patch

import pytest

from artisan.orchestration.prefect_server import (
    PrefectServerInfo,
    PrefectServerNotFound,
    PrefectServerUnreachable,
    _normalize_url,
    _validate_health,
    activate_server,
    discover_server,
)

# =============================================================================
# _normalize_url
# =============================================================================


class TestNormalizeUrl:
    def test_bare_host_port(self) -> None:
        assert _normalize_url("http://host:4200") == "http://host:4200/api"

    def test_already_has_api(self) -> None:
        assert _normalize_url("http://host:4200/api") == "http://host:4200/api"

    def test_trailing_slash(self) -> None:
        assert _normalize_url("http://host:4200/") == "http://host:4200/api"

    def test_trailing_slash_with_api(self) -> None:
        assert _normalize_url("http://host:4200/api/") == "http://host:4200/api"


# =============================================================================
# discover_server
# =============================================================================


@pytest.fixture(autouse=True)
def _no_health_check():
    """Disable health check for all discover_server tests in this module."""
    with patch("artisan.orchestration.prefect_server.health_check", return_value=True):
        yield


@pytest.fixture(autouse=True)
def _isolate_discovery(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Prevent real env vars and discovery files from leaking into tests."""
    monkeypatch.delenv("PREFECT_SUBMITIT_SERVER", raising=False)
    monkeypatch.delenv("PREFECT_API_URL", raising=False)
    monkeypatch.delenv("ARTISAN_PREFECT_SERVER", raising=False)
    monkeypatch.setattr(
        "prefect_submitit.server.discovery.DEFAULT_DATA_DIR",
        tmp_path / "no-discovery",
    )


class TestDiscoverServerExplicit:
    def test_explicit_argument(self) -> None:
        info = discover_server(prefect_server="http://myhost:4200/api")
        assert info.url == "http://myhost:4200/api"
        assert info.source == "argument"

    def test_explicit_argument_normalized(self) -> None:
        info = discover_server(prefect_server="http://myhost:4200")
        assert info.url == "http://myhost:4200/api"


class TestDiscoverServerEnv:
    def test_submitit_env_var(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("PREFECT_SUBMITIT_SERVER", "http://envhost:4200/api")
        monkeypatch.delenv("PREFECT_API_URL", raising=False)
        info = discover_server()
        assert info.url == "http://envhost:4200/api"
        assert info.source == "env:PREFECT_SUBMITIT_SERVER"

    def test_prefect_env_var(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("PREFECT_SUBMITIT_SERVER", raising=False)
        monkeypatch.setenv("PREFECT_API_URL", "http://prefecthost:4200/api")
        info = discover_server()
        assert info.url == "http://prefecthost:4200/api"
        assert info.source == "env:PREFECT_API_URL"


class TestDiscoverServerFile:
    def test_discovery_file(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        monkeypatch.delenv("PREFECT_SUBMITIT_SERVER", raising=False)
        monkeypatch.delenv("PREFECT_API_URL", raising=False)

        discovery_dir = tmp_path / "data"
        discovery_dir.mkdir()
        discovery = discovery_dir / "server.json"
        discovery.write_text('{"url": "http://filehost:4217/api"}')
        monkeypatch.setattr(
            "prefect_submitit.server.discovery.DEFAULT_DATA_DIR", discovery_dir
        )

        info = discover_server()
        assert info.url == "http://filehost:4217/api"
        assert info.source == "discovery_file"


class TestDiscoverServerNotFound:
    def test_not_found(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        monkeypatch.delenv("PREFECT_SUBMITIT_SERVER", raising=False)
        monkeypatch.delenv("PREFECT_API_URL", raising=False)
        monkeypatch.setattr(
            "prefect_submitit.server.discovery.DEFAULT_DATA_DIR",
            tmp_path / "nonexistent",
        )
        with pytest.raises(PrefectServerNotFound):
            discover_server()


class TestDiscoverServerPriority:
    def test_explicit_over_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("PREFECT_SUBMITIT_SERVER", "http://envhost:4200/api")
        monkeypatch.setenv("PREFECT_API_URL", "http://prefecthost:4200/api")
        info = discover_server(prefect_server="http://explicit:4200/api")
        assert info.source == "argument"

    def test_submitit_env_over_prefect_env(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("PREFECT_SUBMITIT_SERVER", "http://submitit:4200/api")
        monkeypatch.setenv("PREFECT_API_URL", "http://prefect:4200/api")
        info = discover_server()
        assert info.source == "env:PREFECT_SUBMITIT_SERVER"


class TestDeprecationWarning:
    def test_old_env_var_warns(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("ARTISAN_PREFECT_SERVER", "http://old:4200/api")
        monkeypatch.delenv("PREFECT_SUBMITIT_SERVER", raising=False)
        monkeypatch.delenv("PREFECT_API_URL", raising=False)
        # Old var is not recognized for resolution, so this should warn AND
        # raise PrefectServerNotFound (nothing else to resolve from).
        # Patch DEFAULT_DATA_DIR to ensure no discovery file is found.
        monkeypatch.setattr(
            "prefect_submitit.server.discovery.DEFAULT_DATA_DIR",
            Path("/nonexistent"),
        )
        with pytest.warns(DeprecationWarning, match="ARTISAN_PREFECT_SERVER"):
            with pytest.raises(PrefectServerNotFound):
                discover_server()

    def test_old_env_var_no_warn_if_new_set(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("ARTISAN_PREFECT_SERVER", "http://old:4200/api")
        monkeypatch.setenv("PREFECT_SUBMITIT_SERVER", "http://new:4200/api")
        import warnings

        with warnings.catch_warnings():
            warnings.simplefilter("error", DeprecationWarning)
            info = discover_server()
        assert info.url == "http://new:4200/api"


# =============================================================================
# _validate_health
# =============================================================================


class TestValidateHealth:
    @pytest.fixture(autouse=True)
    def _allow_real_health_check(self):
        """Override module-level mock to allow real health check in this class."""
        with patch(
            "artisan.orchestration.prefect_server.health_check",
            return_value=False,
        ):
            yield

    def test_unreachable_raises(self) -> None:
        info = PrefectServerInfo(
            url="http://unreachable-host-12345:4200/api", source="argument"
        )
        with pytest.raises(PrefectServerUnreachable, match="not reachable"):
            _validate_health(info)


# =============================================================================
# activate_server
# =============================================================================


class TestActivateServer:
    def test_sets_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("PREFECT_API_URL", raising=False)
        info = PrefectServerInfo(url="http://myhost:4200/api", source="argument")
        activate_server(info)
        assert os.environ["PREFECT_API_URL"] == "http://myhost:4200/api"

    @pytest.fixture(autouse=True)
    def _allow_real_activate(self):
        """Override conftest mock to allow real activate_server in this class."""
        with patch(
            "artisan.orchestration.prefect_server.activate_server",
            wraps=activate_server,
        ):
            yield

    def test_overrides_prefect_cached_settings(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """activate_server should update Prefect's cached SettingsContext.

        When Prefect is imported before activate_server runs, the profile
        settings are cached and os.environ changes alone won't take effect.
        """
        from prefect.context import SettingsContext
        from prefect.settings import PREFECT_API_URL

        # Push a stale settings context to simulate cached profile settings
        current_ctx = SettingsContext.get()
        stale_settings = current_ctx.settings.copy_with_update(
            updates={PREFECT_API_URL: "http://stale:4200/api"}
        )
        stale_ctx = SettingsContext(
            profile=current_ctx.profile, settings=stale_settings
        )
        stale_ctx.__enter__()
        assert PREFECT_API_URL.value() == "http://stale:4200/api"

        # activate_server should override the cached setting
        info = PrefectServerInfo(url="http://correct:4200/api", source="test")
        activate_server(info)
        assert PREFECT_API_URL.value() == "http://correct:4200/api"
        assert os.environ["PREFECT_API_URL"] == "http://correct:4200/api"
