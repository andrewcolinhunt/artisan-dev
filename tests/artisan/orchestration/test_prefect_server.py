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
    PrefectVersionMismatch,
    _build_unreachable_message,
    _check_version_compatibility,
    _get_server_version,
    _is_cloud_url,
    _normalize_url,
    _pid_alive,
    _try_localhost_fallback,
    _validate_health,
    _versions_compatible,
    activate_server,
    discover_server,
)

# =============================================================================
# _is_cloud_url
# =============================================================================


class TestIsCloudUrl:
    def test_cloud_url(self) -> None:
        assert _is_cloud_url("https://api.prefect.cloud/api/accounts/x/workspaces/y")

    def test_self_hosted_url(self) -> None:
        assert not _is_cloud_url("http://host:4200/api")

    def test_localhost(self) -> None:
        assert not _is_cloud_url("http://localhost:4200/api")


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

    def test_cloud_url_unchanged(self) -> None:
        cloud = "https://api.prefect.cloud/api/accounts/x/workspaces/y"
        assert _normalize_url(cloud) == cloud

    def test_cloud_url_trailing_slash(self) -> None:
        assert (
            _normalize_url("https://api.prefect.cloud/api/accounts/x/workspaces/y/")
            == "https://api.prefect.cloud/api/accounts/x/workspaces/y"
        )

    def test_url_with_api_in_path(self) -> None:
        url = "http://host:4200/api/v2/something"
        assert _normalize_url(url) == url


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
        with patch(
            "artisan.orchestration.prefect_server._resolve_from_prefect_settings",
            return_value=None,
        ):
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


CLOUD_URL = "https://api.prefect.cloud/api/accounts/abc/workspaces/xyz"


class TestDiscoverServerCloud:
    def test_cloud_via_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("PREFECT_API_URL", CLOUD_URL)
        info = discover_server()
        assert info.url == CLOUD_URL
        assert info.source == "env:PREFECT_API_URL"

    def test_cloud_via_prefect_profile(self) -> None:
        with patch(
            "artisan.orchestration.prefect_server._resolve_from_prefect_settings",
            return_value=CLOUD_URL,
        ):
            info = discover_server()
        assert info.url == CLOUD_URL
        assert info.source == "prefect_profile"

    def test_cloud_skips_health_check(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("PREFECT_API_URL", CLOUD_URL)
        with patch("artisan.orchestration.prefect_server.health_check") as mock_hc:
            discover_server()
        mock_hc.assert_not_called()


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
        with patch(
            "artisan.orchestration.prefect_server._resolve_from_prefect_settings",
            return_value=None,
        ):
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
# _pid_alive
# =============================================================================


class TestPidAlive:
    def test_current_process_alive(self) -> None:
        assert _pid_alive(os.getpid())

    def test_nonexistent_pid(self) -> None:
        assert not _pid_alive(99999999)


# =============================================================================
# _build_unreachable_message
# =============================================================================


class TestBuildUnreachableMessage:
    def test_includes_url_and_source(self) -> None:
        info = PrefectServerInfo(url="http://host:4200/api", source="argument")
        with patch(
            "artisan.orchestration.prefect_server._read_discovery_file",
            return_value=None,
        ):
            msg = _build_unreachable_message(info)
        assert "http://host:4200/api" in msg
        assert "argument" in msg

    def test_without_discovery_file(self) -> None:
        info = PrefectServerInfo(url="http://host:4200/api", source="argument")
        with patch(
            "artisan.orchestration.prefect_server._read_discovery_file",
            return_value=None,
        ):
            msg = _build_unreachable_message(info)
        assert "No discovery file found" in msg
        assert "pixi run prefect-start" in msg

    def test_different_host(self) -> None:
        info = PrefectServerInfo(url="http://host:4200/api", source="discovery_file")
        disco = {
            "pid": 123,
            "host": "remote-node.cluster",
            "started": "2026-03-29T10:00:00Z",
        }
        with (
            patch(
                "artisan.orchestration.prefect_server._read_discovery_file",
                return_value=disco,
            ),
            patch(
                "artisan.orchestration.prefect_server.socket.getfqdn",
                return_value="local-node.cluster",
            ),
        ):
            msg = _build_unreachable_message(info)
        assert "remote-node.cluster" in msg
        assert "local-node.cluster" in msg
        assert "still running on that node" in msg

    def test_same_host_stale_pid(self) -> None:
        info = PrefectServerInfo(url="http://host:4200/api", source="discovery_file")
        disco = {
            "pid": 99999999,
            "host": "this-node.cluster",
            "started": "2026-03-29T10:00:00Z",
        }
        with (
            patch(
                "artisan.orchestration.prefect_server._read_discovery_file",
                return_value=disco,
            ),
            patch(
                "artisan.orchestration.prefect_server.socket.getfqdn",
                return_value="this-node.cluster",
            ),
        ):
            msg = _build_unreachable_message(info)
        assert "no longer running" in msg
        assert "pixi run prefect-start" in msg

    def test_same_host_alive_pid(self) -> None:
        info = PrefectServerInfo(url="http://host:4200/api", source="discovery_file")
        disco = {
            "pid": os.getpid(),
            "host": "this-node.cluster",
            "started": "2026-03-29T10:00:00Z",
        }
        with (
            patch(
                "artisan.orchestration.prefect_server._read_discovery_file",
                return_value=disco,
            ),
            patch(
                "artisan.orchestration.prefect_server.socket.getfqdn",
                return_value="this-node.cluster",
            ),
        ):
            msg = _build_unreachable_message(info)
        assert "alive but not responding" in msg
        assert "different environment" in msg

    def test_minimal_discovery_file_missing_keys(self) -> None:
        info = PrefectServerInfo(url="http://host:4200/api", source="discovery_file")
        with patch(
            "artisan.orchestration.prefect_server._read_discovery_file",
            return_value={"url": "http://host:4200/api"},
        ):
            msg = _build_unreachable_message(info)
        assert "No discovery file found" in msg


# =============================================================================
# _try_localhost_fallback
# =============================================================================


class TestTryLocalhostFallback:
    def test_already_localhost_returns_none(self) -> None:
        info = PrefectServerInfo(url="http://localhost:4200/api", source="argument")
        assert _try_localhost_fallback(info) is None

    def test_already_127_returns_none(self) -> None:
        info = PrefectServerInfo(url="http://127.0.0.1:4200/api", source="argument")
        assert _try_localhost_fallback(info) is None

    def test_fallback_succeeds(self) -> None:
        info = PrefectServerInfo(
            url="http://remote-host:4200/api", source="discovery_file"
        )
        with patch(
            "artisan.orchestration.prefect_server.health_check",
            return_value=True,
        ):
            result = _try_localhost_fallback(info)
        assert result is not None
        assert result.url == "http://localhost:4200/api"
        assert result.source == "discovery_file->localhost"

    def test_fallback_fails_returns_none(self) -> None:
        info = PrefectServerInfo(
            url="http://remote-host:4200/api", source="discovery_file"
        )
        with patch(
            "artisan.orchestration.prefect_server.health_check",
            return_value=False,
        ):
            result = _try_localhost_fallback(info)
        assert result is None

    def test_preserves_port_and_path(self) -> None:
        info = PrefectServerInfo(
            url="http://remote-host:9999/api", source="env:PREFECT_API_URL"
        )
        with patch(
            "artisan.orchestration.prefect_server.health_check",
            return_value=True,
        ):
            result = _try_localhost_fallback(info)
        assert result is not None
        assert result.url == "http://localhost:9999/api"


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
        with (
            patch(
                "artisan.orchestration.prefect_server._read_discovery_file",
                return_value=None,
            ),
            pytest.raises(PrefectServerUnreachable, match="not reachable"),
        ):
            _validate_health(info)

    def test_returns_info_on_healthy(self) -> None:
        info = PrefectServerInfo(url="http://host:4200/api", source="argument")
        with patch(
            "artisan.orchestration.prefect_server.health_check",
            return_value=True,
        ):
            result = _validate_health(info)
        assert result is info

    def test_cloud_skips_check(self) -> None:
        info = PrefectServerInfo(
            url="https://api.prefect.cloud/api/accounts/x/workspaces/y",
            source="env:PREFECT_API_URL",
        )
        with patch("artisan.orchestration.prefect_server.health_check") as mock_hc:
            result = _validate_health(info)
        mock_hc.assert_not_called()
        assert result is info

    def test_returns_fallback_on_hostname_fail(self) -> None:
        info = PrefectServerInfo(
            url="http://remote-host:4200/api", source="discovery_file"
        )
        with patch(
            "artisan.orchestration.prefect_server.health_check",
            side_effect=[False, True],
        ):
            result = _validate_health(info)
        assert result.url == "http://localhost:4200/api"
        assert "localhost" in result.source


# =============================================================================
# _get_server_version
# =============================================================================


class TestGetServerVersion:
    def test_returns_version_string(self) -> None:
        with patch(
            "artisan.orchestration.prefect_server.urllib.request.urlopen"
        ) as mock_urlopen:
            mock_urlopen.return_value.__enter__ = lambda s: s
            mock_urlopen.return_value.__exit__ = lambda s, *a: None
            mock_urlopen.return_value.read.return_value = b'"3.6.13"'
            result = _get_server_version("http://host:4200/api")
        assert result == "3.6.13"

    def test_returns_none_on_error(self) -> None:
        with patch(
            "artisan.orchestration.prefect_server.urllib.request.urlopen",
            side_effect=OSError("connection refused"),
        ):
            assert _get_server_version("http://host:4200/api") is None

    def test_strips_whitespace_and_quotes(self) -> None:
        with patch(
            "artisan.orchestration.prefect_server.urllib.request.urlopen"
        ) as mock_urlopen:
            mock_urlopen.return_value.__enter__ = lambda s: s
            mock_urlopen.return_value.__exit__ = lambda s, *a: None
            mock_urlopen.return_value.read.return_value = b'  "3.6.13"\n  '
            result = _get_server_version("http://host:4200/api")
        assert result == "3.6.13"


# =============================================================================
# _versions_compatible
# =============================================================================


class TestVersionsCompatible:
    def test_same_version(self) -> None:
        assert _versions_compatible("3.6.13", "3.6.13")

    def test_same_major_minor(self) -> None:
        assert _versions_compatible("3.6.13", "3.6.1")

    def test_different_minor(self) -> None:
        assert not _versions_compatible("3.6.13", "3.7.0")

    def test_different_major(self) -> None:
        assert not _versions_compatible("3.6.13", "4.0.0")


# =============================================================================
# _check_version_compatibility
# =============================================================================


class TestCheckVersionCompatibility:
    def test_compatible_passes(self) -> None:
        info = PrefectServerInfo(url="http://host:4200/api", source="argument")
        with (
            patch(
                "artisan.orchestration.prefect_server._get_server_version",
                return_value="3.6.13",
            ),
            patch("prefect.__version__", "3.6.13"),
        ):
            _check_version_compatibility(info)

    def test_incompatible_raises(self) -> None:
        info = PrefectServerInfo(url="http://host:4200/api", source="argument")
        with (
            patch(
                "artisan.orchestration.prefect_server._get_server_version",
                return_value="3.5.0",
            ),
            patch("prefect.__version__", "3.6.13"),
        ):
            with pytest.raises(PrefectVersionMismatch, match="mismatch"):
                _check_version_compatibility(info)

    def test_server_unreachable_warns(self) -> None:
        info = PrefectServerInfo(url="http://host:4200/api", source="argument")
        with patch(
            "artisan.orchestration.prefect_server._get_server_version",
            return_value=None,
        ):
            _check_version_compatibility(info)

    def test_cloud_skips_check(self) -> None:
        info = PrefectServerInfo(
            url="https://api.prefect.cloud/api/accounts/x/workspaces/y",
            source="env:PREFECT_API_URL",
        )
        with patch(
            "artisan.orchestration.prefect_server._get_server_version"
        ) as mock_ver:
            _check_version_compatibility(info)
        mock_ver.assert_not_called()


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
        from prefect.settings import get_current_settings

        # Push a stale settings context to simulate cached profile settings
        current_ctx = SettingsContext.get()
        stale_api = current_ctx.settings.api.model_copy(
            update={"url": "http://stale:4200/api"}
        )
        stale_settings = current_ctx.settings.model_copy(update={"api": stale_api})
        stale_ctx = SettingsContext(
            profile=current_ctx.profile, settings=stale_settings
        )
        stale_ctx.__enter__()
        assert str(get_current_settings().api.url) == "http://stale:4200/api"

        # activate_server should override the cached setting
        info = PrefectServerInfo(url="http://correct:4200/api", source="test")
        activate_server(info)
        assert str(get_current_settings().api.url) == "http://correct:4200/api"
        assert os.environ["PREFECT_API_URL"] == "http://correct:4200/api"
