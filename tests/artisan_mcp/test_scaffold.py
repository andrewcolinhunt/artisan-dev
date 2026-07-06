"""Scaffold smoke tests: config resolution and app construction."""

from __future__ import annotations

from fastmcp import FastMCP

from artisan_mcp import build_mcp_app
from artisan_mcp.config import ArtisanMCPConfig


class TestConfig:
    def test_defaults_are_read_only(self, monkeypatch) -> None:
        monkeypatch.delenv("ARTISAN_WRITE", raising=False)
        monkeypatch.delenv("ARTISAN_DELTA_ROOT", raising=False)
        monkeypatch.delenv("ARTISAN_LOAD_MODULES", raising=False)
        cfg = ArtisanMCPConfig()
        assert cfg.write_enabled is False
        assert cfg.delta_root is None
        assert cfg.load_modules == []
        assert cfg.log_level == "INFO"

    def test_env_aliases_resolve(self, monkeypatch) -> None:
        monkeypatch.setenv("ARTISAN_WRITE", "true")
        monkeypatch.setenv("ARTISAN_DELTA_ROOT", "/tmp/delta")
        monkeypatch.setenv("ARTISAN_LOAD_MODULES", "foo.bar, baz.qux")
        cfg = ArtisanMCPConfig()
        assert cfg.write_enabled is True
        assert cfg.delta_root == "/tmp/delta"
        assert cfg.load_modules == ["foo.bar", "baz.qux"]


class TestBuildApp:
    def test_returns_fastmcp_named_artisan(self, monkeypatch) -> None:
        monkeypatch.delenv("ARTISAN_WRITE", raising=False)
        app = build_mcp_app(ArtisanMCPConfig())
        assert isinstance(app, FastMCP)
        assert app.name == "artisan"
