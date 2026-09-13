"""Scaffold smoke tests: config resolution and app construction."""

from __future__ import annotations

from fastmcp import FastMCP

from artisan_mcp import build_mcp_app
from artisan_mcp.config import ArtisanMCPConfig


class TestConfig:
    def test_defaults(self, monkeypatch) -> None:
        monkeypatch.delenv("ARTISAN_DELTA_ROOT", raising=False)
        monkeypatch.delenv("ARTISAN_LOAD_MODULES", raising=False)
        cfg = ArtisanMCPConfig()
        assert cfg.delta_root is None
        assert cfg.load_modules == []

    def test_env_aliases_resolve(self, monkeypatch) -> None:
        monkeypatch.setenv("ARTISAN_DELTA_ROOT", "/tmp/delta")
        monkeypatch.setenv("ARTISAN_LOAD_MODULES", "foo.bar, baz.qux")
        cfg = ArtisanMCPConfig()
        assert cfg.delta_root == "/tmp/delta"
        assert cfg.load_modules == ["foo.bar", "baz.qux"]


class TestBuildApp:
    def test_returns_fastmcp_named_artisan(self, monkeypatch) -> None:
        app = build_mcp_app(ArtisanMCPConfig())
        assert isinstance(app, FastMCP)
        assert app.name == "artisan"
