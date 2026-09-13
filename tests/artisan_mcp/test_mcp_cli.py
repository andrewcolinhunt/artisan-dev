"""CLI validation for the read-only MCP server."""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest
from typer.testing import CliRunner

from artisan_mcp.cli import app

runner = CliRunner()


class TestCLI:
    def test_help_exposes_no_write_flag(self) -> None:
        result = runner.invoke(app, ["--help"])

        assert result.exit_code == 0
        assert "--write" not in result.stdout

    def test_invalid_transport_fails_at_cli_boundary(self) -> None:
        result = runner.invoke(app, ["--transport", "websocket"])

        assert result.exit_code == 2
        assert "Invalid value" in result.stdout

    def test_print_config_contains_only_live_settings(self) -> None:
        result = runner.invoke(
            app,
            ["--delta-root", "/tmp/example", "--load", "example.ops", "--print-config"],
        )

        assert result.exit_code == 0
        assert json.loads(result.stdout) == {
            "delta_root": "/tmp/example",
            "load_modules": ["example.ops"],
        }

    @pytest.mark.parametrize("transport", ["stdio", "http"])
    def test_passes_supported_transport_to_server(self, monkeypatch, transport) -> None:
        seen: list[str] = []
        fake_app = SimpleNamespace(run=lambda *, transport: seen.append(transport))
        monkeypatch.setattr(
            "artisan_mcp.server.build_mcp_app", lambda _config: fake_app
        )

        result = runner.invoke(app, ["--transport", transport])

        assert result.exit_code == 0
        assert seen == [transport]
