"""CLI validation for the read-only MCP server."""

from __future__ import annotations

import json
import sys
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from typer.testing import CliRunner

import artisan_mcp
from artisan_mcp import build_mcp_app
from artisan_mcp import cli as cli_mod
from artisan_mcp.cli import _build_app, main

runner = CliRunner()


@pytest.fixture
def app():
    return _build_app()


class TestCLI:
    def test_help_exposes_no_write_flag(self, app) -> None:
        result = runner.invoke(app, ["--help"])

        assert result.exit_code == 0
        assert "--write" not in result.stdout

    def test_invalid_transport_fails_at_cli_boundary(self, app) -> None:
        result = runner.invoke(app, ["--transport", "websocket"])

        assert result.exit_code == 2
        assert "Invalid value" in result.stdout

    def test_print_config_contains_only_live_settings(self, app) -> None:
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
    def test_passes_supported_transport_to_server(
        self, app, monkeypatch, transport
    ) -> None:
        seen: list[str] = []
        fake_app = SimpleNamespace(run=lambda *, transport: seen.append(transport))
        monkeypatch.setattr(
            "artisan_mcp.server.build_mcp_app", lambda _config: fake_app
        )

        result = runner.invoke(app, ["--transport", transport])

        assert result.exit_code == 0
        assert seen == [transport]


class TestOptionalDependencies:
    def test_importing_facade_does_not_import_optional_modules(self) -> None:
        code = """
import sys

class BlockOptional:
    def find_spec(self, fullname, path=None, target=None):
        root = fullname.partition('.')[0]
        if root in {'fastmcp', 'pydantic_settings', 'typer'}:
            raise AssertionError(f'optional import attempted: {fullname}')
        return None

sys.meta_path.insert(0, BlockOptional())
import artisan_mcp
assert callable(artisan_mcp.build_mcp_app)
assert 'artisan_mcp.server' not in sys.modules
assert 'artisan_mcp.cli' not in sys.modules
"""
        import subprocess

        subprocess.run([sys.executable, "-c", code], check=True)

    @pytest.mark.parametrize("missing_name", ["fastmcp", "pydantic_settings"])
    def test_build_wrapper_translates_only_missing_mcp_packages(
        self, missing_name: str
    ) -> None:
        missing = ModuleNotFoundError(
            f"No module named {missing_name!r}", name=missing_name
        )
        original_import = __import__

        def fail_server_import(name, *args, **kwargs):
            if name == "artisan_mcp.server":
                raise missing
            return original_import(name, *args, **kwargs)

        with (
            patch("builtins.__import__", side_effect=fail_server_import),
            pytest.raises(ImportError) as exc_info,
        ):
            build_mcp_app()

        assert str(exc_info.value) == artisan_mcp._MCP_EXTRA_MESSAGE
        assert exc_info.value.__cause__ is missing

    def test_build_wrapper_propagates_unrelated_import_failure(self) -> None:
        missing = ModuleNotFoundError(
            "No module named 'server_dependency'", name="server_dependency"
        )
        original_import = __import__

        def fail_server_import(name, *args, **kwargs):
            if name == "artisan_mcp.server":
                raise missing
            return original_import(name, *args, **kwargs)

        with (
            patch("builtins.__import__", side_effect=fail_server_import),
            pytest.raises(ModuleNotFoundError) as exc_info,
        ):
            build_mcp_app()

        assert exc_info.value is missing

    @pytest.mark.parametrize("missing_name", ["typer", "fastmcp", "pydantic_settings"])
    def test_cli_missing_extra_exits_once_without_traceback(
        self, monkeypatch, capsys, missing_name: str
    ) -> None:
        missing = ModuleNotFoundError(
            f"No module named {missing_name!r}", name=missing_name
        )
        monkeypatch.setattr(
            cli_mod, "_build_app", lambda: (_ for _ in ()).throw(missing)
        )

        with pytest.raises(SystemExit) as exc_info:
            main([])

        assert exc_info.value.code == 1
        captured = capsys.readouterr()
        assert captured.out == ""
        assert captured.err == artisan_mcp._MCP_EXTRA_MESSAGE + "\n"
        assert "Traceback" not in captured.err

    def test_cli_propagates_unrelated_import_failure(self, monkeypatch) -> None:
        missing = ModuleNotFoundError(
            "No module named 'server_dependency'", name="server_dependency"
        )
        monkeypatch.setattr(
            cli_mod, "_build_app", lambda: (_ for _ in ()).throw(missing)
        )

        with pytest.raises(ModuleNotFoundError) as exc_info:
            main([])

        assert exc_info.value is missing

    def test_cli_module_has_no_public_app(self) -> None:
        assert not hasattr(cli_mod, "app")
