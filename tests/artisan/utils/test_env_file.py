"""Tests for env_or_dotenv discovery."""

from __future__ import annotations

import os
from pathlib import Path

from artisan.utils.env_file import env_or_dotenv


def test_env_var_wins_over_file(tmp_path: Path, monkeypatch):
    (tmp_path / ".env").write_text("MY_KEY=from-file\n")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("MY_KEY", "from-env")
    assert env_or_dotenv("MY_KEY") == "from-env"


def test_file_found_from_nested_cwd(tmp_path: Path, monkeypatch):
    (tmp_path / ".env").write_text("MY_KEY=from-file\n")
    nested = tmp_path / "a" / "b"
    nested.mkdir(parents=True)
    monkeypatch.chdir(nested)
    monkeypatch.delenv("MY_KEY", raising=False)
    assert env_or_dotenv("MY_KEY") == "from-file"


def test_nearer_file_shadows_outer(tmp_path: Path, monkeypatch):
    (tmp_path / ".env").write_text("MY_KEY=outer\n")
    inner = tmp_path / "inner"
    inner.mkdir()
    (inner / ".env").write_text("MY_KEY=inner\n")
    monkeypatch.chdir(inner)
    monkeypatch.delenv("MY_KEY", raising=False)
    assert env_or_dotenv("MY_KEY") == "inner"


def test_missing_key_returns_none(tmp_path: Path, monkeypatch):
    (tmp_path / ".env").write_text("OTHER=x\n")
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("MY_KEY", raising=False)
    assert env_or_dotenv("MY_KEY") is None


def test_no_file_returns_none(tmp_path: Path, monkeypatch):
    # pytest tmp dirs live under the system temp tree, which has no .env
    # on its ancestor path (and the key name is never set anywhere)
    nested = tmp_path / "isolated"
    nested.mkdir()
    monkeypatch.chdir(nested)
    monkeypatch.delenv("DEFINITELY_NOT_SET_ANYWHERE_123", raising=False)
    assert env_or_dotenv("DEFINITELY_NOT_SET_ANYWHERE_123") is None


def test_does_not_mutate_environ(tmp_path: Path, monkeypatch):
    (tmp_path / ".env").write_text("MY_KEY=from-file\nSTRAY=leak\n")
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("MY_KEY", raising=False)
    env_or_dotenv("MY_KEY")
    assert "MY_KEY" not in os.environ
    assert "STRAY" not in os.environ


def test_quoted_values_parse(tmp_path: Path, monkeypatch):
    (tmp_path / ".env").write_text('MY_KEY="quoted value"  # comment\n')
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("MY_KEY", raising=False)
    assert env_or_dotenv("MY_KEY") == "quoted value"
