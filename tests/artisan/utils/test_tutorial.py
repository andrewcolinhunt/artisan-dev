"""Tests for artisan.utils.tutorial."""

from __future__ import annotations

import os
from pathlib import Path

from artisan.utils.tutorial import TutorialEnv, tutorial_setup


def test_creates_directories(tmp_path: Path) -> None:
    env = tutorial_setup("test_tut", base_dir=tmp_path)
    assert os.path.exists(env.delta_root)
    assert os.path.exists(env.staging_root)
    assert os.path.exists(env.working_root)


def test_cleans_existing(tmp_path: Path) -> None:
    runs_dir = tmp_path / "runs" / "test_tut"
    runs_dir.mkdir(parents=True)
    marker = runs_dir / "marker.txt"
    marker.write_text("old")

    tutorial_setup("test_tut", base_dir=tmp_path, clean=True)
    assert not marker.exists()


def test_no_clean_preserves_existing(tmp_path: Path) -> None:
    runs_dir = tmp_path / "runs" / "test_tut"
    runs_dir.mkdir(parents=True)
    marker = runs_dir / "marker.txt"
    marker.write_text("keep")

    tutorial_setup("test_tut", base_dir=tmp_path, clean=False)
    assert marker.exists()
    assert marker.read_text() == "keep"


def test_returns_correct_paths(tmp_path: Path) -> None:
    env = tutorial_setup("test_tut", base_dir=tmp_path)
    assert isinstance(env, TutorialEnv)
    assert env.runs_dir == os.path.join(str(tmp_path), "runs", "test_tut")
    assert env.delta_root == os.path.join(env.runs_dir, "delta")
    assert env.staging_root == os.path.join(env.runs_dir, "staging")
    assert env.working_root == os.path.join(env.runs_dir, "working")


def test_custom_base_dir(tmp_path: Path) -> None:
    custom = tmp_path / "custom_base"
    custom.mkdir()
    env = tutorial_setup("tut", base_dir=custom)
    assert env.runs_dir == os.path.join(str(custom), "runs", "tut")
