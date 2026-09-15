"""Tests for artisan.utils.tutorial."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

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


@pytest.mark.parametrize("name", ["", ".", "..", "../outside"])
def test_rejects_name_outside_runs_directory(tmp_path: Path, name: str) -> None:
    with pytest.raises(ValueError, match="must resolve beneath"):
        tutorial_setup(name, base_dir=tmp_path)


def test_rejects_absolute_name(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="must resolve beneath"):
        tutorial_setup(str(tmp_path / "outside"), base_dir=tmp_path)


def test_rejects_symlink_outside_runs_directory(tmp_path: Path) -> None:
    outside = tmp_path / "outside"
    outside.mkdir()
    runs_root = tmp_path / "runs"
    runs_root.mkdir()
    (runs_root / "linked").symlink_to(outside, target_is_directory=True)

    with pytest.raises(ValueError, match="must resolve beneath"):
        tutorial_setup("linked", base_dir=tmp_path)
