"""Tests for retired legacy utility imports."""

from __future__ import annotations

import importlib

import pytest


def test_get_project_root_removed_from_util_surface():
    """The retired helper is no longer importable from artisan.utils."""
    from artisan import utils

    assert not hasattr(utils, "get_project_root")


def test_root_utils_module_removed():
    """Legacy root_utils module has been deleted."""
    with pytest.raises(ModuleNotFoundError):
        importlib.import_module("artisan.utils.root_utils")
