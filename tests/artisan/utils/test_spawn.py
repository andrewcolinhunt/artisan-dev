"""Tests for multiprocessing spawn utilities."""

from __future__ import annotations

import sys

import pytest

from artisan.utils.spawn import suppress_main_reimport


class TestSuppressMainReimport:
    """Tests for the suppress_main_reimport context manager."""

    def test_neuters_main_file(self) -> None:
        """__main__.__file__ is None inside the context."""
        main_mod = sys.modules["__main__"]
        original = main_mod.__file__

        with suppress_main_reimport():
            assert main_mod.__file__ is None

        assert main_mod.__file__ == original

    def test_restores_main_file(self) -> None:
        """Original __main__.__file__ is restored after exit."""
        main_mod = sys.modules["__main__"]
        original = main_mod.__file__

        guard = suppress_main_reimport()
        guard.__enter__()
        assert main_mod.__file__ is None

        guard.__exit__(None, None, None)
        assert main_mod.__file__ == original

    def test_restores_on_exception(self) -> None:
        """__main__.__file__ is restored even when the body raises."""
        main_mod = sys.modules["__main__"]
        original = main_mod.__file__

        with pytest.raises(ValueError, match="boom"):
            with suppress_main_reimport():
                raise ValueError("boom")

        assert main_mod.__file__ == original
