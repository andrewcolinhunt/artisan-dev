"""Tests for multiprocessing spawn utilities."""

from __future__ import annotations

import sys
import threading

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

        with pytest.raises(ValueError, match="boom"), suppress_main_reimport():
            msg = "boom"
            raise ValueError(msg)

        assert main_mod.__file__ == original

    def test_overlapping_contexts_restore_after_last_exit(self) -> None:
        """One router exiting must not restore while another remains active."""
        main_mod = sys.modules["__main__"]
        original = main_mod.__file__
        first_entered = threading.Event()
        second_entered = threading.Event()
        first_exited = threading.Event()
        observations: list[str | None] = []
        synchronization_errors: list[str] = []

        def _first_context() -> None:
            with suppress_main_reimport():
                first_entered.set()
                if not second_entered.wait(timeout=2):
                    synchronization_errors.append("second context did not enter")
            first_exited.set()

        def _second_context() -> None:
            if not first_entered.wait(timeout=2):
                synchronization_errors.append("first context did not enter")
                return
            with suppress_main_reimport():
                second_entered.set()
                if not first_exited.wait(timeout=2):
                    synchronization_errors.append("first context did not exit")
                observations.append(main_mod.__file__)

        first = threading.Thread(target=_first_context)
        second = threading.Thread(target=_second_context)
        first.start()
        second.start()
        first.join(timeout=2)
        second.join(timeout=2)

        assert not first.is_alive()
        assert not second.is_alive()
        assert synchronization_errors == []
        assert observations == [None]
        assert main_mod.__file__ == original
