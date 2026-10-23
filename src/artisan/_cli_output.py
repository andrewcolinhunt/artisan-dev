"""Shared stdout isolation for CLI response construction."""

from __future__ import annotations

import os
import sys
from collections.abc import Iterator
from contextlib import contextmanager, redirect_stdout


@contextmanager
def _diagnostics_to_stderr() -> Iterator[None]:
    """Keep Python and native application import chatter off machine stdout."""
    sys.stdout.flush()
    saved = os.dup(1)
    try:
        os.dup2(2, 1)
        with redirect_stdout(sys.stderr):
            try:
                yield
            finally:
                sys.stdout.flush()
    finally:
        os.dup2(saved, 1)
        os.close(saved)
