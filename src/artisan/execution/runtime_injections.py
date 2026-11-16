"""Scope literal runtime values to one sequential native invocation."""

from __future__ import annotations

import os
from collections.abc import Iterator, Mapping
from contextlib import contextmanager


@contextmanager
def runtime_injections(environment: Mapping[str, str]) -> Iterator[None]:
    """Scope selected native-phase values to one sequential worker assignment."""
    previous = {key: os.environ.get(key) for key in environment}
    os.environ.update(environment)
    try:
        yield
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
