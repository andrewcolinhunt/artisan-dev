"""Tests for configure_logging utility."""

from __future__ import annotations

import logging
import sys

import pytest

from artisan.utils.logging import _NOISY_LOGGERS, _ConsoleHandler, configure_logging


@pytest.fixture(autouse=True)
def _reset_loggers():
    """Reset the artisan logger and noisy loggers after each test."""
    yield

    artisan_logger = logging.getLogger("artisan")
    artisan_logger.handlers.clear()
    artisan_logger.setLevel(logging.WARNING)
    artisan_logger.propagate = True

    for name in _NOISY_LOGGERS:
        logging.getLogger(name).setLevel(logging.WARNING)


def test_configure_logging_sets_level():
    """Artisan logger should be set to the requested level."""
    configure_logging(level="DEBUG", suppress_noise=False)
    assert logging.getLogger("artisan").level == logging.DEBUG


def test_configure_logging_default_level():
    """Default level should be INFO."""
    configure_logging(suppress_noise=False)
    assert logging.getLogger("artisan").level == logging.INFO


def test_configure_logging_adds_console_handler():
    """Handler should be a _ConsoleHandler with Rich console writing to stdout."""
    configure_logging(suppress_noise=False)
    handlers = logging.getLogger("artisan").handlers
    assert len(handlers) == 1
    assert isinstance(handlers[0], _ConsoleHandler)
    assert handlers[0].console.file is sys.stdout


def test_configure_logging_idempotent():
    """Calling twice should not add a second handler."""
    configure_logging(suppress_noise=False)
    configure_logging(suppress_noise=False)
    assert len(logging.getLogger("artisan").handlers) == 1


def test_configure_logging_suppresses_noise():
    """All noisy loggers should be set to CRITICAL."""
    configure_logging(suppress_noise=True)
    for name in _NOISY_LOGGERS:
        assert logging.getLogger(name).level == logging.CRITICAL


def test_configure_logging_no_suppress():
    """With suppress_noise=False, noisy loggers should be untouched."""
    for name in _NOISY_LOGGERS:
        logging.getLogger(name).setLevel(logging.INFO)

    configure_logging(suppress_noise=False)

    for name in _NOISY_LOGGERS:
        assert logging.getLogger(name).level == logging.INFO


def test_configure_logging_no_propagate():
    """Propagate should be False to prevent duplicate messages."""
    configure_logging(suppress_noise=False)
    assert logging.getLogger("artisan").propagate is False


def test_configure_logging_custom_loggers():
    """Should configure multiple logger hierarchies when specified."""
    configure_logging(suppress_noise=False, loggers=("artisan", "myapp"))

    artisan_logger = logging.getLogger("artisan")
    myapp_logger = logging.getLogger("myapp")

    assert artisan_logger.level == logging.INFO
    assert myapp_logger.level == logging.INFO
    assert len(artisan_logger.handlers) == 1
    assert len(myapp_logger.handlers) == 1

    # Cleanup
    myapp_logger.handlers.clear()
    myapp_logger.setLevel(logging.WARNING)
    myapp_logger.propagate = True
