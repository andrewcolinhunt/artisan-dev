"""Tests for configure_logging utility."""

from __future__ import annotations

import logging
import logging.handlers
import sys
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

import pytest

from artisan.utils.logging import (
    _NOISY_LOGGERS,
    _configure_default_logging,
    _ConsoleHandler,
    _RunLogSession,
    configure_logging,
)


@pytest.fixture(autouse=True)
def _reset_loggers():
    """Reset the artisan logger and noisy loggers after each test."""
    artisan_logger = logging.getLogger("artisan")
    original = (
        artisan_logger.handlers[:],
        artisan_logger.level,
        artisan_logger.propagate,
    )
    artisan_logger.handlers.clear()
    artisan_logger.setLevel(logging.NOTSET)
    yield
    for handler in artisan_logger.handlers[:]:
        handler.close()
    artisan_logger.handlers[:] = original[0]
    artisan_logger.setLevel(original[1])
    artisan_logger.propagate = original[2]
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
    """Disabling suppression sets noisy loggers to INFO."""
    for name in _NOISY_LOGGERS:
        logging.getLogger(name).setLevel(logging.ERROR)

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

    myapp_logger.handlers.clear()
    myapp_logger.setLevel(logging.WARNING)
    myapp_logger.propagate = True


def test_session_paths_are_unique_encoded_and_time_sortable(tmp_path: Path) -> None:
    with patch("artisan.utils.logging.datetime") as clock:
        clock.now.return_value = datetime(2026, 9, 19, tzinfo=UTC)
        first = _RunLogSession(str(tmp_path), "../../run")
        second = _RunLogSession(str(tmp_path), "../../run")
    try:
        assert first.path != second.path
        assert Path(first.path).is_absolute()
        assert Path(first.path).parent.name.startswith("20260919T000000000000Z_")
        assert Path(first.path).parent.name.endswith("_..%2F..%2Frun")
        assert Path(first.path).is_file()
    finally:
        first.close()
        second.close()


def test_sessions_capture_only_bound_context_and_reset_on_error(tmp_path: Path) -> None:
    configure_logging()
    log = logging.getLogger("artisan.test")
    first = _RunLogSession(str(tmp_path), "run")
    second = _RunLogSession(str(tmp_path), "run")
    try:
        with first.bind():
            log.info("first-before")
            with pytest.raises(ValueError), second.bind():
                log.info("second-only")
                msg = "expected"
                raise ValueError(msg)
            log.info("first-after")
        log.info("unbound")
        first.close()
        with second.bind():
            log.info("second-after-close")
    finally:
        first.close()
        second.close()
    assert "second" not in Path(first.path).read_text()
    assert "first" not in Path(second.path).read_text()
    assert "unbound" not in Path(first.path).read_text() + Path(second.path).read_text()
    assert "first-after" in Path(first.path).read_text()
    assert "second-after-close" in Path(second.path).read_text()


@pytest.mark.parametrize("level", [logging.WARNING, logging.DEBUG])
def test_automatic_logging_preserves_explicit_level_without_handlers(
    level: int,
) -> None:
    logging.getLogger("artisan").setLevel(level)
    _configure_default_logging()
    assert logging.getLogger("artisan").level == level
    assert len(logging.getLogger("artisan").handlers) == 1


def test_automatic_logging_defaults_unconfigured_logger_to_info() -> None:
    _configure_default_logging()
    assert logging.getLogger("artisan").level == logging.INFO


def test_later_debug_configuration_reaches_existing_session(tmp_path: Path) -> None:
    _configure_default_logging()
    session = _RunLogSession(str(tmp_path), "run")
    try:
        with session.bind():
            logging.getLogger("artisan.test").debug("hidden")
            configure_logging("DEBUG")
            logging.getLogger("artisan.test").debug("visible")
    finally:
        session.close()
    content = Path(session.path).read_text()
    assert "visible" in content
    assert "hidden" not in content


def test_rotation_and_stale_emit_cannot_touch_other_session(tmp_path: Path) -> None:
    configure_logging()
    first = _RunLogSession(str(tmp_path), "same-run")
    second = _RunLogSession(str(tmp_path), "same-run")
    first._handler.maxBytes = 100
    with first.bind():
        for index in range(10):
            logging.getLogger("artisan.test").info("rotate %d %s", index, "x" * 30)
    first.close()
    Path(first.path).unlink()
    first._handler.emit(
        logging.makeLogRecord({"msg": "stale", "levelno": logging.INFO})
    )
    assert not Path(first.path).exists()
    assert Path(first.path + ".1").exists()
    assert not Path(second.path + ".1").exists()
    assert second._handler in logging.getLogger("artisan").handlers
    second.close()


@pytest.mark.parametrize("failing_method", ["write", "flush", "close"])
def test_session_io_errors_are_best_effort_and_warn_once(
    tmp_path: Path, capsys, failing_method: str
) -> None:
    configure_logging()
    session = _RunLogSession(str(tmp_path), "run")
    stream = session._handler.stream

    class BrokenStream:
        def __getattr__(self, name):
            if name == failing_method:

                def fail(*args, **kwargs):
                    msg = "broken sink"
                    raise OSError(msg)

                return fail
            return getattr(stream, name)

    session._handler.stream = BrokenStream()
    try:
        with session.bind():
            logging.getLogger("artisan.test").info("first")
            logging.getLogger("artisan.test").info("second")
        session.close()
        session.close()
    finally:
        stream.close()
    assert session._handler not in logging.getLogger("artisan").handlers
    assert capsys.readouterr().err.count("pipeline log sink failed") == 1


def test_session_setup_error_closes_partially_initialized_handler(
    tmp_path, monkeypatch
):
    from artisan.utils.logging import _SessionFileHandler

    handlers = []
    original = _SessionFileHandler.close

    def close(handler):
        handlers.append(handler)
        original(handler)

    monkeypatch.setattr(_SessionFileHandler, "close", close)
    monkeypatch.setattr(
        _SessionFileHandler,
        "setFormatter",
        lambda *args: (_ for _ in ()).throw(OSError("formatter failed")),
    )
    with pytest.raises(OSError, match="formatter failed"):
        _RunLogSession(str(tmp_path), "broken")
    assert len(handlers) == 1
    assert handlers[0].stream is None
    assert handlers[0] not in logging.getLogger("artisan").handlers


def test_session_attach_error_closes_open_sink(tmp_path, monkeypatch):
    logger = logging.getLogger("artisan")
    handlers = []

    def fail(handler):
        handlers.append(handler)
        msg = "attach failed"
        raise OSError(msg)

    monkeypatch.setattr(logger, "addHandler", fail)
    with pytest.raises(OSError, match="attach failed"):
        _RunLogSession(str(tmp_path), "broken")
    assert handlers[0].stream is None
    assert handlers[0] not in logger.handlers
