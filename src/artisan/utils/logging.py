"""Logging configuration for pipeline execution.

Uses a Rich console handler that colorizes log levels and URLs via regex
highlighting while keeping output clean (no Rich chrome).
"""

from __future__ import annotations

import logging
import logging.handlers
import sys
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from contextvars import ContextVar
from datetime import UTC, datetime
from pathlib import Path
from typing import ClassVar
from urllib.parse import quote
from uuid import uuid4

from rich.console import Console
from rich.highlighter import RegexHighlighter
from rich.theme import Theme

_NOISY_LOGGERS = ("httpx", "httpcore", "asyncio")

_LOG_FORMAT = "%(asctime)s.%(msecs)03d | %(levelname)-7s | %(name)s - %(message)s"
_LOG_DATEFMT = "%H:%M:%S"


class _LogHighlighter(RegexHighlighter):
    """Highlight log levels and URLs in plain-text log lines."""

    base_style = "log."
    highlights: ClassVar[list[str]] = [
        r"(?P<debug_level>DEBUG)",
        r"(?P<info_level>INFO)",
        r"(?P<warning_level>WARNING)",
        r"(?P<error_level>ERROR)",
        r"(?P<critical_level>CRITICAL)",
        r"(?P<web_url>(https|http|ws|wss):\/\/[0-9a-zA-Z\$\-\_\+\!`\(\)\,\.\?\/\;\:\&\=\%\#]*)",
        r"(?P<local_url>(file):\/\/[0-9a-zA-Z\$\-\_\+\!`\(\)\,\.\?\/\;\:\&\=\%\#]*)",
        r"(?P<number>(?<!\w)\-?\d[\d,]*\.?\d*(?!\w))",
        r"(?P<uuid>[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})",
        r"(?P<quoted>'[^']*')",
    ]


_LOG_STYLES = {
    "log.info_level": "cyan",
    "log.warning_level": "yellow3",
    "log.error_level": "red3",
    "log.critical_level": "bright_red",
    "log.debug_level": "dim",
    "log.web_url": "bright_blue",
    "log.local_url": "bright_blue",
    "log.number": "bright_magenta",
    "log.uuid": "bright_cyan",
    "log.quoted": "bright_green",
}


class _ConsoleHandler(logging.StreamHandler):  # type: ignore[type-arg]  # StreamHandler generic only in typeshed; supports any stream-like object
    """StreamHandler that renders formatted log lines through Rich Console.

    A standard ``logging.Formatter`` produces the log line as plain text, then
    ``Console.print()`` colorizes it via ``RegexHighlighter``.
    """

    def __init__(self, stream: object = None) -> None:
        """Initialize with a Rich console bound to the output stream."""
        super().__init__(stream=stream)
        self.console = Console(
            highlighter=_LogHighlighter(),
            theme=Theme(_LOG_STYLES, inherit=False),
            file=self.stream,
            markup=False,
        )

    def emit(self, record: logging.LogRecord) -> None:
        """Format and print a log record through the Rich console."""
        try:
            message = self.format(record)
            self.console.print(message, soft_wrap=True)
        except RecursionError:
            raise
        except Exception:
            self.handleError(record)


def configure_logging(
    level: str = "INFO",
    suppress_noise: bool = True,
    loggers: tuple[str, ...] = ("artisan",),
) -> None:
    """Configure logging for artisan execution.

    Sets up named loggers with a :class:`_ConsoleHandler` that renders
    colored output via Rich.  Idempotent — safe to call multiple times
    (e.g. in Jupyter cells that are re-executed).

    Args:
        level: Log level for the configured loggers. Defaults to ``"INFO"``.
        suppress_noise: If True, suppress noisy third-party loggers such as
            HTTP client chatter.
        loggers: Root logger names to configure. Defaults to ``("artisan",)``.
    """
    for logger_name in loggers:
        logger = logging.getLogger(logger_name)
        logger.setLevel(getattr(logging, level.upper()))

        _install_console_handler(logger)

    if suppress_noise:
        for name in _NOISY_LOGGERS:
            logging.getLogger(name).setLevel(logging.CRITICAL)
    else:
        for name in _NOISY_LOGGERS:
            logging.getLogger(name).setLevel(logging.INFO)


def _install_console_handler(logger: logging.Logger) -> None:
    """Install the shared console format while preserving configured handlers."""
    if not logger.handlers:
        handler = _ConsoleHandler(stream=sys.stdout)
        handler.setFormatter(logging.Formatter(_LOG_FORMAT, datefmt=_LOG_DATEFMT))
        logger.addHandler(handler)
    logger.propagate = False


_LOG_SESSION: ContextVar[str | None] = ContextVar("artisan_log_session", default=None)


class _SessionFileHandler(logging.handlers.RotatingFileHandler):
    """An owned sink whose closed state prevents stale emits from reopening it."""

    def __init__(self, path: Path, session_id: str) -> None:
        self._session_id = session_id
        self._session_closed = False
        self._warned = False
        super().__init__(
            path, maxBytes=50 * 1024 * 1024, backupCount=3, encoding="utf-8"
        )
        try:
            self.setLevel(logging.DEBUG)
            self.setFormatter(logging.Formatter(_LOG_FORMAT, datefmt=_LOG_DATEFMT))
        except BaseException:
            self.close()
            raise

    def filter(self, record: logging.LogRecord) -> bool | logging.LogRecord:
        """Accept only the owning context, without retaining a manager."""
        return _LOG_SESSION.get() == self._session_id and super().filter(record)

    def emit(self, record: logging.LogRecord) -> None:
        """Serialize emission with close, including direct stale handler calls."""
        self.acquire()
        try:
            if not self._session_closed:
                super().emit(record)
        finally:
            self.release()

    def handleError(self, record: logging.LogRecord) -> None:
        """Report sink failure once without recursively entering Artisan logging."""
        self._warn_once()

    def _warn_once(self) -> None:
        if not self._warned:
            self._warned = True
            with suppress(Exception):
                sys.stderr.write(
                    "Artisan pipeline log sink failed; execution continues.\n"
                )

    def flush(self) -> None:
        """Keep file I/O failures from escaping normal execution or shutdown."""
        try:
            super().flush()
        except Exception:
            self._warn_once()

    def close(self) -> None:
        """Close at most once, even when stream cleanup fails."""
        self.acquire()
        try:
            if self._session_closed:
                return
            self._session_closed = True
            try:
                super().close()
            except Exception:
                self._warn_once()
                self.stream = None  # type: ignore[assignment]  # FileHandler clears its stream on close.
                logging.Handler.close(self)
        finally:
            self.release()


class _RunLogSession:
    """Own one unique local pipeline file and its context-scoped lifetime."""

    def __init__(self, logs_root: str, pipeline_run_id: str) -> None:
        self.session_id = str(uuid4())
        prefix = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
        directory = (
            Path(logs_root).absolute()
            / "runs"
            / (f"{prefix}_{self.session_id}_{quote(pipeline_run_id, safe='')}")
        )
        directory.mkdir(parents=True, exist_ok=False)
        self.path = str(directory / "pipeline.log")
        self._handler = _SessionFileHandler(Path(self.path), self.session_id)
        try:
            logging.getLogger("artisan").addHandler(self._handler)
        except BaseException:
            self.close()
            raise

    @contextmanager
    def bind(self) -> Iterator[None]:
        """Bind only this call boundary and restore the previous nested context."""
        token = _LOG_SESSION.set(self.session_id)
        try:
            yield
        finally:
            _LOG_SESSION.reset(token)

    def close(self) -> None:
        """Detach before taking the emit lock, preserving all other handlers."""
        logging.getLogger("artisan").removeHandler(self._handler)
        self._handler.close()


def _configure_default_logging() -> None:
    """Install automatic console output without changing explicit severity."""
    logger = logging.getLogger("artisan")
    if not logger.handlers:
        if logger.level == logging.NOTSET:
            logger.setLevel(logging.INFO)
        _install_console_handler(logger)
        for name in _NOISY_LOGGERS:
            logging.getLogger(name).setLevel(logging.CRITICAL)
