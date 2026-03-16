"""Logging configuration for pipeline execution.

Uses a Prefect-inspired Rich console handler that colorizes log levels and URLs
via regex highlighting while keeping output clean (no Rich chrome).
"""

from __future__ import annotations

import logging
import logging.handlers
import os
import sys
from pathlib import Path
from typing import ClassVar

from rich.console import Console
from rich.highlighter import RegexHighlighter
from rich.theme import Theme

_NOISY_LOGGERS = ("prefect", "httpx", "httpcore", "asyncio")

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


class _ConsoleHandler(logging.StreamHandler):
    """StreamHandler that renders formatted log lines through Rich Console.

    Follows the same pattern as Prefect's ``PrefectConsoleHandler``:
    a standard ``logging.Formatter`` produces the log line as plain text,
    then ``Console.print()`` colorizes it via ``RegexHighlighter``.
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
    logs_root: Path | None = None,
) -> None:
    """Configure logging for artisan execution.

    Sets up named loggers with a :class:`_ConsoleHandler` that renders
    colored output via Rich.  Idempotent — safe to call multiple times
    (e.g. in Jupyter cells that are re-executed).

    Args:
        level: Log level for the configured loggers. Defaults to ``"INFO"``.
        suppress_noise: If True, suppress noisy third-party loggers
            (Prefect lifecycle, HTTP client chatter) and set the
            ``PREFECT_LOGGING_LEVEL`` env var to ``"CRITICAL"`` so
            child processes (workers) inherit the suppression.
        loggers: Root logger names to configure. Defaults to ``("artisan",)``.
        logs_root: When provided with ``level="DEBUG"``, a rotating file
            handler is added that writes to ``logs_root / "pipeline.log"``.
    """
    for logger_name in loggers:
        logger = logging.getLogger(logger_name)
        logger.setLevel(getattr(logging, level.upper()))

        if not logger.handlers:
            handler = _ConsoleHandler(stream=sys.stdout)
            handler.setFormatter(logging.Formatter(_LOG_FORMAT, datefmt=_LOG_DATEFMT))
            logger.addHandler(handler)

        # Add file handler when DEBUG + logs_root (idempotent check)
        if (
            logs_root is not None
            and level.upper() == "DEBUG"
            and not any(
                isinstance(h, logging.handlers.RotatingFileHandler)
                for h in logger.handlers
            )
        ):
            logs_root.mkdir(parents=True, exist_ok=True)
            file_handler = logging.handlers.RotatingFileHandler(
                logs_root / "pipeline.log",
                maxBytes=50 * 1024 * 1024,
                backupCount=3,
            )
            file_handler.setFormatter(
                logging.Formatter(_LOG_FORMAT, datefmt=_LOG_DATEFMT)
            )
            file_handler.setLevel(logging.DEBUG)
            logger.addHandler(file_handler)

        logger.propagate = False

    if suppress_noise:
        os.environ.setdefault("PREFECT_LOGGING_LEVEL", "CRITICAL")
        for name in _NOISY_LOGGERS:
            logging.getLogger(name).setLevel(logging.CRITICAL)
