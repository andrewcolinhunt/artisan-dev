"""Sanitized live checkpoints separate from immutable qualification receipts."""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from pathlib import Path

from artisan_modal.diagnostics import CallObservation, redacted_message


class Progress(logging.Handler):
    """Atomically retain phase and bounded owner observations without raising."""

    def __init__(self, path: Path) -> None:
        if path.exists():
            message = "Preserve earlier qualification checkpoints"
            raise FileExistsError(message)
        super().__init__()
        self.path = path
        self.started = time.monotonic()
        self._guard = threading.RLock()
        self._values: tuple[str, ...] = ()
        self._data: dict[str, object] = {"phase": "created", "owners": {}}
        self.write_error: str | None = None
        self._logger = logging.getLogger("artisan_modal.call_owner")
        self._old_level = self._logger.level
        self._logger.setLevel(logging.INFO)
        self._logger.addHandler(self)
        self.update()

    def learn(self, values: tuple[str, ...]) -> None:
        with self._guard:
            self._values = tuple(dict.fromkeys((*self._values, *values)))

    def update(self, **values: object) -> None:
        with self._guard:
            self._data.update(values)
            self._write()

    def failure(self, phase: str, error: BaseException) -> None:
        try:
            detail = {
                "type": type(error).__name__[:128],
                "message": redacted_message(str(error), self._values),
            }
            self.update(**{phase: detail})
        except Exception:
            self.write_error = "Qualification diagnostic formatting failed"

    def emit(self, record: logging.LogRecord) -> None:
        event = getattr(record, "modal_event", None)
        if not isinstance(event, CallObservation):
            return
        with self._guard:
            owners = self._data["owners"]
            assert isinstance(owners, dict)
            owner_id = str(event.owner_id)
            if owner_id not in owners and len(owners) >= 8:
                return
            events = owners.setdefault(owner_id, {})
            events[event.sequence] = event.model_dump(mode="json")
            self._write()

    def _write(self) -> None:
        try:
            data = {
                **self._data,
                "elapsed_seconds": time.monotonic() - self.started,
                "diagnostic_write_error": self.write_error,
            }
            encoded = json.dumps(data, sort_keys=True, indent=2).encode() + b"\n"
            if len(encoded) > 4 * 1024**2:
                message = "Bounded progress checkpoint exceeds 4 MiB"
                raise ValueError(message)
            temporary = self.path.with_suffix(self.path.suffix + ".tmp")
            descriptor = os.open(
                temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600
            )
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(encoded)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, self.path)
        except Exception:
            self.write_error = "Qualification checkpoint persistence failed"

    def close(self) -> None:
        self._logger.removeHandler(self)
        self._logger.setLevel(self._old_level)
        super().close()
