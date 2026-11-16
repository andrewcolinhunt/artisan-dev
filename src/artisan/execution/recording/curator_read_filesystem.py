"""Observe supported read opens without changing an ingestor's source URI."""

from __future__ import annotations

import io
from collections.abc import Callable
from pathlib import Path
from typing import Any, BinaryIO, cast

from fsspec import AbstractFileSystem


class _ObservedReader(io.BufferedReader):
    def __init__(
        self,
        path: Path,
        uri: str,
        observed: Callable[[], None],
        excluded: Callable[[], None],
    ) -> None:
        super().__init__(io.FileIO(path, "r"))
        self._source_name = uri
        self._observed, self._excluded = observed, excluded
        self._detached: io.RawIOBase | None = None

    @property
    def name(self) -> str:
        return self._source_name

    def __getattribute__(self, name: str) -> Any:
        if name == "raw":
            self._excluded()
        return super().__getattribute__(name)

    def detach(self) -> io.RawIOBase:
        self._excluded()
        self._detached = super().detach()
        return self._detached

    def close(self) -> None:
        if self._detached is not None:
            self._detached.close()
        else:
            super().close()

    def read(self, size: int | None = -1) -> bytes:
        self._observed()
        return super().read(size)

    def read1(self, size: int = -1) -> bytes:
        self._observed()
        return super().read1(size)

    def readinto(self, buffer: Any) -> int:
        self._observed()
        return super().readinto(buffer)

    def readinto1(self, buffer: Any) -> int:
        self._observed()
        return super().readinto1(buffer)

    def readline(self, size: int | None = -1) -> bytes:
        self._observed()
        return super().readline(size)

    def readlines(self, hint: int = -1) -> list[bytes]:
        self._observed()
        return super().readlines(hint)

    def peek(self, size: int = 0) -> bytes:
        self._observed()
        return super().peek(size)

    def fileno(self) -> int:
        self._excluded()
        return super().fileno()


class CuratorReadFileSystem:
    """Route one URI's read opens to a complete capture; exclude other operations.

    This is a bounded fsspec-compatible proxy, not an I/O sandbox. Generic calls
    retain the configured filesystem's behavior, including absent/listed results,
    while permanently excluding this invocation from cache reuse. Direct Python
    I/O outside the supplied adapter is not intercepted.
    """

    def __init__(
        self,
        fs: AbstractFileSystem,
        uri: str,
        capture: Path,
        *,
        exclude: Callable[[], None],
    ) -> None:
        self._fs, self._uri, self._capture = fs, uri, capture
        self._exclude = exclude
        self._paths = {uri, fs._strip_protocol(uri)}
        self.protocol = fs.protocol
        self.read_observed = False
        self._streams: list[BinaryIO | io.TextIOWrapper] = []

    def _observed(self) -> None:
        self.read_observed = True

    def open(self, path: str, mode: str = "rb", **kwargs: Any) -> Any:
        """Keep the original URI visible and allow repeated reads of captured bytes."""
        allowed = {"cache_type", "encoding", "errors", "newline"}
        if (
            path not in self._paths
            or mode not in {"r", "rb", "rt"}
            or (set(kwargs) - allowed)
        ):
            self._exclude()
            return self._fs.open(path, mode, **kwargs)
        stream = _ObservedReader(
            self._capture, self._uri, self._observed, self._exclude
        )
        result: BinaryIO | io.TextIOWrapper = cast(BinaryIO, stream)
        if "b" not in mode:
            result = io.TextIOWrapper(
                stream,
                encoding=kwargs.get("encoding"),
                errors=kwargs.get("errors"),
                newline=kwargs.get("newline"),
            )
        self._streams.append(result)
        return result

    def close(self) -> None:
        """Close only streams owned by this per-conversion adapter."""
        for stream in self._streams:
            stream.close()

    def __getattr__(self, name: str) -> Any:
        self._exclude()
        return getattr(self._fs, name)
