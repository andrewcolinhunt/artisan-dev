"""Bulk-file transport between the endpoint client and the tool worker.

``DataTransport`` is the seam; ``InlineTransport`` is v1 — input bytes and
the output tar ride the endpoint↔worker function-call hop, bounded at
100 MB per direction. ``s3://`` input refs are fetched worker-side via
fsspec and bypass the bound. A later object-store transport implements the
same Protocol without touching operation code.
"""

from __future__ import annotations

import io
import os
import tarfile
from typing import Any, Protocol

from artisan.execution.tool_endpoint.protocol import InputRef

MAX_INLINE_BYTES = 100 * 1024 * 1024
"""Modal's function-call payload cap — the v1 inline bound per direction."""


class DataTransport(Protocol):
    """Moves bulk tool files between client and worker."""

    def pack_inputs(self, files: dict[str, str]) -> list[InputRef]:
        """Client-side: local paths / object-store URIs → input refs."""
        ...

    def unpack_inputs(
        self, refs: list[InputRef], dest: str, fs: Any = None
    ) -> dict[str, str]:
        """Worker-side: input refs → local paths under ``dest``."""
        ...

    def pack_outputs(self, src: str, names: list[str]) -> Any:
        """Worker-side: output files under ``src`` → data-plane payload."""
        ...

    def unpack_outputs(self, payload: Any, dest: str) -> None:
        """Client-side: data-plane payload → files under ``dest``."""
        ...


class InlineTransport:
    """v1 — inputs as inline bytes or ``s3://`` URIs; outputs as a tar."""

    def pack_inputs(self, files: dict[str, str]) -> list[InputRef]:
        """Pack local files inline; pass object-store URIs through as refs.

        Args:
            files: Input name → local path or ``scheme://`` URI.

        Returns:
            One InputRef per input.

        Raises:
            ValueError: When the inline bytes exceed ``MAX_INLINE_BYTES``.
        """
        refs: list[InputRef] = []
        total = 0
        for name, source in files.items():
            filename = os.path.basename(source.rstrip("/"))
            if "://" in source:
                refs.append(InputRef(name=name, filename=filename, uri=source))
                continue
            with open(source, "rb") as f:
                data = f.read()
            total += len(data)
            if total > MAX_INLINE_BYTES:
                msg = (
                    f"Inline inputs exceed {MAX_INLINE_BYTES >> 20} MB; pass "
                    "object-store URIs (s3://…) for large inputs — "
                    "already-external artifacts re-upload nothing"
                )
                raise ValueError(msg)
            refs.append(InputRef(name=name, filename=filename, data=data))
        return refs

    def unpack_inputs(
        self, refs: list[InputRef], dest: str, fs: Any = None
    ) -> dict[str, str]:
        """Resolve refs into files under ``dest``; return name → local path.

        Args:
            refs: Input refs from the request.
            dest: Directory to write input files into.
            fs: Optional fsspec filesystem for ``uri`` refs. When None, a
                filesystem is derived from each URI's scheme.

        Returns:
            Input name → local path.

        Raises:
            ValueError: When a ref carries neither ``uri`` nor ``data``.
        """
        os.makedirs(dest, exist_ok=True)
        paths: dict[str, str] = {}
        for ref in refs:
            # original filename when carried — execute_command and lineage
            # stem-matching must see the same basename as a local run
            local = os.path.join(dest, os.path.basename(ref.filename or ref.name))
            if ref.uri is not None:
                ref_fs, remote = _resolve_fs(ref.uri, fs)
                ref_fs.get(remote, local)
            elif ref.data is not None:
                with open(local, "wb") as f:
                    f.write(ref.data)
            else:
                msg = f"InputRef {ref.name!r} carries neither uri nor data"
                raise ValueError(msg)
            paths[ref.name] = local
        return paths

    def pack_outputs(self, src: str, names: list[str]) -> bytes:
        """Tar the named output files (paths relative to ``src``).

        Raises:
            ValueError: When the tar exceeds ``MAX_INLINE_BYTES`` (full
                object-store output delivery is the deferred fix).
        """
        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w") as tar:
            for name in names:
                tar.add(os.path.join(src, name), arcname=name)
        payload = buf.getvalue()
        if len(payload) > MAX_INLINE_BYTES:
            msg = (
                f"Output tar exceeds {MAX_INLINE_BYTES >> 20} MB — the inline "
                "transport cannot return it; object-store output delivery is "
                "not implemented yet"
            )
            raise ValueError(msg)
        return payload

    def unpack_outputs(self, payload: bytes, dest: str) -> None:
        """Extract the output tar into ``dest`` (path-traversal safe)."""
        os.makedirs(dest, exist_ok=True)
        with tarfile.open(fileobj=io.BytesIO(payload), mode="r") as tar:
            tar.extractall(dest, filter="data")


def _resolve_fs(uri: str, fs: Any) -> tuple[Any, str]:
    """Return (filesystem, path) for a URI, deriving the fs when not given."""
    if fs is not None:
        return fs, uri
    import fsspec

    derived_fs, path = fsspec.core.url_to_fs(uri)
    return derived_fs, path
