"""Bulk-file transport between the endpoint client and the tool worker.

``InlineTransport`` is the inline mode — input bytes and the output tar
ride the endpoint↔worker function-call hop, bounded at 100 MB per
direction. ``s3://`` input refs are fetched worker-side via fsspec and
bypass the bound. ``upload_outputs`` is the stored output mode: when a
request names an ``output_store``, the worker delivers the output tarball
there and only a ``StoredOutputs`` pointer rides the control plane.
"""

from __future__ import annotations

import io
import os
import shutil
import tarfile
import tempfile
import uuid
from typing import Any

from artisan.execution.tool_endpoint.protocol import InputRef, StoredOutputs
from artisan.utils.path import uri_join

MAX_INLINE_BYTES = 100 * 1024 * 1024
"""Modal's function-call payload cap — the inline bound per direction."""

PRESIGN_EXPIRY_SECONDS = 7 * 24 * 3600
"""SigV4 maximum — matches Modal's 7-day FunctionCall result retention."""


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
            ValueError: When the tar exceeds ``MAX_INLINE_BYTES`` — pass
                ``output_store`` for object-store delivery instead.
        """
        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w") as tar:
            for name in names:
                tar.add(os.path.join(src, name), arcname=name)
        payload = buf.getvalue()
        if len(payload) > MAX_INLINE_BYTES:
            msg = (
                f"Output tar exceeds {MAX_INLINE_BYTES >> 20} MB — the inline "
                "transport cannot return it; pass output_store for "
                "object-store delivery"
            )
            raise ValueError(msg)
        return payload

    def unpack_outputs(self, payload: bytes, dest: str) -> None:
        """Extract the output tar into ``dest`` (path-traversal safe)."""
        os.makedirs(dest, exist_ok=True)
        with tarfile.open(fileobj=io.BytesIO(payload), mode="r") as tar:
            tar.extractall(dest, filter="data")


def upload_outputs(
    src: str, names: list[str], store: str, op_name: str
) -> StoredOutputs:
    """Worker-side: tar the named outputs and deliver them to ``store``.

    Spools the gzipped tar to disk (stored outputs are exactly the ones too
    large to buffer), then delivers by destination form: an ``http(s)://``
    value is a caller-minted presigned PUT URL — the tar is PUT there
    directly, touching no store credentials; anything else is an
    object-store prefix — the tar is uploaded under it via the same
    ambient-credential fs resolution input refs use, and a presigned GET
    is minted once.

    Args:
        src: Directory holding the output files.
        names: Output paths relative to ``src``.
        store: Caller-supplied destination — object-store root URI
            (``s3://bucket/prefix``) or presigned PUT URL.
        op_name: Deployed op name — namespaces keys under a prefix.

    Returns:
        Pointer to the delivered tarball.

    Raises:
        NotImplementedError: When a prefix's filesystem cannot presign
            (prefix mode requires a signing object store).
        httpx.HTTPStatusError: When a presigned PUT is refused.
    """
    # Modal reuses warm containers across requests; the spool must not
    # outlive the call or gzipped tars accumulate in the container.
    spool_dir = tempfile.mkdtemp(prefix="artisan-tool-tar-")
    spool = os.path.join(spool_dir, "out.tar.gz")
    try:
        with tarfile.open(spool, "w:gz") as tar:
            for name in names:
                tar.add(os.path.join(src, name), arcname=name)
        if store.startswith(("http://", "https://")):
            import httpx

            # Explicit Content-Length, or httpx sends the file body as
            # Transfer-Encoding: chunked — S3 answers plain chunked PUTs with
            # 501 (MinIO tolerates them). Never in the signed set: callers
            # mint with default (host-only) signed headers.
            headers = {"Content-Length": str(os.path.getsize(spool))}
            with open(spool, "rb") as f:
                # streamed body; bounded by the worker's Modal timeout
                response = httpx.put(store, content=f, headers=headers, timeout=None)
            response.raise_for_status()
            return StoredOutputs(uri=store.split("?", 1)[0])
        uri = uri_join(store, op_name, f"{uuid.uuid4().hex}.tar.gz")
        # Force SigV4 on s3: requests sign v4 either way, but presigned URLs
        # come out legacy SigV2 without the explicit opt-in — accepted by
        # MinIO, rejected (401) by R2 and modern AWS buckets.
        options = (
            {"config_kwargs": {"signature_version": "s3v4"}}
            if uri.startswith("s3://")
            else {}
        )
        fs, remote = _resolve_fs(uri, None, **options)
        fs.put(spool, remote)
        return StoredOutputs(
            uri=uri, presigned_url=fs.sign(remote, expiration=PRESIGN_EXPIRY_SECONDS)
        )
    finally:
        shutil.rmtree(spool_dir, ignore_errors=True)


def _resolve_fs(uri: str, fs: Any, **storage_options: Any) -> tuple[Any, str]:
    """Return (filesystem, path) for a URI, deriving the fs when not given.

    ``storage_options`` are forwarded to the derived filesystem's
    constructor (ignored when ``fs`` is given).
    """
    if fs is not None:
        return fs, uri
    import fsspec

    derived_fs, path = fsspec.core.url_to_fs(uri, **storage_options)
    return derived_fs, path
