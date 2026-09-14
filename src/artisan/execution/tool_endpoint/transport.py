"""Bulk-file transport between the endpoint client and the tool worker.

``InlineTransport`` is the inline mode — input bytes and the output tar
ride the endpoint↔worker function-call hop, bounded at 100 MB per
direction. ``s3://`` input refs are fetched worker-side via fsspec and
bypass the bound; the producer is ``materialize_inputs`` under
``endpoint_routed=True``, which hands a cloud-hosted input's
``external_path`` straight to ``pack_inputs`` rather than downloading it.
``upload_outputs`` is the stored output mode: when a request names an
``output_store``, the worker delivers the output tarball there and only a
``StoredOutputs`` pointer rides the control plane. Stored archives spool to
disk and are bounded by compressed size, expanded size, and member count.
"""

from __future__ import annotations

import io
import os
import shutil
import tarfile
import tempfile
import uuid
from collections.abc import Iterable
from typing import Any, BinaryIO
from urllib.parse import quote

from artisan.execution.tool_endpoint.protocol import InputRef, StoredOutputs
from artisan.schemas.artifact.external import copy_verified_file
from artisan.utils.path import uri_join

MAX_INLINE_BYTES = 100 * 1024 * 1024
"""Modal's function-call payload cap — the inline bound per direction."""

MAX_ARCHIVE_BYTES = 1024 * 1024 * 1024
"""Maximum compressed size of a stored output archive (1 GiB)."""

MAX_EXPANDED_BYTES = 10 * 1024 * 1024 * 1024
"""Maximum total regular-file bytes represented by an archive (10 GiB)."""

MAX_ARCHIVE_MEMBERS = 10_000
"""Maximum number of entries represented by an archive."""

PRESIGN_EXPIRY_SECONDS = 7 * 24 * 3600
"""SigV4 maximum — matches Modal's 7-day FunctionCall result retention."""


class InlineTransport:
    """v1 — inputs as inline bytes or ``s3://`` URIs; outputs as a tar."""

    def pack_inputs(
        self,
        files: dict[str, str],
        expected_content: dict[str, tuple[str, int]] | None = None,
    ) -> list[InputRef]:
        """Pack local files inline; pass object-store URIs through as refs.

        Args:
            files: Input name → local path or ``scheme://`` URI.
            expected_content: URI → expected digest and byte count.

        Returns:
            One InputRef per input.

        Raises:
            ValueError: When the inline bytes exceed ``MAX_INLINE_BYTES``.
        """
        local_sizes = 0
        for source in files.values():
            if "://" not in source:
                local_sizes += os.path.getsize(source)
                if local_sizes > MAX_INLINE_BYTES:
                    raise ValueError(_inline_input_limit_message())

        refs: list[InputRef] = []
        total = 0
        for name, source in files.items():
            filename = os.path.basename(source.rstrip("/"))
            if "://" in source:
                expected = (expected_content or {}).get(source)
                if expected is None:
                    msg = f"External input {name!r} lacks expected digest and size"
                    raise ValueError(msg)
                refs.append(
                    InputRef(
                        name=name,
                        filename=filename,
                        uri=source,
                        content_digest=expected[0],
                        size_bytes=expected[1],
                    )
                )
                continue
            with open(source, "rb") as f:
                data = f.read(MAX_INLINE_BYTES - total + 1)
            total += len(data)
            if total > MAX_INLINE_BYTES:
                raise ValueError(_inline_input_limit_message())
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
            ValueError: When roles repeat or a ref carries neither ``uri``
                nor ``data``.
        """
        duplicates = _duplicates(ref.name for ref in refs)
        if duplicates:
            msg = (
                f"Duplicate input roles: {', '.join(repr(name) for name in duplicates)}"
            )
            raise ValueError(msg)

        os.makedirs(dest, exist_ok=True)
        paths: dict[str, str] = {}
        for ref in refs:
            # original filename when carried — execute_command and lineage
            # stem-matching must see the same basename as a local run
            role_dir = os.path.join(dest, _safe_role(ref.name))
            os.makedirs(role_dir)
            filename = os.path.basename(ref.filename or ref.name)
            if filename in {"", ".", ".."}:
                filename = "input"
            local = os.path.join(role_dir, filename)
            if ref.uri is not None:
                ref_fs, remote = _resolve_fs(ref.uri, fs)
                copy_verified_file(
                    artifact_id=None,
                    artifact_type="tool input",
                    uri=remote,
                    destination=local,
                    expected_digest=ref.content_digest,
                    expected_size=ref.size_bytes,
                    fs=ref_fs,
                )
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

        Args:
            src: Directory holding the output files.
            names: Output paths relative to ``src``.

        Returns:
            The tar payload as bytes.

        Raises:
            ValueError: When the tar exceeds an archive budget. Pass
                ``output_store`` when only the inline byte limit is exceeded.
        """
        _preflight_archive(src, names)
        buf = io.BytesIO()
        writer = _BoundedWriter(buf, MAX_INLINE_BYTES, _inline_output_limit_message())
        budget = _ArchiveBudget()
        with tarfile.open(fileobj=writer, mode="w") as tar:
            for name in names:
                tar.add(os.path.join(src, name), arcname=name, filter=budget)
        return buf.getvalue()

    def unpack_outputs(self, payload: bytes | BinaryIO, dest: str) -> None:
        """Extract a seekable output tar into ``dest`` within archive budgets.

        Extraction retains ``tarfile``'s ``data`` filter after validating every
        member, so an invalid archive cannot partially extract before a budget
        failure is discovered.
        """
        fileobj = io.BytesIO(payload) if isinstance(payload, bytes) else payload
        _check_archive_size(fileobj)
        os.makedirs(dest, exist_ok=True)
        fileobj.seek(0)
        with tarfile.open(fileobj=fileobj, mode="r:*") as tar:
            members = _bounded_members(tar)
            tar.extractall(dest, members=members, filter="data")

    def unpack_output_stream(self, chunks: Iterable[bytes], dest: str) -> None:
        """Spool bounded archive chunks to disk, then extract into ``dest``."""
        spool_dir = tempfile.mkdtemp(prefix="artisan-tool-download-")
        spool = os.path.join(spool_dir, "out.tar")
        try:
            with open(spool, "wb") as raw:
                writer = _BoundedWriter(
                    raw, MAX_ARCHIVE_BYTES, _compressed_limit_message()
                )
                for chunk in chunks:
                    writer.write(chunk)
            with open(spool, "rb") as payload:
                self.unpack_outputs(payload, dest)
        finally:
            shutil.rmtree(spool_dir, ignore_errors=True)


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
        ValueError: When an archive budget is exceeded.
        NotImplementedError: When a prefix's filesystem cannot presign
            (prefix mode requires a signing object store).
        httpx.RequestError: When a presigned PUT cannot reach its destination.
        httpx.HTTPStatusError: When a presigned PUT is refused.
    """
    # Modal reuses warm containers across requests; the spool must not
    # outlive the call or gzipped tars accumulate in the container.
    spool_dir = tempfile.mkdtemp(prefix="artisan-tool-tar-")
    spool = os.path.join(spool_dir, "out.tar.gz")
    try:
        _preflight_archive(src, names)
        with open(spool, "wb") as raw:
            writer = _BoundedWriter(raw, MAX_ARCHIVE_BYTES, _compressed_limit_message())
            budget = _ArchiveBudget()
            with tarfile.open(fileobj=writer, mode="w:gz") as tar:
                for name in names:
                    tar.add(os.path.join(src, name), arcname=name, filter=budget)
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


class _ArchiveBudget:
    """Validate archive members as tarfile discovers them."""

    def __init__(self) -> None:
        self.members = 0
        self.expanded_bytes = 0

    def __call__(self, member: tarfile.TarInfo) -> tarfile.TarInfo:
        self.members += 1
        if self.members > MAX_ARCHIVE_MEMBERS:
            msg = f"Archive exceeds {MAX_ARCHIVE_MEMBERS} members"
            raise ValueError(msg)
        if member.isfile():
            self.expanded_bytes += member.size
            if self.expanded_bytes > MAX_EXPANDED_BYTES:
                msg = f"Archive expands beyond {MAX_EXPANDED_BYTES >> 20} MB"
                raise ValueError(msg)
        return member


class _BoundedWriter:
    """Write-through wrapper that raises before crossing a byte bound."""

    def __init__(self, raw: BinaryIO, limit: int, message: str) -> None:
        self.raw = raw
        self.limit = limit
        self.message = message
        self.written = 0

    def write(self, data: bytes) -> int:
        if len(data) > self.limit - self.written:
            raise ValueError(self.message)
        written = self.raw.write(data)
        self.written += written
        return written

    def __getattr__(self, name: str) -> Any:
        return getattr(self.raw, name)


def _preflight_archive(src: str, names: list[str]) -> None:
    """Reject oversized source sets before opening an output archive."""
    if len(names) > MAX_ARCHIVE_MEMBERS:
        msg = f"Archive exceeds {MAX_ARCHIVE_MEMBERS} members"
        raise ValueError(msg)
    expanded = 0
    for name in names:
        path = os.path.join(src, name)
        if os.path.isfile(path):
            expanded += os.path.getsize(path)
            if expanded > MAX_EXPANDED_BYTES:
                msg = f"Archive expands beyond {MAX_EXPANDED_BYTES >> 20} MB"
                raise ValueError(msg)


def _check_archive_size(fileobj: BinaryIO) -> None:
    """Reject an oversized seekable archive without reading its contents."""
    position = fileobj.tell()
    fileobj.seek(0, os.SEEK_END)
    size = fileobj.tell()
    fileobj.seek(position)
    if size > MAX_ARCHIVE_BYTES:
        raise ValueError(_compressed_limit_message())


def _bounded_members(tar: tarfile.TarFile) -> list[tarfile.TarInfo]:
    """Read archive headers until a member budget is crossed."""
    budget = _ArchiveBudget()
    members: list[tarfile.TarInfo] = []
    for member in tar:
        members.append(budget(member))
    return members


def _duplicates(names: Iterable[str]) -> list[str]:
    """Return sorted values that occur more than once."""
    seen: set[str] = set()
    duplicates: set[str] = set()
    for name in names:
        if name in seen:
            duplicates.add(name)
        seen.add(name)
    return sorted(duplicates)


def _safe_role(name: str) -> str:
    """Return a traversal-safe, collision-resistant directory component."""
    if (
        name
        and name not in {".", ".."}
        and all(char.isalnum() or char in "._-" for char in name)
    ):
        return name
    return f"%{quote(name, safe='')}"


def _inline_input_limit_message() -> str:
    return (
        f"Inline inputs exceed {MAX_INLINE_BYTES >> 20} MB; pass object-store "
        "URIs (s3://…) for large inputs — already-external artifacts re-upload "
        "nothing"
    )


def _inline_output_limit_message() -> str:
    return (
        f"Output tar exceeds {MAX_INLINE_BYTES >> 20} MB — the inline transport "
        "cannot return it; pass output_store for object-store delivery"
    )


def _compressed_limit_message() -> str:
    return f"Archive exceeds {MAX_ARCHIVE_BYTES >> 20} MB compressed"


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
