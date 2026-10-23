"""Bulk-file transport between the endpoint client and the tool worker.

``InlineTransport`` is the inline mode — input bytes and the output tar
ride the endpoint↔worker function-call hop, bounded at 100 MB per
direction. Authorized ``s3://`` input refs are fetched with deployment
credentials; authorized HTTP(S) refs use bare capability GETs. Both bypass
the inline bound and are verified against their complete-file contracts.
``upload_outputs`` is the stored output mode: when a request names an
``output_store``, the worker delivers the output tarball there and only a
``StoredOutputs`` pointer rides the control plane. Stored archives spool to
disk and are bounded by compressed size, expanded size, and member count.
"""

from __future__ import annotations

import io
import os
import posixpath
import shutil
import tarfile
import tempfile
import uuid
from collections.abc import Iterable
from typing import Any, BinaryIO
from urllib.parse import quote, unquote, urlsplit

import httpx

from artisan.errors import ArtifactIntegrityError
from artisan.execution.tool_endpoint.protocol import InputRef, StoredOutputs
from artisan.schemas.artifact.external import copy_verified_chunks, copy_verified_file
from artisan.schemas.operation_config.endpoint_policy import (
    ToolEndpointDataPolicy,
    _EndpointUri,
)
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

_HTTP_TIMEOUT = 120.0


class EndpointTransportError(RuntimeError):
    """Sanitized failure at an endpoint-mediated data boundary."""


class InlineTransport:
    """Transfer file inputs as inline bytes or authorized URIs, outputs as a tar."""

    def pack_inputs(
        self,
        files: dict[str, str],
        expected_content: dict[str, tuple[str, int]] | None = None,
    ) -> list[InputRef]:
        """Pack local files inline; pass object-store URIs through as refs.

        Args:
            files: Input name → local path or remote URI.
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
            filename = _input_filename(source)
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
        self,
        refs: list[InputRef],
        dest: str,
        policy: ToolEndpointDataPolicy | None = None,
    ) -> dict[str, str]:
        """Resolve refs into files under ``dest``; return name → local path.

        Args:
            refs: Input refs from the request.
            dest: Directory to write input files into.
            policy: Deployment-owned URI permissions. Omission denies every
                remote ref while leaving inline refs available.

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

        inline_size = sum(len(ref.data) for ref in refs if ref.data is not None)
        if inline_size > MAX_INLINE_BYTES:
            raise ValueError(_inline_input_limit_message())

        data_policy = policy or ToolEndpointDataPolicy()
        authorized = {
            id(ref): data_policy.authorize_input(ref.uri)
            for ref in refs
            if ref.uri is not None
        }

        os.makedirs(dest, exist_ok=True)
        paths: dict[str, str] = {}
        for ref in refs:
            # original filename when carried — execute_command and operation-owned
            # stem-matching must see the same basename as a local run
            role_dir = os.path.join(dest, _safe_role(ref.name))
            os.makedirs(role_dir)
            filename = os.path.basename(ref.filename or ref.name)
            if filename in {"", ".", ".."}:
                filename = "input"
            local = os.path.join(role_dir, filename)
            if ref.uri is not None:
                target = authorized[id(ref)]
                if target.scheme == "s3":
                    _copy_s3_input(ref, target, local)
                else:
                    _copy_http_input(ref, target, local)
            elif ref.data is not None:
                with open(local, "wb") as f:
                    f.write(ref.data)
            else:
                msg = f"InputRef {ref.name!r} carries neither uri nor data"
                raise ValueError(msg)
            paths[ref.name] = local
        return paths

    def pack_outputs(
        self, src: str, names: list[str], *, max_bytes: int | None = None
    ) -> bytes:
        """Tar the named output files (paths relative to ``src``).

        Args:
            src: Directory holding the output files.
            names: Output paths relative to ``src``.
            max_bytes: Remaining shared inline-result budget, when smaller.

        Returns:
            The tar payload as bytes.

        Raises:
            ValueError: When the tar exceeds an archive budget. Pass
                ``output_store`` when only the inline byte limit is exceeded.
        """
        _preflight_archive(src, names)
        buf = io.BytesIO()
        limit = min(MAX_INLINE_BYTES, MAX_ARCHIVE_BYTES)
        if max_bytes is not None:
            limit = min(limit, max_bytes)
        writer = _BoundedWriter(buf, limit, _inline_output_limit_message())
        budget = _ArchiveBudget()
        with tarfile.open(fileobj=writer, mode="w") as tar:
            for name in names:
                tar.add(os.path.join(src, name), arcname=name, filter=budget)
        return buf.getvalue()

    def unpack_outputs(
        self,
        payload: bytes | BinaryIO,
        dest: str,
        *,
        prefixes: tuple[str, ...] | None = None,
    ) -> None:
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
            if prefixes is not None and any(
                not member.name.startswith(prefixes) for member in members
            ):
                msg = "Diagnostic archive contains an unexpected prefix"
                raise ValueError(msg)
            tar.extractall(dest, members=members, filter="data")

    def unpack_output_stream(
        self,
        chunks: Iterable[bytes],
        dest: str,
        *,
        prefixes: tuple[str, ...] | None = None,
    ) -> None:
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
                self.unpack_outputs(payload, dest, prefixes=prefixes)
        finally:
            shutil.rmtree(spool_dir, ignore_errors=True)

    def download_outputs(
        self,
        uri: str,
        dest: str,
        policy: ToolEndpointDataPolicy | None = None,
        *,
        prefixes: tuple[str, ...] | None = None,
    ) -> None:
        """Authorize, fetch, and extract a stored output capability."""
        target = (policy or ToolEndpointDataPolicy()).authorize_output(uri)
        if target.scheme not in {"http", "https"}:
            msg = f"stored output capability must use HTTP: {target.safe_display}"
            raise ValueError(msg)
        try:
            with (
                _http_client() as client,
                client.stream("GET", target.transport_target) as response,
            ):
                _check_http_response(response, "output download", target)
                self.unpack_output_stream(
                    response.iter_bytes(chunk_size=1024 * 1024), dest, prefixes=prefixes
                )
        except (ArtifactIntegrityError, EndpointTransportError, ValueError):
            raise
        except Exception as exc:
            msg = f"output download failed for {target.safe_display}"
            raise EndpointTransportError(msg) from exc


def upload_outputs(
    src: str,
    names: list[str],
    store: str,
    op_name: str,
    policy: ToolEndpointDataPolicy | None = None,
) -> StoredOutputs:
    """Worker-side: tar the named outputs and deliver them to ``store``.

    Spools the gzipped tar to disk, then delivers by destination form. An
    authorized HTTP(S) value is a caller-minted PUT capability. An authorized
    S3 prefix uses worker credentials and yields a presigned GET capability.

    Args:
        src: Directory holding the output files.
        names: Output paths relative to ``src``.
        store: Caller-supplied destination — object-store root URI
            (``s3://bucket/prefix``) or presigned PUT URL.
        op_name: Deployed op name — namespaces keys under a prefix.
        policy: Deployment-owned URI permissions. Omission denies remote
            delivery.

    Returns:
        Pointer to the delivered tarball.

    Raises:
        ValueError: When an archive budget is exceeded.
        ImportError: When the output filesystem dependency is unavailable.
        NotImplementedError: When the output filesystem cannot transfer or sign.
        EndpointTransportError: When transfer or signing fails.
    """
    data_policy = policy or ToolEndpointDataPolicy()
    requested = data_policy.authorize_output(store)
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
        if requested.scheme in {"http", "https"}:
            _put_http_file(spool, requested)
            return StoredOutputs(uri=requested.safe_display)
        uri = uri_join(
            requested.transport_target, op_name, f"{uuid.uuid4().hex}.tar.gz"
        )
        final_target = data_policy.authorize_output(uri)
        # Force SigV4 on s3: requests sign v4 either way, but presigned URLs
        # come out legacy SigV2 without the explicit opt-in — accepted by
        # MinIO, rejected (401) by R2 and modern AWS buckets.
        options = {"config_kwargs": {"signature_version": "s3v4"}}
        try:
            fs, remote = _resolve_s3(final_target, **options)
            presigned = str(fs.sign(remote, expiration=PRESIGN_EXPIRY_SECONDS))
            data_policy.authorize_output(presigned)
            fs.put(spool, remote)
        except ImportError:
            msg = "output transport is unavailable"
            raise ImportError(msg) from None
        except NotImplementedError:
            msg = "output transport is unavailable"
            raise NotImplementedError(msg) from None
        except Exception as exc:
            msg = f"output transfer failed for {final_target.safe_display}"
            raise EndpointTransportError(msg) from exc
        return StoredOutputs(uri=final_target.transport_target, presigned_url=presigned)
    finally:
        shutil.rmtree(spool_dir, ignore_errors=True)


class _ArchiveBudget:
    """Validate archive members as tarfile discovers them."""

    def __init__(self) -> None:
        self.members = 0
        self.expanded_bytes = 0
        self.names: set[str] = set()

    def __call__(self, member: tarfile.TarInfo) -> tarfile.TarInfo:
        self.members += 1
        if self.members > MAX_ARCHIVE_MEMBERS:
            msg = f"Archive exceeds {MAX_ARCHIVE_MEMBERS} members"
            raise ValueError(msg)
        _validate_archive_name(member.name)
        if member.name in self.names:
            msg = "Archive contains duplicate member names"
            raise ValueError(msg)
        self.names.add(member.name)
        if not member.isfile() and not member.isdir():
            msg = "Archive contains a link or special file"
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
    if os.path.islink(src):
        msg = "Archive root is a symbolic link"
        raise ValueError(msg)
    if len(names) > MAX_ARCHIVE_MEMBERS:
        msg = f"Archive exceeds {MAX_ARCHIVE_MEMBERS} members"
        raise ValueError(msg)
    expanded = 0
    for name in names:
        _validate_archive_name(name)
        path = os.path.join(src, name)
        current = src
        for part in name.split("/"):
            current = os.path.join(current, part)
            if os.path.islink(current):
                msg = "Archive contains a symbolic link"
                raise ValueError(msg)
        if os.path.isfile(path):
            expanded += os.path.getsize(path)
            if expanded > MAX_EXPANDED_BYTES:
                msg = f"Archive expands beyond {MAX_EXPANDED_BYTES >> 20} MB"
                raise ValueError(msg)


def _validate_archive_name(name: str) -> None:
    """Reject paths whose extraction could leave their archive root."""
    if (
        not name
        or name.startswith("/")
        or "\\" in name
        or any(part in {"", ".", ".."} for part in name.split("/"))
    ):
        msg = "Archive contains an unsafe relative path"
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
        f"Inline inputs exceed {MAX_INLINE_BYTES >> 20} MB; pass an authorized "
        "S3 (s3://) or HTTP(S) URI for eligible complete-file inputs"
    )


def _inline_output_limit_message() -> str:
    return (
        f"Output tar exceeds {MAX_INLINE_BYTES >> 20} MB — the inline transport "
        "cannot return it; pass output_store for object-store delivery"
    )


def _compressed_limit_message() -> str:
    return f"Archive exceeds {MAX_ARCHIVE_BYTES >> 20} MB compressed"


def _copy_s3_input(ref: InputRef, target: _EndpointUri, local: str) -> None:
    """Fetch one authorized S3 object and verify its complete contents."""
    try:
        fs, remote = _resolve_s3(target)
        copy_verified_file(
            artifact_id=None,
            artifact_type="tool input",
            uri=remote,
            destination=local,
            expected_digest=ref.content_digest,
            expected_size=ref.size_bytes,
            fs=fs,
        )
    except ArtifactIntegrityError:
        msg = f"External tool input failed integrity at {target.safe_display}"
        raise ArtifactIntegrityError(msg) from None
    except Exception as exc:
        msg = f"input transfer failed for {target.safe_display}"
        raise EndpointTransportError(msg) from exc


def _copy_http_input(ref: InputRef, target: _EndpointUri, local: str) -> None:
    """Fetch one authorized HTTP capability and verify its complete contents."""
    try:
        with (
            _http_client() as client,
            client.stream("GET", target.transport_target) as response,
        ):
            _check_http_response(response, "input", target)
            copy_verified_chunks(
                response.iter_bytes(chunk_size=1024 * 1024),
                artifact_id=None,
                artifact_type="tool input",
                uri=target.safe_display,
                destination=local,
                expected_digest=ref.content_digest,
                expected_size=ref.size_bytes,
            )
    except (ArtifactIntegrityError, EndpointTransportError):
        raise
    except Exception as exc:
        msg = f"input transfer failed for {target.safe_display}"
        raise EndpointTransportError(msg) from exc


def _put_http_file(spool: str, target: _EndpointUri) -> None:
    """Deliver one archive through an authorized bare HTTP capability."""
    headers = {"Content-Length": str(os.path.getsize(spool))}
    try:
        with _http_client() as client, open(spool, "rb") as payload:
            response = client.put(
                target.transport_target,
                content=payload,
                headers=headers,
            )
            _check_http_response(response, "output", target)
    except EndpointTransportError:
        raise
    except Exception as exc:
        msg = f"output transfer failed for {target.safe_display}"
        raise EndpointTransportError(msg) from exc


def _http_client() -> httpx.Client:
    """Return a credential-free, non-redirecting HTTP capability client."""
    return httpx.Client(
        follow_redirects=False,
        trust_env=False,
        timeout=_HTTP_TIMEOUT,
    )


def _check_http_response(
    response: httpx.Response, operation: str, target: _EndpointUri
) -> None:
    """Require a 2xx capability response without exposing its signed URI."""
    if 300 <= response.status_code < 400:
        msg = f"{operation} redirect refused for {target.safe_display}"
        raise EndpointTransportError(msg)
    if not 200 <= response.status_code < 300:
        msg = (
            f"{operation} request was rejected ({response.status_code}) for "
            f"{target.safe_display}"
        )
        raise EndpointTransportError(msg)


def _resolve_s3(target: object, **storage_options: Any) -> tuple[Any, str]:
    """Return an S3 filesystem and path for an already-authorized target."""
    if not isinstance(target, _EndpointUri) or target.scheme != "s3":
        msg = "endpoint credentialed transport supports s3:// only"
        raise ValueError(msg)
    import fsspec

    derived_fs, _path = fsspec.core.url_to_fs(
        f"s3://{target.authority}",
        **storage_options,
    )
    return derived_fs, _s3_remote_path(target)


def _s3_remote_path(target: _EndpointUri) -> str:
    """Build the fs-native object name from the authorized decoded segments."""
    return "/".join((target.authority, *target.segments))


def _input_filename(source: str) -> str:
    """Return a local basename that never contains a remote capability query."""
    if "://" not in source:
        return os.path.basename(source.rstrip("/"))
    try:
        path = urlsplit(source).path
    except ValueError:
        return "input"
    basename = posixpath.basename(path.rstrip("/"))
    decoded = unquote(basename, errors="replace")
    return posixpath.basename(decoded.replace("\\", "/")) or "input"
