"""Tests for the inline and stored data transports."""

from __future__ import annotations

import os
import re
import shutil
import tarfile
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock
from urllib.parse import urlsplit

import httpx
import pytest

from artisan.errors import ArtifactIntegrityError
from artisan.execution.tool_endpoint import transport as transport_mod
from artisan.execution.tool_endpoint.protocol import InputRef
from artisan.execution.tool_endpoint.transport import (
    EndpointTransportError,
    InlineTransport,
    upload_outputs,
)
from artisan.schemas.artifact.external import copy_verified_chunks
from artisan.schemas.operation_config.endpoint_policy import ToolEndpointDataPolicy
from artisan.utils.hashing import compute_content_digest


class _FakeFs:
    """Record open/put/sign calls and stream fixed input bytes."""

    protocol = "s3"

    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []
        self.puts: list[tuple[str, str]] = []
        self.signed: list[tuple[str, int]] = []
        self.put_bytes = b""

    def open(self, remote: str, mode: str):
        assert mode == "rb"
        self.calls.append((remote, "open"))
        return BytesIO(b"remote-bytes")

    def put(self, local: str, remote: str) -> None:
        self.puts.append((local, remote))
        self.put_bytes = Path(local).read_bytes()

    def sign(self, remote: str, expiration: int = 100) -> str:
        self.signed.append((remote, expiration))
        return f"https://signed.example/{remote}?sig=abc"


def _make_outputs(tmp_path: Path) -> str:
    src = tmp_path / "outputs"
    (src / "nested").mkdir(parents=True)
    (src / "out.txt").write_text("payload")
    (src / "nested" / "deep.txt").write_text("deep")
    return str(src)


def _policy(
    *, inputs: tuple[str, ...] = (), outputs: tuple[str, ...] = ()
) -> ToolEndpointDataPolicy:
    return ToolEndpointDataPolicy(
        input_allowlist=inputs,
        output_allowlist=outputs,
    )


def _fake_s3_resolver(fs):
    def resolve(target, **_options):
        return fs, transport_mod._s3_remote_path(target)

    return resolve


def _origin(uri: str) -> str:
    parts = urlsplit(uri)
    return f"{parts.scheme}://{parts.netloc}"


class TestPackInputs:
    def test_local_files_become_inline_refs(self, tmp_path: Path):
        src = tmp_path / "input.pdb"
        src.write_bytes(b"ATOM")
        refs = InlineTransport().pack_inputs({"pdb": str(src)})
        assert refs == [InputRef(name="pdb", filename="input.pdb", data=b"ATOM")]

    def test_uri_passes_through_without_reading(self):
        uri = "s3://bucket/key.pdb"
        refs = InlineTransport().pack_inputs(
            {"pdb": uri},
            {uri: ("a" * 32, 4)},
        )
        assert refs == [
            InputRef(
                name="pdb",
                filename="key.pdb",
                uri=uri,
                content_digest="a" * 32,
                size_bytes=4,
            )
        ]

    def test_http_capability_query_never_enters_input_filename(self):
        uri = "https://downloads.example/input.csv?X-Amz-Signature=fake-secret"
        refs = InlineTransport().pack_inputs(
            {"dataset": uri},
            {uri: ("a" * 32, 4)},
        )

        assert refs[0].filename == "input.csv"
        assert "fake-secret" not in (refs[0].filename or "")

    def test_remote_filename_without_a_path_uses_safe_fallback(self):
        uri = "https://alice:fake-secret@downloads.example?signature=token"
        refs = InlineTransport().pack_inputs(
            {"dataset": uri},
            {uri: ("a" * 32, 4)},
        )

        assert refs[0].filename == "input"

    def test_over_limit_raises(self, tmp_path: Path, monkeypatch):
        monkeypatch.setattr(transport_mod, "MAX_INLINE_BYTES", 4)
        src = tmp_path / "big.bin"
        src.write_bytes(b"x" * 5)
        with pytest.raises(ValueError, match="s3://"):
            InlineTransport().pack_inputs({"big": str(src)})

    def test_aggregate_size_is_preflighted_before_open(
        self, tmp_path: Path, monkeypatch
    ):
        monkeypatch.setattr(transport_mod, "MAX_INLINE_BYTES", 4)
        first = tmp_path / "first.bin"
        second = tmp_path / "second.bin"
        first.write_bytes(b"123")
        second.write_bytes(b"45")

        def fail_open(*args, **kwargs):
            pytest.fail("oversized inputs must fail before a file is opened")

        monkeypatch.setattr("builtins.open", fail_open)
        with pytest.raises(ValueError, match="Inline inputs exceed"):
            InlineTransport().pack_inputs({"first": str(first), "second": str(second)})

    def test_read_bound_catches_file_growth(self, tmp_path: Path, monkeypatch):
        monkeypatch.setattr(transport_mod, "MAX_INLINE_BYTES", 4)
        src = tmp_path / "growing.bin"
        src.write_bytes(b"12345")
        monkeypatch.setattr(transport_mod.os.path, "getsize", lambda _path: 4)

        with pytest.raises(ValueError, match="Inline inputs exceed"):
            InlineTransport().pack_inputs({"growing": str(src)})


class TestUnpackInputs:
    def test_inline_ref_written_to_dest(self, tmp_path: Path):
        dest = tmp_path / "inputs"
        paths = InlineTransport().unpack_inputs(
            [InputRef(name="pdb", data=b"ATOM")], str(dest)
        )
        assert Path(paths["pdb"]).read_bytes() == b"ATOM"
        assert Path(paths["pdb"]).parent == dest / "pdb"

    def test_uri_ref_fetched_via_fs(self, tmp_path: Path, monkeypatch):
        fs = _FakeFs()
        content = b"remote-bytes"
        monkeypatch.setattr(transport_mod, "_resolve_s3", _fake_s3_resolver(fs))
        paths = InlineTransport().unpack_inputs(
            [
                InputRef(
                    name="pdb",
                    uri="s3://bucket/key.pdb",
                    content_digest=compute_content_digest(content),
                    size_bytes=len(content),
                )
            ],
            str(tmp_path),
            policy=_policy(inputs=("s3://bucket",)),
        )
        assert fs.calls == [("bucket/key.pdb", "open")]
        assert Path(paths["pdb"]).read_bytes() == b"remote-bytes"

    def test_changed_uri_bytes_are_rejected(self, tmp_path: Path, monkeypatch):
        fs = _FakeFs()
        monkeypatch.setattr(transport_mod, "_resolve_s3", _fake_s3_resolver(fs))
        ref = InputRef(
            name="pdb",
            uri="s3://bucket/key.pdb",
            content_digest=compute_content_digest(b"original"),
            size_bytes=len(b"original"),
        )

        with pytest.raises(Exception, match="failed integrity"):
            InlineTransport().unpack_inputs(
                [ref],
                str(tmp_path),
                policy=_policy(inputs=("s3://bucket",)),
            )

        assert not (tmp_path / "pdb" / "key.pdb").exists()

    def test_s3_integrity_error_uses_bounded_safe_display(
        self, tmp_path: Path, monkeypatch
    ):
        fs = _FakeFs()
        monkeypatch.setattr(transport_mod, "_resolve_s3", _fake_s3_resolver(fs))
        uri = f"s3://bucket/{'a' * 1_000}.bin"
        ref = InputRef(
            name="data",
            filename="data.bin",
            uri=uri,
            content_digest=compute_content_digest(b"expected"),
            size_bytes=len(b"expected"),
        )

        with pytest.raises(Exception, match="failed integrity") as exc_info:
            InlineTransport().unpack_inputs(
                [ref],
                str(tmp_path / "inputs"),
                policy=_policy(inputs=("s3://bucket",)),
            )

        assert len(str(exc_info.value)) < 400
        assert not (tmp_path / "inputs" / "data" / "data.bin").exists()

    @pytest.mark.parametrize(
        ("uri", "remote"),
        [
            ("s3://bucket/a%20b/item.bin", "bucket/a b/item.bin"),
            ("s3://bucket/a%23b/item.bin", "bucket/a#b/item.bin"),
            (
                "s3://bucket/caf%C3%A9/item.bin",  # codespell:ignore caf
                "bucket/café/item.bin",
            ),
            ("s3://bucket/a%2520b/item.bin", "bucket/a%20b/item.bin"),
            ("s3://bucket/percent%25key/item.bin", "bucket/percent%key/item.bin"),
        ],
    )
    def test_s3_sink_opens_exactly_authorized_decoded_segments(
        self, uri, remote, tmp_path, monkeypatch
    ):
        import fsspec

        fs = _FakeFs()
        resolved_roots: list[str] = []

        def resolve(root, **_options):
            resolved_roots.append(root)
            return fs, "ignored"

        monkeypatch.setattr(fsspec.core, "url_to_fs", resolve)
        body = b"remote-bytes"
        ref = InputRef(
            name="data",
            filename="item.bin",
            uri=uri,
            content_digest=compute_content_digest(body),
            size_bytes=len(body),
        )

        InlineTransport().unpack_inputs(
            [ref],
            str(tmp_path / "inputs"),
            policy=_policy(inputs=("s3://bucket",)),
        )

        assert resolved_roots == ["s3://bucket"]
        assert fs.calls == [(remote, "open")]

    def test_encoded_question_mark_is_rejected_before_s3_resolution(
        self, tmp_path, monkeypatch
    ):
        resolver = MagicMock()
        monkeypatch.setattr(transport_mod, "_resolve_s3", resolver)
        ref = InputRef(
            name="data",
            filename="item.bin",
            uri="s3://bucket/a%3Fb/item.bin",
            content_digest=compute_content_digest(b"remote-bytes"),
            size_bytes=len(b"remote-bytes"),
        )

        with pytest.raises(ValueError, match="input URI is invalid"):
            InlineTransport().unpack_inputs(
                [ref],
                str(tmp_path / "inputs"),
                policy=_policy(inputs=("s3://bucket",)),
            )

        resolver.assert_not_called()

    def test_original_filename_preserved_on_disk(self, tmp_path: Path):
        """Lineage stem-matching needs the worker-side basename to match local."""
        src = tmp_path / "dataset_00001.csv"
        src.write_bytes(b"a,b\n1,2\n")
        refs = InlineTransport().pack_inputs({"dataset": str(src)})
        paths = InlineTransport().unpack_inputs(refs, str(tmp_path / "inputs"))
        assert Path(paths["dataset"]).name == "dataset_00001.csv"

    def test_name_is_sanitized_to_safe_role_directory(self, tmp_path: Path):
        paths = InlineTransport().unpack_inputs(
            [InputRef(name="../evil.txt", data=b"x")], str(tmp_path)
        )
        local = Path(paths["../evil.txt"])
        assert tmp_path in local.parents
        assert local.parent.parent == tmp_path
        assert local.parent.name != ".."

    def test_filename_is_sanitized_to_basename(self, tmp_path: Path):
        paths = InlineTransport().unpack_inputs(
            [InputRef(name="x", filename="../../evil.txt", data=b"x")], str(tmp_path)
        )
        assert Path(paths["x"]).parent == tmp_path / "x"

    def test_same_basename_roles_materialize_distinct_bytes(self, tmp_path: Path):
        paths = InlineTransport().unpack_inputs(
            [
                InputRef(name="left", filename="data.csv", data=b"left"),
                InputRef(name="right", filename="data.csv", data=b"right"),
            ],
            str(tmp_path),
        )

        assert paths["left"] != paths["right"]
        assert Path(paths["left"]).read_bytes() == b"left"
        assert Path(paths["right"]).read_bytes() == b"right"
        assert Path(paths["left"]).relative_to(tmp_path) == Path("left/data.csv")
        assert Path(paths["right"]).relative_to(tmp_path) == Path("right/data.csv")

    def test_duplicate_roles_fail_before_materialization(self, tmp_path: Path):
        dest = tmp_path / "inputs"
        with pytest.raises(ValueError, match="Duplicate input roles.*'source'"):
            InlineTransport().unpack_inputs(
                [
                    InputRef(name="source", data=b"one"),
                    InputRef(name="source", data=b"two"),
                ],
                str(dest),
            )
        assert not dest.exists()

    def test_direct_inline_unpack_enforces_aggregate_bound(
        self, tmp_path: Path, monkeypatch
    ):
        monkeypatch.setattr(transport_mod, "MAX_INLINE_BYTES", 3)

        with pytest.raises(ValueError, match="Inline inputs exceed"):
            InlineTransport().unpack_inputs(
                [InputRef(name="value", data=b"four")],
                str(tmp_path / "inputs"),
            )

        assert not (tmp_path / "inputs").exists()

    def test_empty_ref_raises(self, tmp_path: Path):
        with pytest.raises(ValueError, match="neither uri nor data"):
            InlineTransport().unpack_inputs(
                [InputRef.model_construct(name="x")], str(tmp_path)
            )

    @pytest.mark.parametrize("uri", ["file:///tmp/x", "memory://bucket/x"])
    def test_remote_scheme_denied_before_destination_or_resolver(
        self, uri, tmp_path, monkeypatch
    ):
        resolver = MagicMock()
        monkeypatch.setattr(transport_mod, "_resolve_s3", resolver)
        dest = tmp_path / "inputs"
        ref = InputRef(
            name="x",
            uri=uri,
            content_digest="a" * 32,
            size_bytes=1,
        )

        with pytest.raises(ValueError, match="input URI is invalid") as exc_info:
            InlineTransport().unpack_inputs([ref], str(dest))

        assert not dest.exists()
        resolver.assert_not_called()
        assert "input URI is invalid" in str(exc_info.value)

    def test_off_policy_s3_denied_before_destination_or_resolver(
        self, tmp_path, monkeypatch
    ):
        resolver = MagicMock()
        monkeypatch.setattr(transport_mod, "_resolve_s3", resolver)
        dest = tmp_path / "inputs"
        ref = InputRef(
            name="x",
            uri="s3://bucket/private/x",
            content_digest="a" * 32,
            size_bytes=1,
        )

        with pytest.raises(ValueError, match="not allowed"):
            InlineTransport().unpack_inputs(
                [ref],
                str(dest),
                policy=_policy(inputs=("s3://bucket/public",)),
            )

        assert not dest.exists()
        resolver.assert_not_called()


class TestOutputs:
    def test_pack_unpack_round_trip(self, tmp_path: Path):
        src = tmp_path / "outputs"
        (src / "nested").mkdir(parents=True)
        (src / "a.txt").write_text("alpha")
        (src / "nested" / "b.txt").write_text("beta")

        payload = InlineTransport().pack_outputs(str(src), ["a.txt", "nested/b.txt"])
        dest = tmp_path / "restored"
        InlineTransport().unpack_outputs(payload, str(dest))

        assert (dest / "a.txt").read_text() == "alpha"
        assert (dest / "nested" / "b.txt").read_text() == "beta"

    def test_pack_over_limit_raises_naming_the_fix(self, tmp_path: Path, monkeypatch):
        monkeypatch.setattr(transport_mod, "MAX_INLINE_BYTES", 4)
        src = tmp_path / "outputs"
        src.mkdir()
        (src / "big.bin").write_bytes(b"x" * 1024)
        with pytest.raises(ValueError, match="output_store"):
            InlineTransport().pack_outputs(str(src), ["big.bin"])

    def test_unpack_rejects_path_traversal(self, tmp_path: Path):
        # Hand-craft a malicious tar with an absolute-escaping member.
        buf = BytesIO()
        with tarfile.open(fileobj=buf, mode="w") as tar:
            payload_file = tmp_path / "x.txt"
            payload_file.write_text("evil")
            tar.add(str(payload_file), arcname="../escape.txt")
        dest = tmp_path / "dest"
        with pytest.raises(ValueError, match="unsafe relative path"):
            InlineTransport().unpack_outputs(buf.getvalue(), str(dest))
        shutil.rmtree(dest, ignore_errors=True)

    def test_pack_expanded_limit_is_preflighted(self, tmp_path: Path, monkeypatch):
        src = tmp_path / "outputs"
        src.mkdir()
        (src / "big.bin").write_bytes(b"12345")
        monkeypatch.setattr(transport_mod, "MAX_EXPANDED_BYTES", 4)

        with pytest.raises(ValueError, match="expands beyond"):
            InlineTransport().pack_outputs(str(src), ["big.bin"])

    @pytest.mark.parametrize("second_type", [tarfile.REGTYPE, tarfile.DIRTYPE])
    def test_unpack_rejects_duplicate_members_before_extracting(
        self, tmp_path: Path, second_type: bytes
    ):
        payload = BytesIO()
        with tarfile.open(fileobj=payload, mode="w") as archive:
            first = tarfile.TarInfo("out.txt")
            first.size = 5
            archive.addfile(first, BytesIO(b"first"))
            duplicate = tarfile.TarInfo("out.txt")
            duplicate.type = second_type
            duplicate.size = 6 if second_type == tarfile.REGTYPE else 0
            archive.addfile(duplicate, BytesIO(b"second"))
        dest = tmp_path / "dest"

        with pytest.raises(ValueError, match="duplicate member names"):
            InlineTransport().unpack_outputs(payload.getvalue(), str(dest))

        assert list(dest.iterdir()) == []

    def test_pack_rejects_duplicate_members(self, tmp_path: Path):
        (tmp_path / "out.txt").write_text("content")

        with pytest.raises(ValueError, match="duplicate member names"):
            InlineTransport().pack_outputs(str(tmp_path), ["out.txt", "out.txt"])

    def test_pack_member_limit_is_preflighted(self, tmp_path: Path, monkeypatch):
        src = tmp_path / "outputs"
        src.mkdir()
        (src / "a").touch()
        (src / "b").touch()
        monkeypatch.setattr(transport_mod, "MAX_ARCHIVE_MEMBERS", 1)

        with pytest.raises(ValueError, match="exceeds 1 members"):
            InlineTransport().pack_outputs(str(src), ["a", "b"])

    def test_pack_counts_recursive_directories_and_symlinks(
        self, tmp_path: Path, monkeypatch
    ):
        src = tmp_path / "outputs"
        tree = src / "tree"
        tree.mkdir(parents=True)
        (tree / "file.txt").write_text("payload")
        (tree / "link.txt").symlink_to("file.txt")
        monkeypatch.setattr(transport_mod, "MAX_ARCHIVE_MEMBERS", 2)

        with pytest.raises(ValueError, match="exceeds 2 members"):
            InlineTransport().pack_outputs(str(src), ["tree"])

    def test_unpack_rejects_compressed_size_before_extracting(
        self, tmp_path: Path, monkeypatch
    ):
        payload = _tar_with_files({"x": b"payload"})
        monkeypatch.setattr(transport_mod, "MAX_ARCHIVE_BYTES", len(payload) - 1)
        dest = tmp_path / "dest"

        with pytest.raises(ValueError, match="compressed"):
            InlineTransport().unpack_outputs(payload, str(dest))
        assert not dest.exists()

    def test_unpack_rejects_expanded_size_before_extracting(
        self, tmp_path: Path, monkeypatch
    ):
        payload = _tar_with_files({"a": b"123", "b": b"45"})
        monkeypatch.setattr(transport_mod, "MAX_EXPANDED_BYTES", 4)
        dest = tmp_path / "dest"

        with pytest.raises(ValueError, match="expands beyond"):
            InlineTransport().unpack_outputs(payload, str(dest))
        assert list(dest.iterdir()) == []

    def test_unpack_rejects_member_count_before_extracting(
        self, tmp_path: Path, monkeypatch
    ):
        payload = _tar_with_files({"a": b"", "b": b""})
        monkeypatch.setattr(transport_mod, "MAX_ARCHIVE_MEMBERS", 1)
        dest = tmp_path / "dest"

        with pytest.raises(ValueError, match="exceeds 1 members"):
            InlineTransport().unpack_outputs(payload, str(dest))
        assert list(dest.iterdir()) == []

    def test_streamed_archive_is_spooled_and_extracted(
        self, tmp_path: Path, capture_tempdirs
    ):
        payload = _tar_with_files({"out.txt": b"streamed"})
        chunks = (payload[index : index + 7] for index in range(0, len(payload), 7))
        dest = tmp_path / "dest"
        created = capture_tempdirs()

        InlineTransport().unpack_output_stream(chunks, str(dest))

        assert (dest / "out.txt").read_bytes() == b"streamed"
        assert all(not os.path.exists(path) for path in created)

    def test_streamed_archive_stops_at_compressed_limit(
        self, tmp_path: Path, monkeypatch, capture_tempdirs
    ):
        monkeypatch.setattr(transport_mod, "MAX_ARCHIVE_BYTES", 4)
        created = capture_tempdirs()
        consumed: list[bytes] = []

        def chunks():
            for chunk in (b"123", b"45", b"unread"):
                consumed.append(chunk)
                yield chunk

        with pytest.raises(ValueError, match="compressed"):
            InlineTransport().unpack_output_stream(chunks(), str(tmp_path / "dest"))

        assert consumed == [b"123", b"45"]
        assert all(not os.path.exists(path) for path in created)


def _tar_with_files(files: dict[str, bytes]) -> bytes:
    """Build an in-memory tar from name-to-bytes fixtures."""
    buf = BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as tar:
        for name, data in files.items():
            member = tarfile.TarInfo(name)
            member.size = len(data)
            tar.addfile(member, BytesIO(data))
    return buf.getvalue()


class TestUploadOutputsPrefixMode:
    @pytest.fixture
    def fake_fs(self, monkeypatch) -> _FakeFs:
        fake = _FakeFs()
        monkeypatch.setattr(transport_mod, "_resolve_s3", _fake_s3_resolver(fake))
        return fake

    def test_s3_filesystem_derived_with_sigv4(self, tmp_path, monkeypatch):
        fake = _FakeFs()
        captured: dict = {}

        def resolve(target, **options):
            captured.update(options)
            return fake, transport_mod._s3_remote_path(target)

        monkeypatch.setattr(transport_mod, "_resolve_s3", resolve)
        upload_outputs(
            _make_outputs(tmp_path),
            ["out.txt"],
            "s3://b/p",
            "op",
            policy=_policy(outputs=("s3://b/p", "https://signed.example")),
        )
        # without the opt-in, presigned URLs come out legacy SigV2 —
        # MinIO accepts them, R2 and modern AWS buckets return 401
        assert captured == {"config_kwargs": {"signature_version": "s3v4"}}

    def test_gzipped_tar_uploaded_under_namespaced_key(self, tmp_path, fake_fs):
        stored = upload_outputs(
            _make_outputs(tmp_path),
            ["out.txt", "nested/deep.txt"],
            "s3://bucket/prefix",
            "my_op",
            policy=_policy(outputs=("s3://bucket/prefix", "https://signed.example")),
        )
        assert re.fullmatch(
            r"s3://bucket/prefix/my_op/[0-9a-f]{32}\.tar\.gz", stored.uri
        )
        assert f"s3://{fake_fs.puts[0][1]}" == stored.uri
        # the gzipped tar extracts via the existing client-side unpack
        dest = tmp_path / "extracted"
        InlineTransport().unpack_outputs(fake_fs.put_bytes, str(dest))
        assert (dest / "out.txt").read_text() == "payload"
        assert (dest / "nested" / "deep.txt").read_text() == "deep"

    def test_presigned_get_minted_with_max_expiry(self, tmp_path, fake_fs):
        stored = upload_outputs(
            _make_outputs(tmp_path),
            ["out.txt"],
            "s3://b/p",
            "op",
            policy=_policy(outputs=("s3://b/p", "https://signed.example")),
        )
        assert stored.presigned_url is not None
        assert stored.presigned_url.startswith("https://signed.example/")
        assert fake_fs.signed == [
            (
                stored.uri.removeprefix("s3://"),
                transport_mod.PRESIGN_EXPIRY_SECONDS,
            )
        ]
        assert transport_mod.PRESIGN_EXPIRY_SECONDS == 7 * 24 * 3600

    def test_non_signing_fs_propagates(self, tmp_path, monkeypatch):
        class _NoSignFs(_FakeFs):
            def sign(self, remote: str, expiration: int = 100) -> str:
                msg = "Sign unavailable: credential-secret"
                raise NotImplementedError(msg)

        fake = _NoSignFs()
        monkeypatch.setattr(transport_mod, "_resolve_s3", _fake_s3_resolver(fake))
        with pytest.raises(
            NotImplementedError, match="output transport is unavailable"
        ) as caught:
            upload_outputs(
                _make_outputs(tmp_path),
                ["out.txt"],
                "s3://bucket/x",
                "op",
                policy=_policy(outputs=("s3://bucket",)),
            )

        assert "credential-secret" not in str(caught.value)
        assert caught.value.__suppress_context__


class TestUploadOutputsCapabilityMode:
    PUT_URL = "https://bucket.s3.amazonaws.com/run42.tar.gz?X-Amz-Signature=abc"

    def test_put_streams_spool_with_explicit_content_length(
        self, tmp_path, monkeypatch
    ):
        captured: dict = {}
        client = MagicMock()
        client.__enter__.return_value = client
        client.put.side_effect = lambda url, content, headers: (
            captured.update(url=url, body=content.read(), headers=headers)
            or SimpleNamespace(status_code=200)
        )
        monkeypatch.setattr(transport_mod, "_http_client", lambda: client)
        stored = upload_outputs(
            _make_outputs(tmp_path),
            ["out.txt", "nested/deep.txt"],
            self.PUT_URL,
            "op",
            policy=_policy(outputs=("https://bucket.s3.amazonaws.com",)),
        )
        assert captured["url"] == self.PUT_URL
        # explicit Content-Length: chunked-TE regressions pass MinIO but
        # fail real S3 (501) — this assertion is the only guard
        assert captured["headers"] == {"Content-Length": str(len(captured["body"]))}
        dest = tmp_path / "extracted"
        InlineTransport().unpack_outputs(captured["body"], str(dest))
        assert (dest / "out.txt").read_text() == "payload"
        # pointer: PUT URL sans query; no presigned GET — caller owns the bucket
        assert stored.uri == "https://bucket.s3.amazonaws.com/run42.tar.gz"
        assert stored.presigned_url is None

    def test_refused_put_raises(self, tmp_path, monkeypatch):
        client = MagicMock()
        client.__enter__.return_value = client
        client.put.return_value = SimpleNamespace(status_code=403)
        monkeypatch.setattr(transport_mod, "_http_client", lambda: client)
        with pytest.raises(EndpointTransportError, match=r"rejected \(403\)"):
            upload_outputs(
                _make_outputs(tmp_path),
                ["out.txt"],
                self.PUT_URL,
                "op",
                policy=_policy(outputs=("https://bucket.s3.amazonaws.com",)),
            )

    def test_redirect_is_rejected_without_leaking_signature(
        self, tmp_path, monkeypatch
    ):
        client = MagicMock()
        client.__enter__.return_value = client
        client.put.return_value = SimpleNamespace(status_code=307)
        monkeypatch.setattr(transport_mod, "_http_client", lambda: client)

        with pytest.raises(EndpointTransportError) as exc_info:
            upload_outputs(
                _make_outputs(tmp_path),
                ["out.txt"],
                self.PUT_URL,
                "op",
                policy=_policy(outputs=("https://bucket.s3.amazonaws.com",)),
            )

        assert "redirect refused" in str(exc_info.value)
        assert "X-Amz-Signature" not in str(exc_info.value)
        assert "abc" not in str(exc_info.value)


class TestUploadOutputsSpoolCleanup:
    """The gzipped-tar spool dir must not outlive the call (warm containers)."""

    def test_spool_removed_on_success(self, tmp_path, monkeypatch, capture_tempdirs):
        monkeypatch.setattr(transport_mod, "_resolve_s3", _fake_s3_resolver(_FakeFs()))
        created = capture_tempdirs()
        upload_outputs(
            _make_outputs(tmp_path),
            ["out.txt"],
            "s3://b/p",
            "op",
            policy=_policy(outputs=("s3://b/p", "https://signed.example")),
        )
        assert created  # the call did allocate a spool dir
        assert all(not os.path.exists(p) for p in created)

    def test_spool_removed_on_put_failure(
        self, tmp_path, monkeypatch, capture_tempdirs
    ):
        client = MagicMock()
        client.__enter__.return_value = client
        client.put.return_value = SimpleNamespace(status_code=403)
        monkeypatch.setattr(transport_mod, "_http_client", lambda: client)
        created = capture_tempdirs()
        put_url = "https://bucket.s3.amazonaws.com/x.tar.gz?X-Amz-Signature=abc"
        with pytest.raises(EndpointTransportError):
            upload_outputs(
                _make_outputs(tmp_path),
                ["out.txt"],
                put_url,
                "op",
                policy=_policy(outputs=("https://bucket.s3.amazonaws.com",)),
            )
        assert created  # the call did allocate a spool dir
        assert all(not os.path.exists(p) for p in created)

    def test_compressed_limit_stops_archive_before_upload(
        self, tmp_path, monkeypatch, capture_tempdirs
    ):
        fake = _FakeFs()
        monkeypatch.setattr(transport_mod, "_resolve_s3", _fake_s3_resolver(fake))
        monkeypatch.setattr(transport_mod, "MAX_ARCHIVE_BYTES", 4)
        created = capture_tempdirs()

        with pytest.raises(ValueError, match="compressed"):
            upload_outputs(
                _make_outputs(tmp_path),
                ["out.txt"],
                "s3://b/p",
                "op",
                policy=_policy(outputs=("s3://b/p", "https://signed.example")),
            )

        assert fake.puts == []
        assert all(not os.path.exists(path) for path in created)


class TestHttpCapabilityTransport:
    def test_http_client_disables_redirects_and_environment(self, monkeypatch):
        constructor = MagicMock()
        monkeypatch.setattr(transport_mod.httpx, "Client", constructor)

        transport_mod._http_client()

        constructor.assert_called_once_with(
            follow_redirects=False,
            trust_env=False,
            timeout=transport_mod._HTTP_TIMEOUT,
        )

    def test_input_get_is_bare_and_verified(self, tmp_path, monkeypatch):
        body = b"verified-input"
        requests: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            return httpx.Response(200, content=body)

        client = httpx.Client(
            transport=httpx.MockTransport(handler),
            follow_redirects=False,
            trust_env=False,
        )
        monkeypatch.setattr(transport_mod, "_http_client", lambda: client)
        uri = "https://downloads.example/object?signature=fake"
        ref = InputRef(
            name="dataset",
            filename="dataset.bin",
            uri=uri,
            content_digest=compute_content_digest(body),
            size_bytes=len(body),
        )

        paths = InlineTransport().unpack_inputs(
            [ref],
            str(tmp_path / "inputs"),
            policy=_policy(inputs=("https://downloads.example",)),
        )

        assert Path(paths["dataset"]).read_bytes() == body
        assert requests[0].method == "GET"
        assert requests[0].url == uri
        assert "Modal-Key" not in requests[0].headers
        assert "Authorization" not in requests[0].headers

    def test_stored_output_get_is_bare_and_extracts(self, tmp_path, monkeypatch):
        payload = InlineTransport().pack_outputs(
            _make_outputs(tmp_path), ["out.txt", "nested/deep.txt"]
        )
        requests: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            return httpx.Response(200, content=payload)

        client = httpx.Client(
            transport=httpx.MockTransport(handler),
            follow_redirects=False,
            trust_env=False,
        )
        monkeypatch.setattr(transport_mod, "_http_client", lambda: client)
        uri = "https://downloads.example/result?signature=fake"
        dest = tmp_path / "restored"

        InlineTransport().download_outputs(
            uri,
            str(dest),
            policy=_policy(outputs=("https://downloads.example",)),
        )

        assert (dest / "out.txt").read_text() == "payload"
        assert (dest / "nested" / "deep.txt").read_text() == "deep"
        assert requests[0].method == "GET"
        assert requests[0].url == uri
        assert "Modal-Key" not in requests[0].headers
        assert "Authorization" not in requests[0].headers

    @pytest.mark.parametrize("status", [302, 403])
    def test_input_http_failure_is_sanitized_and_leaves_no_partial_file(
        self, status, tmp_path, monkeypatch
    ):
        uri = "https://downloads.example/object?signature=fake-secret"

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                status,
                headers={"Location": "https://redirect.example/token-value"},
            )

        client = httpx.Client(
            transport=httpx.MockTransport(handler),
            follow_redirects=False,
            trust_env=False,
        )
        monkeypatch.setattr(transport_mod, "_http_client", lambda: client)
        ref = InputRef(
            name="dataset",
            filename="dataset.bin",
            uri=uri,
            content_digest="a" * 32,
            size_bytes=1,
        )

        with pytest.raises(EndpointTransportError) as exc_info:
            InlineTransport().unpack_inputs(
                [ref],
                str(tmp_path / "inputs"),
                policy=_policy(inputs=("https://downloads.example",)),
            )

        message = str(exc_info.value)
        for secret in ("signature", "fake-secret", "redirect.example", "token-value"):
            assert secret not in message
        assert not (tmp_path / "inputs" / "dataset" / "dataset.bin").exists()

    def test_http_integrity_mismatch_deletes_partial_file(self, tmp_path, monkeypatch):
        client = httpx.Client(
            transport=httpx.MockTransport(
                lambda _request: httpx.Response(200, content=b"changed")
            ),
            follow_redirects=False,
            trust_env=False,
        )
        monkeypatch.setattr(transport_mod, "_http_client", lambda: client)
        uri = "https://downloads.example/object?signature=fake"
        ref = InputRef(
            name="dataset",
            filename="dataset.bin",
            uri=uri,
            content_digest=compute_content_digest(b"expected"),
            size_bytes=len(b"expected"),
        )

        with pytest.raises(Exception, match="failed integrity") as exc_info:
            InlineTransport().unpack_inputs(
                [ref],
                str(tmp_path / "inputs"),
                policy=_policy(inputs=("https://downloads.example",)),
            )

        assert not (tmp_path / "inputs" / "dataset" / "dataset.bin").exists()
        assert "signature" not in str(exc_info.value)
        assert "fake" not in str(exc_info.value)

    def test_stream_stops_at_declared_size_and_deletes_partial_file(self, tmp_path):
        destination = tmp_path / "partial.bin"
        consumed: list[str] = []

        def chunks():
            consumed.append("oversized")
            yield b"ab"
            consumed.append("past-bound")
            pytest.fail("stream must stop after crossing its declared size")

        with pytest.raises(ArtifactIntegrityError, match="more than 1 bytes"):
            copy_verified_chunks(
                chunks(),
                artifact_id=None,
                artifact_type="tool input",
                uri="https://downloads.example/input",
                destination=str(destination),
                expected_digest=compute_content_digest(b"a"),
                expected_size=1,
            )

        assert consumed == ["oversized"]
        assert not destination.exists()

    def test_stored_download_redirect_is_rejected_and_redacted(
        self, tmp_path, monkeypatch
    ):
        uri = "https://downloads.example/result?signature=fake-secret"
        client = httpx.Client(
            transport=httpx.MockTransport(
                lambda _request: httpx.Response(
                    307,
                    headers={"Location": "https://redirect.example/token-value"},
                )
            ),
            follow_redirects=False,
            trust_env=False,
        )
        monkeypatch.setattr(transport_mod, "_http_client", lambda: client)

        with pytest.raises(EndpointTransportError) as exc_info:
            InlineTransport().download_outputs(
                uri,
                str(tmp_path / "outputs"),
                policy=_policy(outputs=("https://downloads.example",)),
            )

        message = str(exc_info.value)
        assert "redirect refused" in message
        for secret in ("signature", "fake-secret", "redirect.example", "token-value"):
            assert secret not in message

    def test_presigned_origin_is_rechecked_before_s3_upload(
        self, tmp_path, monkeypatch
    ):
        fake = _FakeFs()
        monkeypatch.setattr(
            transport_mod,
            "_resolve_s3",
            _fake_s3_resolver(fake),
        )

        with pytest.raises(EndpointTransportError) as exc_info:
            upload_outputs(
                _make_outputs(tmp_path),
                ["out.txt"],
                "s3://bucket/results",
                "op",
                policy=_policy(outputs=("s3://bucket/results",)),
            )

        assert fake.puts == []
        assert "sig=abc" not in str(exc_info.value)

    def test_off_policy_output_denied_before_spool_or_resolver(
        self, tmp_path, monkeypatch, capture_tempdirs
    ):
        resolver = MagicMock()
        monkeypatch.setattr(transport_mod, "_resolve_s3", resolver)
        created = capture_tempdirs()

        with pytest.raises(ValueError, match="not allowed"):
            upload_outputs(
                _make_outputs(tmp_path),
                ["out.txt"],
                "s3://bucket/private",
                "op",
                policy=_policy(outputs=("s3://bucket/public",)),
            )

        assert created == []
        resolver.assert_not_called()

    def test_credentialed_resolver_rejects_non_s3_without_discovery(self):
        with pytest.raises(ValueError, match="supports s3:// only"):
            transport_mod._resolve_s3("file:///tmp/private")


class TestUploadOutputsMinIO:
    """Stored delivery end to end against MinIO (s3 marker via ``s3_fs``)."""

    def test_prefix_mode_uploads_and_presigned_get_fetches(
        self, s3_fs, tmp_path, configure_ambient_s3
    ):
        import s3fs as s3fs_mod

        _fs, storage, uri_prefix = s3_fs
        configure_ambient_s3(storage)

        stored = upload_outputs(
            _make_outputs(tmp_path),
            ["out.txt", "nested/deep.txt"],
            f"{uri_prefix}/results",
            "my_op",
            policy=_policy(
                outputs=(
                    f"{uri_prefix}/results",
                    _origin(storage.options["client_kwargs"]["endpoint_url"]),
                )
            ),
        )

        s3fs_mod.S3FileSystem.clear_instance_cache()
        # the consumer story: plain HTTP GET, no AWS credentials
        response = httpx.get(stored.presigned_url)
        response.raise_for_status()
        dest = tmp_path / "extracted"
        InlineTransport().unpack_outputs(response.content, str(dest))
        assert (dest / "out.txt").read_text() == "payload"
        assert (dest / "nested" / "deep.txt").read_text() == "deep"

    def test_capability_mode_delivers_via_presigned_put(self, s3_fs, tmp_path):
        from datetime import timedelta
        from urllib.parse import urlparse

        from minio import Minio

        fs, storage, uri_prefix = s3_fs
        bucket = uri_prefix.removeprefix("s3://")
        # the caller mints a presigned PUT for its own bucket; the worker
        # delivers through it with no store credentials of its own
        minted = Minio(
            urlparse(storage.options["client_kwargs"]["endpoint_url"]).netloc,
            access_key=storage.options["key"],
            secret_key=storage.options["secret"],
            secure=False,
        ).presigned_put_object(bucket, "run42.tar.gz", expires=timedelta(minutes=10))

        stored = upload_outputs(
            _make_outputs(tmp_path),
            ["out.txt"],
            minted,
            "my_op",
            policy=_policy(outputs=(_origin(minted),)),
        )

        assert stored.presigned_url is None
        local = tmp_path / "fetched.tar.gz"
        fs.get(f"{bucket}/run42.tar.gz", str(local))
        dest = tmp_path / "extracted"
        InlineTransport().unpack_outputs(local.read_bytes(), str(dest))
        assert (dest / "out.txt").read_text() == "payload"


class TestUnpackInputsMinIO:
    """Worker-side URI fetch end to end against MinIO (s3 marker via ``s3_fs``).

    ``AWS_ENDPOINT_URL`` is set so the endpoint-resolution path is
    exercised, not default AWS routing — MinIO tolerates shapes real
    stores reject, so the round-trip must run against a custom endpoint.
    """

    def test_uri_ref_fetched_from_ambient_config(
        self, s3_fs, tmp_path, configure_ambient_s3
    ):
        fs, storage, uri_prefix = s3_fs
        bucket = uri_prefix.removeprefix("s3://")
        fs.pipe_file(f"{bucket}/inputs/model.bin", b"weights-bytes")
        configure_ambient_s3(storage)

        dest = tmp_path / "inputs"
        paths = InlineTransport().unpack_inputs(
            [
                InputRef(
                    name="weights",
                    filename="model.bin",
                    uri=f"{uri_prefix}/inputs/model.bin",
                    content_digest=compute_content_digest(b"weights-bytes"),
                    size_bytes=len(b"weights-bytes"),
                )
            ],
            str(dest),
            policy=_policy(inputs=(f"{uri_prefix}/inputs",)),
        )

        # lands under its basename with correct bytes — the URI never
        # touched the inline cap
        assert Path(paths["weights"]).name == "model.bin"
        assert Path(paths["weights"]).read_bytes() == b"weights-bytes"

    @pytest.mark.parametrize(
        ("object_key", "uri_key"),
        [
            ("inputs/space key.bin", "inputs/space%20key.bin"),
            ("inputs/hash#key.bin", "inputs/hash%23key.bin"),
            ("inputs/café.bin", "inputs/caf%C3%A9.bin"),  # codespell:ignore caf
            ("inputs/literal%20.bin", "inputs/literal%2520.bin"),
        ],
    )
    def test_encoded_s3_uri_opens_exact_authorized_object(
        self, object_key, uri_key, s3_fs, tmp_path, configure_ambient_s3
    ):
        fs, storage, uri_prefix = s3_fs
        bucket = uri_prefix.removeprefix("s3://")
        body = f"content:{object_key}".encode()
        fs.pipe_file(f"{bucket}/{object_key}", body)
        configure_ambient_s3(storage)

        paths = InlineTransport().unpack_inputs(
            [
                InputRef(
                    name="data",
                    filename="input.bin",
                    uri=f"{uri_prefix}/{uri_key}",
                    content_digest=compute_content_digest(body),
                    size_bytes=len(body),
                )
            ],
            str(tmp_path / "inputs"),
            policy=_policy(inputs=(f"{uri_prefix}/inputs",)),
        )

        assert Path(paths["data"]).read_bytes() == body

    def test_missing_key_surfaces_as_input_resolution_failed(
        self, s3_fs, tmp_path, configure_ambient_s3
    ):
        from artisan.execution.tool_endpoint.protocol import ToolRequest
        from artisan.execution.tool_endpoint.server import run_tool_request
        from artisan.operations.examples import WaitTool

        _fs, storage, uri_prefix = s3_fs
        configure_ambient_s3(storage)

        # a ref to a missing object: s3fs maps 404 → FileNotFoundError,
        # which run_tool_request maps to INPUT_RESOLUTION_FAILED before any
        # compute runs (output_tar is None — the fetch precedes the tool)
        result = run_tool_request(
            WaitTool,
            ToolRequest(
                params={"seconds": 1},
                inputs=[
                    InputRef(
                        name="dataset",
                        filename="missing.csv",
                        uri=f"{uri_prefix}/inputs/does-not-exist.csv",
                        content_digest="a" * 32,
                        size_bytes=1,
                    )
                ],
            ),
            data_policy=_policy(inputs=(f"{uri_prefix}/inputs",)),
        )
        assert result.output_tar is None
        assert result.manifest.error is not None
        assert result.manifest.error.code == "input_resolution_failed"
        assert result.manifest.error.recovery_hint == "CHECK_INPUT"
