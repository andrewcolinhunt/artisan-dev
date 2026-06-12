"""Tests for the inline and stored data transports."""

from __future__ import annotations

import re
import shutil
import tarfile
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest

from artisan.execution.tool_endpoint import transport as transport_mod
from artisan.execution.tool_endpoint.protocol import InputRef
from artisan.execution.tool_endpoint.transport import InlineTransport, upload_outputs


class _FakeFs:
    """fsspec stand-in: records get/put/sign calls; get writes a marker file."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []
        self.puts: list[tuple[str, str]] = []
        self.signed: list[tuple[str, int]] = []
        self.put_bytes = b""

    def get(self, remote: str, local: str) -> None:
        self.calls.append((remote, local))
        Path(local).write_bytes(b"remote-bytes")

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


class TestPackInputs:
    def test_local_files_become_inline_refs(self, tmp_path: Path):
        src = tmp_path / "input.pdb"
        src.write_bytes(b"ATOM")
        refs = InlineTransport().pack_inputs({"pdb": str(src)})
        assert refs == [InputRef(name="pdb", filename="input.pdb", data=b"ATOM")]

    def test_uri_passes_through_without_reading(self):
        refs = InlineTransport().pack_inputs({"pdb": "s3://bucket/key.pdb"})
        assert refs == [
            InputRef(name="pdb", filename="key.pdb", uri="s3://bucket/key.pdb")
        ]

    def test_over_limit_raises(self, tmp_path: Path, monkeypatch):
        monkeypatch.setattr(transport_mod, "MAX_INLINE_BYTES", 4)
        src = tmp_path / "big.bin"
        src.write_bytes(b"x" * 5)
        with pytest.raises(ValueError, match="s3://"):
            InlineTransport().pack_inputs({"big": str(src)})


class TestUnpackInputs:
    def test_inline_ref_written_to_dest(self, tmp_path: Path):
        dest = tmp_path / "inputs"
        paths = InlineTransport().unpack_inputs(
            [InputRef(name="pdb", data=b"ATOM")], str(dest)
        )
        assert Path(paths["pdb"]).read_bytes() == b"ATOM"
        assert Path(paths["pdb"]).parent == dest

    def test_uri_ref_fetched_via_fs(self, tmp_path: Path):
        fs = _FakeFs()
        paths = InlineTransport().unpack_inputs(
            [InputRef(name="pdb", uri="s3://bucket/key.pdb")],
            str(tmp_path),
            fs=fs,
        )
        assert fs.calls == [("s3://bucket/key.pdb", paths["pdb"])]
        assert Path(paths["pdb"]).read_bytes() == b"remote-bytes"

    def test_original_filename_preserved_on_disk(self, tmp_path: Path):
        """Lineage stem-matching needs the worker-side basename to match local."""
        src = tmp_path / "dataset_00001.csv"
        src.write_bytes(b"a,b\n1,2\n")
        refs = InlineTransport().pack_inputs({"dataset": str(src)})
        paths = InlineTransport().unpack_inputs(refs, str(tmp_path / "inputs"))
        assert Path(paths["dataset"]).name == "dataset_00001.csv"

    def test_name_is_sanitized_to_basename(self, tmp_path: Path):
        paths = InlineTransport().unpack_inputs(
            [InputRef(name="../evil.txt", data=b"x")], str(tmp_path)
        )
        assert Path(paths["../evil.txt"]).parent == tmp_path

    def test_filename_is_sanitized_to_basename(self, tmp_path: Path):
        paths = InlineTransport().unpack_inputs(
            [InputRef(name="x", filename="../../evil.txt", data=b"x")], str(tmp_path)
        )
        assert Path(paths["x"]).parent == tmp_path

    def test_empty_ref_raises(self, tmp_path: Path):
        with pytest.raises(ValueError, match="neither uri nor data"):
            InlineTransport().unpack_inputs([InputRef(name="x")], str(tmp_path))


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
        import io

        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w") as tar:
            payload_file = tmp_path / "x.txt"
            payload_file.write_text("evil")
            tar.add(str(payload_file), arcname="../escape.txt")
        dest = tmp_path / "dest"
        with pytest.raises(tarfile.TarError):
            InlineTransport().unpack_outputs(buf.getvalue(), str(dest))
        shutil.rmtree(dest, ignore_errors=True)


class TestUploadOutputsPrefixMode:
    @pytest.fixture
    def fake_fs(self, monkeypatch) -> _FakeFs:
        fake = _FakeFs()
        monkeypatch.setattr(transport_mod, "_resolve_fs", lambda uri, fs: (fake, uri))
        return fake

    def test_gzipped_tar_uploaded_under_namespaced_key(self, tmp_path, fake_fs):
        stored = upload_outputs(
            _make_outputs(tmp_path),
            ["out.txt", "nested/deep.txt"],
            "s3://bucket/prefix",
            "my_op",
        )
        assert re.fullmatch(
            r"s3://bucket/prefix/my_op/[0-9a-f]{32}\.tar\.gz", stored.uri
        )
        assert fake_fs.puts[0][1] == stored.uri
        # the gzipped tar extracts via the existing client-side unpack
        dest = tmp_path / "extracted"
        InlineTransport().unpack_outputs(fake_fs.put_bytes, str(dest))
        assert (dest / "out.txt").read_text() == "payload"
        assert (dest / "nested" / "deep.txt").read_text() == "deep"

    def test_presigned_get_minted_with_max_expiry(self, tmp_path, fake_fs):
        stored = upload_outputs(_make_outputs(tmp_path), ["out.txt"], "s3://b/p", "op")
        assert stored.presigned_url is not None
        assert stored.presigned_url.startswith("https://signed.example/")
        assert fake_fs.signed == [(stored.uri, transport_mod.PRESIGN_EXPIRY_SECONDS)]
        assert transport_mod.PRESIGN_EXPIRY_SECONDS == 7 * 24 * 3600

    def test_non_signing_fs_propagates(self, tmp_path, monkeypatch):
        class _NoSignFs(_FakeFs):
            def sign(self, remote: str, expiration: int = 100) -> str:
                raise NotImplementedError("Sign is not implemented for this fs")

        fake = _NoSignFs()
        monkeypatch.setattr(transport_mod, "_resolve_fs", lambda uri, fs: (fake, uri))
        with pytest.raises(NotImplementedError):
            upload_outputs(_make_outputs(tmp_path), ["out.txt"], "file:///tmp/x", "op")


class TestUploadOutputsCapabilityMode:
    PUT_URL = "https://bucket.s3.amazonaws.com/run42.tar.gz?X-Amz-Signature=abc"

    def test_put_streams_spool_with_explicit_content_length(
        self, tmp_path, monkeypatch
    ):
        captured: dict = {}

        def fake_put(url, content=None, headers=None, timeout="unset"):
            captured.update(
                url=url, body=content.read(), headers=headers, timeout=timeout
            )
            return SimpleNamespace(raise_for_status=lambda: None)

        monkeypatch.setattr("httpx.put", fake_put)
        stored = upload_outputs(
            _make_outputs(tmp_path), ["out.txt", "nested/deep.txt"], self.PUT_URL, "op"
        )
        assert captured["url"] == self.PUT_URL
        # explicit Content-Length: chunked-TE regressions pass MinIO but
        # fail real S3 (501) — this assertion is the only guard
        assert captured["headers"] == {"Content-Length": str(len(captured["body"]))}
        assert captured["timeout"] is None
        dest = tmp_path / "extracted"
        InlineTransport().unpack_outputs(captured["body"], str(dest))
        assert (dest / "out.txt").read_text() == "payload"
        # pointer: PUT URL sans query; no presigned GET — caller owns the bucket
        assert stored.uri == "https://bucket.s3.amazonaws.com/run42.tar.gz"
        assert stored.presigned_url is None

    def test_refused_put_raises(self, tmp_path, monkeypatch):
        def fake_put(url, content=None, headers=None, timeout=None):
            return httpx.Response(403, request=httpx.Request("PUT", url))

        monkeypatch.setattr("httpx.put", fake_put)
        with pytest.raises(httpx.HTTPStatusError):
            upload_outputs(_make_outputs(tmp_path), ["out.txt"], self.PUT_URL, "op")


class TestUploadOutputsMinIO:
    """Stored delivery end to end against MinIO (s3 marker via ``s3_fs``)."""

    def test_prefix_mode_uploads_and_presigned_get_fetches(
        self, s3_fs, tmp_path, monkeypatch
    ):
        import s3fs as s3fs_mod

        _fs, storage, uri_prefix = s3_fs
        # worker-style ambient credentials: env vars, exactly how the Modal
        # Secret hands them to the worker; the instance cache would otherwise
        # serve a filesystem built before the env was patched
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", storage.options["key"])
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", storage.options["secret"])
        monkeypatch.setenv(
            "AWS_ENDPOINT_URL", storage.options["client_kwargs"]["endpoint_url"]
        )
        s3fs_mod.S3FileSystem.clear_instance_cache()

        stored = upload_outputs(
            _make_outputs(tmp_path),
            ["out.txt", "nested/deep.txt"],
            f"{uri_prefix}/results",
            "my_op",
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

        stored = upload_outputs(_make_outputs(tmp_path), ["out.txt"], minted, "my_op")

        assert stored.presigned_url is None
        local = tmp_path / "fetched.tar.gz"
        fs.get(f"{bucket}/run42.tar.gz", str(local))
        dest = tmp_path / "extracted"
        InlineTransport().unpack_outputs(local.read_bytes(), str(dest))
        assert (dest / "out.txt").read_text() == "payload"
