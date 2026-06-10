"""Tests for the inline data transport."""

from __future__ import annotations

import shutil
import tarfile
from pathlib import Path

import pytest

from artisan.execution.tool_endpoint import transport as transport_mod
from artisan.execution.tool_endpoint.protocol import InputRef
from artisan.execution.tool_endpoint.transport import InlineTransport


class _FakeFs:
    """fsspec stand-in: records get() calls and writes a marker file."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    def get(self, remote: str, local: str) -> None:
        self.calls.append((remote, local))
        Path(local).write_bytes(b"remote-bytes")


class TestPackInputs:
    def test_local_files_become_inline_refs(self, tmp_path: Path):
        src = tmp_path / "input.pdb"
        src.write_bytes(b"ATOM")
        refs = InlineTransport().pack_inputs({"pdb": str(src)})
        assert refs == [InputRef(name="pdb", data=b"ATOM")]

    def test_uri_passes_through_without_reading(self):
        refs = InlineTransport().pack_inputs({"pdb": "s3://bucket/key.pdb"})
        assert refs == [InputRef(name="pdb", uri="s3://bucket/key.pdb")]

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

    def test_name_is_sanitized_to_basename(self, tmp_path: Path):
        paths = InlineTransport().unpack_inputs(
            [InputRef(name="../evil.txt", data=b"x")], str(tmp_path)
        )
        assert Path(paths["../evil.txt"]).parent == tmp_path

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

    def test_pack_over_limit_raises(self, tmp_path: Path, monkeypatch):
        monkeypatch.setattr(transport_mod, "MAX_INLINE_BYTES", 4)
        src = tmp_path / "outputs"
        src.mkdir()
        (src / "big.bin").write_bytes(b"x" * 1024)
        with pytest.raises(ValueError, match="Output tar exceeds"):
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
