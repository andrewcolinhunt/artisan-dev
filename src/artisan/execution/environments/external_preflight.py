"""Encode small external-access evidence for the qualified static command witness."""

from __future__ import annotations

import struct
from pathlib import Path

from artisan.schemas.artifact.external_descriptor import (
    PreparedExternalFiles,
    expected_receipt_bytes,
)

MAGIC = b"ARTEXT1\0"
MAX_RECORD_BYTES = 16 * 1024 * 1024


def _field(value: bytes) -> bytes:
    return struct.pack("!I", len(value)) + value


def encode_external_preflight(prepared: tuple[PreparedExternalFiles, ...]) -> bytes:
    """Serialize expected roots, receipts and file sizes without reading payloads."""
    chunks = [MAGIC, struct.pack("!I", len(prepared))]
    for item in prepared:
        chunks.extend(
            (
                _field(str(item.root).encode()),
                _field(expected_receipt_bytes(item.descriptor)),
                struct.pack("!I", len(item.descriptor.files)),
            )
        )
        for member in item.descriptor.files:
            chunks.extend(
                (_field(member.path.encode()), struct.pack("!Q", member.size_bytes))
            )
    encoded = b"".join(chunks)
    if not prepared or len(encoded) > MAX_RECORD_BYTES:
        msg = "External command access evidence must be nonempty and at most 16 MiB"
        raise ValueError(msg)
    return encoded


def write_external_preflight(
    prepared: tuple[PreparedExternalFiles, ...], path: Path
) -> Path:
    """Write one immutable invocation descriptor in the readonly control area."""
    encoded = encode_external_preflight(prepared)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as stream:
        stream.write(encoded)
    path.chmod(0o444)
    return path
