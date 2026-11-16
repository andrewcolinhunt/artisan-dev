"""Structural selection checks for the qualified Apptainer ELF reader."""

from __future__ import annotations

import struct
from pathlib import Path

import pytest

from artisan.execution.environments.apptainer_elf import elf_machine


def _elf(*, machine: int = 62, sections: bool = False) -> bytes:
    ident = b"\x7fELF\x02\x01\x01" + bytes(9)
    header = struct.pack(
        "<HHIQQQIHHHHHH",
        3,
        machine,
        1,
        0,
        0,
        64 if sections else 0,
        0,
        64,
        0,
        0,
        64,
        2 if sections else 0,
        1 if sections else 0,
    )
    if not sections:
        return ident + header
    null = bytes(64)
    strings = struct.pack("<IIQQQQIIQQ", 1, 3, 0, 0, 192, 11, 0, 0, 1, 0)
    return ident + header + null + strings + b"\0.shstrtab\0"


@pytest.mark.parametrize("machine", [62, 183])
def test_elf_machine_reads_complete_header(tmp_path: Path, machine: int) -> None:
    path = tmp_path / "library.so"
    path.write_bytes(_elf(machine=machine))
    assert elf_machine(path) == machine


def test_elf_machine_reads_section_names(tmp_path: Path) -> None:
    path = tmp_path / "library.so"
    path.write_bytes(_elf(sections=True))
    assert elf_machine(path) == 62


@pytest.mark.parametrize("content", [b"text", _elf()[:20], _elf(sections=True)[:-1]])
def test_elf_machine_rejects_truncated_structure(
    tmp_path: Path, content: bytes
) -> None:
    path = tmp_path / "library.so"
    path.write_bytes(content)
    assert elf_machine(path) is None


def test_elf_machine_rejects_invalid_name_index(tmp_path: Path) -> None:
    path = tmp_path / "library.so"
    content = bytearray(_elf(sections=True))
    struct.pack_into("<I", content, 128, 200)
    path.write_bytes(content)
    assert elf_machine(path) is None


def test_elf_machine_rejects_invalid_program_size(tmp_path: Path) -> None:
    path = tmp_path / "library.so"
    content = bytearray(_elf())
    struct.pack_into("<HH", content, 54, 1, 1)
    path.write_bytes(content)
    assert elf_machine(path) is None


def test_elf_machine_rejects_missing_file(tmp_path: Path) -> None:
    assert elf_machine(tmp_path / "missing") is None


@pytest.mark.parametrize(("order", "encoding"), [("<", 1), (">", 2)])
def test_elf_machine_reads_32_bit_endianness(
    tmp_path: Path, order: str, encoding: int
) -> None:
    path = tmp_path / "library.so"
    ident = b"\x7fELF" + bytes([1, encoding, 1]) + bytes(9)
    header = struct.pack(
        order + "HHIIIIIHHHHHH", 3, 3, 1, 0, 0, 0, 0, 52, 0, 0, 40, 0, 0
    )
    path.write_bytes(ident + header)
    assert elf_machine(path) == 3


def test_elf_machine_rejects_compressed_names_explicitly(tmp_path: Path) -> None:
    path = tmp_path / "library.so"
    content = bytearray(_elf(sections=True))
    struct.pack_into("<Q", content, 136, 0x800)
    struct.pack_into("<Q", content, 160, 35)
    content[192:] = struct.pack("<IIQQ", 1, 0, 11, 1) + b"\0.shstrtab\0"
    path.write_bytes(content)
    with pytest.raises(ValueError, match="compressed ELF section-name"):
        elf_machine(path)


def test_elf_machine_accepts_compressed_non_name_section(tmp_path: Path) -> None:
    path = tmp_path / "library.so"
    content = bytearray(_elf(sections=True))
    struct.pack_into("<H", content, 62, 0)
    struct.pack_into("<Q", content, 136, 0x800)
    struct.pack_into("<Q", content, 160, 35)
    content[192:] = struct.pack("<IIQQ", 1, 0, 11, 1) + b"\0.shstrtab\0"
    path.write_bytes(content)
    assert elf_machine(path) == 62


def test_elf_machine_uses_literal_maximum_program_count(tmp_path: Path) -> None:
    path = tmp_path / "library.so"
    content = bytearray(_elf(sections=True))
    program_offset = len(content)
    struct.pack_into("<Q", content, 32, program_offset)
    struct.pack_into("<HH", content, 54, 56, 0xFFFF)
    content.extend(bytes(56 * 0xFFFF))
    path.write_bytes(content)
    assert elf_machine(path) == 62


def test_elf_machine_reads_extended_section_name_index(tmp_path: Path) -> None:
    path = tmp_path / "library.so"
    count, name_index = 0xFF01, 0xFF00
    content = bytearray(_elf())
    struct.pack_into("<Q", content, 40, 64)
    struct.pack_into("<HHH", content, 58, 64, 0, 0xFFFF)
    content.extend(bytes(64 * count))
    struct.pack_into("<Q", content, 64 + 32, count)
    struct.pack_into("<I", content, 64 + 40, name_index)
    name_section = 64 + 64 * name_index
    struct.pack_into(
        "<IIQQQQIIQQ", content, name_section, 0, 3, 0, 0, len(content), 1, 0, 0, 1, 0
    )
    content.extend(b"\0")
    path.write_bytes(content)
    assert elf_machine(path) == 62


def test_elf_machine_rejects_extended_names_without_extended_sections(
    tmp_path: Path,
) -> None:
    path = tmp_path / "library.so"
    content = bytearray(_elf(sections=True))
    struct.pack_into("<H", content, 62, 0xFFFF)
    struct.pack_into("<I", content, 64 + 40, 1)
    path.write_bytes(content)
    assert elf_machine(path) is None
