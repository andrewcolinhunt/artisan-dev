"""Read the ELF machine only after validating Apptainer's ELF selection inputs."""

from __future__ import annotations

import os
import struct
from pathlib import Path
from typing import BinaryIO


class _InvalidELF(ValueError):
    """A file the pinned Go ELF reader cannot open."""


def elf_machine(path: Path) -> int | None:
    """Return the machine for a structurally readable ELF, or no match.

    Raises:
        ValueError: When a compressed or oversized section-name table exceeds the
            qualified reader profile. Such a file must not silently disappear from identity.
    """
    try:
        with path.open("rb") as stream:
            return _machine(stream)
    except (OSError, _InvalidELF, struct.error):
        return None


def _read(stream: BinaryIO, offset: int, size: int) -> bytes:
    if not 0 <= offset < 2**63 or not 0 <= size < 2**63:
        msg = "Invalid ELF file range"
        raise _InvalidELF(msg)
    stream.seek(offset)
    data = stream.read(size)
    if len(data) != size:
        msg = "Truncated ELF data"
        raise _InvalidELF(msg)
    return data


def _table_range(stream: BinaryIO, offset: int, count: int, stride: int) -> None:
    if offset >= 2**63 or (
        count and offset + count * stride > os.fstat(stream.fileno()).st_size
    ):
        msg = "Truncated ELF header table"
        raise _InvalidELF(msg)


def _machine(stream: BinaryIO) -> int:
    ident = _read(stream, 0, 16)
    if (
        ident[:4] != b"\x7fELF"
        or ident[4] not in (1, 2)
        or ident[5] not in (1, 2)
        or ident[6] != 1
    ):
        msg = "Invalid ELF identifier"
        raise _InvalidELF(msg)
    order = "<" if ident[5] == 1 else ">"
    wide = ident[4] == 2
    header = struct.Struct(order + ("HHIQQQIHHHHHH" if wide else "HHIIIIIHHHHHH"))
    section = struct.Struct(order + ("IIQQQQIIQQ" if wide else "IIIIIIIIII"))
    program = struct.Struct(order + ("IIQQQQQQ" if wide else "IIIIIIII"))
    fields = header.unpack(_read(stream, 16, header.size))
    (
        _,
        machine,
        version,
        _,
        phoff,
        shoff,
        _,
        _,
        phstride,
        phcount,
        shstride,
        shcount,
        names,
    ) = fields
    if version != 1 or (shoff == 0 and shcount) or (shcount and names >= shcount):
        msg = "Invalid ELF header"
        raise _InvalidELF(msg)
    if phcount and phstride < program.size:
        msg = "Invalid ELF program-header size"
        raise _InvalidELF(msg)
    # Apptainer's Go 1.25.7 reader uses the program count literally, even 65535.
    _table_range(stream, phoff, phcount, phstride)
    _programs(stream, program, phoff, phcount, phstride, wide)
    if shoff and shcount == 0:
        initial = section.unpack(_read(stream, shoff, section.size))
        shcount, names = _extended_sections(initial, names)
    if shcount and shstride < section.size:
        msg = "Invalid ELF section-header size"
        raise _InvalidELF(msg)
    _table_range(stream, shoff, shcount, shstride)
    _sections(stream, section, shoff, shcount, shstride, names, wide)
    return int(machine)


def _extended_sections(initial: tuple[int, ...], names: int) -> tuple[int, int]:
    if initial[1] != 0:
        msg = "Invalid initial ELF section"
        raise _InvalidELF(msg)
    count = initial[5]
    if count < 0xFF00:
        msg = "Invalid extended ELF section count"
        raise _InvalidELF(msg)
    if names == 0xFFFF:
        names = initial[6]
        if names < 0xFF00 or names >= count:
            msg = "Invalid extended ELF name-table index"
            raise _InvalidELF(msg)
    return count, names


def _programs(
    stream: BinaryIO,
    layout: struct.Struct,
    offset: int,
    count: int,
    stride: int,
    wide: bool,
) -> None:
    for index in range(count):
        entry = layout.unpack(_read(stream, offset + index * stride, layout.size))
        source, size = (entry[2], entry[5]) if wide else (entry[1], entry[4])
        if source >= 2**63 or size >= 2**63:
            msg = "Invalid ELF segment range"
            raise _InvalidELF(msg)


def _sections(
    stream: BinaryIO,
    layout: struct.Struct,
    offset: int,
    count: int,
    stride: int,
    names: int,
    wide: bool,
) -> None:
    name_offsets = []
    name_table = None
    for index in range(count):
        entry = layout.unpack(_read(stream, offset + index * stride, layout.size))
        name, kind, flags, _, source, size, _, _, _, _ = entry
        if source >= 2**63 or size >= 2**63:
            msg = "Invalid ELF section range"
            raise _InvalidELF(msg)
        if flags & 0x800:
            compression_size = 24 if wide else 12
            if size < compression_size:
                msg = "Truncated ELF compression header"
                raise _InvalidELF(msg)
            _read(stream, source, compression_size)
        name_offsets.append(name)
        if index == names:
            name_table = (kind, flags, source, size)
    if not count or names == 0:
        return
    if name_table is None or name_table[0] != 3:
        msg = "Invalid ELF section-name table"
        raise _InvalidELF(msg)
    _, flags, source, size = name_table
    if flags & 0x800:
        msg = (
            "Apptainer GPU identity does not support compressed ELF section-name tables"
        )
        raise ValueError(msg)
    if size > 64 * 1024 * 1024:
        msg = "Apptainer GPU ELF section-name table exceeds the qualified limit"
        raise ValueError(msg)
    table = _read(stream, source, size)
    if any(name >= len(table) or table.find(b"\0", name) < 0 for name in name_offsets):
        msg = "Invalid ELF section name"
        raise _InvalidELF(msg)
