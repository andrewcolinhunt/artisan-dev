"""Filename manipulation utilities.

Provides functions for stripping extensions and suffixes from filenames,
primarily used for lineage inference and file matching.
"""

from __future__ import annotations

from pathlib import Path


def strip_extensions(
    filename: str,
    *,
    strip_all: bool = True,
    suffixes_to_strip: list[str] | None = None,
) -> str:
    """Strip file extensions and optional suffixes from a filename.

    Remove extensions from a filename or path string. By default, all
    extensions are removed (e.g. ``"data.tar.gz"`` becomes ``"data"``).
    When *suffixes_to_strip* is provided, those trailing substrings are
    also removed from the stem after extension stripping.

    Args:
        filename: Filename or path string to process.
        strip_all: If True, remove all extensions; if False, remove only
            the final extension. Defaults to True.
        suffixes_to_strip: Additional trailing substrings to remove from
            the stem after extension stripping.

    Returns:
        Bare stem with extensions (and optional suffixes) removed.

    Example:
        >>> strip_extensions("archive.tar.gz")
        'archive'
        >>> strip_extensions("archive.tar.gz", strip_all=False)
        'archive.tar'
    """

    if strip_all and not suffixes_to_strip:
        # Fast path: pure string ops, no Path objects.
        # Extract basename (everything after last '/')
        slash_pos = filename.rfind("/")
        name = filename if slash_pos < 0 else filename[slash_pos + 1 :]
        # Find first real extension dot: position > 0, followed by a non-dot char.
        # This matches Path.stem behavior for edge cases like "file..txt" -> "file."
        for i in range(1, len(name)):
            if name[i] == "." and i + 1 < len(name) and name[i + 1] != ".":
                return name[:i]
        return name

    file_path = Path(filename)

    if strip_all:
        # Remove all extensions by repeatedly taking stem
        name = file_path.name
        while "." in name:
            stem = Path(name).stem
            # Prevent infinite loop for dotfiles like .gitignore
            # where stem == name (no extension to strip)
            if stem == name:
                break
            name = stem
    else:
        # Remove only final extension (standard Path.stem behavior)
        name = file_path.stem

    # Remove specified suffixes in order
    if suffixes_to_strip:
        for suffix in suffixes_to_strip:
            if name.endswith(suffix):
                name = name[: -len(suffix)]

    return name
