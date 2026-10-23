"""Filename manipulation utilities.

Provides functions for stripping extensions and suffixes from filenames,
used by materialization and operation-owned filename matching.
"""

from __future__ import annotations

import posixpath


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

    name = posixpath.basename(filename)
    if strip_all:
        # Preserve leading, consecutive, and trailing dots in the stem.
        for i in range(1, len(name)):
            if name[i] == "." and i + 1 < len(name) and name[i + 1] != ".":
                name = name[:i]
                break
    else:
        name, _ = posixpath.splitext(name)

    if suffixes_to_strip:
        for suffix in suffixes_to_strip:
            if name.endswith(suffix):
                name = name[: -len(suffix)]

    return name
