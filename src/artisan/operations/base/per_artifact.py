"""Per-artifact wrapper signaling that a preprocess value should be sliced.

Operations whose ``preprocess()`` returns per-artifact lists must wrap them
in ``PerArtifact(...)``. The framework slices a ``PerArtifact`` value once
per artifact in ``_split_prepared_inputs``; raw lists pass through as shared
data regardless of length.
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence


class PerArtifact[T](Sequence[T]):
    """Marker wrapper for per-artifact preprocess values.

    Preprocess returns ``{"paths": PerArtifact([p1, p2, p3]), "config": cfg}``;
    the framework slices ``paths`` per artifact and forwards ``config``
    unchanged. Raw lists are always passed through as shared data regardless
    of length — use ``PerArtifact`` to opt in to slicing.

    Implements ``Sequence[T]`` so authors can iterate and ``len()`` it just
    like a list.

    Args:
        items: The per-artifact values, one per artifact in batch order.
    """

    __slots__ = ("_items",)

    def __init__(self, items: Sequence[T]) -> None:
        self._items: list[T] = list(items)

    def __getitem__(self, index: int) -> T:  # type: ignore[override]
        return self._items[index]

    def __len__(self) -> int:
        return len(self._items)

    def __iter__(self) -> Iterator[T]:
        return iter(self._items)

    def __repr__(self) -> str:
        return f"PerArtifact({self._items!r})"
