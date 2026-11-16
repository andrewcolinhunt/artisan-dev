"""Tests for the pure CompositeRef wiring reference."""

from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

from artisan.schemas.composites.composite_ref import CompositeRef


class TestCompositeRef:
    def test_distinguishes_boundary_and_child_streams(self) -> None:
        boundary = CompositeRef(owner="composition", call=None, role="data")
        child = CompositeRef(owner="composition", call="transform", role="data")
        assert boundary != child
        assert boundary.owner == child.owner == "composition"
        assert boundary.role == child.role == "data"
        assert boundary.call is None
        assert child.call == "transform"

    def test_frozen(self) -> None:
        ref = CompositeRef(owner="composition", call=None, role="x")
        with pytest.raises(FrozenInstanceError):
            ref.role = "y"  # type: ignore[misc]
