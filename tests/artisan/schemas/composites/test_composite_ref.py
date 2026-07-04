"""Tests for the pure CompositeRef wiring reference."""

from __future__ import annotations

import pytest

from artisan.schemas.composites.composite_ref import CompositeRef
from artisan.schemas.orchestration.output_reference import OutputReference


class TestCompositeRef:
    def test_holds_output_reference(self):
        out_ref = OutputReference(source_step=0, role="data")
        ref = CompositeRef(source=None, output_reference=out_ref, role="data")
        assert ref.source is None
        assert ref.output_reference is out_ref
        assert ref.role == "data"

    def test_frozen(self):
        ref = CompositeRef(source=None, output_reference=None, role="x")
        with pytest.raises(AttributeError):
            ref.role = "y"
