"""Tests for OutputSource protocol compliance."""

from __future__ import annotations

from concurrent.futures import Future

from artisan.orchestration.step_future import StepFuture
from artisan.schemas.composites.composite_ref import ExpandedCompositeResult
from artisan.schemas.orchestration.output_reference import OutputReference
from artisan.schemas.orchestration.output_source import OutputSource
from artisan.schemas.orchestration.step_result import StepResult


class TestOutputSourceProtocol:
    """Verify OutputSource protocol compliance for all implementing types."""

    def test_step_future_satisfies_protocol(self):
        """StepFuture is recognized as an OutputSource."""
        future = StepFuture(
            step_number=0,
            step_name="Test",
            output_roles=frozenset(["data"]),
            output_types={"data": "data"},
            future=Future(),
        )
        assert isinstance(future, OutputSource)

    def test_step_result_satisfies_protocol(self):
        """StepResult is recognized as an OutputSource."""
        result = StepResult(
            step_name="Test",
            step_number=0,
            success=True,
            output_roles=frozenset(["data"]),
            output_types={"data": "data"},
        )
        assert isinstance(result, OutputSource)

    def test_expanded_composite_result_satisfies_protocol(self):
        """ExpandedCompositeResult is recognized as an OutputSource."""
        result = ExpandedCompositeResult(
            output_map={"data": OutputReference(source_step=0, role="data")},
            output_types={"data": "data"},
        )
        assert isinstance(result, OutputSource)
