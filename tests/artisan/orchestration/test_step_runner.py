"""Pipeline placement uses an explicit provider instance when overridden."""

from __future__ import annotations

from inspect import signature

from artisan.orchestration import PipelineManager


def test_pipeline_entry_accepts_positional_operation_and_explicit_provider() -> None:
    for method in (PipelineManager.run, PipelineManager.submit):
        parameters = signature(method).parameters
        assert list(parameters)[:2] == ["self", "operation"]
        assert parameters["step_runner"].default is None
        assert (
            not {"compute_backend", "environment", "prepared_environment", "compact"}
            & parameters.keys()
        )
