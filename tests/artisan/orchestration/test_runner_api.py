"""Contract tests for the public third-party runner facade."""

from __future__ import annotations

from artisan.orchestration.runner_api import (
    BatchStrategy,
    ExecutionUnit,
    LifecycleRouter,
    OrchestratorTraits,
    RunnerBase,
    RunnerResources,
    RuntimeEnvironment,
    UnitResult,
    WorkerTraits,
    execute_unit,
    execute_unit_batch,
    failure_results_for_units,
    pack_units,
    validate_batch_results,
)


def test_runner_api_exports_provider_contract() -> None:
    """Every provider-facing contract component imports from one module."""
    assert RunnerBase
    assert WorkerTraits
    assert OrchestratorTraits
    assert LifecycleRouter
    assert ExecutionUnit
    assert RuntimeEnvironment
    assert UnitResult
    assert RunnerResources
    assert BatchStrategy
    assert pack_units
    assert execute_unit
    assert execute_unit_batch
    assert failure_results_for_units
    assert validate_batch_results
