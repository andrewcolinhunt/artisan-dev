"""Pipeline bootstrap happens before storage and never imports saved requirements."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import Mock

import pytest

from artisan.errors import ApplicationRegistrationError
from artisan.orchestration import PipelineConfig, PipelineManager
from artisan.schemas.orchestration.step_start_record import StepStartRecord


def test_constructor_rejects_missing_application_before_store_creation(
    tmp_path: Path,
) -> None:
    config = PipelineConfig(
        name="test",
        delta_root=str(tmp_path / "new_delta"),
        staging_root=str(tmp_path / "new_staging"),
        application_modules=("missing_manager_application",),
    )
    with pytest.raises(ApplicationRegistrationError):
        PipelineManager(config)
    assert not (tmp_path / "new_delta").exists()
    assert not (tmp_path / "new_staging").exists()


def test_direct_configuration_does_not_merge_environment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("ARTISAN_LOAD_MODULES", "missing_ambient_application")
    config = PipelineConfig(
        name="test",
        delta_root=str(tmp_path / "delta"),
        staging_root=str(tmp_path / "staging"),
    )
    with PipelineManager(config) as manager:
        assert manager.config.application_modules == ()


@pytest.mark.parametrize(
    "recorded", [["missing_recorded_application"], "malformed", [42], None]
)
def test_resume_checks_all_attempt_requirements_before_recovery(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, recorded: object
) -> None:
    monkeypatch.delenv("ARTISAN_LOAD_MODULES", raising=False)
    delta, staging = str(tmp_path / "delta"), str(tmp_path / "staging")
    with PipelineManager.create(
        "source", delta_root=delta, staging_root=staging
    ) as manager:
        run = manager.config.pipeline_run_id
        for number, modules in enumerate((recorded, [])):
            manager._step_tracker.create_attempt(
                StepStartRecord(
                    step_run_id=str(number) * 32,
                    step_number=0,
                    step_name="attempt",
                    operation_class="artisan.operations.examples.DataGenerator",
                    params_json="{}",
                    input_refs_json="{}",
                    compute_backend="local",
                    output_roles_json="[]",
                    output_types_json="{}",
                    compute_options_json=json.dumps(
                        {
                            "application_modules": modules,
                            "pipeline_default_step_runner": "local",
                        }
                    ),
                )
            )
    repair = Mock(side_effect=AssertionError("recovery before requirement check"))
    monkeypatch.setattr("artisan.storage.io.repair.repair_store", repair)
    with pytest.raises(ApplicationRegistrationError):
        PipelineManager.resume(
            delta_root=delta, staging_root=staging, pipeline_run_id=run
        )
    repair.assert_not_called()
