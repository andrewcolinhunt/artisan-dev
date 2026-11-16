"""Pipeline bootstrap happens before storage and never imports saved requirements."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import Mock
from uuid import uuid4

import pytest
from fixtures.composition_operations import Transform

from artisan.errors import ApplicationRegistrationError
from artisan.orchestration import PipelineConfig, PipelineManager
from artisan.orchestration import pipeline_manager as module


def test_factory_rejects_missing_application_before_store_creation(
    tmp_path: Path,
) -> None:
    with pytest.raises(ApplicationRegistrationError):
        PipelineManager.create(
            name="test",
            delta_root=str(tmp_path / "new_delta"),
            staging_root=str(tmp_path / "new_staging"),
            application_modules=("missing_manager_application",),
        )
    assert not (tmp_path / "new_delta").exists()
    assert not (tmp_path / "new_staging").exists()


def test_direct_configuration_does_not_merge_environment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with PipelineManager.create(
        "test", str(tmp_path / "delta"), str(tmp_path / "staging")
    ) as original:
        pass
    monkeypatch.setenv("ARTISAN_LOAD_MODULES", "missing_ambient_application")
    config = PipelineConfig(
        name="test",
        delta_root=original.config.delta_root,
        staging_root=original.config.staging_root,
    )
    assert config.application_modules == ()
    with PipelineManager(
        original.committer,
        original.dispatch,
        pipeline_run_id="explicit-dispatch",
    ) as manager:
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
        manager.run(Transform, inputs={"structure": []})
        record = manager.executions[0].record
        options = json.loads(record.compute_options_json)
        for modules in (recorded, []):
            copied = record.model_copy(
                update={
                    "step_run_id": uuid4().hex,
                    "compute_options_json": json.dumps(
                        {**options, "application_modules": modules}
                    ),
                }
            )
            manager.tracker.create_attempt(copied)
            manager.tracker.skip_pending(copied.step_run_id, reason="pipeline_stopped")
    recover = Mock(side_effect=AssertionError("recovery before requirement check"))
    bootstrap = Mock(wraps=module.bootstrap_applications)
    monkeypatch.setattr(module, "recover_pipeline_results", recover)
    monkeypatch.setattr(module, "bootstrap_applications", bootstrap)
    with pytest.raises(ApplicationRegistrationError):
        PipelineManager.resume(
            delta_root=delta, staging_root=staging, pipeline_run_id=run
        )
    recover.assert_not_called()
    bootstrap.assert_called_once_with((), purpose="pipeline resume")
