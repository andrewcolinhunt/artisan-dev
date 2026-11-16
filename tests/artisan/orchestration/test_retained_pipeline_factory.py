"""Creation and resume preserve pipeline defaults without provisioning software."""

from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path
from typing import Any
from uuid import uuid4

import polars as pl
import pytest
from fixtures.composition_operations import PredictMeasure, Transform, structure
from fixtures.curator_cache import MeasureCSV
from fixtures.retained_pipeline import NativePipelineRunner

from artisan.errors import (
    ApplicationRegistrationError,
    PersistenceIntegrityError,
    StoreIntegrityError,
)
from artisan.execution.environments.container_runtime import (
    ApptainerRuntime,
    DockerRuntime,
)
from artisan.operations.curator.merge import Merge
from artisan.orchestration import pipeline_manager as module
from artisan.orchestration.engine.current_outputs import read_current_step_outputs
from artisan.orchestration.engine.retained_step_results import RetainedStepResults
from artisan.orchestration.engine.retained_step_tracker import RetainedStepTracker
from artisan.orchestration.pipeline_manager import PipelineManager
from artisan.orchestration.runners.local import LocalRunner
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.schemas.operation_config.environment import Environment
from artisan.schemas.orchestration.step_lifecycle import (
    CancellationAcknowledgement,
    CancellationStatus,
    StepStatus,
)
from artisan.schemas.orchestration.step_start_record import StepStartRecord
from artisan.storage.core.retained_reader import RetainedReader
from artisan.storage.io.retained_commit import RetainedCommitter


class _ExternalRunner(LocalRunner):
    name = "external-test"


class _FalseyRunner(_ExternalRunner):
    def __bool__(self) -> bool:
        return False


def _create(tmp_path: Path, **overrides: Any) -> PipelineManager:
    arguments = {
        "name": "original-name",
        "delta_root": str(tmp_path / "factory-delta"),
        "staging_root": str(tmp_path / "staging"),
        "working_root": str(tmp_path / "work"),
        **overrides,
    }
    return PipelineManager.create(**arguments)


def _resume(pipeline: PipelineManager, **overrides: Any) -> PipelineManager:
    arguments = {
        "delta_root": pipeline.config.delta_root,
        "staging_root": pipeline.config.staging_root,
        "working_root": pipeline.config.working_root,
        "files_root": pipeline.config.files_root,
        "pipeline_run_id": pipeline.config.pipeline_run_id,
        **overrides,
    }
    return PipelineManager.resume(**arguments)


def _skip(pipeline: PipelineManager) -> StepStartRecord:
    assert (
        pipeline.run(Transform, inputs={"structure": []}).status == StepStatus.SKIPPED
    )
    return pipeline.executions[-1].record


def _append_skipped(
    tracker: RetainedStepTracker, record: StepStartRecord
) -> StepStartRecord:
    copied = record.model_copy(update={"step_run_id": uuid4().hex})
    tracker.create_attempt(copied)
    tracker.skip_pending(copied.step_run_id, reason="pipeline_stopped")
    return copied


def test_create_resume_science_curator_and_downstream_refs_use_existing_controllers(
    tmp_path: Path,
) -> None:
    with _create(tmp_path, preserve_working=True, preserve_staging=True) as pipeline:
        runner = NativePipelineRunner(pipeline.committer)
        source = pipeline.run(
            PredictMeasure, inputs={"structure": [structure(0)]}, step_runner=runner
        )
        selected = pipeline.submit(
            Merge, inputs=[source.output("structure")], step_runner=runner
        ).result()
        assert source.status == selected.status == StepStatus.SUCCEEDED
        assert runner.curator_launches == 1
        assert pipeline.dispatch.preserve_working
        assert pipeline.dispatch.preserve_staging
        original = read_current_step_outputs(pipeline.committer, source.step_run_id)

    with _resume(pipeline, name="display-name") as resumed:
        assert resumed.config.name == "display-name"
        assert type(resumed.runner) is LocalRunner
        assert resumed._step_run_ids == {0: source.step_run_id, 1: selected.step_run_id}
        assert (
            read_current_step_outputs(resumed.committer, source.step_run_id) == original
        )
        result = resumed.run(
            MeasureCSV,
            inputs={"source": selected.output("merged")},
            step_runner=NativePipelineRunner(resumed.committer),
        )
        assert result.status == StepStatus.SUCCEEDED
        assert result.step_number == 2
        for state in resumed.tracker.load_current_states(
            resumed.config.pipeline_run_id
        ):
            options = json.loads(state.compute_options_json)
            assert options["pipeline_name"] == "original-name"
            assert options["pipeline_default_step_runner"] == "local"
            assert options["pipeline_default_runner_requires_instance"] is False
            assert options["application_modules"] == []
            assert state.compute_backend == "native-test"
    with _resume(resumed) as again:
        assert again.config.name == "original-name"
        assert again._next_step == 3


@pytest.mark.parametrize(
    "overrides",
    [
        {"name": " "},
        {"delta_root": ""},
        {"staging_root": ""},
        {"files_root": ""},
        {"working_root": "relative"},
        {"storage": StorageConfig(protocol="s3")},
        {"staging_root": "https://example.invalid/path?token=private"},
        {"default_step_runner": "unknown-provider"},
        {"delta_root": "s3://example/factory-delta"},
    ],
)
def test_invalid_factory_configuration_rejects_before_store_or_pool_creation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, overrides: dict[str, object]
) -> None:
    def forbidden(*args: object, **kwargs: object) -> None:
        pytest.fail("Invalid factory input caused an operational side effect")

    monkeypatch.setattr(module, "create_retained_store", forbidden)
    monkeypatch.setattr(module, "ThreadPoolExecutor", forbidden)
    with pytest.raises(ValueError):
        _create(tmp_path, **overrides)
    assert not (tmp_path / "factory-delta").exists()


def test_bare_local_reconstruction_never_prepares_or_resolves_software(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def forbidden(*args: object, **kwargs: object) -> None:
        pytest.fail("Factory or empty-input handling attempted software resolution")

    for method in ("prepare", "prepare_curator", "resolve", "resolve_curator"):
        monkeypatch.setattr(LocalRunner, method, forbidden)
    monkeypatch.setattr(module, "recover_pipeline_results", forbidden)
    with _create(tmp_path) as pipeline:
        assert pipeline.config.files_root == str(tmp_path / "files")
        _skip(pipeline)
    with _resume(pipeline, recover_staging=False) as resumed:
        assert type(resumed.runner) is LocalRunner
        assert resumed._next_step == 1
    with _resume(pipeline, default_step_runner="local", recover_staging=False):
        pass


def _configured_runner(tmp_path: Path, field: str) -> LocalRunner:
    if field == "subclass":
        return _ExternalRunner()
    if field == "falsey":
        return _FalseyRunner()
    executable = tmp_path / ("docker" if field == "docker" else "apptainer")
    executable.write_text("#!/bin/sh\nexit 0\n")
    executable.chmod(0o700)
    values: dict[str, object] = {
        "environment": Environment.from_pixi(tmp_path / "pixi.toml"),
        "cache_root": tmp_path / "cache-private-locator",
        "tool_backend": "docker",
        "pixi": tmp_path / "pixi-private-locator",
        "system_env": tmp_path / "env-private-locator",
        "docker": DockerRuntime(executable) if field == "docker" else None,
        "apptainer": ApptainerRuntime(executable),
        "base_image": "example.invalid/image:qualified",
    }
    return replace(LocalRunner(), **{field: values[field]})


@pytest.mark.parametrize(
    "field",
    [
        "environment",
        "cache_root",
        "tool_backend",
        "pixi",
        "system_env",
        "docker",
        "apptainer",
        "base_image",
        "subclass",
        "falsey",
    ],
)
def test_every_configured_local_or_external_default_requires_explicit_instance(
    tmp_path: Path, field: str
) -> None:
    runner = _configured_runner(tmp_path, field)
    with _create(tmp_path, default_step_runner=runner) as pipeline:
        record = _skip(pipeline)
        options = json.loads(record.compute_options_json)
        assert options["pipeline_default_runner_requires_instance"] is True
        assert str(tmp_path) not in record.compute_options_json
    with pytest.raises(ValueError, match="explicit matching"):
        _resume(pipeline)
    with pytest.raises(ValueError, match="explicit matching"):
        _resume(pipeline, default_step_runner="local")
    with _resume(pipeline, default_step_runner=runner) as resumed:
        assert resumed.runner is runner


def test_bare_default_cannot_silently_resume_as_configured_local(
    tmp_path: Path,
) -> None:
    with _create(tmp_path) as pipeline:
        _skip(pipeline)
    with pytest.raises(ValueError, match="bare local"):
        _resume(
            pipeline,
            default_step_runner=replace(LocalRunner(), cache_root=tmp_path),
        )
    with pytest.raises(ValueError, match="runner name"):
        _resume(pipeline, default_step_runner=_ExternalRunner())


def test_explicit_instance_requirement_remains_durable_after_resume(
    tmp_path: Path,
) -> None:
    configured = replace(LocalRunner(), cache_root=tmp_path / "configured-cache")
    with _create(tmp_path, default_step_runner=configured) as pipeline:
        _skip(pipeline)
    with _resume(pipeline, default_step_runner=LocalRunner()) as resumed:
        record = _skip(resumed)
        assert (
            json.loads(record.compute_options_json)[
                "pipeline_default_runner_requires_instance"
            ]
            is True
        )
    with pytest.raises(ValueError, match="explicit matching"):
        _resume(resumed)
    with _resume(resumed, default_step_runner=LocalRunner()):
        pass


@pytest.mark.parametrize(
    "change",
    [
        {"pipeline_name": "another-name"},
        {"pipeline_default_step_runner": "another-provider"},
        {"pipeline_default_runner_requires_instance": True},
        {"pipeline_default_runner_requires_instance": 1},
        {"pipeline_default_runner_requires_instance": None},
    ],
)
def test_hidden_historical_defaults_reject_before_recovery(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, change: dict[str, object]
) -> None:
    with _create(tmp_path) as pipeline:
        record = _skip(pipeline)
        options = {**json.loads(record.compute_options_json), **change}
        _append_skipped(
            pipeline.tracker,
            record.model_copy(update={"compute_options_json": json.dumps(options)}),
        )
        _append_skipped(pipeline.tracker, record)

    def forbidden(*args: object, **kwargs: object) -> None:
        pytest.fail("Invalid historical defaults reached recovery or a new pool")

    monkeypatch.setattr(module, "recover_pipeline_results", forbidden)
    monkeypatch.setattr(module, "ThreadPoolExecutor", forbidden)
    with pytest.raises(ValueError, match="Recorded pipeline defaults"):
        _resume(pipeline)


@pytest.mark.parametrize("entry", ["resume", "constructor"])
def test_completed_defaults_require_exact_terminal_effect_before_reconstruction(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, entry: str
) -> None:
    with _create(tmp_path) as pipeline:
        pipeline.run(
            PredictMeasure,
            inputs={"structure": [structure(0)]},
            step_runner=NativePipelineRunner(pipeline.committer),
        )
        record = pipeline.executions[0].record
    options = json.loads(record.compute_options_json)
    changed = json.dumps({**options, "pipeline_name": "forged-coherent-name"})
    path = f"{pipeline.config.delta_root}/orchestration/steps"
    pl.read_delta(path).with_columns(
        pl.lit(changed).alias("compute_options_json")
    ).write_delta(path, mode="overwrite")

    def forbidden(*args: object, **kwargs: object) -> None:
        pytest.fail("Changed completed defaults reached recovery or pool creation")

    monkeypatch.setattr(module, "recover_pipeline_results", forbidden)
    monkeypatch.setattr(module, "ThreadPoolExecutor", forbidden)
    with pytest.raises(StoreIntegrityError):
        if entry == "resume":
            _resume(pipeline)
        else:
            PipelineManager(
                pipeline.committer,
                pipeline.dispatch,
                pipeline_run_id=pipeline.config.pipeline_run_id,
            )


def test_authorized_application_bootstrap_precedes_schema_open_and_never_uses_stored_names(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bootstrap = module.bootstrap_applications
    create_store = module.create_retained_store
    observed: list[tuple[str, ...]] = []

    def capture(modules: tuple[str, ...], *, purpose: str) -> None:
        observed.append(modules)
        bootstrap(modules, purpose=purpose)

    def create(*args: Any, **kwargs: Any) -> RetainedCommitter:
        assert observed == [("math", "json")]
        return create_store(*args, **kwargs)

    monkeypatch.setenv("ARTISAN_LOAD_MODULES", "math")
    monkeypatch.setattr(module, "bootstrap_applications", capture)
    monkeypatch.setattr(module, "create_retained_store", create)
    with _create(tmp_path, application_modules=("json", "math")) as pipeline:
        record = _skip(pipeline)
        options = json.loads(record.compute_options_json)
        assert options["application_modules"] == ["math", "json"]
        _append_skipped(
            pipeline.tracker,
            record.model_copy(
                update={
                    "compute_options_json": json.dumps(
                        {**options, "application_modules": ["not_authorized.module"]}
                    )
                }
            ),
        )
        _append_skipped(pipeline.tracker, record)
    with pytest.raises(ApplicationRegistrationError, match="Authorize"):
        _resume(pipeline, application_modules=("json",))
    assert observed == [("math", "json"), ("math", "json")]


@pytest.mark.parametrize("unknown", [False, True])
def test_hidden_unresolved_attempt_blocks_resume_and_keeps_other_runs_untouched(
    tmp_path: Path, unknown: bool
) -> None:
    with _create(tmp_path) as pipeline:
        record = _skip(pipeline)
        pending = record.model_copy(update={"step_run_id": uuid4().hex})
        pipeline.tracker.create_attempt(pending)
        if unknown:
            for status in (CancellationStatus.REQUESTED, CancellationStatus.UNKNOWN):
                pipeline.tracker.record_cancellation(
                    pending.step_run_id,
                    StepStatus.PENDING,
                    CancellationAcknowledgement(status),
                )
        latest = _append_skipped(pipeline.tracker, record)
        other = RetainedStepTracker(pipeline.committer, "unrelated-run")
        unrelated = record.model_copy(update={"step_run_id": uuid4().hex})
        other.create_attempt(unrelated)
    with pytest.raises(
        PersistenceIntegrityError, match="unfinished pipeline ownership"
    ):
        _resume(pipeline)
    assert (
        pipeline.tracker.current_state(pending.step_run_id).status == StepStatus.PENDING
    )
    assert (
        pipeline.tracker.current_state(latest.step_run_id).status == StepStatus.SKIPPED
    )
    assert other.current_state(unrelated.step_run_id).status == StepStatus.PENDING


def test_metadata_only_resume_does_not_hydrate_payloads_and_uses_latest_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with _create(tmp_path) as first:
        runner = NativePipelineRunner(first.committer)
        first.run(
            PredictMeasure, inputs={"structure": [structure(0)]}, step_runner=runner
        )
    with _create(tmp_path, name="latest-name") as latest:
        _skip(latest)

    def forbidden(*args: object, **kwargs: object) -> None:
        pytest.fail("Metadata-only resume attempted payload hydration")

    monkeypatch.setattr(RetainedReader, "read", forbidden)
    monkeypatch.setattr(RetainedReader, "read_many", forbidden)
    with _resume(first, pipeline_run_id=None, recover_staging=False) as resumed:
        assert resumed.config.pipeline_run_id == latest.config.pipeline_run_id
        assert resumed.config.name == "latest-name"


def test_resume_recovers_only_selected_terminal_without_reexecuting_science(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class ControllerInterrupted(BaseException):
        pass

    def interrupt(_self: RetainedStepResults, event: str) -> None:
        if event == "plan":
            raise ControllerInterrupted()

    pipeline = _create(tmp_path)
    runner = NativePipelineRunner(pipeline.committer)
    with monkeypatch.context() as patch:
        patch.setattr(RetainedStepResults, "_checkpoint", interrupt)
        with pytest.raises(ControllerInterrupted):
            pipeline.run(
                PredictMeasure, inputs={"structure": [structure(0)]}, step_runner=runner
            )
    pipeline.close()
    source = pipeline.executions[0].record.step_run_id
    assert pipeline.tracker.current_state(source).status == StepStatus.RUNNING
    with pytest.raises(PersistenceIntegrityError):
        _resume(pipeline, recover_staging=False)
    with _resume(pipeline) as resumed:
        assert resumed.tracker.current_state(source).status == StepStatus.SUCCEEDED
        assert resumed._step_run_ids == {0: source}
        assert read_current_step_outputs(resumed.committer, source)
    assert runner.resolutions == 1


def test_empty_store_and_missing_registered_table_do_not_trigger_resume_initialization(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with _create(tmp_path) as pipeline:
        pass
    with pytest.raises(ValueError, match="No step attempts"):
        _resume(pipeline)
    pipeline.committer.fs.rm(
        f"{pipeline.config.delta_root}/orchestration/steps", recursive=True
    )

    def forbidden(*args: object, **kwargs: object) -> None:
        pytest.fail("Resume tried to initialize a missing table")

    monkeypatch.setattr(module, "create_retained_store", forbidden)
    with pytest.raises(StoreIntegrityError, match="missing required table"):
        _resume(pipeline)
