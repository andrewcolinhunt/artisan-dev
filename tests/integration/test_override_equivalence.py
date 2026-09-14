"""End-to-end equivalence between dict and typed-model override forms.

The PipelineManager refactor advertises that every override kwarg
accepts either a dict (zero-import default) or a typed Pydantic model
(IDE autocomplete on demand). Both forms must produce identical
observable behavior — same operation instance fields, same persisted
record, same step_spec_id where applicable.

This file covers the equivalence at the integration level: actual
pipelines run with the dict form, then again with the typed-model
form, and the resulting state is compared. It catches the bug where
typed-model input crashed ``_validate_resources`` (set() over a
Pydantic model hit unhashable dict values), and any future regression
where the two paths drift.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, ClassVar

import polars as pl
import pytest

from artisan.operations.examples import DataGenerator
from artisan.orchestration import PipelineManager, Runner
from artisan.orchestration.runners import LocalRunner
from artisan.schemas.execution.batch_strategy import BatchStrategy
from artisan.schemas.operation_config.compute import (
    ComputeProvider,
    LocalComputeConfig,
)
from artisan.schemas.operation_config.compute_resources import ComputeResources
from artisan.schemas.operation_config.environment_spec import LocalEnvironmentSpec
from artisan.schemas.operation_config.environments import Environments
from artisan.schemas.operation_config.runner_resources import RunnerResources
from artisan.schemas.orchestration.step_lifecycle import StepStatus

pytestmark = pytest.mark.integration


class _OverrideDefaultsGenerator(DataGenerator):
    """Generator with defaults that differ from target-model schema defaults."""

    name: ClassVar[str] = "override_defaults_generator"
    runner_resources: RunnerResources = RunnerResources(
        cpus=8,
        memory_gb=32,
        time_limit="00:30:00",
    )
    compute_resources: ComputeResources = ComputeResources(
        gpu="A100",
        memory_gb=32,
    )


def _read_steps_table(delta_root: str) -> pl.DataFrame:
    """Read the orchestration/steps Delta table for the run."""
    table_path = Path(delta_root) / "orchestration" / "steps"
    return pl.read_delta(str(table_path))


def _succeeded_step(delta_root: str, name: str) -> dict[str, Any]:
    """Return the most recent COMPLETED row for a given step name as a dict."""
    df = _read_steps_table(delta_root)
    matches = df.filter(
        (pl.col("step_name") == name) & (pl.col("status") == "succeeded")
    )
    assert len(matches) >= 1, f"no succeeded step named {name!r} in delta"
    return matches.sort("started_at", descending=True).row(0, named=True)


def _succeeded_step_for_run(delta_root: str, pipeline_run_id: str) -> dict[str, Any]:
    """Return the succeeded step row for one single-step pipeline run."""
    matches = _read_steps_table(delta_root).filter(
        (pl.col("pipeline_run_id") == pipeline_run_id)
        & (pl.col("status") == "succeeded")
    )
    assert len(matches) == 1
    return matches.row(0, named=True)


def _execution_spec_ids(delta_root: str) -> list[str]:
    """Return execution spec IDs recorded for the override regression op."""
    table_path = Path(delta_root) / "orchestration" / "executions"
    frame = pl.read_delta(str(table_path)).filter(
        pl.col("operation_name") == _OverrideDefaultsGenerator.name
    )
    return frame.get_column("execution_spec_id").to_list()


# ---------------------------------------------------------------------------
# Override forms — dict ⇄ typed model
# ---------------------------------------------------------------------------

# Each row: (kwarg_name, dict_form, model_form). Both forms encode the
# same hardware/strategy spec and must produce identical step_spec_ids.
DICT_VS_MODEL_CASES = [
    pytest.param(
        "runner_resources",
        {"cpus": 4, "memory_gb": 8, "gpus": 0, "time_limit": "00:30:00"},
        RunnerResources(cpus=4, memory_gb=8, gpus=0, time_limit="00:30:00"),
        id="runner_resources",
    ),
    pytest.param(
        "batch_strategy",
        {"artifacts_per_unit": 2, "units_per_worker": 2, "max_workers": 4},
        BatchStrategy(artifacts_per_unit=2, units_per_worker=2, max_workers=4),
        id="batch_strategy",
    ),
    pytest.param(
        "compute_resources",
        {"memory_gb": 16, "timeout": 7200},
        ComputeResources(memory_gb=16, timeout=7200),
        id="compute_resources",
    ),
]


@pytest.mark.parametrize(("kwarg", "dict_form", "model_form"), DICT_VS_MODEL_CASES)
def test_dict_and_model_forms_produce_identical_step_spec_id(
    pipeline_env: dict[str, str],
    tmp_path: Path,
    kwarg: str,
    dict_form: Any,
    model_form: Any,
) -> None:
    """Two pipelines with the same override expressed as dict vs typed model
    must produce the same step_spec_id (cache equivalence)."""
    # Run 1: dict form
    p1 = PipelineManager.create(
        name="dict_form",
        delta_root=str(tmp_path / "d1"),
        staging_root=str(tmp_path / "s1"),
        working_root=str(tmp_path / "w1"),
    )
    p1.run(
        DataGenerator,
        params={"count": 1, "seed": 42},
        step_runner=Runner.LOCAL,
        **{kwarg: dict_form},
    )
    p1.finalize()
    spec_id_dict = p1._step_spec_ids[0]  # contract under test

    # Run 2: typed-model form
    p2 = PipelineManager.create(
        name="model_form",
        delta_root=str(tmp_path / "d2"),
        staging_root=str(tmp_path / "s2"),
        working_root=str(tmp_path / "w2"),
    )
    p2.run(
        DataGenerator,
        params={"count": 1, "seed": 42},
        step_runner=Runner.LOCAL,
        **{kwarg: model_form},
    )
    p2.finalize()
    spec_id_model = p2._step_spec_ids[0]

    assert spec_id_dict == spec_id_model, (
        f"step_spec_id drifted between dict and typed-model forms of {kwarg!r}: "
        f"dict={spec_id_dict!r} model={spec_id_model!r}"
    )


@pytest.mark.parametrize(("kwarg", "dict_form", "model_form"), DICT_VS_MODEL_CASES)
def test_dict_and_model_forms_succeed_end_to_end(
    pipeline_env: dict[str, str],
    kwarg: str,
    dict_form: Any,
    model_form: Any,
) -> None:
    """Both forms must produce a successful step. Catches the typed-model
    crash in _validate_resources / compute_options_data JSON encoding."""
    pipeline = PipelineManager.create(name="end_to_end", **pipeline_env)
    pipeline.run(
        DataGenerator,
        params={"count": 1, "seed": 42},
        step_runner=Runner.LOCAL,
        **{kwarg: model_form},
    )
    summary = pipeline.finalize()
    assert summary["overall_success"] is True


PRESENCE_PATCH_CASES = [
    pytest.param(
        "runner_resources",
        {"cpus": 1},
        RunnerResources(cpus=1),
        "resources",
        {"cpus": 1},
        id="explicit-schema-default",
    ),
    pytest.param(
        "compute_resources",
        {"gpu": None},
        ComputeResources(gpu=None),
        "compute_resources",
        {"gpu": None},
        id="explicit-none",
    ),
]


@pytest.mark.parametrize(
    ("kwarg", "mapping_patch", "typed_patch", "record_key", "expected_patch"),
    PRESENCE_PATCH_CASES,
)
def test_presence_based_forms_match_execution_persistence_and_cache(
    tmp_path: Path,
    kwarg: str,
    mapping_patch: Any,
    typed_patch: Any,
    record_key: str,
    expected_patch: dict[str, Any],
) -> None:
    """Equivalent patches share both cache identities and a real cache hit."""
    delta_root = str(tmp_path / "delta")

    def _run(
        label: str, patch_value: Any, *, skip_cache: bool
    ) -> tuple[str, dict[str, Any]]:
        pipeline = PipelineManager.create(
            name=f"presence_{label}",
            delta_root=delta_root,
            staging_root=str(tmp_path / f"staging_{label}"),
            working_root=str(tmp_path / f"working_{label}"),
        )
        result = pipeline.run(
            _OverrideDefaultsGenerator,
            params={"count": 1, "seed": 42},
            step_runner=Runner.LOCAL,
            skip_cache=skip_cache,
            **{kwarg: patch_value},
        )
        assert result.status is StepStatus.SUCCEEDED
        row = _succeeded_step_for_run(delta_root, pipeline.config.pipeline_run_id)
        pipeline.finalize()
        return pipeline._step_spec_ids[0], row

    mapping_step_id, mapping_row = _run("mapping", mapping_patch, skip_cache=False)
    first_execution_ids = _execution_spec_ids(delta_root)
    assert len(first_execution_ids) == 1

    typed_step_id, typed_row = _run("typed", typed_patch, skip_cache=True)
    executed_ids = _execution_spec_ids(delta_root)
    assert len(executed_ids) == 2
    assert len(set(executed_ids)) == 1

    cached_step_id, cached_row = _run("cached", typed_patch, skip_cache=False)
    assert _execution_spec_ids(delta_root) == executed_ids

    mapping_options = json.loads(mapping_row["compute_options_json"])
    typed_options = json.loads(typed_row["compute_options_json"])
    cached_options = json.loads(cached_row["compute_options_json"])
    assert mapping_options == typed_options == cached_options
    assert mapping_options[record_key] == expected_patch
    assert mapping_step_id == typed_step_id == cached_step_id


# ---------------------------------------------------------------------------
# String shorthand for environment / compute_provider / step_runner
# ---------------------------------------------------------------------------

STRING_FORM_CASES = [
    pytest.param(
        "step_runner",
        Runner.LOCAL,  # namespace instance
        "local",  # string shorthand
        LocalRunner(),  # bare typed instance — first coverage
        id="step_runner",
    ),
    pytest.param(
        "environment",
        Environments(active="local", local=LocalEnvironmentSpec()),
        "local",
        Environments(active="local", local=LocalEnvironmentSpec()),
        id="environment",
    ),
    pytest.param(
        "compute_provider",
        ComputeProvider(active="local", local=LocalComputeConfig()),
        "local",
        ComputeProvider(active="local", local=LocalComputeConfig()),
        id="compute_provider",
    ),
]


@pytest.mark.parametrize(
    ("kwarg", "namespace_form", "string_form", "typed_form"),
    STRING_FORM_CASES,
)
def test_string_namespace_and_typed_forms_all_succeed(
    pipeline_env: dict[str, str],
    kwarg: str,
    namespace_form: Any,
    string_form: str,
    typed_form: Any,
) -> None:
    """All three input shapes (string / namespace / typed model) must run."""
    for form_label, form in (
        ("string", string_form),
        ("namespace", namespace_form),
        ("typed", typed_form),
    ):
        pipeline = PipelineManager.create(
            name=f"smoke_{kwarg}_{form_label}",
            delta_root=str(Path(pipeline_env["delta_root"]) / form_label),
            staging_root=str(Path(pipeline_env["staging_root"]) / form_label),
            working_root=str(Path(pipeline_env["working_root"]) / form_label),
        )
        Path(pipeline.config.delta_root).mkdir(parents=True, exist_ok=True)
        Path(pipeline.config.staging_root).mkdir(parents=True, exist_ok=True)
        Path(pipeline.config.working_root).mkdir(parents=True, exist_ok=True)

        kwargs = {kwarg: form}
        # step_runner is the only kwarg here that's also the runner-axis
        # selector; the other two are operation-level overrides and need
        # an explicit step_runner to dispatch.
        if kwarg != "step_runner":
            kwargs["step_runner"] = Runner.LOCAL

        pipeline.run(
            DataGenerator,
            params={"count": 1, "seed": 42},
            **kwargs,
        )
        summary = pipeline.finalize()
        assert summary["overall_success"] is True, (
            f"{kwarg}={form_label} form failed: {summary}"
        )
