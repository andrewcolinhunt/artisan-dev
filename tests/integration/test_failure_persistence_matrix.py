"""Closed phase failures retain public failure metadata and immutable owned logs.

The real admitted worker/controller runs with explicit synthetic in-process
software. Installed transport and process-death ownership have separate gates;
unobserved crashes are never promoted into invented scientific executions.
"""

from __future__ import annotations

from pathlib import Path

import pytest

pytestmark = pytest.mark.integration

from fixtures import failure_ops

from artisan.operations.examples import DataGenerator
from artisan.orchestration import PipelineManager
from artisan.schemas.enums import FailurePolicy
from artisan.schemas.orchestration.step_lifecycle import StepStatus
from artisan.storage.core.artifact_store import ArtifactStore
from artisan.visualization.inspect import inspect_failures

from .conftest import (
    FailingTransformer,
    create_native_pipeline,
    get_execution_outputs,
    get_failed_executions,
    get_successful_executions,
)


def _pipeline(pipeline_env: dict[str, str], policy: FailurePolicy) -> PipelineManager:
    return create_native_pipeline(
        name="fp_matrix",
        delta_root=pipeline_env["delta_root"],
        staging_root=pipeline_env["staging_root"],
        working_root=pipeline_env["working_root"],
        failure_policy=policy,
    )


def _failure_log(pipeline_env: dict[str, str], row: dict) -> Path:
    """Resolve the on-disk failure log for one ``inspect_failures`` row."""
    runs_dir = Path(pipeline_env["delta_root"]).parent
    return runs_dir / "logs" / "failures" / row["log"]


def _assert_one_failure(
    pipeline_env: dict[str, str],
    *,
    operation: str,
    step: int,
    code_populated: bool,
) -> dict:
    """Assert inspect_failures shows exactly one row for op/step, with a log."""
    failures = inspect_failures(pipeline_env["delta_root"])
    rows = [r for r in failures.to_dicts() if r["operation"] == operation]
    assert len(rows) == 1, f"expected 1 failure row for {operation}, got {rows}"
    row = rows[0]
    assert row["step"] == step
    if code_populated:
        assert row["code"] is not None, f"expected populated envelope: {row}"
    else:
        assert row["code"] is None, f"expected null envelope: {row}"
    assert row["error"]
    log_path = _failure_log(pipeline_env, row)
    assert log_path.exists(), f"missing failure log at {log_path}"
    return row


# ===========================================================================
# Creator, CONTINUE — one representative per phase / shape (already-working).
# code_populated: ArtisanError and command-op failures carry an envelope;
# a plain ValueError or a success=False return does not.
# ===========================================================================


@pytest.mark.parametrize(
    ("op", "code_populated"),
    [
        (failure_ops.FailPreprocess, False),
        (failure_ops.FailExecute, False),
        (failure_ops.FailExecuteArtisan, True),
        (failure_ops.FailPostprocessRaise, False),
        (failure_ops.FailPostprocessReturn, False),
    ],
    ids=[
        "preprocess",
        "execute-valueerror",
        "execute-artisanerror",
        "postprocess-raise",
        "postprocess-return-false",
    ],
)
def test_creator_continue_records_failure(
    pipeline_env: dict[str, str], op, code_populated: bool
) -> None:
    """CONTINUE creator failure: one readable row + log, correct envelope."""
    pipeline = _pipeline(pipeline_env, FailurePolicy.CONTINUE)
    step = pipeline.run(op)
    assert step.status is StepStatus.FAILED
    pipeline.finalize()

    _assert_one_failure(
        pipeline_env, operation=op.name, step=0, code_populated=code_populated
    )


# ===========================================================================
# FAIL_FAST commits the failure record before aborting.
# ===========================================================================


def test_creator_fail_fast_commits_failure_row(pipeline_env: dict[str, str]) -> None:
    """FAIL_FAST publishes a readable failure row before returning."""
    pipeline = _pipeline(pipeline_env, FailurePolicy.FAIL_FAST)
    step = pipeline.run(failure_ops.FailExecute)
    assert step.status is StepStatus.FAILED
    pipeline.finalize()

    _assert_one_failure(
        pipeline_env,
        operation=failure_ops.FailExecute.name,
        step=0,
        code_populated=False,
    )


@pytest.mark.parametrize(
    "policy",
    [FailurePolicy.CONTINUE, FailurePolicy.FAIL_FAST],
    ids=["continue", "fail_fast"],
)
def test_creator_partial_keeps_sibling_success(
    pipeline_env: dict[str, str], policy: FailurePolicy
) -> None:
    """A completed sibling stays selected when the next unit fails."""
    delta_root = pipeline_env["delta_root"]
    pipeline = _pipeline(pipeline_env, policy)

    gen = pipeline.run(DataGenerator, params={"count": 2, "seed": 42})
    assert gen.status == StepStatus.SUCCEEDED
    store = ArtifactStore(delta_root)
    sources = [
        store.get_artifact(artifact_id)
        for artifact_id in get_execution_outputs(delta_root, 0, "datasets")
    ]
    assert all(source is not None for source in sources)
    ordered_ids = [
        source.artifact_id
        for source in sorted(sources, key=lambda source: source.original_name)
    ]
    step1 = pipeline.run(
        FailingTransformer,
        inputs={"dataset": ordered_ids},
        params={"fail_on_index": 1},
        batch_strategy={"max_workers": 1, "units_per_worker": 1},
    )
    expected = (
        StepStatus.PARTIAL if policy is FailurePolicy.CONTINUE else StepStatus.FAILED
    )
    assert step1.status is expected
    pipeline.finalize()

    # Exactly one failure row for the transformer step, with a log.
    _assert_one_failure(
        pipeline_env,
        operation="failing_transformer",
        step=1,
        code_populated=False,
    )
    # The sibling success is committed too (not stranded).
    assert get_successful_executions(delta_root, 1) == 1
    assert get_failed_executions(delta_root, 1) == 1


def test_unqualified_command_exit_does_not_claim_scientific_failure(
    pipeline_env: dict[str, str],
) -> None:
    """A command without the prepared exit witness remains infrastructure evidence."""
    from artisan.orchestration.engine.retained_execution_query import (
        read_retained_execution_membership,
    )
    from artisan.schemas.execution.worker_ownership import OwnershipState

    with _pipeline(pipeline_env, FailurePolicy.CONTINUE) as pipeline:
        result = pipeline.run(failure_ops.FailCommand)
    assert result.status is StepStatus.FAILED
    assert "Command failed with exit code 1" in (result.error or "")
    assert pipeline.ownership_state() is OwnershipState.CLOSED
    assert (
        read_retained_execution_membership(
            pipeline.committer, pipeline_run_id=pipeline.config.pipeline_run_id
        )
        == ()
    )
    assert inspect_failures(pipeline_env["delta_root"]).is_empty()
    handle = pipeline.executions[0].handles[0]
    decision = pipeline.committer.journal.state(handle.attempt.assignment)
    assert decision is not None
    assert decision.cancelled
    assert decision.winner is None


# ===========================================================================
# Curator lifecycle — CONTINUE representatives + FAIL_FAST guard.
# ===========================================================================


@pytest.mark.parametrize(
    ("op", "code_populated"),
    [
        (failure_ops.FailCuratorRaise, False),
        (failure_ops.FailCuratorArtisan, True),
        (failure_ops.FailCuratorReturn, False),
    ],
    ids=["body-raise", "body-artisanerror", "body-return-false"],
)
def test_curator_continue_records_failure(
    pipeline_env: dict[str, str], op, code_populated: bool
) -> None:
    """CONTINUE curator failure: one readable row + log, correct envelope."""
    pipeline = _pipeline(pipeline_env, FailurePolicy.CONTINUE)
    gen = pipeline.run(DataGenerator, params={"count": 2, "seed": 5})
    step1 = pipeline.run(op, inputs={"passthrough": gen.output("datasets")})
    assert step1.status is StepStatus.FAILED
    pipeline.finalize()

    _assert_one_failure(
        pipeline_env, operation=op.name, step=1, code_populated=code_populated
    )


def test_curator_fail_fast_commits_failure_row(pipeline_env: dict[str, str]) -> None:
    """FAIL_FAST publishes a readable curator failure row."""
    pipeline = _pipeline(pipeline_env, FailurePolicy.FAIL_FAST)
    gen = pipeline.run(DataGenerator, params={"count": 2, "seed": 5})
    step1 = pipeline.run(
        failure_ops.FailCuratorRaise,
        inputs={"passthrough": gen.output("datasets")},
    )
    assert step1.status is StepStatus.FAILED
    pipeline.finalize()

    _assert_one_failure(
        pipeline_env,
        operation=failure_ops.FailCuratorRaise.name,
        step=1,
        code_populated=False,
    )


# ===========================================================================
# Composite lifecycle — the internal step's failure is readable.
# ===========================================================================


@pytest.mark.parametrize(
    "policy",
    [FailurePolicy.CONTINUE, FailurePolicy.FAIL_FAST],
    ids=["continue", "fail_fast"],
)
def test_composite_internal_failure_recorded(
    pipeline_env: dict[str, str], policy: FailurePolicy
) -> None:
    """A failing internal composite step records a readable failure row."""
    pipeline = _pipeline(pipeline_env, policy)
    result = pipeline.submit(failure_ops.FailingComposite)
    assert result.result().status == StepStatus.FAILED
    pipeline.finalize()

    _assert_one_failure(
        pipeline_env,
        operation=failure_ops.FailingComposite.name,
        step=0,
        code_populated=False,
    )
