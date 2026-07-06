"""Failure-persistence matrix: every failure shape lands a readable row.

Asserts *through the reader* (``inspect_failures``) — the envelope-policy
boundary — that each failure shape produces exactly one executions row with
the right step/operation identity, the right envelope code (populated for
ArtisanError and command-op failures, null for a plain ``ValueError``), and a
failure log on disk. Partial-failure cells additionally assert the sibling
SUCCESS rows survive.

The cells are pruned to those that guard the two fixed mechanisms — FAIL_FAST
committing before it aborts, and orchestrator-side synthesis when the worker
records nothing — plus one representative per already-working mechanism
(creator function/command phases, curator, composite, envelope populated/null).

Failing ops live in the importable ``fixtures.failure_ops`` module (not
path-loaded) so spawn workers can import them and exercise the real execute path.
"""

from __future__ import annotations

from pathlib import Path

import pytest

pytestmark = pytest.mark.integration

from fixtures import failure_ops as fo

from artisan.operations.examples import DataGenerator
from artisan.orchestration import PipelineManager
from artisan.orchestration.runners import Runner
from artisan.schemas.enums import FailurePolicy
from artisan.visualization.inspect import inspect_failures

from .conftest import (
    FailingTransformer,
    get_failed_executions,
    get_successful_executions,
)


def _pipeline(pipeline_env: dict[str, str], policy: FailurePolicy) -> PipelineManager:
    return PipelineManager.create(
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
        (fo.FailPreprocess, False),
        (fo.FailExecute, False),
        (fo.FailExecuteArtisan, True),
        (fo.FailPostprocessRaise, False),
        (fo.FailPostprocessReturn, False),
        (fo.FailCommand, True),
    ],
    ids=[
        "preprocess",
        "execute-valueerror",
        "execute-artisanerror",
        "postprocess-raise",
        "postprocess-return-false",
        "command-nonzero",
    ],
)
def test_creator_continue_records_failure(
    pipeline_env: dict[str, str], op, code_populated: bool
) -> None:
    """CONTINUE creator failure: one readable row + log, correct envelope."""
    pipeline = _pipeline(pipeline_env, FailurePolicy.CONTINUE)
    step = pipeline.run(op, step_runner=Runner.LOCAL)
    assert step.success is False
    pipeline.finalize()

    _assert_one_failure(
        pipeline_env, operation=op.name, step=0, code_populated=code_populated
    )


# ===========================================================================
# Fix 1 — FAIL_FAST commits the failure record before aborting.
# ===========================================================================


def test_creator_fail_fast_commits_failure_row(pipeline_env: dict[str, str]) -> None:
    """FAIL_FAST full failure lands a readable row (was stranded in staging)."""
    pipeline = _pipeline(pipeline_env, FailurePolicy.FAIL_FAST)
    step = pipeline.run(fo.FailExecute, step_runner=Runner.LOCAL)
    assert step.success is False
    pipeline.finalize()

    _assert_one_failure(
        pipeline_env, operation=fo.FailExecute.name, step=0, code_populated=False
    )


@pytest.mark.parametrize(
    "policy",
    [FailurePolicy.CONTINUE, FailurePolicy.FAIL_FAST],
    ids=["continue", "fail_fast"],
)
def test_creator_partial_keeps_sibling_success(
    pipeline_env: dict[str, str], policy: FailurePolicy
) -> None:
    """Partial failure: the failed row is readable AND the sibling committed.

    Under FAIL_FAST this is the core fix — before it, the succeeding sibling's
    record was stranded in staging alongside the failure. Both must land.
    """
    delta_root = pipeline_env["delta_root"]
    pipeline = _pipeline(pipeline_env, policy)

    gen = pipeline.run(
        DataGenerator, params={"count": 2, "seed": 42}, step_runner=Runner.LOCAL
    )
    step1 = pipeline.run(
        FailingTransformer,
        inputs={"dataset": gen.output("datasets")},
        params={"fail_on_index": 0},
        step_runner=Runner.LOCAL,
    )
    assert step1.success is False
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


# ===========================================================================
# Curator lifecycle — CONTINUE representatives + FAIL_FAST guard.
# ===========================================================================


@pytest.mark.parametrize(
    ("op", "code_populated"),
    [
        (fo.FailCuratorRaise, False),
        (fo.FailCuratorArtisan, True),
        (fo.FailCuratorReturn, False),
    ],
    ids=["body-raise", "body-artisanerror", "body-return-false"],
)
def test_curator_continue_records_failure(
    pipeline_env: dict[str, str], op, code_populated: bool
) -> None:
    """CONTINUE curator failure: one readable row + log, correct envelope."""
    pipeline = _pipeline(pipeline_env, FailurePolicy.CONTINUE)
    gen = pipeline.run(
        DataGenerator, params={"count": 2, "seed": 5}, step_runner=Runner.LOCAL
    )
    step1 = pipeline.run(
        op, inputs={"passthrough": gen.output("datasets")}, step_runner=Runner.LOCAL
    )
    assert step1.success is False
    pipeline.finalize()

    _assert_one_failure(
        pipeline_env, operation=op.name, step=1, code_populated=code_populated
    )


def test_curator_fail_fast_commits_failure_row(pipeline_env: dict[str, str]) -> None:
    """FAIL_FAST curator failure lands a readable row (Fix 1, curator path)."""
    pipeline = _pipeline(pipeline_env, FailurePolicy.FAIL_FAST)
    gen = pipeline.run(
        DataGenerator, params={"count": 2, "seed": 5}, step_runner=Runner.LOCAL
    )
    step1 = pipeline.run(
        fo.FailCuratorRaise,
        inputs={"passthrough": gen.output("datasets")},
        step_runner=Runner.LOCAL,
    )
    assert step1.success is False
    pipeline.finalize()

    _assert_one_failure(
        pipeline_env, operation=fo.FailCuratorRaise.name, step=1, code_populated=False
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
    result = pipeline.submit_composite(fo.FailingComposite, step_runner=Runner.LOCAL)
    result.wait()
    pipeline.finalize()

    _assert_one_failure(
        pipeline_env, operation=fo.FailExecute.name, step=0, code_populated=False
    )


# ===========================================================================
# Fix 2 — synthesized records when the worker hard-crashes (Mechanism B).
# ===========================================================================


def test_creator_worker_crash_synthesizes_record(
    pipeline_env: dict[str, str],
) -> None:
    """A creator that os._exit's the worker still lands a synthesized row + log."""
    pipeline = _pipeline(pipeline_env, FailurePolicy.CONTINUE)
    step = pipeline.run(fo.WorkerCrash, step_runner=Runner.LOCAL)
    assert step.success is False
    pipeline.finalize()

    _assert_one_failure(
        pipeline_env, operation=fo.WorkerCrash.name, step=0, code_populated=False
    )


def test_curator_worker_crash_synthesizes_record(
    pipeline_env: dict[str, str],
) -> None:
    """A curator that os._exit's the subprocess still lands a synthesized row + log."""
    pipeline = _pipeline(pipeline_env, FailurePolicy.CONTINUE)
    gen = pipeline.run(
        DataGenerator, params={"count": 1, "seed": 5}, step_runner=Runner.LOCAL
    )
    step1 = pipeline.run(
        fo.CuratorWorkerCrash,
        inputs={"passthrough": gen.output("datasets")},
        step_runner=Runner.LOCAL,
    )
    assert step1.success is False
    pipeline.finalize()

    _assert_one_failure(
        pipeline_env,
        operation=fo.CuratorWorkerCrash.name,
        step=1,
        code_populated=False,
    )
