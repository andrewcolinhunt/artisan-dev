"""Tests for pipeline_manager.py failure handling and cancellation.

Tests that PipelineManager never raises from step execution,
that finalize() always returns a summary, and that cancel()
correctly stops the pipeline.
"""

from __future__ import annotations

import hashlib
import inspect
import io
import json
import signal
from concurrent.futures import Future
from enum import StrEnum, auto
from pathlib import Path
from typing import Any, ClassVar
from unittest.mock import MagicMock, patch

import polars as pl
import pytest
from fixtures.logical_commit_store import commit_test_inputs
from pydantic import BaseModel

from artisan.errors import ArtifactIntegrityError, CommitError
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.orchestration.engine.step_tracker import StepTracker
from artisan.orchestration.pipeline_manager import (
    PipelineManager,
    _is_file_path_input,
    _StepStatusReader,
)
from artisan.orchestration.runners.local import LocalRunner
from artisan.orchestration.step_future import StepFuture
from artisan.schemas.artifact.data import DataArtifact
from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.schemas.enums import CachePolicy, FailurePolicy, GroupByStrategy, TablePath
from artisan.schemas.execution.curator_result import ArtifactResult
from artisan.schemas.operation_config.compute import ComputeProvider, ModalComputeConfig
from artisan.schemas.operation_config.compute_resources import ComputeResources
from artisan.schemas.operation_config.environment_spec import DockerEnvironmentSpec
from artisan.schemas.operation_config.environments import Environments
from artisan.schemas.operation_config.tool_spec import ToolSpec
from artisan.schemas.orchestration.output_reference import OutputReference
from artisan.schemas.orchestration.pipeline_config import PipelineConfig
from artisan.schemas.orchestration.step_lifecycle import (
    CancellationStatus,
    StepDisposition,
    StepStatus,
)
from artisan.schemas.orchestration.step_overrides import StepOverrides
from artisan.schemas.orchestration.step_result import StepResult
from artisan.schemas.orchestration.step_start_record import StepStartRecord
from artisan.schemas.specs.input_models import PreprocessInput
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec
from artisan.storage.core.store_format import STORE_MANIFEST
from artisan.storage.core.table_schemas import ARTIFACT_INDEX_SCHEMA

_INPUT_ARTIFACT = DataArtifact.draft(
    b"value\n1\n",
    "pipeline-input.csv",
    step_number=0,
).finalize()
assert _INPUT_ARTIFACT.artifact_id is not None
_INPUT_ID = _INPUT_ARTIFACT.artifact_id
_SEEDED_INPUT_ROOTS: set[str] = set()


def _configure_pipeline_test_doubles(pipeline: PipelineManager) -> None:
    """Back mocked boundaries with the real persistence contracts."""
    tracker = pipeline._step_tracker
    if isinstance(tracker, MagicMock) and not isinstance(
        getattr(tracker, "_lifecycle_backend", None), StepTracker
    ):
        backend = StepTracker(
            pipeline.config.delta_root,
            pipeline.config.pipeline_run_id,
            fs=pipeline.config.storage.filesystem(),
            storage_options=pipeline.config.storage.delta_storage_options(),
        )
        tracker._lifecycle_backend = backend
        for method in (
            "create_attempt",
            "transition",
            "prepare_terminal_candidate",
            "record_cancellation",
            "current_state",
        ):
            getattr(tracker, method).side_effect = getattr(backend, method)
        if isinstance(tracker.check_cache.return_value, MagicMock):
            tracker.check_cache.return_value = None

    import artisan.orchestration.pipeline_manager as pipeline_manager_module

    execute_mock = pipeline_manager_module.execute_step
    if isinstance(execute_mock, MagicMock) and execute_mock.side_effect is None:

        def _persist_mock_result(**kwargs: Any) -> StepResult:
            result = execute_mock.return_value
            if not isinstance(result, StepResult):
                return result
            operation = kwargs["operation"]
            owned = result.model_copy(
                update={
                    "step_run_id": kwargs["step_run_id"],
                    "output_roles": frozenset(operation.outputs),
                    "output_types": {
                        role: spec.artifact_type
                        for role, spec in operation.outputs.items()
                    },
                }
            )
            if owned.status in {StepStatus.SUCCEEDED, StepStatus.PARTIAL}:
                return kwargs["persist_result"](owned, ())
            return owned

        execute_mock.side_effect = _persist_mock_result


def _prime_attempt(
    pipeline: PipelineManager,
    step_run_id: str,
    *,
    status: StepStatus = StepStatus.PENDING,
    step_name: str = "step",
) -> None:
    """Prime manager-owned attempt context for direct private-helper tests."""
    _configure_pipeline_test_doubles(pipeline)
    record = StepStartRecord(
        step_run_id=step_run_id,
        step_number=0,
        step_name=step_name,
        operation_class="test.MockOperation",
        params_json="{}",
        input_refs_json="{}",
        compute_backend="local",
        compute_options_json="{}",
        output_roles_json="[]",
        output_types_json="{}",
    )
    pipeline._step_start_records[0] = record
    pipeline._step_status_readers[0] = _StepStatusReader(status)
    backend = pipeline._step_tracker._lifecycle_backend
    backend.create_attempt(record)
    if status == StepStatus.RUNNING:
        backend.transition(step_run_id, StepStatus.PENDING, StepStatus.RUNNING)
    pipeline._step_tracker.reset_mock()


def _seed_input_artifact(delta_root: Path) -> None:
    """Seed the concrete input consumed by pipeline-manager tests."""
    root_key = str(delta_root.resolve())
    if root_key in _SEEDED_INPUT_ROOTS:
        return
    owner = hashlib.sha256(root_key.encode()).hexdigest()[:32]
    commit_test_inputs(
        delta_root,
        delta_root.parent / "fixture-staging",
        {
            ArtifactTypeDef.get_table_path(ArtifactTypes.DATA): pl.DataFrame(
                [_INPUT_ARTIFACT.to_row()],
                schema=DataArtifact.POLARS_SCHEMA,
            ),
            TablePath.ARTIFACT_INDEX.value: pl.DataFrame(
                [
                    {
                        "artifact_id": _INPUT_ID,
                        "artifact_type": ArtifactTypes.DATA,
                        "origin_step_number": 0,
                        "metadata": "{}",
                    }
                ],
                schema=ARTIFACT_INDEX_SCHEMA,
            ),
        },
        step_run_id=owner,
    )
    _SEEDED_INPUT_ROOTS.add(root_key)


@pytest.fixture(autouse=True)
def _seed_valid_input_artifact(tmp_path: Path) -> None:
    """Seed the conventional per-test Delta root."""
    _seed_input_artifact(tmp_path / "delta")


class _MockOp(OperationDefinition):
    class InputRole(StrEnum):
        data = auto()

    class OutputRole(StrEnum):
        output = auto()

    name: ClassVar[str] = "mock_op"
    inputs: ClassVar[dict[str, InputSpec]] = {
        InputRole.data: InputSpec(artifact_type=ArtifactTypes.DATA, required=True),
    }
    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.output: OutputSpec(
            artifact_type=ArtifactTypes.DATA,
            infer_lineage_from={"inputs": ["data"]},
        ),
    }

    def preprocess(self, inputs: Any) -> dict:
        return {}

    def execute_function(self, inputs: Any, output_dir: Any) -> Any:
        return None


class _ComputeDefaultsOp(_MockOp):
    """Mock op whose compute defaults differ from schema defaults."""

    name: ClassVar[str] = "compute_defaults_op"
    compute_resources: ComputeResources = ComputeResources(
        gpu="A100",
        memory_gb=32,
    )


class _RecursivePatchOp(OperationDefinition):
    """Command op with nested configuration defaults for public patch tests."""

    name: ClassVar[str] = "recursive_patch_op"
    inputs: ClassVar[dict[str, InputSpec]] = {}
    outputs: ClassVar[dict[str, OutputSpec]] = {}
    tool: ToolSpec = ToolSpec(executable="bash")
    environments: Environments = Environments(
        active="docker",
        docker=DockerEnvironmentSpec(
            image="image:v1",
            env={"KEEP": "yes", "CHANGE": "old"},
        ),
    )
    compute_provider: ComputeProvider = ComputeProvider(
        active="modal",
        modal=ModalComputeConfig(
            retries=5,
            env={"KEEP": "yes", "CHANGE": "old"},
        ),
    )

    def execute_command(self, inputs: dict[str, Any]) -> list[str]:
        return ["bash", "-c", "true"]


class _ExternalRunner(LocalRunner):
    """Concrete stand-in for a runner supplied by an external provider."""

    name = "external_test"


def _make_pipeline(tmp_path) -> PipelineManager:
    """Create an isolated manager backed by the test persistence contracts."""
    _seed_input_artifact(tmp_path / "delta")
    config = PipelineConfig(
        name="test",
        delta_root=str(tmp_path / "delta"),
        staging_root=str(tmp_path / "staging"),
        working_root=str(tmp_path / "working"),
    )
    pipeline = PipelineManager(config)
    _configure_pipeline_test_doubles(pipeline)
    return pipeline


def _recorded_step_spec(pipeline: PipelineManager, step_number: int) -> str:
    """Read the concrete cache identity from the authoritative attempt."""
    state = pipeline._step_tracker.current_state(pipeline._step_run_ids[step_number])
    assert state.status is StepStatus.SUCCEEDED
    assert isinstance(state.step_spec_id, str)
    assert state.step_spec_id
    return state.step_spec_id


class TestDefaultRunnerRetention:
    """The runtime provider object must survive configuration serialization."""

    @patch("artisan.orchestration.pipeline_manager.execute_step")
    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_custom_default_instance_reaches_dispatch(
        self, mock_tracker_cls, mock_execute, tmp_path
    ):
        mock_tracker = MagicMock()
        mock_tracker.check_cache.return_value = None
        mock_tracker_cls.return_value = mock_tracker
        mock_execute.return_value = StepResult(
            step_name=_MockOp.name,
            step_number=0,
            status=StepStatus.SUCCEEDED,
            disposition=StepDisposition.EXECUTED,
        )
        runner = _ExternalRunner()
        pipeline = PipelineManager.create(
            name="external",
            delta_root=str(tmp_path / "delta"),
            staging_root=str(tmp_path / "staging"),
            default_step_runner=runner,
        )
        _configure_pipeline_test_doubles(pipeline)

        pipeline.run(_MockOp, inputs={"data": [_INPUT_ID]})

        assert pipeline.config.default_step_runner == "external_test"
        assert mock_execute.call_args.kwargs["step_runner"] is runner


class TestPreparedOperationSnapshot:
    """Step hashing and execution share one prepared operation instance."""

    @patch("artisan.orchestration.pipeline_manager.execute_step")
    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_hashed_operation_is_passed_to_execution(
        self, mock_tracker_cls, mock_execute, tmp_path
    ) -> None:
        tracker = MagicMock()
        tracker.check_cache.return_value = None
        mock_tracker_cls.return_value = tracker
        mock_execute.return_value = StepResult(
            step_name=_MockOp.name,
            step_number=0,
            status=StepStatus.SUCCEEDED,
            disposition=StepDisposition.EXECUTED,
        )
        pipeline = _make_pipeline(tmp_path)

        with patch.object(
            pipeline,
            "_prepare_step_spec",
            wraps=pipeline._prepare_step_spec,
        ) as mock_prepare:
            pipeline.run(_MockOp, inputs={"data": [_INPUT_ID]})

        prepared = mock_prepare.call_args.args[0]
        assert mock_execute.call_args.kwargs["operation"] is prepared
        assert "operation_class" not in mock_execute.call_args.kwargs

    @patch("artisan.orchestration.pipeline_manager.execute_step")
    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_reset_forms_share_record_and_hash_but_change_effective_identity(
        self, mock_tracker_cls, mock_execute, tmp_path
    ) -> None:
        mock_execute.return_value = StepResult(
            step_name=_ComputeDefaultsOp.name,
            step_number=0,
            status=StepStatus.SUCCEEDED,
            disposition=StepDisposition.EXECUTED,
        )

        def _submit(
            path: Path,
            override: ComputeResources | dict[str, Any] | None = None,
        ) -> tuple[str, dict[str, Any]]:
            tracker = MagicMock()
            tracker.check_cache.return_value = None
            mock_tracker_cls.return_value = tracker
            pipeline = _make_pipeline(path)
            kwargs = {} if override is None else {"compute_resources": override}
            pipeline.run(
                _ComputeDefaultsOp,
                inputs={"data": [_INPUT_ID]},
                **kwargs,
            )
            options = json.loads(pipeline._step_start_records[0].compute_options_json)
            pipeline.finalize()
            return _recorded_step_spec(pipeline, 0), options

        base_id, _ = _submit(tmp_path / "base")
        mapping_id, mapping_record = _submit(tmp_path / "mapping", {"gpu": None})
        typed_id, typed_record = _submit(tmp_path / "typed", ComputeResources(gpu=None))

        assert base_id != mapping_id
        assert mapping_id == typed_id
        assert mapping_record == typed_record
        assert mapping_record["compute_resources"] == {"gpu": None}

    @patch("artisan.orchestration.pipeline_manager.execute_step")
    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_submit_recursively_merges_selected_environment_and_provider(
        self, mock_tracker_cls, mock_execute, tmp_path
    ) -> None:
        tracker = MagicMock()
        tracker.check_cache.return_value = None
        mock_tracker_cls.return_value = tracker
        mock_execute.return_value = StepResult(
            step_name=_RecursivePatchOp.name,
            step_number=0,
            status=StepStatus.SUCCEEDED,
            disposition=StepDisposition.EXECUTED,
        )
        pipeline = _make_pipeline(tmp_path)

        future = pipeline.submit(
            _RecursivePatchOp,
            environment={
                "active": "docker",
                "docker": {"env": {"CHANGE": "new"}},
            },
            compute_provider={
                "active": "modal",
                "modal": {"env": {"CHANGE": "new"}},
            },
        )
        future.result()

        operation = mock_execute.call_args.kwargs["operation"]
        assert operation.environments.active == "docker"
        assert operation.environments.docker.image == "image:v1"
        assert operation.environments.docker.env == {
            "KEEP": "yes",
            "CHANGE": "new",
        }
        assert operation.compute_provider.active == "modal"
        assert operation.compute_provider.modal.retries == 5
        assert operation.compute_provider.modal.env == {
            "KEEP": "yes",
            "CHANGE": "new",
        }


class TestRunReturnsFailedStepResult:
    """Tests for _run() returns StepResult instead of raising."""

    @patch("artisan.orchestration.pipeline_manager.execute_step")
    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_failed_step_appears_in_step_results(
        self, mock_tracker_cls, mock_execute, tmp_path
    ):
        """A background exception should publish a failed terminal result."""
        mock_tracker = MagicMock()
        mock_tracker.check_cache.return_value = None
        mock_tracker.current_state.return_value = MagicMock(
            status=StepStatus.RUNNING,
            cancellation_status=None,
        )
        mock_tracker_cls.return_value = mock_tracker

        mock_execute.side_effect = RuntimeError("something broke")

        pipeline = _make_pipeline(tmp_path)
        result = pipeline.run(_MockOp, inputs={"data": [_INPUT_ID]})

        assert result.status == StepStatus.FAILED
        assert result.error is not None
        assert "RuntimeError" in result.error
        assert result in pipeline._step_results
        assert pipeline._step_status_readers[0].get() == StepStatus.FAILED

    @patch("artisan.orchestration.pipeline_manager.execute_step")
    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_failed_step_records_failure_in_tracker(
        self, mock_tracker_cls, mock_execute, tmp_path
    ):
        """Failed step should persist running then failed snapshots."""
        mock_tracker = MagicMock()
        mock_tracker.check_cache.return_value = None
        mock_tracker.current_state.return_value = MagicMock(
            status=StepStatus.RUNNING,
            cancellation_status=None,
        )
        mock_tracker_cls.return_value = mock_tracker

        mock_execute.side_effect = ValueError("bad input")

        pipeline = _make_pipeline(tmp_path)
        pipeline.run(_MockOp, inputs={"data": [_INPUT_ID]})

        transitions = mock_tracker.transition.call_args_list
        assert [call.args[1:3] for call in transitions] == [
            (StepStatus.PENDING, StepStatus.RUNNING),
            (StepStatus.RUNNING, StepStatus.FAILED),
        ]
        failed = transitions[-1].kwargs["result"]
        assert failed.error is not None
        assert "ValueError" in failed.error


class TestResilientFinalize:
    """Tests for finalize() survives failed futures."""

    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_finalize_survives_failed_future(self, mock_tracker_cls, tmp_path):
        """finalize() should return summary even if a future raised."""
        mock_tracker_cls.return_value = MagicMock()

        pipeline = _make_pipeline(tmp_path)

        # Simulate a failed future
        failed_future = Future()
        failed_future.set_exception(RuntimeError("step exploded"))
        mock_step_future = MagicMock()
        mock_step_future.result.side_effect = RuntimeError("step exploded")
        pipeline._active_futures[0] = mock_step_future

        summary = pipeline.finalize()

        assert "pipeline_name" in summary
        assert "total_steps" in summary
        assert "overall_success" in summary

    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_finalize_includes_all_step_results(self, mock_tracker_cls, tmp_path):
        """finalize() summary includes results from all steps."""
        mock_tracker_cls.return_value = MagicMock()

        pipeline = _make_pipeline(tmp_path)
        pipeline._step_results = [
            StepResult(
                step_name="op1",
                step_number=0,
                status=StepStatus.SUCCEEDED,
                disposition=StepDisposition.EXECUTED,
                total_count=5,
                succeeded_count=5,
                failed_count=0,
            ),
            StepResult(
                step_name="op2",
                step_number=1,
                status=StepStatus.FAILED,
                error="test failure",
                total_count=3,
                succeeded_count=0,
                failed_count=3,
            ),
        ]

        summary = pipeline.finalize()

        assert summary["total_steps"] == 2
        assert summary["overall_success"] is False
        assert len(summary["steps"]) == 2


class TestPipelineCleanup:
    """Tests for resource cleanup safety net (process-leak fix)."""

    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_finalized_flag_set_on_finalize(self, mock_tracker_cls, tmp_path):
        """finalize() sets _finalized to True."""
        mock_tracker_cls.return_value = MagicMock()
        pipeline = _make_pipeline(tmp_path)
        assert not pipeline._finalized
        pipeline.finalize()
        assert pipeline._finalized

    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_double_finalize_returns_cached_summary(self, mock_tracker_cls, tmp_path):
        """Second finalize() returns the same cached summary object."""
        mock_tracker_cls.return_value = MagicMock()
        pipeline = _make_pipeline(tmp_path)
        summary1 = pipeline.finalize()
        summary2 = pipeline.finalize()
        assert summary1 is summary2

    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_double_finalize_does_not_shutdown_twice(self, mock_tracker_cls, tmp_path):
        """Second finalize() is a no-op — executor is already None."""
        mock_tracker_cls.return_value = MagicMock()
        pipeline = _make_pipeline(tmp_path)
        pipeline.finalize()
        assert pipeline._executor is None
        pipeline.finalize()  # should not raise

    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_del_shuts_down_executor_when_not_finalized(
        self, mock_tracker_cls, tmp_path
    ):
        """__del__ shuts down the executor if finalize was never called."""
        mock_tracker_cls.return_value = MagicMock()
        pipeline = _make_pipeline(tmp_path)
        executor = pipeline._executor
        pipeline.__del__()
        assert executor._shutdown

    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_del_noop_when_finalized(self, mock_tracker_cls, tmp_path):
        """__del__ is a no-op after finalize()."""
        mock_tracker_cls.return_value = MagicMock()
        pipeline = _make_pipeline(tmp_path)
        pipeline.finalize()
        pipeline.__del__()  # should not raise

    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_context_manager_calls_finalize(self, mock_tracker_cls, tmp_path):
        """Exiting a with-block calls finalize()."""
        mock_tracker_cls.return_value = MagicMock()
        config = PipelineConfig(
            name="test",
            delta_root=str(tmp_path / "delta"),
            staging_root=str(tmp_path / "staging"),
            working_root=str(tmp_path / "working"),
        )
        with PipelineManager(config) as pipeline:
            assert not pipeline._finalized
        assert pipeline._finalized

    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_context_manager_with_explicit_finalize(self, mock_tracker_cls, tmp_path):
        """Explicit finalize() inside with-block doesn't cause errors on exit."""
        mock_tracker_cls.return_value = MagicMock()
        config = PipelineConfig(
            name="test",
            delta_root=str(tmp_path / "delta"),
            staging_root=str(tmp_path / "staging"),
            working_root=str(tmp_path / "working"),
        )
        with PipelineManager(config) as pipeline:
            summary = pipeline.finalize()
        assert pipeline._finalized
        assert pipeline._summary is summary


class TestResilientPredecessorWaiting:
    """Tests for _wait_for_predecessors() survives failed predecessors."""

    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_predecessor_failure_does_not_raise(self, mock_tracker_cls, tmp_path):
        """Failed predecessor should log warning, not raise."""
        mock_tracker_cls.return_value = MagicMock()

        pipeline = _make_pipeline(tmp_path)

        # Simulate a failed predecessor
        mock_future = MagicMock()
        mock_future.result.side_effect = RuntimeError("pred failed")
        pipeline._active_futures[0] = mock_future

        inputs = {"data": OutputReference(source_step=0, role="data")}

        pipeline._wait_for_predecessors(inputs)


@pytest.mark.parametrize("default", list(CachePolicy))
@pytest.mark.parametrize("override", [None, *CachePolicy])
@pytest.mark.parametrize(
    "outcome", ["executed", "preparation_failed", "skipped", "cancelled"]
)
def test_cache_policy_audited_from_pending_through_terminal(
    tmp_path: Path, default: CachePolicy, override: CachePolicy | None, outcome: str
) -> None:
    pipeline = PipelineManager.create(
        name="policy_audit",
        delta_root=str(tmp_path / "delta"),
        staging_root=str(tmp_path / "staging"),
        cache_policy=default,
    )
    inputs = {"data": [] if outcome == "skipped" else [_INPUT_ID]}
    expected_status = {
        "executed": StepStatus.SUCCEEDED,
        "preparation_failed": StepStatus.FAILED,
        "skipped": StepStatus.SKIPPED,
        "cancelled": StepStatus.CANCELLED,
    }[outcome]
    if outcome == "cancelled":
        pipeline.cancel()

    def execute(**kwargs: Any) -> StepResult:
        result = StepResult(
            step_name="mock_op",
            step_number=0,
            status=StepStatus.SUCCEEDED,
            disposition=StepDisposition.EXECUTED,
            step_run_id=kwargs["step_run_id"],
            output_roles=frozenset({"output"}),
            output_types={"output": "data"},
        )
        return kwargs["persist_result"](result, ())

    import artisan.orchestration.pipeline_manager as manager_module

    prepare = manager_module.prepare_inputs
    with (
        patch.object(manager_module, "execute_step", side_effect=execute),
        patch.object(
            manager_module,
            "prepare_inputs",
            side_effect=(
                RuntimeError("preparation failed")
                if outcome == "preparation_failed"
                else prepare
            ),
        ),
    ):
        result = pipeline.run(
            _MockOp, inputs=inputs, cache_policy=override, compact=False
        )
    pipeline.finalize()
    assert result.status is expected_status
    rows = pl.read_delta(tmp_path / "delta" / "orchestration" / "steps").filter(
        pl.col("step_run_id") == result.step_run_id
    )
    assert {"pending", expected_status.value} <= set(rows["status"])
    expected_policy = override if override is not None else default
    assert {
        json.loads(value)["cache_policy"] for value in rows["compute_options_json"]
    } == {expected_policy.value}


class TestEmptyInputsHandling:
    """Tests for empty-inputs detection and pipeline stopping."""

    @patch("artisan.orchestration.pipeline_manager.execute_step")
    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_pipeline_stops_after_empty_inputs(
        self, mock_tracker_cls, mock_execute, tmp_path
    ):
        """Known-empty inputs persist skipped and stop downstream execution."""
        mock_tracker = MagicMock()
        mock_tracker.check_cache.return_value = None
        mock_tracker_cls.return_value = mock_tracker

        pipeline = _make_pipeline(tmp_path)

        # Run step 0 — should trigger _stopped
        result0 = pipeline.run(_MockOp, inputs={"data": []})
        assert result0.status == StepStatus.SKIPPED
        assert result0.metadata["skip_reason"] == "empty_inputs"
        assert mock_tracker.transition.call_args.args[1:3] == (
            StepStatus.PENDING,
            StepStatus.SKIPPED,
        )
        mock_execute.assert_not_called()

        # Run step 1 — should be immediately skipped without calling execute_step
        mock_execute.reset_mock()
        result1 = pipeline.run(
            _MockOp,
            inputs={"data": pipeline.output("mock_op", "output")},
        )
        assert result1.status == StepStatus.SKIPPED
        assert result1.metadata.get("skip_reason") == "pipeline_stopped"
        mock_execute.assert_not_called()

    @patch("artisan.orchestration.pipeline_manager.execute_step")
    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_succeeded_zero_outputs_still_cached(
        self, mock_tracker_cls, mock_execute, tmp_path
    ):
        """A succeeded zero-output step remains distinct from skipped."""
        mock_tracker = MagicMock()
        mock_tracker.check_cache.return_value = None
        mock_tracker_cls.return_value = mock_tracker

        # execute_step returns a result with 0 succeeded but NO skipped metadata
        zero_result = StepResult(
            step_name="mock_op",
            step_number=0,
            status=StepStatus.SUCCEEDED,
            disposition=StepDisposition.EXECUTED,
            total_count=0,
            succeeded_count=0,
            failed_count=0,
            output_roles=frozenset(["output"]),
            output_types={"output": "data"},
        )
        mock_execute.return_value = zero_result

        pipeline = _make_pipeline(tmp_path)
        result = pipeline.run(_MockOp, inputs={"data": [_INPUT_ID]})

        assert result.succeeded_count == 0
        assert mock_tracker.transition.call_args.args[1:3] == (
            StepStatus.PENDING,
            StepStatus.RUNNING,
        )
        assert (
            mock_tracker.current_state(result.step_run_id).status
            == StepStatus.SUCCEEDED
        )
        # Pipeline should NOT be stopped
        assert pipeline._stopped is False

    def test_preparation_failure_transitions_running_before_failure(
        self, tmp_path
    ) -> None:
        pipeline = _make_pipeline(tmp_path)

        with patch(
            "artisan.orchestration.pipeline_manager.instantiate_operation",
            side_effect=ValueError("invalid operation preparation"),
        ):
            result = pipeline.run(_MockOp, inputs={"data": [_INPUT_ID]})

        assert result.status == StepStatus.FAILED
        rows = pl.read_delta(str(tmp_path / "delta" / TablePath.STEPS)).filter(
            pl.col("step_run_id") == result.step_run_id
        )
        assert rows.sort("state_sequence")["status"].to_list() == [
            "pending",
            "running",
            "failed",
        ]


class TestStepNameOverride:
    """Tests for the name parameter on run()/submit()."""

    @patch("artisan.orchestration.pipeline_manager.execute_step")
    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_custom_name_propagates_to_result(
        self, mock_tracker_cls, mock_execute, tmp_path
    ):
        """run(name='custom') should set result.step_name to 'custom'."""
        mock_tracker = MagicMock()
        mock_tracker.check_cache.return_value = None
        mock_tracker_cls.return_value = mock_tracker

        mock_execute.return_value = StepResult(
            step_name="mock_op",
            step_number=0,
            status=StepStatus.SUCCEEDED,
            disposition=StepDisposition.EXECUTED,
            total_count=3,
            succeeded_count=3,
            failed_count=0,
            output_roles=frozenset(["output"]),
            output_types={"output": "data"},
        )

        pipeline = _make_pipeline(tmp_path)
        result = pipeline.run(_MockOp, inputs={"data": [_INPUT_ID]}, name="acyl_rmsd")

        assert result.step_name == "acyl_rmsd"

    @patch("artisan.orchestration.pipeline_manager.execute_step")
    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_default_name_uses_operation_name(
        self, mock_tracker_cls, mock_execute, tmp_path
    ):
        """run() without name should use operation.name as step_name."""
        mock_tracker = MagicMock()
        mock_tracker.check_cache.return_value = None
        mock_tracker_cls.return_value = mock_tracker

        mock_execute.return_value = StepResult(
            step_name="mock_op",
            step_number=0,
            status=StepStatus.SUCCEEDED,
            disposition=StepDisposition.EXECUTED,
            total_count=3,
            succeeded_count=3,
            failed_count=0,
            output_roles=frozenset(["output"]),
            output_types={"output": "data"},
        )

        pipeline = _make_pipeline(tmp_path)
        result = pipeline.run(_MockOp, inputs={"data": [_INPUT_ID]})

        assert result.step_name == "mock_op"

    @patch("artisan.orchestration.pipeline_manager.execute_step")
    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_custom_name_appears_in_start_record(
        self, mock_tracker_cls, mock_execute, tmp_path
    ):
        """StepStartRecord should contain the custom name, not operation.name."""
        mock_tracker = MagicMock()
        mock_tracker.check_cache.return_value = None
        mock_tracker_cls.return_value = mock_tracker

        mock_execute.return_value = StepResult(
            step_name="mock_op",
            step_number=0,
            status=StepStatus.SUCCEEDED,
            disposition=StepDisposition.EXECUTED,
            total_count=1,
            succeeded_count=1,
            failed_count=0,
            output_roles=frozenset(["output"]),
            output_types={"output": "data"},
        )

        pipeline = _make_pipeline(tmp_path)
        pipeline.run(_MockOp, inputs={"data": [_INPUT_ID]}, name="compute_metrics")

        mock_tracker.create_attempt.assert_called_once()
        start_record = mock_tracker.create_attempt.call_args.args[0]
        assert start_record.step_name == "compute_metrics"

    @patch("artisan.orchestration.pipeline_manager.execute_step")
    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_custom_name_on_failed_step(self, mock_tracker_cls, mock_execute, tmp_path):
        """Failed step with custom name should still use that name."""
        mock_tracker = MagicMock()
        mock_tracker.check_cache.return_value = None
        mock_tracker.current_state.return_value = MagicMock(
            status=StepStatus.RUNNING,
            cancellation_status=None,
        )
        mock_tracker_cls.return_value = mock_tracker

        mock_execute.side_effect = RuntimeError("boom")

        pipeline = _make_pipeline(tmp_path)
        result = pipeline.run(_MockOp, inputs={"data": [_INPUT_ID]}, name="custom_fail")

        assert result.status == StepStatus.FAILED
        assert result.step_name == "custom_fail"


class TestPipelineOutputByName:
    """Tests for pipeline.output(name, role) name-based step lookup."""

    @patch("artisan.orchestration.pipeline_manager.execute_step")
    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_returns_correct_reference(self, mock_tracker_cls, mock_execute, tmp_path):
        """pipeline.output(name, role) returns OutputReference with correct fields."""
        mock_tracker = MagicMock()
        mock_tracker.check_cache.return_value = None
        mock_tracker_cls.return_value = mock_tracker

        mock_execute.return_value = StepResult(
            step_name="mock_op",
            step_number=0,
            status=StepStatus.SUCCEEDED,
            disposition=StepDisposition.EXECUTED,
            total_count=3,
            succeeded_count=3,
            failed_count=0,
            output_roles=frozenset(["output"]),
            output_types={"output": "data"},
        )

        pipeline = _make_pipeline(tmp_path)
        pipeline.run(_MockOp, inputs={"data": [_INPUT_ID]}, name="foo")

        ref = pipeline.output("foo", "output")
        assert isinstance(ref, OutputReference)
        assert ref.source_step == 0
        assert ref.role == "output"

    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_unknown_name_raises(self, mock_tracker_cls, tmp_path):
        """pipeline.output() with unknown name raises ValueError."""
        mock_tracker_cls.return_value = MagicMock()

        pipeline = _make_pipeline(tmp_path)

        import pytest

        with pytest.raises(ValueError, match="No step named 'nonexistent'"):
            pipeline.output("nonexistent", "role")

    @patch("artisan.orchestration.pipeline_manager.execute_step")
    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_unknown_role_raises(self, mock_tracker_cls, mock_execute, tmp_path):
        """pipeline.output() rejects roles outside the declaration registry."""
        mock_tracker = MagicMock()
        mock_tracker.check_cache.return_value = None
        mock_tracker_cls.return_value = mock_tracker

        mock_execute.return_value = StepResult(
            step_name="mock_op",
            step_number=0,
            status=StepStatus.SUCCEEDED,
            disposition=StepDisposition.EXECUTED,
            total_count=1,
            succeeded_count=1,
            failed_count=0,
            output_roles=frozenset(["output"]),
            output_types={"output": "data"},
        )

        pipeline = _make_pipeline(tmp_path)
        pipeline.run(_MockOp, inputs={"data": [_INPUT_ID]}, name="foo")

        import pytest

        with pytest.raises(ValueError, match="Output role 'bad_role' not available"):
            pipeline.output("foo", "bad_role")

    @patch("artisan.orchestration.pipeline_manager.execute_step")
    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_last_wins_for_duplicate_names(
        self, mock_tracker_cls, mock_execute, tmp_path
    ):
        """When two steps share a name, output() returns the second step's reference."""
        mock_tracker = MagicMock()
        mock_tracker.check_cache.return_value = None
        mock_tracker_cls.return_value = mock_tracker

        # Step 0
        mock_execute.return_value = StepResult(
            step_name="mock_op",
            step_number=0,
            status=StepStatus.SUCCEEDED,
            disposition=StepDisposition.EXECUTED,
            total_count=1,
            succeeded_count=1,
            failed_count=0,
            output_roles=frozenset(["output"]),
            output_types={"output": "data"},
        )
        pipeline = _make_pipeline(tmp_path)
        pipeline.run(_MockOp, inputs={"data": [_INPUT_ID]}, name="dup")

        # Step 1
        mock_execute.return_value = StepResult(
            step_name="mock_op",
            step_number=1,
            status=StepStatus.SUCCEEDED,
            disposition=StepDisposition.EXECUTED,
            total_count=2,
            succeeded_count=2,
            failed_count=0,
            output_roles=frozenset(["output"]),
            output_types={"output": "data"},
        )
        pipeline.run(_MockOp, inputs={"data": [_INPUT_ID]}, name="dup")

        ref = pipeline.output("dup", "output")
        assert ref.source_step == 1

    @patch("artisan.orchestration.pipeline_manager.execute_step")
    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_works_with_default_name(self, mock_tracker_cls, mock_execute, tmp_path):
        """Step without explicit name= can be looked up by operation.name."""
        mock_tracker = MagicMock()
        mock_tracker.check_cache.return_value = None
        mock_tracker_cls.return_value = mock_tracker

        mock_execute.return_value = StepResult(
            step_name="mock_op",
            step_number=0,
            status=StepStatus.SUCCEEDED,
            disposition=StepDisposition.EXECUTED,
            total_count=1,
            succeeded_count=1,
            failed_count=0,
            output_roles=frozenset(["output"]),
            output_types={"output": "data"},
        )

        pipeline = _make_pipeline(tmp_path)
        pipeline.run(_MockOp, inputs={"data": [_INPUT_ID]})

        ref = pipeline.output("mock_op", "output")
        assert ref.source_step == 0
        assert ref.role == "output"

    @patch("artisan.orchestration.pipeline_manager.execute_step")
    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_contains_still_works(self, mock_tracker_cls, mock_execute, tmp_path):
        """'name in pipeline' returns True after running a step with that name."""
        mock_tracker = MagicMock()
        mock_tracker.check_cache.return_value = None
        mock_tracker_cls.return_value = mock_tracker

        mock_execute.return_value = StepResult(
            step_name="mock_op",
            step_number=0,
            status=StepStatus.SUCCEEDED,
            disposition=StepDisposition.EXECUTED,
            total_count=1,
            succeeded_count=1,
            failed_count=0,
            output_roles=frozenset(["output"]),
            output_types={"output": "data"},
        )

        pipeline = _make_pipeline(tmp_path)
        pipeline.run(_MockOp, inputs={"data": [_INPUT_ID]}, name="foo")

        assert "foo" in pipeline
        assert "bar" not in pipeline

    @patch("artisan.orchestration.pipeline_manager.execute_step")
    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_output_with_explicit_step_number(
        self, mock_tracker_cls, mock_execute, tmp_path
    ):
        """output(name, role, step_number=N) returns the step with that number."""
        mock_tracker = MagicMock()
        mock_tracker.check_cache.return_value = None
        mock_tracker_cls.return_value = mock_tracker

        # Step 0
        mock_execute.return_value = StepResult(
            step_name="mock_op",
            step_number=0,
            status=StepStatus.SUCCEEDED,
            disposition=StepDisposition.EXECUTED,
            total_count=1,
            succeeded_count=1,
            failed_count=0,
            output_roles=frozenset(["output"]),
            output_types={"output": "data"},
        )
        pipeline = _make_pipeline(tmp_path)
        pipeline.run(_MockOp, inputs={"data": [_INPUT_ID]}, name="dup")

        # Step 1
        mock_execute.return_value = StepResult(
            step_name="mock_op",
            step_number=1,
            status=StepStatus.SUCCEEDED,
            disposition=StepDisposition.EXECUTED,
            total_count=2,
            succeeded_count=2,
            failed_count=0,
            output_roles=frozenset(["output"]),
            output_types={"output": "data"},
        )
        pipeline.run(_MockOp, inputs={"data": [_INPUT_ID]}, name="dup")

        ref = pipeline.output("dup", "output", step_number=0)
        assert ref.source_step == 0
        assert ref.role == "output"

    @patch("artisan.orchestration.pipeline_manager.execute_step")
    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_output_step_number_not_found(
        self, mock_tracker_cls, mock_execute, tmp_path
    ):
        """step_number that doesn't exist for given name raises ValueError."""
        import pytest

        mock_tracker = MagicMock()
        mock_tracker.check_cache.return_value = None
        mock_tracker_cls.return_value = mock_tracker

        mock_execute.return_value = StepResult(
            step_name="mock_op",
            step_number=0,
            status=StepStatus.SUCCEEDED,
            disposition=StepDisposition.EXECUTED,
            total_count=1,
            succeeded_count=1,
            failed_count=0,
            output_roles=frozenset(["output"]),
            output_types={"output": "data"},
        )
        pipeline = _make_pipeline(tmp_path)
        pipeline.run(_MockOp, inputs={"data": [_INPUT_ID]}, name="foo")

        with pytest.raises(ValueError, match="has no entry with step_number=99"):
            pipeline.output("foo", "output", step_number=99)

    @patch("artisan.orchestration.pipeline_manager.execute_step")
    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_output_step_number_wrong_name(
        self, mock_tracker_cls, mock_execute, tmp_path
    ):
        """step_number exists under a different name raises ValueError."""
        import pytest

        mock_tracker = MagicMock()
        mock_tracker.check_cache.return_value = None
        mock_tracker_cls.return_value = mock_tracker

        # Step 0 named "alpha"
        mock_execute.return_value = StepResult(
            step_name="mock_op",
            step_number=0,
            status=StepStatus.SUCCEEDED,
            disposition=StepDisposition.EXECUTED,
            total_count=1,
            succeeded_count=1,
            failed_count=0,
            output_roles=frozenset(["output"]),
            output_types={"output": "data"},
        )
        pipeline = _make_pipeline(tmp_path)
        pipeline.run(_MockOp, inputs={"data": [_INPUT_ID]}, name="alpha")

        # Step 1 named "beta"
        mock_execute.return_value = StepResult(
            step_name="mock_op",
            step_number=1,
            status=StepStatus.SUCCEEDED,
            disposition=StepDisposition.EXECUTED,
            total_count=1,
            succeeded_count=1,
            failed_count=0,
            output_roles=frozenset(["output"]),
            output_types={"output": "data"},
        )
        pipeline.run(_MockOp, inputs={"data": [_INPUT_ID]}, name="beta")

        # step_number=1 belongs to "beta", not "alpha"
        with pytest.raises(ValueError, match="has no entry with step_number=1"):
            pipeline.output("alpha", "output", step_number=1)

    @patch("artisan.orchestration.pipeline_manager.execute_step")
    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_output_single_step_with_step_number(
        self, mock_tracker_cls, mock_execute, tmp_path
    ):
        """Explicit step_number works even for non-ambiguous single-step case."""
        mock_tracker = MagicMock()
        mock_tracker.check_cache.return_value = None
        mock_tracker_cls.return_value = mock_tracker

        mock_execute.return_value = StepResult(
            step_name="mock_op",
            step_number=0,
            status=StepStatus.SUCCEEDED,
            disposition=StepDisposition.EXECUTED,
            total_count=1,
            succeeded_count=1,
            failed_count=0,
            output_roles=frozenset(["output"]),
            output_types={"output": "data"},
        )
        pipeline = _make_pipeline(tmp_path)
        pipeline.run(_MockOp, inputs={"data": [_INPUT_ID]}, name="solo")

        ref = pipeline.output("solo", "output", step_number=0)
        assert ref.source_step == 0
        assert ref.role == "output"

    @patch("artisan.orchestration.pipeline_manager.execute_step")
    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_named_outputs_preserve_all_step_occurrences(
        self, mock_tracker_cls, mock_execute, tmp_path
    ):
        """After 3 steps with same name, all 3 are retrievable via step_number."""
        mock_tracker = MagicMock()
        mock_tracker.check_cache.return_value = None
        mock_tracker_cls.return_value = mock_tracker

        pipeline = _make_pipeline(tmp_path)
        for i in range(3):
            mock_execute.return_value = StepResult(
                step_name="mock_op",
                step_number=i,
                status=StepStatus.SUCCEEDED,
                disposition=StepDisposition.EXECUTED,
                total_count=1,
                succeeded_count=1,
                failed_count=0,
                output_roles=frozenset(["output"]),
                output_types={"output": "data"},
            )
            pipeline.run(_MockOp, inputs={"data": [_INPUT_ID]}, name="repeat")

        for i in range(3):
            ref = pipeline.output("repeat", "output", step_number=i)
            assert ref.source_step == i


class TestCancellation:
    """Tests for PipelineManager.cancel() and signal handling."""

    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_cancel_sets_event_idempotent(self, mock_tracker_cls, tmp_path):
        """cancel() sets the internal event and is idempotent."""
        mock_tracker_cls.return_value = MagicMock()
        pipeline = _make_pipeline(tmp_path)

        assert not pipeline._cancel_event.is_set()
        pipeline.cancel()
        assert pipeline._cancel_event.is_set()
        # Second call is a no-op (no error)
        pipeline.cancel()
        assert pipeline._cancel_event.is_set()

    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_submit_cancels_steps_when_cancelled(self, mock_tracker_cls, tmp_path):
        """submit() persists cancellation when its event is already set."""
        mock_tracker = MagicMock()
        mock_tracker.check_cache.return_value = None
        mock_tracker_cls.return_value = mock_tracker

        pipeline = _make_pipeline(tmp_path)
        pipeline.cancel()  # Cancel before any steps run

        result_future = pipeline.submit(_MockOp, inputs={"data": [_INPUT_ID]})
        result = result_future.result()

        assert result.status == StepStatus.CANCELLED
        assert result.cancellation_status == CancellationStatus.CONFIRMED
        assert result_future.status == StepStatus.CANCELLED
        transitions = mock_tracker.transition.call_args_list
        assert transitions[-1].args[1:3] == (
            StepStatus.PENDING,
            StepStatus.CANCELLED,
        )

    @patch("artisan.orchestration.pipeline_manager.execute_step")
    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_cancel_between_early_gate_and_executor_submit(
        self, mock_tracker_cls, mock_execute, tmp_path
    ):
        """Cancellation intent cannot close the executor under submit()."""
        mock_tracker = MagicMock()
        mock_tracker.check_cache.return_value = None
        mock_tracker_cls.return_value = mock_tracker
        pipeline = _make_pipeline(tmp_path)
        prepare = pipeline._prepare_step_spec

        def _prepare_then_cancel(*args, **kwargs):
            prepared = prepare(*args, **kwargs)
            pipeline.cancel()
            return prepared

        with patch.object(
            pipeline,
            "_prepare_step_spec",
            side_effect=_prepare_then_cancel,
        ):
            future = pipeline.submit(_MockOp, inputs={"data": [_INPUT_ID]})

        result = future.result(timeout=2)
        pipeline.finalize()

        assert result.status == StepStatus.CANCELLED
        assert result.cancellation_status == CancellationStatus.CONFIRMED
        mock_execute.assert_not_called()

    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_finalize_returns_cleanly_after_cancellation(
        self, mock_tracker_cls, tmp_path
    ):
        """finalize() should return summary dict after cancel."""
        mock_tracker_cls.return_value = MagicMock()

        pipeline = _make_pipeline(tmp_path)
        pipeline.cancel()

        summary = pipeline.finalize()

        assert "pipeline_name" in summary
        assert "overall_success" in summary

    @patch("artisan.orchestration.pipeline_manager.execute_step")
    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_cancel_during_predecessor_wait(
        self, mock_tracker_cls, mock_execute, tmp_path
    ):
        """Dependent step returns promptly when cancelled during predecessor wait."""
        import threading
        import time

        mock_tracker = MagicMock()
        mock_tracker.check_cache.return_value = None
        mock_tracker_cls.return_value = mock_tracker

        barrier = threading.Event()

        def slow_execute(**_kwargs):
            barrier.wait(timeout=10)
            return StepResult(
                step_name="mock_op",
                step_number=0,
                status=StepStatus.SUCCEEDED,
                disposition=StepDisposition.EXECUTED,
                total_count=1,
                succeeded_count=1,
                failed_count=0,
                output_roles=frozenset(["output"]),
                output_types={"output": "data"},
            )

        mock_execute.side_effect = slow_execute

        pipeline = _make_pipeline(tmp_path)
        step0 = pipeline.submit(_MockOp, inputs={"data": [_INPUT_ID]}, name="slow")

        # Submit the dependent step from another thread (submit() blocks
        # in _wait_for_predecessors on the calling thread)
        dep_result_holder: list[StepResult] = []

        def submit_dependent():
            future = pipeline.submit(
                _MockOp,
                inputs={"data": OutputReference(source_step=0, role="output")},
                name="dependent",
            )
            dep_result_holder.append(future.result(timeout=5))

        dep_thread = threading.Thread(target=submit_dependent)
        dep_thread.start()

        # Give the dependent submit time to reach _wait_for_predecessors
        time.sleep(0.3)
        pipeline.cancel()

        # The dependent thread should finish within ~2s (not hang)
        dep_thread.join(timeout=3.0)
        assert not dep_thread.is_alive(), "Dependent step hung after cancel"
        assert len(dep_result_holder) == 1
        result = dep_result_holder[0]
        assert result.status == StepStatus.CANCELLED
        assert result.cancellation_status == CancellationStatus.CONFIRMED

        # Release step0 so the executor can shut down
        barrier.set()
        step0.result(timeout=5)

    @patch("artisan.orchestration.pipeline_manager.execute_step")
    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_queued_run_closure_exits_on_cancel(
        self, mock_tracker_cls, mock_execute, tmp_path
    ):
        """_run closure bails out immediately if cancelled while queued."""
        mock_tracker = MagicMock()
        mock_tracker.check_cache.return_value = None
        mock_tracker_cls.return_value = mock_tracker

        mock_execute.return_value = StepResult(
            step_name="mock_op",
            step_number=0,
            status=StepStatus.SUCCEEDED,
            disposition=StepDisposition.EXECUTED,
            total_count=1,
            succeeded_count=1,
            failed_count=0,
            output_roles=frozenset(["output"]),
            output_types={"output": "data"},
        )

        pipeline = _make_pipeline(tmp_path)

        # Submit a step, then cancel before it can run.
        # Use a single-thread executor so the closure is queued.
        # We cancel after submit but the closure checks cancel at start.
        step0 = pipeline.submit(_MockOp, inputs={"data": [_INPUT_ID]}, name="step0")
        step0.result(timeout=5)  # let step0 finish

        # Now cancel and submit another step
        pipeline.cancel()
        step1 = pipeline.submit(
            _MockOp,
            inputs={"data": OutputReference(source_step=0, role="output")},
            name="step1",
        )
        result = step1.result(timeout=5)

        assert result.status == StepStatus.CANCELLED
        assert result.cancellation_status == CancellationStatus.CONFIRMED

    @patch("artisan.orchestration.pipeline_manager.execute_step")
    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_queued_run_closure_records_terminal_cancelled_state(
        self,
        mock_tracker_cls,
        mock_execute,
        tmp_path,
    ):
        """A recorded running step becomes terminal when cancelled in the queue."""
        import threading

        mock_tracker = MagicMock()
        mock_tracker.check_cache.return_value = None
        mock_tracker.current_state.return_value.status = StepStatus.RUNNING
        mock_tracker_cls.return_value = mock_tracker
        first_started = threading.Event()
        release_first = threading.Event()

        def _execute(**kwargs):
            if kwargs["step_number"] == 0:
                first_started.set()
                release_first.wait(timeout=5)
            return StepResult(
                step_name="mock_op",
                step_number=kwargs["step_number"],
                status=StepStatus.SUCCEEDED,
                disposition=StepDisposition.EXECUTED,
                total_count=1,
                succeeded_count=1,
                failed_count=0,
                output_roles=frozenset(["output"]),
                output_types={"output": "data"},
            )

        mock_execute.side_effect = _execute
        pipeline = _make_pipeline(tmp_path)
        pipeline.submit(_MockOp, inputs={"data": [_INPUT_ID]}, name="first")
        assert first_started.wait(timeout=2)
        queued = pipeline.submit(
            _MockOp,
            inputs={"data": [_INPUT_ID]},
            name="queued",
        )

        pipeline.cancel()
        finalized = threading.Event()

        def _finalize() -> None:
            pipeline.finalize()
            finalized.set()

        finalize_thread = threading.Thread(target=_finalize)
        finalize_thread.start()
        assert queued.status == StepStatus.RUNNING
        release_first.set()
        finalize_thread.join(timeout=5)

        assert finalized.is_set()
        assert queued.status == StepStatus.CANCELLED
        result = next(result for result in pipeline if result.step_number == 1)
        assert result.status == StepStatus.CANCELLED
        assert result.cancellation_status == CancellationStatus.CONFIRMED
        terminal = mock_tracker.transition.call_args_list[-1]
        assert terminal.args[1:3] == (
            StepStatus.RUNNING,
            StepStatus.CANCELLED,
        )

    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_finalize_cancel_during_future_wait(self, mock_tracker_cls, tmp_path):
        """finalize() returns cleanly when cancel fires while waiting on futures."""
        import threading
        import time

        mock_tracker_cls.return_value = MagicMock()

        pipeline = _make_pipeline(tmp_path)

        # Create a blocking future and inject it into _active_futures
        blocker = threading.Event()
        future = Future()

        def _resolve_after_cancel():
            blocker.wait(timeout=10)
            future.set_result(None)

        resolver = threading.Thread(target=_resolve_after_cancel)
        resolver.start()

        pipeline._active_futures[0] = future

        # Release the blocked future after cancellation so finalization can join.
        def _cancel_later():
            time.sleep(0.5)
            pipeline.cancel()
            blocker.set()

        cancel_thread = threading.Thread(target=_cancel_later)
        cancel_thread.start()

        start = time.time()
        summary = pipeline.finalize()
        elapsed = time.time() - start

        cancel_thread.join(timeout=2)
        resolver.join(timeout=2)

        assert "pipeline_name" in summary
        assert "overall_success" in summary
        assert elapsed < 3.0
        # Signal handlers should be restored
        assert pipeline._prev_sigint is None

    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_finalize_cancel_already_set(self, mock_tracker_cls, tmp_path):
        """cancel() records intent without shutting down from a signal path."""
        mock_tracker_cls.return_value = MagicMock()
        pipeline = _make_pipeline(tmp_path)
        executor = pipeline._executor

        pipeline.cancel()

        assert executor is not None
        assert executor._shutdown is False
        pipeline.finalize()

    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_cancelled_finalize_waits_beyond_five_seconds_for_terminal_write(
        self, mock_tracker_cls, tmp_path
    ):
        """Finalization joins late writers before synthesizing cancellations."""
        import threading
        import time

        mock_tracker_cls.return_value = MagicMock()
        pipeline = _make_pipeline(tmp_path)
        started = threading.Event()
        release = threading.Event()

        def _late_terminal_write() -> StepResult:
            started.set()
            release.wait(timeout=10)
            result = StepResult(
                step_name="late",
                step_number=0,
                status=StepStatus.SUCCEEDED,
                disposition=StepDisposition.EXECUTED,
                total_count=0,
                succeeded_count=0,
                failed_count=0,
                metadata={"terminal_writer": True},
            )
            pipeline._step_results.append(result)
            return result

        assert pipeline._executor is not None
        future = pipeline._executor.submit(_late_terminal_write)
        pipeline._active_futures[0] = StepFuture(
            step_number=0,
            step_name="late",
            output_roles=frozenset(),
            output_types={},
            future=future,
            status_reader=lambda: StepStatus.RUNNING,
        )
        pipeline._step_start_records[0] = MagicMock()
        assert started.wait(timeout=1)
        pipeline.cancel()
        timer = threading.Timer(5.1, release.set)
        timer.start()
        started_at = time.monotonic()
        pipeline.finalize()
        elapsed = time.monotonic() - started_at
        timer.join(timeout=1)

        assert elapsed >= 5.0
        assert pipeline._executor is None
        assert len(pipeline._step_results) == 1
        assert pipeline._step_results[0].metadata == {"terminal_writer": True}
        mock_tracker_cls.return_value.transition.assert_not_called()


class TestStepRegistry:
    """Tests for _step_registry: output() works before step completion."""

    @patch("artisan.orchestration.pipeline_manager.execute_step")
    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_output_before_submit_completes(
        self, mock_tracker_cls, mock_execute, tmp_path
    ):
        """output() returns OutputReference for a submitted-but-not-completed step."""
        mock_tracker = MagicMock()
        mock_tracker.check_cache.return_value = None
        mock_tracker_cls.return_value = mock_tracker

        # Make execute_step block until we release it
        import threading

        barrier = threading.Event()

        def slow_execute(**_kwargs):
            barrier.wait(timeout=5)
            return StepResult(
                step_name="mock_op",
                step_number=0,
                status=StepStatus.SUCCEEDED,
                disposition=StepDisposition.EXECUTED,
                total_count=1,
                succeeded_count=1,
                failed_count=0,
                output_roles=frozenset(["output"]),
                output_types={"output": "data"},
            )

        mock_execute.side_effect = slow_execute

        pipeline = _make_pipeline(tmp_path)
        future = pipeline.submit(_MockOp, inputs={"data": [_INPUT_ID]}, name="gen")

        # Step is still running — output() should work via _step_registry
        ref = pipeline.output("gen", "output")
        assert isinstance(ref, OutputReference)
        assert ref.source_step == 0
        assert ref.role == "output"
        assert ref.artifact_type == "data"

        # Release the step and clean up
        barrier.set()
        future.result(timeout=5)

    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_output_after_cancellation(self, mock_tracker_cls, tmp_path):
        """The declaration registry remains available after cancellation."""
        mock_tracker_cls.return_value = MagicMock()

        pipeline = _make_pipeline(tmp_path)
        pipeline.cancel()

        pipeline.submit(_MockOp, inputs={"data": [_INPUT_ID]}, name="cancelled_step")

        ref = pipeline.output("cancelled_step", "output")
        assert isinstance(ref, OutputReference)
        assert ref.source_step == 0
        assert ref.role == "output"

    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_output_invalid_name_raises(self, mock_tracker_cls, tmp_path):
        """output() with nonexistent step name raises ValueError."""
        import pytest

        mock_tracker_cls.return_value = MagicMock()
        pipeline = _make_pipeline(tmp_path)

        with pytest.raises(ValueError, match="No step named 'ghost'"):
            pipeline.output("ghost", "output")

    @patch("artisan.orchestration.pipeline_manager.execute_step")
    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_output_invalid_role_raises(self, mock_tracker_cls, mock_execute, tmp_path):
        """output() with wrong role raises ValueError."""
        import pytest

        mock_tracker = MagicMock()
        mock_tracker.check_cache.return_value = None
        mock_tracker_cls.return_value = mock_tracker

        mock_execute.return_value = StepResult(
            step_name="mock_op",
            step_number=0,
            status=StepStatus.SUCCEEDED,
            disposition=StepDisposition.EXECUTED,
            total_count=1,
            succeeded_count=1,
            failed_count=0,
            output_roles=frozenset(["output"]),
            output_types={"output": "data"},
        )

        pipeline = _make_pipeline(tmp_path)
        pipeline.run(_MockOp, inputs={"data": [_INPUT_ID]}, name="foo")

        with pytest.raises(ValueError, match="Output role 'bad' not available"):
            pipeline.output("foo", "bad")


class TestIsFilePathInput:
    """Tests for _is_file_path_input."""

    def test_valid_file_paths(self):
        assert _is_file_path_input(["/path/to/file.nc"]) is True
        assert _is_file_path_input(["relative/path.csv", "another.csv"]) is True

    def test_empty_list(self):
        assert _is_file_path_input([]) is False

    def test_non_list(self):
        assert _is_file_path_input({"data": "val"}) is False
        assert _is_file_path_input({}) is False
        assert _is_file_path_input(None) is False

    def test_output_reference_list(self):
        refs = [OutputReference(source_step=0, role="data")]
        assert _is_file_path_input(refs) is False

    def test_non_string_list(self):
        assert _is_file_path_input([123]) is False


class _ParamsOp(OperationDefinition):
    """Op with a params model for validation testing."""

    class Params(BaseModel):
        """Params for ``_ParamsOp``.

        Attributes:
            alpha: Mock parameter.
            beta: Mock parameter.
        """

        alpha: float = 1.0
        beta: int = 2

    class InputRole(StrEnum):
        data = auto()

    class OutputRole(StrEnum):
        output = auto()

    name: ClassVar[str] = "params_op"
    inputs: ClassVar[dict[str, InputSpec]] = {
        "data": InputSpec(artifact_type=ArtifactTypes.DATA, required=True),
    }
    outputs: ClassVar[dict[str, OutputSpec]] = {
        "output": OutputSpec(
            artifact_type=ArtifactTypes.DATA,
            infer_lineage_from={"inputs": ["data"]},
        ),
    }

    params: Params = Params()

    def preprocess(self, inputs: Any) -> dict:
        return {}

    def execute_function(self, inputs: Any, output_dir: Any) -> Any:
        return None


class TestPipelineManagerDunderMethods:
    """Tests for PipelineManager __repr__, __str__, __len__, etc."""

    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_len(self, mock_tracker_cls, tmp_path):
        mock_tracker_cls.return_value = MagicMock()
        pipeline = _make_pipeline(tmp_path)
        assert len(pipeline) == 0
        pipeline._step_results.append(
            StepResult(
                step_name="a",
                step_number=0,
                status=StepStatus.SUCCEEDED,
                disposition=StepDisposition.EXECUTED,
            )
        )
        assert len(pipeline) == 1

    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_getitem_slice(self, mock_tracker_cls, tmp_path):
        mock_tracker_cls.return_value = MagicMock()
        pipeline = _make_pipeline(tmp_path)
        r0 = StepResult(
            step_name="a",
            step_number=0,
            status=StepStatus.SUCCEEDED,
            disposition=StepDisposition.EXECUTED,
        )
        r1 = StepResult(
            step_name="b",
            step_number=1,
            status=StepStatus.SUCCEEDED,
            disposition=StepDisposition.EXECUTED,
        )
        pipeline._step_results = [r0, r1]
        assert pipeline[0:1] == [r0]

    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_getitem_out_of_range(self, mock_tracker_cls, tmp_path):
        mock_tracker_cls.return_value = MagicMock()
        pipeline = _make_pipeline(tmp_path)
        with pytest.raises(IndexError):
            pipeline[0]


class TestBuildOutputTypes:
    """Tests for PipelineManager._build_output_types."""

    def test_extracts_types(self):
        outputs = {
            "data": MagicMock(artifact_type="data"),
            "metric": MagicMock(artifact_type="metric"),
        }
        result = PipelineManager._build_output_types(outputs)
        assert result == {"data": "data", "metric": "metric"}

    def test_none_artifact_type(self):
        outputs = {"out": MagicMock(artifact_type=None)}
        result = PipelineManager._build_output_types(outputs)
        assert result == {"out": None}

    def test_empty_outputs(self):
        assert PipelineManager._build_output_types({}) == {}


class TestRegisterStep:
    """Tests for PipelineManager._register_step."""

    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_registers_entry(self, mock_tracker_cls, tmp_path):
        mock_tracker_cls.return_value = MagicMock()
        pipeline = _make_pipeline(tmp_path)
        outputs = {"data": MagicMock(artifact_type="data")}
        pipeline._register_step("my_step", 0, outputs)
        assert "my_step" in pipeline._step_registry
        assert pipeline._step_registry["my_step"][0].step_number == 0
        assert "data" in pipeline._step_registry["my_step"][0].output_roles

    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_appends_for_duplicate_names(self, mock_tracker_cls, tmp_path):
        mock_tracker_cls.return_value = MagicMock()
        pipeline = _make_pipeline(tmp_path)
        outputs = {"out": MagicMock(artifact_type="data")}
        pipeline._register_step("dup", 0, outputs)
        pipeline._register_step("dup", 1, outputs)
        assert len(pipeline._step_registry["dup"]) == 2


class TestSkipStep:
    """Tests for PipelineManager._skip_step."""

    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_returns_resolved_future(self, mock_tracker_cls, tmp_path):
        mock_tracker_cls.return_value = MagicMock()
        pipeline = _make_pipeline(tmp_path)
        outputs = {"output": MagicMock(artifact_type="data")}
        step_run_id = "a" * 32
        _prime_attempt(pipeline, step_run_id, step_name="skipped")
        future = pipeline._skip_step("skipped", outputs, "test_reason", step_run_id)
        result = future.result()
        assert result.status == StepStatus.SKIPPED
        assert result.metadata["skip_reason"] == "test_reason"
        assert result.step_name == "skipped"
        assert result.step_run_id == step_run_id
        assert future.status == StepStatus.SKIPPED
        assert pipeline._step_tracker.transition.call_args.args[1:3] == (
            StepStatus.PENDING,
            StepStatus.SKIPPED,
        )

    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_increments_step_counter(self, mock_tracker_cls, tmp_path):
        mock_tracker_cls.return_value = MagicMock()
        pipeline = _make_pipeline(tmp_path)
        assert pipeline._current_step == 0
        outputs = {"output": MagicMock(artifact_type="data")}
        _prime_attempt(pipeline, "a" * 32, step_name="s")
        pipeline._skip_step("s", outputs, "reason", "a" * 32)
        assert pipeline._current_step == 1

    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_populates_bookkeeping(self, mock_tracker_cls, tmp_path):
        mock_tracker_cls.return_value = MagicMock()
        pipeline = _make_pipeline(tmp_path)
        outputs = {"output": MagicMock(artifact_type="data")}
        _prime_attempt(pipeline, "a" * 32, step_name="my_step")
        pipeline._skip_step("my_step", outputs, "reason", "a" * 32)
        assert len(pipeline._step_results) == 1
        assert "my_step" in pipeline._step_registry
        assert pipeline.output("my_step", "output").source_step == 0


class TestWholeStepCacheReuse:
    """Whole-step hits retain the new current-run attempt identity."""

    def test_hit_keeps_current_id_and_captures_source_executions(self, tmp_path):
        from artisan.orchestration.engine.step_tracker import _WholeStepCacheHit

        pipeline = _make_pipeline(tmp_path)
        tracker = MagicMock()
        pipeline._step_tracker = tracker
        source = "a" * 32
        current = "b" * 32
        cached_execution = "c" * 32
        _prime_attempt(pipeline, current, status=StepStatus.RUNNING)
        tracker.check_cache.return_value = _WholeStepCacheHit(
            result=StepResult(
                step_name="source",
                step_number=8,
                status=StepStatus.SUCCEEDED,
                disposition=StepDisposition.EXECUTED,
                duration_seconds=99.0,
                output_roles=frozenset({"output"}),
                output_types={"output": ArtifactTypes.DATA},
                metadata={"timings": {"total": 99.0}, "diagnostic": "kept"},
                step_run_id=source,
            ),
            source_step_run_id=source,
            execution_run_ids=(cached_execution,),
        )

        def _commit_reuse(*_args: Any, **kwargs: Any) -> StepResult:
            return kwargs["result"].model_copy(update={"duration_seconds": 4.0})

        with (
            patch.object(
                pipeline, "_commit_whole_step_reuse", side_effect=_commit_reuse
            ) as commit_reuse,
            patch(
                "artisan.orchestration.pipeline_manager.time.perf_counter",
                return_value=14.0,
            ),
        ):
            future = pipeline._try_cached_step(
                _MockOp,
                StepOverrides.from_user(),
                step_spec_id="spec",
                step_number=0,
                step_name="current",
                prepared_operation=_MockOp(),
                step_run_id=current,
                attempt_started_at=10.0,
            )

        assert future is not None
        result = future.result()
        assert result.step_run_id == current
        assert result.step_run_id != source
        assert result.duration_seconds == 4.0
        assert result.metadata == {"diagnostic": "kept"}
        assert pipeline._step_run_ids[0] == current
        commit_reuse.assert_called_once()
        assert commit_reuse.call_args.args == (current, (cached_execution,))
        assert commit_reuse.call_args.kwargs["step_number"] == 0
        assert commit_reuse.call_args.kwargs["operation_name"] == _MockOp.name
        assert commit_reuse.call_args.kwargs["step_spec_id"] == "spec"
        persisted = commit_reuse.call_args.kwargs["result"]
        assert persisted.disposition == StepDisposition.CACHE_HIT
        tracker.transition.assert_not_called()

    def test_compaction_failure_follows_terminal_and_preserves_success(
        self, tmp_path
    ) -> None:
        from artisan.orchestration.engine.step_tracker import _WholeStepCacheHit

        pipeline = _make_pipeline(tmp_path)
        tracker = MagicMock()
        pipeline._step_tracker = tracker
        current = "b" * 32
        _prime_attempt(pipeline, current, status=StepStatus.RUNNING)
        tracker.check_cache.return_value = _WholeStepCacheHit(
            result=StepResult(
                step_name="source",
                step_number=8,
                status=StepStatus.SUCCEEDED,
                disposition=StepDisposition.EXECUTED,
                output_roles=frozenset({"output"}),
                output_types={"output": ArtifactTypes.DATA},
                step_run_id="a" * 32,
            ),
            source_step_run_id="a" * 32,
            execution_run_ids=("c" * 32,),
        )
        events: list[str] = []

        def _commit_reuse(*_args: Any, **kwargs: Any) -> StepResult:
            events.append("terminal")
            return kwargs["result"]

        def _fail_compaction(*args, **kwargs) -> None:
            events.append("compact")
            msg = "maintenance unavailable"
            raise OSError(msg)

        with (
            patch.object(
                pipeline,
                "_commit_whole_step_reuse",
                side_effect=_commit_reuse,
            ),
            patch(
                "artisan.orchestration.engine.step_executor._compact_step_tables",
                side_effect=_fail_compaction,
            ),
        ):
            future = pipeline._try_cached_step(
                _MockOp,
                StepOverrides.from_user(),
                step_spec_id="spec",
                step_number=0,
                step_name="current",
                prepared_operation=_MockOp(),
                step_run_id=current,
                attempt_started_at=0.0,
            )

        assert future is not None
        assert future.result().status == StepStatus.SUCCEEDED
        assert events == ["terminal", "compact"]

    def test_relation_commit_failure_prevents_terminal_success(self, tmp_path):
        from artisan.orchestration.engine.step_tracker import _WholeStepCacheHit

        pipeline = _make_pipeline(tmp_path)
        tracker = MagicMock()
        pipeline._step_tracker = tracker
        _prime_attempt(
            pipeline,
            "b" * 32,
            status=StepStatus.RUNNING,
            step_name="current",
        )
        tracker.check_cache.return_value = _WholeStepCacheHit(
            result=StepResult(
                step_name="source",
                step_number=8,
                status=StepStatus.SUCCEEDED,
                disposition=StepDisposition.EXECUTED,
                output_roles=frozenset({"output"}),
                output_types={"output": ArtifactTypes.DATA},
                step_run_id="a" * 32,
            ),
            source_step_run_id="a" * 32,
            execution_run_ids=("c" * 32,),
        )

        with (
            patch.object(
                pipeline,
                "_commit_whole_step_reuse",
                side_effect=CommitError(
                    "step_result:" + "b" * 32,
                    TablePath.CACHE_REUSE.value,
                    "d" * 32,
                    [],
                    ["cache_reuse.parquet"],
                    "cache reuse commit failed",
                ),
            ),
            pytest.raises(CommitError),
        ):
            pipeline._try_cached_step(
                _MockOp,
                StepOverrides.from_user(),
                step_spec_id="spec",
                step_number=0,
                step_name="current",
                prepared_operation=_MockOp(),
                step_run_id="b" * 32,
                attempt_started_at=0.0,
            )

        tracker.transition.assert_not_called()
        assert pipeline._step_results == []

    def test_final_cancellation_prevents_relation_staging(self, tmp_path):
        pipeline = _make_pipeline(tmp_path)
        pipeline._cancel_event.set()

        with (
            patch(
                "artisan.storage.core.run_scope.validate_cached_executions",
                return_value=["c" * 32],
            ) as validate,
            patch("artisan.storage.io.staging.StagingManager") as staging,
            patch("artisan.storage.io.commit.DeltaCommitter") as committer,
        ):
            result = StepResult(
                step_name="mock_op",
                step_number=0,
                status=StepStatus.SUCCEEDED,
                disposition=StepDisposition.CACHE_HIT,
                step_run_id="b" * 32,
            )
            committed = pipeline._commit_whole_step_reuse(
                "b" * 32,
                ("c" * 32,),
                step_number=0,
                operation_name="mock_op",
                result=result,
                step_spec_id="spec",
                attempt_started_at=0.0,
            )

        assert committed is None
        validate.assert_called_once()
        staging.assert_not_called()
        committer.assert_not_called()

    def test_final_cancellation_records_current_attempt_as_cancelled(self, tmp_path):
        from artisan.orchestration.engine.step_tracker import _WholeStepCacheHit

        pipeline = _make_pipeline(tmp_path)
        tracker = MagicMock()
        pipeline._step_tracker = tracker
        _prime_attempt(
            pipeline,
            "b" * 32,
            status=StepStatus.RUNNING,
            step_name="current",
        )
        tracker.check_cache.return_value = _WholeStepCacheHit(
            result=StepResult(
                step_name="source",
                step_number=8,
                status=StepStatus.SUCCEEDED,
                disposition=StepDisposition.EXECUTED,
                output_roles=frozenset({"output"}),
                output_types={"output": ArtifactTypes.DATA},
                step_run_id="a" * 32,
            ),
            source_step_run_id="a" * 32,
            execution_run_ids=("c" * 32,),
        )

        with patch.object(pipeline, "_commit_whole_step_reuse", return_value=None):
            future = pipeline._try_cached_step(
                _MockOp,
                StepOverrides.from_user(),
                step_spec_id="spec",
                step_number=0,
                step_name="current",
                prepared_operation=_MockOp(),
                step_run_id="b" * 32,
                attempt_started_at=0.0,
            )

        assert future is not None
        result = future.result()
        assert result.step_run_id == "b" * 32
        assert result.status == StepStatus.CANCELLED
        assert result.cancellation_status == CancellationStatus.CONFIRMED
        assert tracker.record_cancellation.call_count == 2
        assert tracker.transition.call_args.args[1:3] == (
            StepStatus.RUNNING,
            StepStatus.CANCELLED,
        )


class TestSignalHandling:
    """Tests for signal handler install / handle / restore."""

    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_install_and_restore(self, mock_tracker_cls, tmp_path):
        """Signal handlers can be installed and restored."""
        mock_tracker_cls.return_value = MagicMock()
        pipeline = _make_pipeline(tmp_path)
        prev_sigint = signal.getsignal(signal.SIGINT)
        pipeline._install_signal_handlers()
        assert signal.getsignal(signal.SIGINT) == pipeline._handle_signal
        pipeline._restore_signal_handlers()
        assert signal.getsignal(signal.SIGINT) == prev_sigint

    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_handle_signal_first_call_cancels(self, mock_tracker_cls, tmp_path):
        """First signal call triggers cancel."""
        mock_tracker_cls.return_value = MagicMock()
        pipeline = _make_pipeline(tmp_path)
        pipeline._install_signal_handlers()
        try:
            assert not pipeline._cancel_event.is_set()
            pipeline._handle_signal(signal.SIGINT, None)
            assert pipeline._cancel_event.is_set()
        finally:
            pipeline._restore_signal_handlers()

    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_handle_signal_second_call_restores(self, mock_tracker_cls, tmp_path):
        """Second signal call restores default handlers."""
        mock_tracker_cls.return_value = MagicMock()
        pipeline = _make_pipeline(tmp_path)
        pipeline._install_signal_handlers()
        try:
            pipeline._handle_signal(signal.SIGINT, None)
            pipeline._handle_signal(signal.SIGINT, None)
            assert pipeline._prev_sigint is None
        finally:
            # Ensure we don't leave bad handlers
            signal.signal(signal.SIGINT, signal.default_int_handler)

    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_restore_is_noop_without_install(self, mock_tracker_cls, tmp_path):
        """Restoring without installing is a no-op."""
        mock_tracker_cls.return_value = MagicMock()
        pipeline = _make_pipeline(tmp_path)
        pipeline._restore_signal_handlers()


class TestValidateOperationOverrides:
    """Tests for PipelineManager._validate_operation_overrides."""

    def test_no_overrides(self):
        PipelineManager._validate_operation_overrides(
            _MockOp,
            {"data": [_INPUT_ID]},
            StepOverrides.from_user(),
        )

    def test_invalid_params_raises(self):
        with pytest.raises(ValueError, match="Unknown params"):
            PipelineManager._validate_operation_overrides(
                _ParamsOp,
                None,
                StepOverrides.from_user(params={"bad_param": 1}),
            )

    def test_invalid_resources_raises(self):
        with pytest.raises(ValueError, match="Unknown resource"):
            PipelineManager._validate_operation_overrides(
                _MockOp,
                None,
                StepOverrides.from_user(runner_resources={"bogus": 1}),
            )

    def test_invalid_execution_raises(self):
        with pytest.raises(ValueError, match="Unknown execution"):
            PipelineManager._validate_operation_overrides(
                _MockOp,
                None,
                StepOverrides.from_user(batch_strategy={"bad_key": 1}),
            )

    def test_invalid_input_roles_raises(self):
        with pytest.raises(ValueError, match="Unknown input roles"):
            PipelineManager._validate_operation_overrides(
                _MockOp,
                {"bad_role": "val"},
                StepOverrides.from_user(),
            )

    def test_invalid_group_by_raises_type_error(self):
        """Passing a non-GroupByStrategy value raises with valid-member listing."""
        from artisan.schemas.enums import GroupByStrategy

        with pytest.raises(TypeError, match="GroupByStrategy") as exc_info:
            PipelineManager._validate_operation_overrides(
                _MockOp,
                {"data": [_INPUT_ID]},
                StepOverrides.from_user(
                    group_by="cross_product"
                ),  # str instead of enum
            )
        # Error message must enumerate valid members for usability.
        for member in GroupByStrategy:
            assert member.name in str(exc_info.value)

    def test_valid_group_by_passes(self):
        """A GroupByStrategy member is accepted without error."""
        from artisan.schemas.enums import GroupByStrategy

        PipelineManager._validate_operation_overrides(
            _MockOp,
            {"data": [_INPUT_ID]},
            StepOverrides.from_user(group_by=GroupByStrategy.CROSS_PRODUCT),
        )

    def test_group_by_none_passes(self):
        """``None`` is a valid value and means 'preserve class default'."""
        PipelineManager._validate_operation_overrides(
            _MockOp,
            {"data": [_INPUT_ID]},
            StepOverrides.from_user(),
        )


class TestCheckEarlyExit:
    """Tests for PipelineManager._check_early_exit."""

    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_returns_none_when_no_exit_conditions(self, mock_tracker_cls, tmp_path):
        mock_tracker_cls.return_value = MagicMock()
        pipeline = _make_pipeline(tmp_path)
        outputs = {"output": MagicMock(artifact_type="data")}
        result = pipeline._check_early_exit("step", outputs, None, "a" * 32)
        assert result is None

    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_returns_skipped_when_stopped(self, mock_tracker_cls, tmp_path):
        mock_tracker_cls.return_value = MagicMock()
        pipeline = _make_pipeline(tmp_path)
        pipeline._stopped = True
        outputs = {"output": MagicMock(artifact_type="data")}
        _prime_attempt(pipeline, "a" * 32)
        result = pipeline._check_early_exit("step", outputs, None, "a" * 32)
        assert result is not None
        assert result.status == StepStatus.SKIPPED
        assert result.result().metadata["skip_reason"] == "pipeline_stopped"

    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_returns_cancelled_when_cancelled(self, mock_tracker_cls, tmp_path):
        mock_tracker_cls.return_value = MagicMock()
        pipeline = _make_pipeline(tmp_path)
        pipeline._cancel_event.set()
        outputs = {"output": MagicMock(artifact_type="data")}
        _prime_attempt(pipeline, "a" * 32)
        result = pipeline._check_early_exit("step", outputs, None, "a" * 32)
        assert result is not None
        assert result.status == StepStatus.CANCELLED
        assert result.result().cancellation_status == CancellationStatus.CONFIRMED


class TestFilesRootThreading:
    """Tests for files_root propagation through PipelineManager."""

    def test_config_gets_default_files_root(self, tmp_path):
        """PipelineManager config derives files_root from delta_root."""
        config = PipelineConfig(
            name="test",
            delta_root=str(tmp_path / "pipeline" / "delta"),
            staging_root=str(tmp_path / "staging"),
        )
        pipeline = PipelineManager(config)
        assert pipeline.config.files_root == str(tmp_path / "pipeline" / "files")

    def test_config_gets_explicit_files_root(self, tmp_path):
        """PipelineManager config preserves explicit files_root."""
        custom_files = str(tmp_path / "bulk" / "files")
        config = PipelineConfig(
            name="test",
            delta_root=str(tmp_path / "delta"),
            staging_root=str(tmp_path / "staging"),
            files_root=custom_files,
        )
        pipeline = PipelineManager(config)
        assert pipeline.config.files_root == custom_files


class TestConfigureLoggingCloudGuard:
    """Owned pipeline file sinks exist only for local storage."""

    def test_local_storage_creates_owned_log_path(self, tmp_path):
        """Local sessions live under the sibling logs/runs directory."""
        from artisan.schemas.execution.storage_config import StorageConfig

        config = PipelineConfig(
            name="test",
            delta_root=str(tmp_path / "delta"),
            staging_root=str(tmp_path / "staging"),
            working_root=str(tmp_path / "working"),
            storage=StorageConfig(),  # protocol="file"
        )
        pipeline = PipelineManager(config)
        try:
            assert pipeline.log_path is not None
            assert Path(pipeline.log_path).is_relative_to(tmp_path / "logs" / "runs")
        finally:
            pipeline.finalize()

    def test_cloud_storage_has_no_pipeline_file(self, tmp_path):
        """Cloud stores never pass a URI to a local file handler."""
        from artisan.schemas.execution.storage_config import StorageConfig

        config = PipelineConfig(
            name="test",
            delta_root="s3://bucket/delta",
            staging_root="s3://bucket/staging",
            working_root=str(tmp_path / "working"),
            files_root="s3://bucket/files",
            storage=StorageConfig(protocol="s3"),
        )
        # StepTracker construction reads from delta_root via fs.exists,
        # which would fail without a real S3 step_runner; mock the fs.
        fake_fs = MagicMock()
        fake_fs.exists.return_value = True
        fake_fs.open.side_effect = lambda *_args, **_kwargs: io.StringIO(
            json.dumps(STORE_MANIFEST)
        )
        with (
            patch(
                "artisan.orchestration.pipeline_manager._RunLogSession"
            ) as mock_session,
            patch.object(StorageConfig, "filesystem", return_value=fake_fs),
            patch(
                "artisan.storage.io.commit.prepare_store_initialization",
                return_value=False,
            ),
            patch("artisan.orchestration.pipeline_manager.StepTracker"),
        ):
            pipeline = PipelineManager(config)
            assert pipeline.log_path is None
            pipeline.finalize()
        mock_session.assert_not_called()


class TestPromoteFilePathsCloudUri:
    """_promote_file_paths_to_store accepts cloud URIs via resolve_fs.

    These tests use MemoryFileSystem (via the protocol-match step of
    resolve_fs) so they don't depend on MinIO or Docker. The MemoryFS
    "happens to" act like a cloud step_runner from the function's POV —
    a different protocol than the local pipeline default.
    """

    def test_memory_uri_inputs_succeed(self, tmp_path):
        """memory:// inputs resolve via resolve_fs and get promoted."""
        import contextlib

        import fsspec
        from fixtures.store_format import publish_test_store

        from artisan.orchestration.pipeline_manager import (
            _promote_file_paths_to_store,
        )
        from artisan.schemas.execution.storage_config import StorageConfig

        mem_fs = fsspec.filesystem("memory")
        for i in range(2):
            with mem_fs.open(f"/promote-cloud/data_{i}.csv", "wb") as f:
                f.write(f"x,y\n{i},{i + 1}\n".encode())

        # The input URI uses MemoryFileSystem while the committed supported-format
        # store stays local, exercising the cross-protocol resolution path.
        config = PipelineConfig(
            name="test",
            delta_root=str(tmp_path / "promote-cloud-delta"),
            staging_root=str(tmp_path / "promote-cloud-staging"),
            working_root=str(tmp_path / "working"),
            files_root=str(tmp_path / "promote-cloud-files"),
            storage=StorageConfig(),
        )
        publish_test_store(config.delta_root, config.storage.filesystem())
        result, count, verified = _promote_file_paths_to_store(
            [
                "memory:///promote-cloud/data_0.csv",
                "memory:///promote-cloud/data_1.csv",
            ],
            config,
            step_number=1,
            operation_name="ingest",
            step_run_id="a" * 32,
        )
        assert count == 2
        assert result is not None
        assert "file" in result
        assert len(result["file"]) == 2
        assert verified == set(result["file"])

        # Best-effort cleanup of the in-memory fs. MemoryFileSystem
        # doesn't track empty dirs, so a missing parent on rm is fine.
        for path in ("/promote-cloud",):
            with contextlib.suppress(FileNotFoundError):
                mem_fs.rm(path, recursive=True)

    def test_invalid_uri_fails_closed(self, tmp_path):
        """An inaccessible cloud URI aborts raw-input preparation."""
        from artisan.orchestration.pipeline_manager import (
            _promote_file_paths_to_store,
        )

        config = PipelineConfig(
            name="test",
            delta_root=str(tmp_path / "delta"),
            staging_root=str(tmp_path / "staging"),
            working_root=str(tmp_path / "working"),
        )
        (tmp_path / "delta").mkdir(parents=True, exist_ok=True)
        (tmp_path / "staging").mkdir(parents=True, exist_ok=True)

        # gcs:// without gcsfs installed → ImportError surfaces as
        # Inaccessible, not a crash. (gcsfs is in optional deps.)
        with pytest.raises(ArtifactIntegrityError, match="Inaccessible"):
            _promote_file_paths_to_store(
                ["gcs://nope/notfound.csv"],
                config,
                step_number=1,
                operation_name="ingest",
                step_run_id="a" * 32,
            )


class _OpForTests(OperationDefinition):
    """Minimal OperationDefinition for composite-fail-fast tests."""

    name: ClassVar[str] = "_op_for_composite_tests"
    inputs: ClassVar[dict] = {}
    outputs: ClassVar[dict] = {}

    def execute_function(self, _inputs):
        return {}


class _CompositeForTests:
    """Synthetic CompositeDefinition stand-in for fail-fast tests.

    Does not need to be functional — these tests only check the early
    rejection paths in submit / run / submit_composite.
    """

    name = "_composite_for_tests"
    inputs: ClassVar[dict] = {}
    outputs: ClassVar[dict] = {}


# Real CompositeDefinition for fail-fast tests
from artisan.composites.base.composite_definition import (
    CompositeDefinition,
)


class _RealComposite(CompositeDefinition):
    name: ClassVar[str] = "_real_composite_for_tests"
    inputs: ClassVar[dict] = {}
    outputs: ClassVar[dict] = {}

    def compose(self, ctx):
        return None


class TestCompositeFailFast:
    """submit() and run() must reject CompositeDefinition subclasses."""

    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_submit_rejects_composite(self, mock_tracker_cls, tmp_path):
        mock_tracker_cls.return_value = MagicMock()
        pipeline = _make_pipeline(tmp_path)

        with pytest.raises(TypeError, match="submit_composite"):
            pipeline.submit(_RealComposite)

    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_run_rejects_composite(self, mock_tracker_cls, tmp_path):
        mock_tracker_cls.return_value = MagicMock()
        pipeline = _make_pipeline(tmp_path)

        with pytest.raises(TypeError, match="submit_composite"):
            pipeline.run(_RealComposite)

    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_submit_composite_rejects_operation(self, mock_tracker_cls, tmp_path):
        mock_tracker_cls.return_value = MagicMock()
        pipeline = _make_pipeline(tmp_path)

        with pytest.raises(TypeError, match="CompositeDefinition"):
            pipeline.submit_composite(_OpForTests)

    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_submit_composite_rejects_expand_kwarg(self, mock_tracker_cls, tmp_path):
        """The removed expand kwarg is rejected as an unexpected keyword."""
        mock_tracker_cls.return_value = MagicMock()
        pipeline = _make_pipeline(tmp_path)

        removed_kwarg = {"expand": True}
        with pytest.raises(TypeError):
            pipeline.submit_composite(_RealComposite, **removed_kwarg)

    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_submit_composite_rejects_intermediates_kwarg(
        self, mock_tracker_cls, tmp_path
    ):
        """The removed intermediates kwarg is rejected as an unexpected keyword."""
        mock_tracker_cls.return_value = MagicMock()
        pipeline = _make_pipeline(tmp_path)

        removed_kwarg = {"intermediates": "persist"}
        with pytest.raises(TypeError):
            pipeline.submit_composite(_RealComposite, **removed_kwarg)

    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_submit_composite_returns_composite_result(
        self, mock_tracker_cls, tmp_path
    ):
        """submit_composite returns a CompositeResult."""
        mock_tracker_cls.return_value = MagicMock()
        pipeline = _make_pipeline(tmp_path)

        result = pipeline.submit_composite(_RealComposite)
        from artisan.composites.base.results import CompositeResult

        assert isinstance(result, CompositeResult)

    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_submit_composite_accepts_compute_resources(
        self, mock_tracker_cls, tmp_path
    ):
        """submit_composite accepts compute_resources kwarg (symmetry with submit)."""
        mock_tracker_cls.return_value = MagicMock()
        pipeline = _make_pipeline(tmp_path)

        result = pipeline.submit_composite(
            _RealComposite,
            compute_resources={"memory_gb": 16, "timeout": 7200},
        )
        from artisan.composites.base.results import CompositeResult

        assert isinstance(result, CompositeResult)

    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_run_composite_accepts_compute_resources(self, mock_tracker_cls, tmp_path):
        """run_composite accepts compute_resources kwarg (symmetry with run)."""
        mock_tracker_cls.return_value = MagicMock()
        pipeline = _make_pipeline(tmp_path)

        result = pipeline.run_composite(
            _RealComposite,
            compute_resources={"memory_gb": 16},
        )
        from artisan.composites.base.results import CompositeResult

        assert isinstance(result, CompositeResult)

    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_submit_composite_rejects_bad_runner_resources(
        self, mock_tracker_cls, tmp_path
    ):
        """Composite-level override keys are validated fail-fast."""
        mock_tracker_cls.return_value = MagicMock()
        pipeline = _make_pipeline(tmp_path)

        with pytest.raises(ValueError, match="Unknown resource keys"):
            pipeline.submit_composite(_RealComposite, runner_resources={"not_a_key": 1})

    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_submit_composite_rejects_inactive_provider_environment(
        self, mock_tracker_cls, tmp_path
    ):
        """A composite-level env dict configuring an inactive provider raises."""
        mock_tracker_cls.return_value = MagicMock()
        pipeline = _make_pipeline(tmp_path)

        with pytest.raises(ValueError, match="Configured inactive provider"):
            pipeline.submit_composite(
                _RealComposite,
                environment={"docker": {"image": "biocontainers/samtools:1.17"}},
            )


class _IngestForCompositeTests(OperationDefinition):
    """Minimal curator op with one output role for composite blocking tests.

    Has no inputs, so a composite can call ``ctx.run`` repeatedly without
    plumbing artifact references.
    """

    class OutputRole(StrEnum):
        file = auto()

    name: ClassVar[str] = "ingest_for_composite_blocking_tests"
    inputs: ClassVar[dict[str, InputSpec]] = {}
    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.file: OutputSpec(artifact_type=ArtifactTypes.DATA),
    }

    def execute_curator(self, *args, **kwargs):
        from artisan.schemas.execution.curator_result import ArtifactResult

        return ArtifactResult(success=True)


class _TwoStepComposite(CompositeDefinition):
    """Calls ``ctx.run`` twice — drives child-future capture."""

    name: ClassVar[str] = "_two_step_composite_for_blocking_tests"
    inputs: ClassVar[dict] = {}

    class OutputRole(StrEnum):
        file = auto()

    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.file: OutputSpec(artifact_type=ArtifactTypes.DATA),
    }

    def compose(self, ctx):
        # Two ctx.run() calls — drives the child-future capture even though
        # the same op appears twice (each call dispatches its own step).
        ctx.run(_IngestForCompositeTests)
        second = ctx.run(_IngestForCompositeTests)
        ctx.output("file", second.output("file"))


def _slow_execute_step(**kwargs):
    """Mock execute_step — sleep then return a successful StepResult."""
    import time

    from artisan.orchestration.engine.step_executor import build_step_result
    from artisan.schemas.enums import FailurePolicy

    time.sleep(0.2)
    return build_step_result(
        operation=kwargs["operation"],
        step_number=kwargs["step_number"],
        succeeded_count=1,
        failed_count=0,
        failure_policy=kwargs["ov"].failure_policy or FailurePolicy.CONTINUE,
    )


class TestRunComposite:
    """Blocking semantics of ``run_composite``."""

    @patch(
        "artisan.orchestration.pipeline_manager.execute_step",
        side_effect=_slow_execute_step,
    )
    def test_run_composite_blocks_until_children_done(self, mock_exec, tmp_path):
        """``run_composite`` returns only after every child future is done."""
        pipeline = PipelineManager.create(
            name="test_run_composite",
            delta_root=str(tmp_path / "delta"),
            staging_root=str(tmp_path / "staging"),
        )

        result = pipeline.run_composite(_TwoStepComposite)

        from artisan.composites.base.results import CompositeResult

        assert isinstance(result, CompositeResult)
        # Both child futures captured by the context.
        assert len(result._child_futures) == 2
        # And every one of them is done after run_composite returns.
        assert all(f.done for f in result._child_futures)


class TestSilentMisconfigRejection:
    """Dict overrides without a matching ``active`` selector must raise."""

    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_environment_dict_with_inactive_provider_raises(
        self, mock_tracker_cls, tmp_path
    ):
        mock_tracker_cls.return_value = MagicMock()
        pipeline = _make_pipeline(tmp_path)

        with pytest.raises(ValueError, match="Configured inactive provider"):
            pipeline.submit(
                _OpForTests,
                environment={"docker": {"image": "biocontainers/samtools:1.17"}},
            )

    @patch("artisan.orchestration.pipeline_manager.StepTracker")
    def test_compute_provider_dict_with_inactive_provider_raises(
        self, mock_tracker_cls, tmp_path
    ):
        mock_tracker_cls.return_value = MagicMock()
        pipeline = _make_pipeline(tmp_path)

        with pytest.raises(ValueError, match="Configured inactive provider"):
            pipeline.submit(
                _OpForTests,
                compute_provider={"modal": {"image": "ghcr.io/x/y:latest"}},
            )


# Golden IDs cover effective configuration and concrete input identity. Changing
# one intentionally invalidates cache reuse for that override shape.
# ``bare`` and ``environment_local`` match because local is already the default.
_GOLDEN_STEP_SPEC_IDS: dict[str, str] = {
    "bare": "92387ad3e5a1814738c3fdca68691d22",
    "environment_local": "92387ad3e5a1814738c3fdca68691d22",
    "environment_docker_dict": "03ec1a49005ae3dccb3b40a946c3831c",
    "compute_resources_a100": "826868038f08147c42efef26c5ce9a32",
    "group_by_cross": "d74e91064aabff4a5e9c8996f6d60e8f",
}

_GOLDEN_OVERRIDES: dict[str, dict[str, Any]] = {
    "bare": {},
    "environment_local": {"environment": "local"},
    "environment_docker_dict": {
        "environment": {"active": "docker", "docker": {"image": "img:v2"}}
    },
    "compute_resources_a100": {"compute_resources": {"gpu": "A100", "memory_gb": 32}},
    "group_by_cross": {"group_by": GroupByStrategy.CROSS_PRODUCT},
}


@patch("artisan.orchestration.pipeline_manager.compute_step_spec_id")
@patch("artisan.orchestration.pipeline_manager.StepTracker")
def test_unconfigured_compute_selector_terminalizes_after_attempt_creation(
    mock_tracker_cls, mock_hash, tmp_path
) -> None:
    """Provider resolution failures become durable failed attempts."""
    tracker = MagicMock()
    tracker.current_state.return_value = MagicMock(
        status=StepStatus.RUNNING,
        cancellation_status=None,
    )
    mock_tracker_cls.return_value = tracker
    pipeline = _make_pipeline(tmp_path)

    result = pipeline.submit(
        _MockOp,
        inputs={"data": [_INPUT_ID]},
        compute_provider="modal",
    ).result()

    assert result.status == StepStatus.FAILED
    assert result.error is not None
    assert "not configured" in result.error
    tracker.create_attempt.assert_called_once()
    assert tracker.transition.call_args_list[-1].args[1:3] == (
        StepStatus.RUNNING,
        StepStatus.FAILED,
    )
    mock_hash.assert_not_called()


@pytest.mark.parametrize("label", sorted(_GOLDEN_STEP_SPEC_IDS))
@patch("artisan.orchestration.pipeline_manager.execute_step")
@patch("artisan.orchestration.pipeline_manager.StepTracker")
def test_step_spec_id_is_byte_identical(
    mock_tracker_cls, mock_execute, label, tmp_path
):
    """submit() reproduces the expected ID for each concrete override."""
    mock_tracker = MagicMock()
    mock_tracker.check_cache.return_value = None
    mock_tracker_cls.return_value = mock_tracker
    mock_execute.return_value = StepResult(
        step_name=_MockOp.name,
        step_number=0,
        status=StepStatus.SUCCEEDED,
        disposition=StepDisposition.EXECUTED,
        total_count=0,
        succeeded_count=0,
        failed_count=0,
        duration_seconds=0.0,
    )

    pipeline = _make_pipeline(tmp_path)
    pipeline.submit(_MockOp, inputs={"data": [_INPUT_ID]}, **_GOLDEN_OVERRIDES[label])
    pipeline.finalize()

    assert _recorded_step_spec(pipeline, 0) == _GOLDEN_STEP_SPEC_IDS[label]


# Two ops sharing a name but differing only in their class-default image —
# the "before" and "after" of an image bump. Same name → identical
# operation_name in the hash, so the image is the sole variable.
class _ImageOpV1(OperationDefinition):
    """Op whose container image is a class-level default (v1)."""

    class InputRole(StrEnum):
        data = auto()

    class OutputRole(StrEnum):
        output = auto()

    name: ClassVar[str] = "mock_image_bump_op"
    environments: Environments = Environments(
        active="docker", docker=DockerEnvironmentSpec(image="lab/tool:v1")
    )
    inputs: ClassVar[dict[str, InputSpec]] = {
        InputRole.data: InputSpec(artifact_type=ArtifactTypes.DATA, required=True),
    }
    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.output: OutputSpec(
            artifact_type=ArtifactTypes.DATA,
            infer_lineage_from={"inputs": ["data"]},
        ),
    }

    def preprocess(self, inputs: Any) -> dict:
        return {}

    def execute_function(self, inputs: Any, output_dir: Any) -> Any:
        return None


class _ImageOpV2(OperationDefinition):
    """Identical to _ImageOpV1 but the class-default image is bumped to v2."""

    class InputRole(StrEnum):
        data = auto()

    class OutputRole(StrEnum):
        output = auto()

    name: ClassVar[str] = "mock_image_bump_op"
    environments: Environments = Environments(
        active="docker", docker=DockerEnvironmentSpec(image="lab/tool:v2")
    )
    inputs: ClassVar[dict[str, InputSpec]] = {
        InputRole.data: InputSpec(artifact_type=ArtifactTypes.DATA, required=True),
    }
    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.output: OutputSpec(
            artifact_type=ArtifactTypes.DATA,
            infer_lineage_from={"inputs": ["data"]},
        ),
    }

    def preprocess(self, inputs: Any) -> dict:
        return {}

    def execute_function(self, inputs: Any, output_dir: Any) -> Any:
        return None


@patch("artisan.orchestration.pipeline_manager.execute_step")
@patch("artisan.orchestration.pipeline_manager.StepTracker")
def test_class_default_image_bump_changes_step_spec_id(
    mock_tracker_cls, mock_execute, tmp_path
):
    """Bumping only the class-default image (no per-step override) flips the
    step_spec_id — the motivating bug, as an end-to-end regression guard.

    Before this change the image lived only in the class default, invisible to
    the typed-override cache key, so both runs produced the same key and
    the cache served v1 artifacts for a v2 op.
    """

    def _spec_id(op: type[OperationDefinition]) -> str:
        mock_tracker = MagicMock()
        mock_tracker.check_cache.return_value = None
        mock_tracker_cls.return_value = mock_tracker
        mock_execute.return_value = StepResult(
            step_name=op.name,
            step_number=0,
            status=StepStatus.SUCCEEDED,
            disposition=StepDisposition.EXECUTED,
            total_count=0,
            succeeded_count=0,
            failed_count=0,
            duration_seconds=0.0,
        )
        pipeline = _make_pipeline(tmp_path / op.__name__)
        pipeline.submit(op, inputs={"data": [_INPUT_ID]})
        pipeline.finalize()
        return _recorded_step_spec(pipeline, 0)

    assert _spec_id(_ImageOpV1) != _spec_id(_ImageOpV2)


@pytest.mark.parametrize("cacheable", [False, True])
@pytest.mark.parametrize(
    "policy", [CachePolicy.ALL_SUCCEEDED, CachePolicy.STEP_COMPLETED]
)
def test_submit_cacheability_takes_precedence_over_consumer_policy(
    tmp_path,
    monkeypatch,
    cacheable: bool,
    policy: CachePolicy,
) -> None:
    """Only operations with sufficient declared identity enter whole-step lookup."""
    monkeypatch.setattr(_MockOp, "cacheable", cacheable)
    with patch("artisan.orchestration.pipeline_manager.execute_step") as execute:
        execute.return_value = StepResult(
            step_name=_MockOp.name,
            step_number=0,
            status=StepStatus.SUCCEEDED,
            disposition=StepDisposition.EXECUTED,
        )
        pipeline = _make_pipeline(tmp_path)
        with patch.object(pipeline, "_try_cached_step", return_value=None) as lookup:
            result = pipeline.run(
                _MockOp,
                inputs={"data": [_INPUT_ID]},
                skip_cache=False,
                cache_policy=policy,
                compact=False,
            )
        pipeline.finalize()
    assert result.status == StepStatus.SUCCEEDED
    assert lookup.call_count == int(cacheable)
    execute.assert_called_once()


@pytest.fixture
def session_logging():
    import logging

    from artisan.utils.logging import configure_logging

    logger = logging.getLogger("artisan")
    original = (logger.handlers[:], logger.level, logger.propagate)
    logger.handlers.clear()
    configure_logging("DEBUG")
    yield logger
    for handler in logger.handlers[:]:
        handler.close()
    logger.handlers[:] = original[0]
    logger.setLevel(original[1])
    logger.propagate = original[2]


def test_manager_cancel_signal_and_finalize_are_session_scoped(
    tmp_path, session_logging
):
    import logging

    first = PipelineManager.create(
        "first", str(tmp_path / "delta"), str(tmp_path / "staging")
    )
    second = PipelineManager.create(
        "second", str(tmp_path / "delta"), str(tmp_path / "staging")
    )
    first_path, second_path = first.log_path, second.log_path
    assert first_path != second_path
    first._handle_signal(signal.SIGINT, None)
    first._handle_signal(signal.SIGINT, None)
    logging.getLogger("artisan.test").info("unbound sentinel")
    summary = first.finalize()
    assert first.finalize() is summary
    assert first.log_path == first_path
    second.cancel()
    second.finalize()
    one, two = Path(first_path).read_text(), Path(second_path).read_text()
    assert "received SIGINT" in one
    assert "received second SIGINT" in one
    assert "Pipeline 'first' complete" in one
    assert "Pipeline 'second' complete" in two
    assert "Pipeline 'second'" not in one
    assert "Pipeline 'first'" not in two
    assert "unbound sentinel" not in one + two


def test_manager_finalize_failure_closes_sink_and_retry_does_not_reopen(
    tmp_path, session_logging
):
    pipeline = PipelineManager.create(
        "failed", str(tmp_path / "delta"), str(tmp_path / "staging")
    )
    handler = pipeline._log_session._handler
    with (
        patch.object(
            pipeline, "_shutdown_executor", side_effect=RuntimeError("finalize broke")
        ),
        pytest.raises(RuntimeError, match="finalize broke"),
    ):
        pipeline.finalize()
    assert handler not in session_logging.handlers
    assert handler.stream is None
    path = Path(pipeline.log_path)
    content = path.read_text()
    pipeline.finalize()
    assert path.read_text() == content


def test_manager_gc_releases_handler_without_joining_workers(tmp_path, session_logging):
    import gc
    import weakref

    pipeline = PipelineManager.create(
        "forgotten", str(tmp_path / "delta"), str(tmp_path / "staging")
    )
    handler = pipeline._log_session._handler
    executor = pipeline._executor
    reference = weakref.ref(pipeline)
    with patch.object(executor, "shutdown", wraps=executor.shutdown) as shutdown:
        del pipeline
        gc.collect()
        assert reference() is None
        shutdown.assert_called_once_with(wait=False, cancel_futures=False)
    assert handler not in session_logging.handlers
    assert handler.stream is None


def test_manager_automatic_logging_can_be_disabled(tmp_path, session_logging):
    config = PipelineConfig(
        name="disabled",
        delta_root=str(tmp_path / "delta"),
        staging_root=str(tmp_path / "staging"),
    )
    with patch(
        "artisan.orchestration.pipeline_manager._configure_default_logging"
    ) as configure:
        pipeline = PipelineManager(config, configure_logging=False)
    assert pipeline.log_path is None
    configure.assert_not_called()
    pipeline.finalize()


def test_manager_sink_setup_failure_does_not_fail_construction(
    tmp_path, session_logging
):
    with patch(
        "artisan.orchestration.pipeline_manager._RunLogSession",
        side_effect=PermissionError("read only"),
    ):
        pipeline = PipelineManager.create(
            "no-file", str(tmp_path / "delta"), str(tmp_path / "staging")
        )
    assert pipeline.log_path is None
    pipeline.finalize()


def test_manager_partial_construction_never_attaches_sink(tmp_path, session_logging):
    before = session_logging.handlers[:]
    with (
        patch(
            "artisan.storage.io.commit.DeltaCommitter.initialize_tables",
            side_effect=RuntimeError("bad store"),
        ),
        pytest.raises(RuntimeError, match="bad store"),
    ):
        PipelineManager.create(
            "broken", str(tmp_path / "delta"), str(tmp_path / "staging")
        )
    assert session_logging.handlers == before


def test_manager_factory_error_closes_constructed_session(tmp_path, session_logging):
    before = session_logging.handlers[:]
    original = PipelineManager._shutdown_executor
    cleanups = []

    def shutdown(self, *, wait=True):
        cleanups.append(wait)
        original(self, wait=wait)

    with (
        patch(
            "artisan.orchestration.pipeline_manager.logger.info",
            side_effect=RuntimeError("post-construction"),
        ),
        patch.object(PipelineManager, "_shutdown_executor", shutdown),
        pytest.raises(RuntimeError, match="post-construction"),
    ):
        PipelineManager.create(
            "broken", str(tmp_path / "delta"), str(tmp_path / "staging")
        )
    assert cleanups[0] is False
    assert session_logging.handlers == before


def test_manager_session_context_reaches_router_collection(tmp_path, session_logging):
    import logging

    from artisan.orchestration.engine.lifecycle_router import LifecycleRouter
    from artisan.schemas.orchestration.step_lifecycle import (
        CancellationAcknowledgement,
        CancellationStatus,
    )

    class LogRouter(LifecycleRouter):
        def _dispatch(self, units, runtime_env):
            def collect():
                logging.getLogger("artisan.test.router").info(
                    "router collection marker"
                )
                return []

            self._start_background(collect)

        def cancel(self):
            return CancellationAcknowledgement(CancellationStatus.REQUESTED)

    with PipelineManager.create(
        "router", str(tmp_path / "delta"), str(tmp_path / "staging")
    ) as manager:
        with manager._log_context():
            router = LogRouter()
            router._dispatch([], None)
        router._thread.join(timeout=5)
        assert not router._thread.is_alive()
    assert "router collection marker" in Path(manager.log_path).read_text()


def _succeeded_step(step_name: str, step_number: int) -> StepResult:
    """Build a minimal successful terminal step for manager API tests."""
    return StepResult(
        step_name=step_name,
        step_number=step_number,
        status=StepStatus.SUCCEEDED,
        disposition=StepDisposition.EXECUTED,
    )


def _failed_step(step_name: str, step_number: int) -> StepResult:
    """Build a minimal failed terminal step for manager API tests."""
    return StepResult(
        step_name=step_name,
        step_number=step_number,
        status=StepStatus.FAILED,
        error="test failure",
    )


class TestPipelineManagerAPI:
    """Tests for PipelineManager class."""

    @pytest.fixture(autouse=True)
    def _storage_roots(self, tmp_path):
        """Use an isolated writable store for PipelineManager construction."""
        self.delta_root = str(tmp_path / "delta")
        self.staging_root = str(tmp_path / "staging")

    def test_create_factory(self):
        """Test PipelineManager.create() factory method."""
        pipeline = PipelineManager.create(
            name="test_pipeline",
            delta_root=self.delta_root,
            staging_root=self.staging_root,
        )
        assert pipeline.config.name == "test_pipeline"
        assert isinstance(pipeline.config, PipelineConfig)
        assert pipeline.config.delta_root == self.delta_root
        assert pipeline.current_step == 0

    def test_create_with_string_paths(self):
        """Test that create() accepts string paths."""
        pipeline = PipelineManager.create(
            name="test",
            delta_root=self.delta_root,
            staging_root=self.staging_root,
        )
        assert isinstance(pipeline.config.delta_root, str)
        assert isinstance(pipeline.config.staging_root, str)

    def test_create_custom_config(self):
        """Test create() with custom configuration and external runner."""
        runner = _ExternalRunner()
        pipeline = PipelineManager.create(
            name="custom",
            delta_root=self.delta_root,
            staging_root=self.staging_root,
            working_root="/tmp/work",
            failure_policy="fail_fast",
            default_step_runner=runner,
        )
        assert pipeline.config.working_root == "/tmp/work"
        assert pipeline.config.failure_policy == FailurePolicy.FAIL_FAST
        assert pipeline.config.default_step_runner == "external_test"
        assert pipeline._default_step_runner is runner

    def test_prefect_server_removed_from_factory_signatures(self):
        """The removed server adapter must not remain as an ignored shim."""
        assert (
            "prefect_server" not in inspect.signature(PipelineManager.create).parameters
        )
        assert (
            "prefect_server" not in inspect.signature(PipelineManager.resume).parameters
        )
        assert all(
            parameter.kind is not inspect.Parameter.VAR_KEYWORD
            for parameter in inspect.signature(
                PipelineManager.resume
            ).parameters.values()
        )
        with pytest.raises(TypeError, match="unexpected keyword argument"):
            PipelineManager.resume(
                delta_root=self.delta_root,
                staging_root=self.staging_root,
                prefect_server=False,  # type: ignore[call-arg]
            )

    def test_default_compute_provider_removed_from_factory_signatures(self):
        """Factories reject the removed compute-provider default keyword."""
        for factory in (PipelineManager.create, PipelineManager.resume):
            parameters = inspect.signature(factory).parameters
            assert "default_compute_provider" not in parameters
            assert "default_step_runner" in parameters

        with pytest.raises(TypeError, match="unexpected keyword argument"):
            PipelineManager.create(
                name="test",
                delta_root=self.delta_root,
                staging_root=self.staging_root,
                default_compute_provider="modal",  # type: ignore[call-arg]
            )
        with pytest.raises(TypeError, match="unexpected keyword argument"):
            PipelineManager.resume(
                delta_root=self.delta_root,
                staging_root=self.staging_root,
                default_compute_provider="modal",  # type: ignore[call-arg]
            )

    def test_external_runner_name_requires_runtime_instance(self):
        """Core cannot reconstruct an external runner from persisted text."""
        config = PipelineConfig(
            name="external",
            delta_root=self.delta_root,
            staging_root=self.staging_root,
            default_step_runner="external_test",
        )

        with pytest.raises(ValueError, match="initialized provider runner"):
            PipelineManager(config)

    def test_local_runner_name_is_reconstructed(self):
        """Core reconstructs its built-in local runner from persisted text."""
        config = PipelineConfig(
            name="local",
            delta_root=self.delta_root,
            staging_root=self.staging_root,
        )

        pipeline = PipelineManager(config, configure_logging=False)

        assert isinstance(pipeline._default_step_runner, LocalRunner)

    def test_finalize_empty(self):
        """Test finalize() with no steps."""
        pipeline = PipelineManager.create(
            name="empty",
            delta_root=self.delta_root,
            staging_root=self.staging_root,
        )
        summary = pipeline.finalize()
        assert summary["pipeline_name"] == "empty"
        assert summary["total_steps"] == 0
        assert summary["steps"] == []
        assert summary["overall_success"] is False

    def test_step_counter_starts_at_zero(self):
        """A new manager starts at step zero."""
        pipeline = PipelineManager.create(
            name="test",
            delta_root=self.delta_root,
            staging_root=self.staging_root,
        )
        assert pipeline.current_step == 0

    # --- Dunder method tests ---

    def test_repr(self):
        """Test __repr__ returns unambiguous representation."""
        pipeline = PipelineManager.create(
            name="test_pipeline",
            delta_root=self.delta_root,
            staging_root=self.staging_root,
        )
        repr_str = repr(pipeline)
        assert "PipelineManager(" in repr_str
        assert "name='test_pipeline'" in repr_str
        assert "steps=0" in repr_str
        assert "delta_root=" in repr_str

    def test_repr_with_steps(self):
        """Test __repr__ shows step count."""
        pipeline = PipelineManager.create(
            name="test_pipeline",
            delta_root=self.delta_root,
            staging_root=self.staging_root,
        )
        pipeline._step_results.append(_succeeded_step("Ingest", 0))
        pipeline._step_results.append(_succeeded_step("Score", 1))
        repr_str = repr(pipeline)
        assert "steps=2" in repr_str

    def test_str_no_steps(self):
        """Test __str__ with no steps executed."""
        pipeline = PipelineManager.create(
            name="my_pipeline",
            delta_root=self.delta_root,
            staging_root=self.staging_root,
        )
        str_output = str(pipeline)
        assert "Pipeline 'my_pipeline'" in str_output
        assert "no steps executed" in str_output

    def test_str_all_succeeded(self):
        """Test __str__ when all steps succeeded."""
        pipeline = PipelineManager.create(
            name="my_pipeline",
            delta_root=self.delta_root,
            staging_root=self.staging_root,
        )
        pipeline._step_results.append(_succeeded_step("Ingest", 0))
        pipeline._step_results.append(_succeeded_step("Score", 1))
        str_output = str(pipeline)
        assert "2 steps" in str_output
        assert "all succeeded" in str_output

    def test_str_partial_success(self):
        """Test __str__ when some steps failed."""
        pipeline = PipelineManager.create(
            name="my_pipeline",
            delta_root=self.delta_root,
            staging_root=self.staging_root,
        )
        pipeline._step_results.append(_succeeded_step("Ingest", 0))
        pipeline._step_results.append(_failed_step("Score", 1))
        str_output = str(pipeline)
        assert "2 steps" in str_output
        assert "1/2 succeeded" in str_output

    def test_len_empty(self):
        """Test __len__ with no steps."""
        pipeline = PipelineManager.create(
            name="test",
            delta_root=self.delta_root,
            staging_root=self.staging_root,
        )
        assert len(pipeline) == 0

    def test_len_with_steps(self):
        """Test __len__ with steps."""
        pipeline = PipelineManager.create(
            name="test",
            delta_root=self.delta_root,
            staging_root=self.staging_root,
        )
        pipeline._step_results.append(_succeeded_step("Ingest", 0))
        pipeline._step_results.append(_succeeded_step("Score", 1))
        pipeline._step_results.append(_succeeded_step("Filter", 2))
        assert len(pipeline) == 3

    def test_iter(self):
        """Test __iter__ iterates over step results."""
        pipeline = PipelineManager.create(
            name="test",
            delta_root=self.delta_root,
            staging_root=self.staging_root,
        )
        step0 = _succeeded_step("Ingest", 0)
        step1 = _succeeded_step("Score", 1)
        pipeline._step_results.append(step0)
        pipeline._step_results.append(step1)

        results = list(pipeline)
        assert results == [step0, step1]
        assert len(results) == 2
        assert results[0].step_name == "Ingest"
        assert results[1].step_name == "Score"

    def test_iter_in_for_loop(self):
        """Test __iter__ works in for loop."""
        pipeline = PipelineManager.create(
            name="test",
            delta_root=self.delta_root,
            staging_root=self.staging_root,
        )
        pipeline._step_results.append(_succeeded_step("Ingest", 0))
        pipeline._step_results.append(_succeeded_step("Score", 1))

        names = []
        for step in pipeline:
            names.append(step.step_name)
        assert names == ["Ingest", "Score"]

    def test_getitem_single_index(self):
        """Test __getitem__ with single index."""
        pipeline = PipelineManager.create(
            name="test",
            delta_root=self.delta_root,
            staging_root=self.staging_root,
        )
        pipeline._step_results.append(_succeeded_step("Ingest", 0))
        pipeline._step_results.append(_succeeded_step("Score", 1))

        first = pipeline[0]
        assert first == pipeline._step_results[0]
        assert isinstance(first, StepResult)
        assert first.step_name == "Ingest"
        second = pipeline[1]
        assert second == pipeline._step_results[1]
        assert isinstance(second, StepResult)
        assert second.step_name == "Score"
        last = pipeline[-1]
        assert last == pipeline._step_results[1]
        assert isinstance(last, StepResult)
        assert last.step_name == "Score"  # Negative index

    def test_getitem_slice(self):
        """Test __getitem__ with slice."""
        pipeline = PipelineManager.create(
            name="test",
            delta_root=self.delta_root,
            staging_root=self.staging_root,
        )
        pipeline._step_results.append(_succeeded_step("Ingest", 0))
        pipeline._step_results.append(_succeeded_step("Score", 1))
        pipeline._step_results.append(_succeeded_step("Filter", 2))

        last_two = pipeline[-2:]
        assert isinstance(last_two, list)
        assert len(last_two) == 2
        assert last_two[0].step_name == "Score"
        assert last_two[1].step_name == "Filter"

    def test_getitem_index_error(self):
        """Test __getitem__ raises IndexError for invalid index."""
        pipeline = PipelineManager.create(
            name="test",
            delta_root=self.delta_root,
            staging_root=self.staging_root,
        )
        pipeline._step_results.append(_succeeded_step("Ingest", 0))

        with pytest.raises(IndexError):
            _ = pipeline[5]

    def test_bool_empty_is_false(self):
        """Test __bool__ returns False for empty pipeline."""
        pipeline = PipelineManager.create(
            name="test",
            delta_root=self.delta_root,
            staging_root=self.staging_root,
        )
        assert not pipeline
        assert bool(pipeline) is False

    def test_bool_all_succeeded_is_true(self):
        """Test __bool__ returns True when all steps succeeded."""
        pipeline = PipelineManager.create(
            name="test",
            delta_root=self.delta_root,
            staging_root=self.staging_root,
        )
        pipeline._step_results.append(_succeeded_step("Ingest", 0))
        pipeline._step_results.append(_succeeded_step("Score", 1))
        assert pipeline
        assert bool(pipeline) is True

    def test_bool_with_failure_is_false(self):
        """Test __bool__ returns False when any step failed."""
        pipeline = PipelineManager.create(
            name="test",
            delta_root=self.delta_root,
            staging_root=self.staging_root,
        )
        pipeline._step_results.append(_succeeded_step("Ingest", 0))
        pipeline._step_results.append(_failed_step("Score", 1))
        assert not pipeline
        assert bool(pipeline) is False

    def test_contains_existing_step(self):
        """Test __contains__ returns True for existing step name."""
        pipeline = PipelineManager.create(
            name="test",
            delta_root=self.delta_root,
            staging_root=self.staging_root,
        )
        pipeline._step_results.append(_succeeded_step("Ingest", 0))
        pipeline._step_results.append(_succeeded_step("Score", 1))

        assert "Ingest" in pipeline
        assert "Score" in pipeline

    def test_contains_missing_step(self):
        """Test __contains__ returns False for missing step name."""
        pipeline = PipelineManager.create(
            name="test",
            delta_root=self.delta_root,
            staging_root=self.staging_root,
        )
        pipeline._step_results.append(_succeeded_step("Ingest", 0))

        assert "NonExistent" not in pipeline
        assert "Filter" not in pipeline


class _RawPathCreatorOp(OperationDefinition):
    """Mock creator operation for testing."""

    class InputRole(StrEnum):
        data = auto()

    class OutputRole(StrEnum):
        output = auto()

    name: ClassVar[str] = "mock_creator"
    inputs: ClassVar[dict[str, InputSpec]] = {
        InputRole.data: InputSpec(artifact_type=ArtifactTypes.FILE_REF, required=True),
    }
    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.output: OutputSpec(
            artifact_type=ArtifactTypes.FILE_REF,
            infer_lineage_from={"inputs": ["data"]},
        ),
    }

    def preprocess(self, inputs: PreprocessInput) -> dict:
        """Extract materialized paths from input artifacts."""
        return {
            role: [a.materialized_path for a in artifacts]
            for role, artifacts in inputs.input_artifacts.items()
        }

    def execute_function(self, inputs, output_dir):
        """Mock creator execution."""
        return ArtifactResult(success=True)


class TestCreatorRejectsFilePaths:
    """Tests for creator operation rejection of raw file paths via pipeline_manager."""

    def test_creator_rejects_raw_file_paths(self, tmp_path):
        """Creator operations should reject raw file paths at the PipelineManager level."""
        from artisan.orchestration.pipeline_manager import (
            _is_file_path_input,
        )

        test_file = tmp_path / "test.csv"
        test_file.write_text("ATOM content")

        inputs = [str(test_file)]
        assert _is_file_path_input(inputs)

        # Creator operations should raise ValueError (not call _promote)
        # The actual raise happens in submit(), so we test the detection
        from artisan.execution.executors.curator import is_curator_operation

        assert not is_curator_operation(_RawPathCreatorOp())


class TestFilePathPromotion:
    """Tests for _promote_file_paths_to_store in pipeline_manager."""

    def test_missing_file_fails_closed(self, tmp_path):
        """A missing raw input aborts before promotion."""
        from artisan.orchestration.pipeline_manager import (
            _promote_file_paths_to_store,
        )
        from artisan.schemas.orchestration.pipeline_config import PipelineConfig

        config = PipelineConfig(
            name="test_pipeline",
            delta_root=str(tmp_path / "delta"),
            staging_root=str(tmp_path / "staging"),
            working_root=str(tmp_path / "working"),
        )
        (tmp_path / "delta").mkdir(parents=True, exist_ok=True)
        (tmp_path / "staging").mkdir(parents=True, exist_ok=True)

        non_existent = str(tmp_path / "does_not_exist.csv")
        with pytest.raises(ArtifactIntegrityError, match="Not found"):
            _promote_file_paths_to_store(
                [non_existent], config, 1, "mock_ingest", "a" * 32
            )

    def test_directory_path_fails_closed(self, tmp_path):
        """A raw directory input is rejected rather than skipped."""
        from artisan.orchestration.pipeline_manager import (
            _promote_file_paths_to_store,
        )
        from artisan.schemas.orchestration.pipeline_config import PipelineConfig

        config = PipelineConfig(
            name="test_pipeline",
            delta_root=str(tmp_path / "delta"),
            staging_root=str(tmp_path / "staging"),
            working_root=str(tmp_path / "working"),
        )
        (tmp_path / "delta").mkdir(parents=True, exist_ok=True)
        (tmp_path / "staging").mkdir(parents=True, exist_ok=True)

        test_dir = tmp_path / "test_directory"
        test_dir.mkdir()

        with pytest.raises(ArtifactIntegrityError, match="Not a file"):
            _promote_file_paths_to_store(
                [str(test_dir)], config, 1, "mock_ingest", "a" * 32
            )

    def test_valid_files_promoted(self, tmp_path):
        """Valid file paths should be promoted to artifact IDs."""
        from artisan.orchestration.pipeline_manager import (
            _promote_file_paths_to_store,
        )
        from artisan.schemas.orchestration.pipeline_config import PipelineConfig

        config = PipelineConfig(
            name="test_pipeline",
            delta_root=str(tmp_path / "delta"),
            staging_root=str(tmp_path / "staging"),
            working_root=str(tmp_path / "working"),
        )
        (tmp_path / "delta").mkdir(parents=True, exist_ok=True)
        (tmp_path / "staging").mkdir(parents=True, exist_ok=True)

        test_file = tmp_path / "test.csv"
        test_file.write_bytes(b"ATOM content")

        result, count, _verified = _promote_file_paths_to_store(
            [str(test_file)], config, 0, "mock_ingest", "a" * 32
        )

        assert result is not None
        assert "file" in result
        assert len(result["file"]) == 1
        assert count == 1

    def test_mixed_valid_invalid_files_fail_without_partial_promotion(self, tmp_path):
        """One invalid raw input rejects the full ordered input occurrence list."""
        from artisan.orchestration.pipeline_manager import (
            _promote_file_paths_to_store,
        )
        from artisan.schemas.orchestration.pipeline_config import PipelineConfig

        config = PipelineConfig(
            name="test",
            delta_root=str(tmp_path / "delta"),
            staging_root=str(tmp_path / "staging"),
            working_root=str(tmp_path / "working"),
        )
        (tmp_path / "delta").mkdir(parents=True, exist_ok=True)
        (tmp_path / "staging").mkdir(parents=True, exist_ok=True)

        # One valid file, one non-existent
        valid_file = tmp_path / "valid.csv"
        valid_file.write_bytes(b"ATOM content")
        non_existent = str(tmp_path / "missing.csv")

        with pytest.raises(ArtifactIntegrityError, match="missing.csv"):
            _promote_file_paths_to_store(
                [str(valid_file), non_existent],
                config,
                1,
                "mock_ingest",
                "a" * 32,
            )

        assert not list((tmp_path / "staging").rglob("*.parquet"))
