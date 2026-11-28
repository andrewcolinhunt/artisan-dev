"""Pipeline coordination over admitted scientific and curator execution."""

from __future__ import annotations

import json
import logging
import tempfile
import threading
import time
import weakref
from collections.abc import Iterator, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from contextlib import AbstractContextManager, nullcontext, suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from types import TracebackType
from typing import Literal, Protocol, TypedDict, cast, overload
from uuid import uuid4

import polars as pl
from fsspec.registry import get_filesystem_class
from fsspec.utils import get_protocol

from artisan.composites.base.called_operations import CalledOperation
from artisan.composites.base.compiled import CompiledPlan
from artisan.composites.base.compiler import Definition, Parameters
from artisan.errors import PersistenceIntegrityError
from artisan.execution.assignment_transport import AssignmentDispatch
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.orchestration.engine.pipeline_inputs import (
    FrozenPipelineInputs,
    PipelineInputs,
    complete_pipeline_inputs,
    normalize_pipeline_inputs,
    resolve_creator_inputs,
    resolve_curator_inputs,
    validate_pipeline_input_contract,
)
from artisan.orchestration.engine.pipeline_recovery import recover_pipeline_results
from artisan.orchestration.engine.pipeline_signals import (
    PipelineSignalRegistration,
    reconcile_pipeline_signals,
    register_pipeline_signals,
)
from artisan.orchestration.engine.pipeline_submission import (
    PipelineSubmission,
    freeze_submission,
)
from artisan.orchestration.engine.retained_curator_executor import (
    RetainedCuratorExecution,
)
from artisan.orchestration.engine.retained_executor import RetainedStepExecution
from artisan.orchestration.engine.retained_inputs import (
    PreparedAssignmentInputs,
    prepare_step_inputs,
)
from artisan.orchestration.engine.retained_step_tracker import RetainedStepTracker
from artisan.orchestration.runners.assignment import AssignmentRunner
from artisan.orchestration.runners.curator import CuratorRunner
from artisan.orchestration.runners.local import LocalRunner
from artisan.orchestration.step_future import StepFuture
from artisan.registry import bootstrap_applications
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.external import validate_persistable_uri
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.schemas.enums import CachePolicy, FailurePolicy, GroupByStrategy
from artisan.schemas.execution.batch_strategy import BatchStrategy
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.schemas.execution.worker_ownership import OwnershipState
from artisan.schemas.operation_config.runner_resources import RunnerResources
from artisan.schemas.operation_config.tool_spec import ToolSpec
from artisan.schemas.orchestration.output_reference import OutputReference, StepSource
from artisan.schemas.orchestration.pipeline_config import PipelineConfig
from artisan.schemas.orchestration.step_lifecycle import (
    TERMINAL_STEP_STATUSES,
    CancellationAcknowledgement,
    CancellationStatus,
    StepStatus,
)
from artisan.schemas.orchestration.step_result import StepResult
from artisan.schemas.orchestration.step_start_record import StepStartRecord
from artisan.schemas.orchestration.step_state import StepState
from artisan.storage.core.curator_result import dynamic_output_roles
from artisan.storage.core.retained_reader import AcceptedArtifact, RetainedReader
from artisan.storage.io.retained_commit import RetainedCommitter
from artisan.storage.io.retained_store import create_retained_store, open_retained_store
from artisan.utils.application_modules import (
    require_application_modules,
    resolve_application_modules,
)
from artisan.utils.command_supervisor import CommandOwnershipError
from artisan.utils.logging import _configure_default_logging, _RunLogSession
from artisan.utils.path import uri_join, uri_parent

type Execution = RetainedStepExecution | RetainedCuratorExecution
logger = logging.getLogger(__name__)


class _StepSummary(TypedDict):
    step_number: int
    name: str
    status: str
    total: int
    succeeded: int
    failed: int
    duration_seconds: float | None


class _PipelineSummary(TypedDict):
    pipeline_name: str
    total_steps: int
    steps: list[_StepSummary]
    overall_success: bool


def _close_run_log(session: _RunLogSession) -> None:
    with suppress(Exception):
        session.close()


class PipelineRunner(AssignmentRunner, CuratorRunner, Protocol):
    """A placement provider serving both supported top-level operation contracts."""


@dataclass(frozen=True)
class _PipelineDefaults:
    name: str
    runner_name: str
    requires_instance: bool

    def options(self) -> dict[str, str | bool]:
        return {
            "pipeline_name": self.name,
            "pipeline_default_step_runner": self.runner_name,
            "pipeline_default_runner_requires_instance": self.requires_instance,
        }


def _bare_local(runner: PipelineRunner) -> bool:
    return type(runner) is LocalRunner and runner == LocalRunner()


def _resolve_runner(runner: PipelineRunner | Literal["local"]) -> PipelineRunner:
    selected: object = runner
    if isinstance(selected, str):
        if selected != "local":
            msg = "External runners require an explicit runtime instance"
            raise ValueError(msg)
        return LocalRunner()
    resolved = cast(PipelineRunner, selected)
    name: object = resolved.name
    if not isinstance(name, str) or not name.strip():
        msg = "Pipeline runners require a stable nonempty name"
        raise ValueError(msg)
    return resolved


def _factory_dispatch(config: PipelineConfig) -> AssignmentDispatch:
    if not config.name.strip():
        msg = "Pipeline name must be nonempty"
        raise ValueError(msg)
    backend_protocol = get_filesystem_class(config.storage.protocol).protocol
    protocols = (
        (backend_protocol,) if isinstance(backend_protocol, str) else backend_protocol
    )
    for root in (config.delta_root, config.staging_root, config.files_root):
        if root is None or not root.strip():
            msg = "Pipeline storage requires nonempty Delta, staging and files roots"
            raise ValueError(msg)
        validate_persistable_uri(root)
        if get_protocol(root) not in protocols:
            msg = "Pipeline root protocol differs from the configured storage backend"
            raise ValueError(msg)
    assert config.files_root is not None
    return AssignmentDispatch(
        config.delta_root,
        config.files_root,
        config.staging_root,
        config.storage,
        Path(config.working_root),
        application_modules=config.application_modules,
        preserve_working=config.preserve_working,
        preserve_staging=config.preserve_staging,
    )


def _recorded_defaults(
    states: Sequence[StepState], authorized_modules: tuple[str, ...]
) -> _PipelineDefaults:
    selected: _PipelineDefaults | None = None
    for state in states:
        options = json.loads(state.compute_options_json)
        if not isinstance(options, dict):
            msg = "Recorded pipeline options must be an object"
            raise ValueError(msg)
        name = options.get("pipeline_name")
        runner = options.get("pipeline_default_step_runner")
        explicit = options.get("pipeline_default_runner_requires_instance")
        if (
            not isinstance(name, str)
            or not name.strip()
            or not isinstance(runner, str)
            or not runner.strip()
            or type(explicit) is not bool
            or (not explicit and runner != "local")
        ):
            msg = "Recorded pipeline defaults are missing or invalid"
            raise ValueError(msg)
        require_application_modules(
            options.get("application_modules"), authorized_modules
        )
        current = _PipelineDefaults(name, runner, explicit)
        if selected is not None and current != selected:
            msg = "Recorded pipeline defaults differ across historical attempts"
            raise ValueError(msg)
        selected = current
    assert selected is not None
    return selected


def _restore_runner(
    defaults: _PipelineDefaults,
    supplied: PipelineRunner | Literal["local"] | None,
) -> PipelineRunner:
    if defaults.requires_instance and (supplied is None or isinstance(supplied, str)):
        msg = "Resume requires an explicit matching configured runner instance"
        raise ValueError(msg)
    runner = _resolve_runner("local" if supplied is None else supplied)
    if runner.name != defaults.runner_name:
        msg = "Runtime runner name differs from the recorded pipeline default"
        raise ValueError(msg)
    if not defaults.requires_instance and not _bare_local(runner):
        msg = "A bare local pipeline default cannot resume with configured software"
        raise ValueError(msg)
    return runner


class PipelineManager:
    """Own finite submissions, explicit store creation and selected-run recovery."""

    def __init__(
        self,
        committer: RetainedCommitter,
        dispatch: AssignmentDispatch,
        *,
        pipeline_run_id: str,
        default_step_runner: PipelineRunner | None = None,
        failure_policy: FailurePolicy = FailurePolicy.CONTINUE,
        skip_cache: bool = False,
        cache_policy: CachePolicy = CachePolicy.ALL_SUCCEEDED,
        name: str | None = None,
        recorded_defaults: _PipelineDefaults | None = None,
        recover_staging: bool = False,
        configure_logging: bool = True,
    ) -> None:
        if not pipeline_run_id.strip():
            msg = "Pipeline coordination requires an explicit run identity"
            raise ValueError(msg)
        if (committer.delta_root, committer.files_root) != (
            dispatch.delta_root,
            dispatch.files_root,
        ):
            msg = "Worker dispatch and controller must use the same store roots"
            raise ValueError(msg)
        self.committer = committer
        self.dispatch = dispatch
        self.tracker = RetainedStepTracker(committer, pipeline_run_id)
        with committer._read_snapshot() as read:
            _, all_attempts = self.tracker._read_lifecycle(read)
        attempts = [
            state for state in all_attempts if state.pipeline_run_id == pipeline_run_id
        ]
        if any(
            state.status in {StepStatus.PENDING, StepStatus.RUNNING}
            or state.cancellation_status == CancellationStatus.UNKNOWN
            for state in attempts
        ):
            msg = "Resolve unfinished pipeline ownership before resuming submissions"
            raise PersistenceIntegrityError(msg)
        states = self.tracker._latest_by_position(attempts)
        self.runner = (
            LocalRunner() if default_step_runner is None else default_step_runner
        )
        self._config = PipelineConfig(
            name=name if name is not None else pipeline_run_id,
            pipeline_run_id=pipeline_run_id,
            delta_root=dispatch.delta_root,
            staging_root=dispatch.staging_root,
            files_root=dispatch.files_root,
            working_root=str(dispatch.working_root),
            storage=dispatch.storage,
            application_modules=dispatch.application_modules,
            default_step_runner=self.runner.name,
            failure_policy=failure_policy,
            cache_policy=cache_policy,
            skip_cache=skip_cache,
            preserve_staging=dispatch.preserve_staging,
            preserve_working=dispatch.preserve_working,
            recover_staging=recover_staging,
        )
        self._defaults = recorded_defaults or _PipelineDefaults(
            self._config.name,
            self.runner.name,
            not _bare_local(self.runner),
        )
        self.failure_policy = failure_policy
        self.skip_cache = skip_cache
        if not isinstance(cast(object, cache_policy), CachePolicy):
            msg = "cache_policy must be a CachePolicy member"
            raise TypeError(msg)
        self.cache_policy = cache_policy
        self._step_run_ids = {state.step_number: state.step_run_id for state in states}
        self._next_step = max(self._step_run_ids, default=-1) + 1
        self._executions: dict[str, Execution] = {}
        self._futures: dict[str, StepFuture] = {}
        self._lock = threading.RLock()
        self._cancelled = threading.Event()
        self._stopped = False
        self._closed = False
        self._summary: _PipelineSummary | None = None
        self._signals: PipelineSignalRegistration | None = None
        self._log_session: _RunLogSession | None = None
        self._log_finalizer: (
            weakref.finalize[[_RunLogSession], PipelineManager] | None
        ) = None
        self._pool = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="artisan-pipeline"
        )
        if configure_logging:
            self._configure_logging()

    def _configure_logging(self) -> None:
        try:
            _configure_default_logging()
            if self._config.storage.is_local:
                session = _RunLogSession(
                    uri_join(uri_parent(self._config.delta_root), "logs"),
                    self._config.pipeline_run_id,
                )
                self._log_session = session
                self._log_finalizer = weakref.finalize(self, _close_run_log, session)
        except Exception:
            with suppress(Exception):
                logger.warning("Could not configure pipeline logging", exc_info=True)

    def _log_context(self) -> AbstractContextManager[None]:
        return self._log_session.bind() if self._log_session else nullcontext()

    def _close_log_session(self) -> None:
        if self._log_finalizer is not None:
            self._log_finalizer()

    @classmethod
    def create(
        cls,
        name: str,
        delta_root: str,
        staging_root: str,
        *,
        working_root: str | None = None,
        files_root: str | None = None,
        default_step_runner: PipelineRunner | Literal["local"] = "local",
        storage: StorageConfig | None = None,
        failure_policy: FailurePolicy = FailurePolicy.CONTINUE,
        cache_policy: CachePolicy = CachePolicy.ALL_SUCCEEDED,
        skip_cache: bool = False,
        preserve_staging: bool = False,
        preserve_working: bool = False,
        recover_staging: bool = True,
        application_modules: Sequence[str] | None = None,
    ) -> PipelineManager:
        """Create a run without preparing software or recovering unrelated work.

        Only the built-in ``local`` name can construct a runner. External
        providers and configured local workers are supplied as live instances.
        ``recover_staging`` is retained as run configuration; selected recovery
        occurs only when resuming an existing run.
        """
        runner = _resolve_runner(default_step_runner)
        run_id = f"{name}_{datetime.now(UTC):%Y%m%d_%H%M%S}_{uuid4().hex[:8]}"
        config = PipelineConfig(
            name=name,
            pipeline_run_id=run_id,
            delta_root=delta_root,
            staging_root=staging_root,
            working_root=working_root
            if working_root is not None
            else tempfile.gettempdir(),
            files_root=files_root,
            default_step_runner=runner.name,
            storage=storage or StorageConfig(),
            failure_policy=failure_policy,
            cache_policy=cache_policy,
            skip_cache=skip_cache,
            preserve_staging=preserve_staging,
            preserve_working=preserve_working,
            recover_staging=recover_staging,
            application_modules=resolve_application_modules(application_modules),
        )
        dispatch = _factory_dispatch(config)
        bootstrap_applications(config.application_modules, purpose="pipeline startup")
        committer = create_retained_store(
            config.delta_root,
            dispatch.files_root,
            fs=config.storage.filesystem(),
            storage_options=config.storage.delta_storage_options(),
        )
        return cls._from_config(committer, dispatch, config, runner)

    @classmethod
    def resume(
        cls,
        delta_root: str,
        staging_root: str,
        *,
        pipeline_run_id: str | None = None,
        name: str | None = None,
        working_root: str | None = None,
        files_root: str | None = None,
        default_step_runner: PipelineRunner | Literal["local"] | None = None,
        storage: StorageConfig | None = None,
        failure_policy: FailurePolicy = FailurePolicy.CONTINUE,
        cache_policy: CachePolicy = CachePolicy.ALL_SUCCEEDED,
        skip_cache: bool = False,
        preserve_staging: bool = False,
        preserve_working: bool = False,
        recover_staging: bool = True,
        application_modules: Sequence[str] | None = None,
    ) -> PipelineManager:
        """Restore exact run defaults and optionally finish already selected results.

        Caller-authorized application modules are registered before schema checks;
        recorded names never grant import authority. Every historical attempt is
        checked before recovery. Pending, running or UNKNOWN ownership prevents
        new submissions, even when hidden by a newer attempt at the same position.
        A display-name override does not change the original durable pipeline name.
        """
        config = PipelineConfig(
            name=name if name is not None else "resume",
            pipeline_run_id=pipeline_run_id or "",
            delta_root=delta_root,
            staging_root=staging_root,
            working_root=working_root
            if working_root is not None
            else tempfile.gettempdir(),
            files_root=files_root,
            storage=storage or StorageConfig(),
            failure_policy=failure_policy,
            cache_policy=cache_policy,
            skip_cache=skip_cache,
            preserve_staging=preserve_staging,
            preserve_working=preserve_working,
            recover_staging=recover_staging,
            application_modules=resolve_application_modules(application_modules),
        )
        dispatch = _factory_dispatch(config)
        bootstrap_applications(config.application_modules, purpose="pipeline resume")
        committer = open_retained_store(
            config.delta_root,
            dispatch.files_root,
            fs=config.storage.filesystem(),
            storage_options=config.storage.delta_storage_options(),
        )
        tracker = RetainedStepTracker(committer, config.pipeline_run_id)
        with committer._read_snapshot() as read:
            _, attempts = tracker._read_lifecycle(read)
        current = tracker._select_current_states(attempts, pipeline_run_id)
        if not current:
            msg = "No step attempts found for the selected pipeline run"
            raise ValueError(msg)
        run_id = current[0].pipeline_run_id
        defaults = _recorded_defaults(
            [state for state in attempts if state.pipeline_run_id == run_id],
            config.application_modules,
        )
        runner = _restore_runner(defaults, default_step_runner)
        config = config.model_copy(
            update={
                "name": name if name is not None else defaults.name,
                "pipeline_run_id": run_id,
                "default_step_runner": runner.name,
            }
        )
        if recover_staging:
            recover_pipeline_results(
                committer,
                pipeline_run_id=run_id,
                staging_root=staging_root,
                preserve_staging=preserve_staging,
            )
        return cls._from_config(committer, dispatch, config, runner, defaults)

    @classmethod
    def _from_config(
        cls,
        committer: RetainedCommitter,
        dispatch: AssignmentDispatch,
        config: PipelineConfig,
        runner: PipelineRunner,
        defaults: _PipelineDefaults | None = None,
    ) -> PipelineManager:
        return cls(
            committer,
            dispatch,
            pipeline_run_id=config.pipeline_run_id,
            default_step_runner=runner,
            failure_policy=config.failure_policy,
            skip_cache=config.skip_cache,
            cache_policy=config.cache_policy,
            name=config.name,
            recorded_defaults=defaults,
            recover_staging=config.recover_staging,
        )

    @property
    def config(self) -> PipelineConfig:
        """Return current display and runtime configuration, separate from evidence."""
        return self._config

    @property
    def current_step(self) -> int:
        """Return the next submitted logical step number."""
        with self._lock:
            return self._next_step

    @property
    def log_path(self) -> str | None:
        """Return this session's local log path, including after the sink closes."""
        return self._log_session.path if self._log_session else None

    def _snapshot(self) -> tuple[pl.DataFrame, list[StepState]]:
        with self.committer._read_snapshot() as read:
            rows, attempts = self.tracker._read_lifecycle(read)
            states = self.tracker._select_current_states(
                attempts, self._config.pipeline_run_id
            )
            return rows, states

    def _results(self) -> list[StepResult]:
        _, states = self._snapshot()
        return [
            self._bound_result(state)
            for state in states
            if state.status in TERMINAL_STEP_STATUSES
        ]

    def _bound_result(self, state: StepState) -> StepResult:
        return state.to_step_result()._bind_source(
            StepSource(
                store=self.committer.store_identity, step_run_id=state.step_run_id
            )
        )

    def get_step(
        self, step_number: int, *, pipeline_run_id: str | None = None
    ) -> StepResult:
        """Read a saved step result from this manager's store.

        The result retains its selected attempt when wired into another step,
        even if the source run later retries that position. Lookup does not
        resume the source run or take ownership of its workers.

        Args:
            step_number: Nonnegative logical step position, not a sequence index.
            pipeline_run_id: Source run ID. None selects this manager's run.

        Returns:
            The latest attempt at that position as a terminal result. Failed
            attempts are returned for inspection, without selecting an older
            success. Its output references already identify their source store.

        Raises:
            ValueError: If the position/run is invalid or missing, or the selected
                attempt is still pending or running.
        """
        if type(step_number) is not int or step_number < 0:
            msg = "step_number must be a nonnegative integer"
            raise ValueError(msg)
        run_id = (
            self.config.pipeline_run_id if pipeline_run_id is None else pipeline_run_id
        )
        if not run_id.strip():
            msg = "pipeline_run_id must be a nonempty string"
            raise ValueError(msg)
        with self.committer._read_snapshot() as read:
            _, attempts = self.tracker._read_lifecycle(read)
            states = self.tracker._select_current_states(attempts, run_id)
        selected = next(
            (state for state in states if state.step_number == step_number), None
        )
        if selected is None:
            msg = f"Run {run_id!r} has no step at position {step_number}"
            raise ValueError(msg)
        if selected.status not in TERMINAL_STEP_STATUSES:
            msg = f"Step {step_number} in run {run_id!r} is not ready; wait for its result"
            raise ValueError(msg)
        return self._bound_result(selected)

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}(name={self._config.name!r}, "
            f"steps={len(self)}, delta_root={self._config.delta_root!r})"
        )

    def __str__(self) -> str:
        results = self._results()
        if not results:
            return f"Pipeline '{self._config.name}': no steps executed"
        succeeded = sum(
            result.status in {StepStatus.SUCCEEDED, StepStatus.SKIPPED}
            for result in results
        )
        status = (
            "all succeeded"
            if succeeded == len(results)
            else f"{succeeded}/{len(results)} succeeded"
        )
        return f"Pipeline '{self._config.name}': {len(results)} steps, {status}"

    def __len__(self) -> int:
        """Return the number of current terminal step results."""
        return len(self._results())

    def __iter__(self) -> Iterator[StepResult]:
        """Iterate over one verified snapshot in logical step order."""
        return iter(self._results())

    @overload
    def __getitem__(self, index: int) -> StepResult: ...

    @overload
    def __getitem__(self, index: slice) -> list[StepResult]: ...

    def __getitem__(self, index: int | slice) -> StepResult | list[StepResult]:
        """Index terminal results by sequence position, not durable step number."""
        return self._results()[index]

    def __bool__(self) -> bool:
        """Return whether the nonempty terminal sequence all succeeded or skipped."""
        results = self._results()
        return bool(results) and all(
            result.status in {StepStatus.SUCCEEDED, StepStatus.SKIPPED}
            for result in results
        )

    def __contains__(self, step_name: str) -> bool:
        """Return whether a current terminal step has this display name."""
        return any(result.step_name == step_name for result in self._results())

    def output(
        self, name: str, role: str, *, step_number: int | None = None
    ) -> OutputReference:
        """Select an output from a named step in this run.

        The reference retains the exact selected attempt and source store. It can
        be used in another run in the same store once the source is ready. Static
        declarations remain usable before completion and after failure; actual
        input resolution still checks availability. Dynamic curator roles require
        completion.

        Args:
            name: Step display name. Repeated names select the latest position.
            role: Declared output role to select.
            step_number: Optional logical position to disambiguate repeated names.

        Returns:
            A source-bound reference suitable for a downstream step's inputs.

        Raises:
            ValueError: If the named step or output role is unavailable.
        """
        rows, states = self._snapshot()
        matching = [
            state
            for state in states
            if state.step_name == name
            and (step_number is None or state.step_number == step_number)
        ]
        if not matching:
            msg = f"No current step named {name!r} with step_number={step_number!r}"
            raise ValueError(msg)
        selected = matching[-1]
        if not dynamic_output_roles(selected.compute_options_json):
            initial = rows.filter(
                (pl.col("step_run_id") == selected.step_run_id)
                & (pl.col("state_sequence") == 0)
            ).row(0, named=True)
            selected = self.tracker._row_to_state(initial)
        if role not in selected.output_roles:
            msg = f"Output role {role!r} not available for step {name!r}"
            raise ValueError(msg)
        return OutputReference(
            source=StepSource(
                store=self.committer.store_identity, step_run_id=selected.step_run_id
            ),
            role=role,
            artifact_type=selected.output_types.get(role) or ArtifactTypes.ANY,
        )

    @property
    def executions(self) -> tuple[Execution, ...]:
        """Keep all controller and provider owners reachable, including unknown ones."""
        with self._lock:
            return tuple(self._executions.values())

    def submit(
        self,
        definition: Definition,
        *,
        inputs: PipelineInputs = None,
        params: Parameters = None,
        called_operations: Mapping[str, CalledOperation] | None = None,
        step_runner: PipelineRunner | None = None,
        runner_resources: RunnerResources | Mapping[str, object] | None = None,
        batch_strategy: BatchStrategy | Mapping[str, object] | None = None,
        group_by: GroupByStrategy | None = None,
        tool: ToolSpec | Mapping[str, object] | None = None,
        skip_cache: bool | None = None,
        cache_policy: CachePolicy | None = None,
        failure_policy: FailurePolicy | None = None,
        name: str | None = None,
    ) -> StepFuture:
        """Submit an operation or composite and return a nonblocking step handle.

        Caller parameters and input references are captured before this method
        returns. References from this manager's pending steps are waited on during
        execution. References from other runs in the same store must already be
        terminal with resolved worker ownership. Submission does not prepare
        software or transfer ownership of another run's workers.

        Args:
            definition: Operation or composite definition class.
            inputs: Named artifact streams or output references. Ingestion also
                accepts raw paths. Omitted roles use declared fixed artifact defaults.
            params: Scientific parameters as a mapping or the definition's Params.
            called_operations: Configuration-selected creator calls and fixed settings.
            step_runner: Placement runner. None uses the pipeline default.
            runner_resources: Worker resource patch over definition defaults.
            batch_strategy: Unit partitioning and worker-packing patch.
            group_by: Scientific grouping override. None keeps the default.
            tool: Command-root tool patch; unavailable for composite roots.
            skip_cache: Force computation when true. None uses the pipeline default.
            cache_policy: Cache selection policy. None uses the pipeline default.
            failure_policy: Unit failure policy. None uses the pipeline default.
            name: Step display name. None uses the definition's name.

        Returns:
            A future with source-bound output references for downstream wiring.
            Its result records ordinary operation failures through StepResult.status.

        Raises:
            TypeError: If the definition, parameter, or option shape is invalid.
            ValueError: If roles, types, source store, or source readiness are invalid.
            RuntimeError: If the pipeline is closed.
            CommandOwnershipError: If worker ownership is unresolved.
        """
        submission = freeze_submission(
            definition,
            params=params,
            called_operations=called_operations,
            runner_resources=runner_resources,
            batch_strategy=batch_strategy,
            group_by=group_by,
            tool=tool,
        )
        effective_cache_policy = (
            self.cache_policy if cache_policy is None else cache_policy
        )
        if not isinstance(cast(object, effective_cache_policy), CachePolicy):
            msg = "cache_policy must be a CachePolicy member or None"
            raise TypeError(msg)
        runner = self.runner if step_runner is None else step_runner
        submission.recorder._learn_json(self.dispatch.storage)
        submission.recorder.add_sensitive_data(dict(self.dispatch.runtime_env))
        with self._log_context(), self._lock:
            if self._closed:
                msg = "Pipeline coordinator is closed"
                raise RuntimeError(msg)
            selected = normalize_pipeline_inputs(
                inputs, store_identity=self.committer.store_identity
            )
            selected = complete_pipeline_inputs(selected, submission.definition.inputs)
            validate_pipeline_input_contract(
                selected,
                submission.definition.inputs,
                runtime_defined_inputs=getattr(
                    submission.definition, "runtime_defined_inputs", False
                ),
            )
            self._validate_external_sources(selected)
            record = _start_record(
                submission,
                selected,
                self._next_step,
                runner.name,
                effective_cache_policy,
                self._defaults,
                self.dispatch.application_modules,
                name,
            )
            execution = self._execution(
                submission,
                selected,
                record,
                runner,
                skip_cache=self.skip_cache if skip_cache is None else skip_cache,
                cache_policy=effective_cache_policy,
                failure_policy=failure_policy or self.failure_policy,
            )
            return self._submit_execution(execution)

    def _validate_external_sources(self, inputs: FrozenPipelineInputs) -> None:
        """Reject unavailable sources that this manager cannot wait on or close."""
        foreign = set(inputs.dependency_step_run_ids) - self._futures.keys()
        if not foreign:
            return
        with self.committer._read_snapshot() as read:
            _, attempts = self.tracker._read_lifecycle(read)
        selected = {
            state.step_run_id: state
            for state in attempts
            if state.step_run_id in foreign
        }
        missing = foreign - selected.keys()
        if missing:
            msg = f"Output references select unknown step attempts: {sorted(missing)}"
            raise ValueError(msg)
        for state in selected.values():
            if state.cancellation_status in {
                CancellationStatus.REQUESTED,
                CancellationStatus.UNKNOWN,
            }:
                msg = f"Source step {state.step_run_id} has unresolved worker ownership"
                raise CommandOwnershipError(msg)
            if state.status not in TERMINAL_STEP_STATUSES:
                msg = (
                    f"Source step {state.step_run_id} belongs to another controller and "
                    "is not ready; wait for its result before submitting this step"
                )
                raise ValueError(msg)

    def _submit_execution(self, execution: Execution) -> StepFuture:
        """Retain one controller under this manager's run and cleanup ownership."""
        with self._log_context(), self._lock:
            if self._closed:
                msg = "Pipeline coordinator is closed"
                raise RuntimeError(msg)
            if (
                execution.tracker.committer is not self.committer
                or execution.tracker._pipeline_run_id != self.config.pipeline_run_id
                or execution.dispatch != self.dispatch
            ):
                msg = "Execution controller belongs to another pipeline owner"
                raise ValueError(msg)
            record = execution.record
            if (
                record.step_run_id in self._executions
                or record.step_number in self._step_run_ids
            ):
                msg = "Pipeline position already has a submitted execution"
                raise ValueError(msg)
            future = execution.submit(self._pool)
            self._executions[record.step_run_id] = execution
            self._futures[record.step_run_id] = future
            self._step_run_ids[record.step_number] = record.step_run_id
            self._next_step = max(self._next_step, record.step_number + 1)
            if self._signals is None:
                self._signals = register_pipeline_signals(self)
            return future

    def run(
        self,
        definition: Definition,
        *,
        inputs: PipelineInputs = None,
        params: Parameters = None,
        called_operations: Mapping[str, CalledOperation] | None = None,
        step_runner: PipelineRunner | None = None,
        runner_resources: RunnerResources | Mapping[str, object] | None = None,
        batch_strategy: BatchStrategy | Mapping[str, object] | None = None,
        group_by: GroupByStrategy | None = None,
        tool: ToolSpec | Mapping[str, object] | None = None,
        skip_cache: bool | None = None,
        cache_policy: CachePolicy | None = None,
        failure_policy: FailurePolicy | None = None,
        name: str | None = None,
    ) -> StepResult:
        """Run an operation or composite step and wait for its result.

        Accepts the same inputs, scientific parameters, and execution overrides as
        :meth:`submit`. Definition defaults apply unless supplied fields override
        them. A composite is one submitted boundary; these options do not create
        separate child steps.

        Returns:
            The terminal result, with source-bound output references usable by
            subsequent steps or another run in the same store. Check its status:
            ordinary operation failure is recorded in the result rather than
            raised solely because the failure policy is FAIL_FAST.

        Raises:
            TypeError: If the definition, parameter, or option shape is invalid.
            ValueError: If input or execution configuration is invalid.
            RuntimeError: If the pipeline is closed.
            CommandOwnershipError: If worker ownership is unresolved.
        """
        return self.submit(
            definition,
            inputs=inputs,
            params=params,
            called_operations=called_operations,
            step_runner=step_runner,
            runner_resources=runner_resources,
            batch_strategy=batch_strategy,
            group_by=group_by,
            tool=tool,
            skip_cache=skip_cache,
            cache_policy=cache_policy,
            failure_policy=failure_policy,
            name=name,
        ).result()

    def cancel(self) -> tuple[CancellationAcknowledgement, ...]:
        """Fence every pending/running controller without cancelling its Python future."""
        with self._lock:
            self._cancelled.set()
            executions = tuple(self._executions.values())
        acknowledgements = []
        errors = []
        with self._log_context():
            with suppress(Exception):
                logger.warning(
                    "Pipeline '%s': cancellation requested", self.config.name
                )
            for execution in executions:
                try:
                    acknowledgements.append(execution.cancel())
                except Exception as error:
                    errors.append(error)
        if errors:
            msg = "Some pipeline cancellation requests failed"
            raise ExceptionGroup(msg, errors)
        return tuple(acknowledgements)

    def ownership_state(self) -> OwnershipState:
        """Include retained workers after their result futures have completed."""
        states = [execution.ownership_state() for execution in self.executions]
        if self._signals is not None and self._signals.unknown:
            return OwnershipState.UNKNOWN
        if OwnershipState.UNKNOWN in states:
            return OwnershipState.UNKNOWN
        return (
            OwnershipState.PENDING
            if OwnershipState.PENDING in states
            else OwnershipState.CLOSED
        )

    def close(self) -> None:
        """Wait for controllers and surface unresolved ownership without discarding it."""
        try:
            with self._log_context():
                self._close_workers()
        finally:
            self._close_log_session()

    def _close_workers(self) -> None:
        with self._lock:
            self._closed = True
            futures = tuple(self._futures.values())
        while not all(future.done for future in futures):
            if self._signals is not None:
                self._signals.check_pending(waiting=True)
            time.sleep(0.02)
        self._pool.shutdown(wait=True, cancel_futures=False)
        if self._signals is not None:
            self._signals.drain_request()
        if self.ownership_state() != OwnershipState.CLOSED:
            msg = "Pipeline worker cleanup is unresolved; ownership remains retained"
            raise CommandOwnershipError(msg)
        if self._signals is not None:
            self._signals.release()
        reconcile_pipeline_signals()
        if self.ownership_state() != OwnershipState.CLOSED:
            msg = "Pipeline worker cleanup changed during signal dispatch"
            raise CommandOwnershipError(msg)

    def finalize(self) -> _PipelineSummary:
        """Close owned work and summarize only a fully terminal current run.

        Successful finalization is idempotent. Unfinished durable attempts and
        unresolved workers remain errors; finalization never recovers a selection
        or synthesizes a result for a failed controller.
        """
        try:
            with self._log_context():
                if self._summary is not None:
                    return self._summary
                self._close_workers()
                _, states = self._snapshot()
                for state in states:
                    if state.status not in TERMINAL_STEP_STATUSES:
                        future = self._futures.get(state.step_run_id)
                        if future is not None:
                            future.result()
                        msg = "Pipeline has unfinished current attempts; recover explicitly"
                        raise PersistenceIntegrityError(msg)
                results = [state.to_step_result() for state in states]
                summary: _PipelineSummary = {
                    "pipeline_name": self.config.name,
                    "total_steps": len(results),
                    "steps": [
                        {
                            "step_number": result.step_number,
                            "name": result.step_name,
                            "status": result.status.value,
                            "total": result.total_count,
                            "succeeded": result.succeeded_count,
                            "failed": result.failed_count,
                            "duration_seconds": result.duration_seconds,
                        }
                        for result in results
                    ],
                    "overall_success": bool(results)
                    and all(
                        result.status in {StepStatus.SUCCEEDED, StepStatus.SKIPPED}
                        for result in results
                    ),
                }
                with suppress(Exception):
                    logger.info(
                        "Pipeline '%s' complete: %d steps",
                        self.config.name,
                        len(results),
                    )
                self._summary = summary
                return summary
        finally:
            self._close_log_session()

    def __enter__(self) -> PipelineManager:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        errors: list[BaseException] = []
        if exc_type is not None:
            try:
                self.cancel()
            except BaseException as error:
                errors.append(error)
        try:
            self.finalize()
        except BaseException as error:
            errors.append(error)
        if errors:
            msg = "Pipeline context cleanup failed"
            raise BaseExceptionGroup(
                msg, errors if exc_value is None else [exc_value, *errors]
            )

    def _execution(
        self,
        submission: PipelineSubmission,
        inputs: FrozenPipelineInputs,
        record: StepStartRecord,
        runner: PipelineRunner,
        *,
        skip_cache: bool,
        cache_policy: CachePolicy,
        failure_policy: FailurePolicy,
    ) -> Execution:
        creator_values: dict[str, tuple[AcceptedArtifact | Artifact, ...]] | None = None
        curator_values: dict[str, tuple[AcceptedArtifact | str, ...]] | None = None

        def check_ready() -> None:
            self._check_ready(record.step_run_id, inputs)

        def before_resolve() -> StepResult | None:
            nonlocal creator_values, curator_values
            check_ready()
            with self._lock:
                if self._stopped:
                    check_ready()
                    return self.tracker.skip_pending(
                        record.step_run_id, reason="pipeline_stopped"
                    )
            values: Mapping[str, Sequence[AcceptedArtifact | Artifact | str]]
            if submission.is_curator:
                curator_values = resolve_curator_inputs(
                    inputs,
                    record=record,
                    committer=self.committer,
                    storage=self.dispatch.storage,
                    check_cancelled=check_ready,
                    input_specs=submission.definition.inputs,
                )
                values = curator_values
            else:
                creator_values = resolve_creator_inputs(
                    inputs,
                    record=record,
                    committer=self.committer,
                    storage=self.dispatch.storage,
                    check_cancelled=check_ready,
                    input_specs=submission.definition.inputs,
                )
                values = creator_values
            if values and not any(values.values()):
                with self._lock:
                    check_ready()
                    result = self.tracker.skip_pending(
                        record.step_run_id, reason="empty_inputs"
                    )
                    self._stopped = True
                    return result
            return None

        def plan() -> CompiledPlan:
            return submission.compile()

        def creator_inputs(
            compiled: CompiledPlan,
        ) -> tuple[PreparedAssignmentInputs, ...]:
            assert creator_values is not None
            return prepare_step_inputs(
                compiled.recipe,
                creator_values,
                step_run_id=record.step_run_id,
                reader=RetainedReader(self.committer),
                constants=compiled.constants,
            )

        def curator_inputs() -> dict[str, tuple[AcceptedArtifact | str, ...]]:
            assert curator_values is not None
            return curator_values

        if submission.is_curator:
            return RetainedCuratorExecution(
                record,
                operation=cast(OperationDefinition, submission.instantiate()),
                resolve_inputs=curator_inputs,
                tracker=self.tracker,
                runner=runner,
                dispatch=self.dispatch,
                runner_resources=submission.resources,
                skip_cache=skip_cache,
                before_resolve=before_resolve,
                recorder=submission.recorder,
            )
        return RetainedStepExecution(
            record,
            resolve_plan=plan,
            resolve_inputs=creator_inputs,
            tracker=self.tracker,
            runner=runner,
            dispatch=self.dispatch,
            runner_resources=submission.resources,
            batch_strategy=submission.batch_strategy,
            skip_cache=skip_cache,
            cache_policy=cache_policy,
            failure_policy=failure_policy,
            recorder=submission.recorder,
            before_resolve=before_resolve,
            capture_plan=inputs.raw_paths != (),
        )

    def _check_ready(self, step_run_id: str, inputs: FrozenPipelineInputs) -> None:
        with self._lock:
            execution = self._executions[step_run_id]
            predecessors = tuple(
                self._futures[reference]
                for reference in inputs.dependency_step_run_ids
                if reference in self._futures
            )
        if self._cancelled.is_set():
            execution.cancel()
        execution._check_stopped()
        for predecessor in predecessors:
            predecessor.result()
            execution._check_stopped()
        if any(
            prior is not execution
            and prior.record.step_number < execution.record.step_number
            and prior.ownership_state() == OwnershipState.UNKNOWN
            for prior in self.executions
        ):
            msg = "An earlier pipeline worker still has unresolved ownership"
            raise CommandOwnershipError(msg)


def _start_record(
    submission: PipelineSubmission,
    inputs: FrozenPipelineInputs,
    step_number: int,
    runner_name: str,
    cache_policy: CachePolicy,
    defaults: _PipelineDefaults,
    application_modules: tuple[str, ...],
    name: str | None,
) -> StepStartRecord:
    definition = submission.definition
    recorder = submission.recorder
    diagnostics = inputs.diagnostic_data()
    recorder._learn_json(diagnostics)
    recorder._learn_json(submission.resources)
    recorder._learn_json(submission.batch_strategy)
    if not submission.is_curator and recorder.sanitize_json(
        json.loads(submission.params_json)
    ) != json.loads(submission.params_json):
        msg = "Scientific parameters overlap private submission configuration"
        raise ValueError(msg)
    return StepStartRecord(
        step_run_id=uuid4().hex,
        step_number=step_number,
        step_name=definition.name if name is None else name,
        operation_class=f"{definition.__module__}.{definition.__qualname__}",
        params_json=(
            json.dumps(recorder.sanitize_json(json.loads(submission.params_json)))
            if submission.is_curator
            else submission.params_json
        ),
        input_refs_json=json.dumps(recorder.sanitize_json(diagnostics)),
        compute_backend=runner_name,
        compute_options_json=json.dumps(
            {
                **defaults.options(),
                "application_modules": application_modules,
                "dynamic_output_roles": submission.is_curator
                and not definition.outputs,
                "cache_policy": cache_policy.value,
                "resources": recorder.sanitize_json(submission.resources),
                "execution": recorder.sanitize_json(submission.batch_strategy),
            }
        ),
        output_roles_json=json.dumps(sorted(definition.outputs)),
        output_types_json=json.dumps(
            {role: spec.artifact_type for role, spec in definition.outputs.items()}
        ),
    )
