"""Optional delivery observations survive cleanup without granting acceptance."""

from __future__ import annotations

import sys
import time
from collections.abc import Callable, Iterator, Mapping
from concurrent.futures import CancelledError
from contextlib import contextmanager, nullcontext
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any, ClassVar, Literal

import pytest
from fixtures.commands import IdentityCommandEnvironment
from fsspec.implementations.local import LocalFileSystem
from pydantic import BaseModel

from artisan.composites import CompositeContext
from artisan.composites.base.compiled import CompiledPlan
from artisan.composites.base.compiler import compile_definition
from artisan.composites.base.composite_definition import CompositeDefinition
from artisan.errors import ArtisanError
from artisan.execution.assignment import Assignment, execute_assignment
from artisan.execution.composition import ExecuteAdapter, PlanExecution
from artisan.execution.compute.prepared import PreparedExecuteRouter
from artisan.execution.exceptions import AssignmentExecutionError
from artisan.execution.invocation import ExecuteFailure
from artisan.execution.lineage.graph import OccurrenceGraph
from artisan.execution.recording import delivery_diagnostics as recording
from artisan.execution.recording.commands import CommandRecorder, current_recorder
from artisan.operations.base import OperationDefinition
from artisan.schemas import ArtifactResult, OutputSpec
from artisan.schemas.artifact.metric import MetricArtifact
from artisan.schemas.execution.assignment import (
    AssignmentAttempt,
    AssignmentIdentity,
    DeliveryIdentity,
    SealedDelivery,
    SuccessfulDelivery,
)
from artisan.schemas.execution.delivery_diagnostics import DeliveryDiagnostics
from artisan.schemas.operation_config.tool_spec import ToolSpec
from artisan.schemas.specs.input_models import PostprocessInput, PreprocessInput
from artisan.storage.io.retained_seal import (
    capture_retained_delivery,
    stage_retained_result,
)
from artisan.utils.command_supervisor import CommandOwnershipError
from artisan.utils.command_supervisor import close_command_scope as close_owned_scope

_SECRET = "synthetic-private-value"


class _CommandMetric(OperationDefinition):
    """Run a real subprocess; the private value belongs only to diagnostics."""

    name = "delivery_command_metric"

    class OutputRole(StrEnum):
        metrics = "metrics"

    inputs: ClassVar = {}
    outputs: ClassVar = {
        "metrics": OutputSpec(artifact_type="metric", derives_from={"inputs": []})
    }
    tool: ToolSpec = ToolSpec(executable=sys.executable)

    class Params(BaseModel):
        """Choose a failure phase.

        Attributes:
            failure: Lifecycle phase that raises, or none for successful work.
        """

        failure: Literal["none", "preprocess", "execute", "postprocess"] = "none"

    params: Params = Params()

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        if self.params.failure == "preprocess":
            raise ValueError(_SECRET)
        return {}

    def execute_command(self, inputs: dict[str, Any]) -> list[str]:
        return [
            sys.executable,
            "-c",
            "import sys; print(sys.argv[1]); sys.exit(int(sys.argv[2]))",
            _SECRET,
            "1" if self.params.failure == "execute" else "0",
        ]

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        if self.params.failure == "postprocess":
            raise ValueError(_SECRET)
        result = ArtifactResult()
        result.add_artifact(
            "metrics", MetricArtifact.draft({"value": 1}, "metric.json"), sources={}
        )
        return result


class _TwoCalls(CompositeDefinition):
    name = "diagnostic_two_calls"
    inputs: ClassVar = {}

    class OutputRole(StrEnum):
        metrics = "metrics"

    outputs: ClassVar = {"metrics": OutputSpec(artifact_type="metric")}

    def compose(self, ctx: CompositeContext) -> None:
        ctx.call(_CommandMetric, name="discarded", inputs={})
        final = ctx.call(_CommandMetric, name="retained", inputs={})
        ctx.output("metrics", final.output("metrics"))


class _StructuredFailure(_CommandMetric):
    name = "structured_diagnostic_failure"

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        try:
            raise ValueError(_SECRET)
        except ValueError as cause:
            code = "synthetic_failure"
            raise ArtisanError(
                code,
                f"authored {_SECRET}",
                error_type="validation",
                field=f"params.{_SECRET}",
                suggestions=[_SECRET],
                recovery_hint="CHECK_INPUT",
            ) from cause


class _NestedStructuredFailure(_TwoCalls):
    name = "nested_structured_diagnostic_failure"

    def compose(self, ctx: CompositeContext) -> None:
        child = ctx.call(_StructuredFailure, name="failure", inputs={})
        ctx.output("metrics", child.output("metrics"))


@dataclass(frozen=True)
class _DirectSoftware:
    """Exercise local lifecycle diagnostics without claiming provider qualification."""

    software_digest: str = "c" * 64
    software_cacheable: bool = True
    application_modules: tuple[str, ...] = ()

    def verify(self, plan: CompiledPlan) -> None:
        plan.instantiate()

    def create_adapter(
        self,
        assignment: Assignment,
        *,
        working_root: Path,
        runtime_env: Mapping[str, str],
    ) -> ExecuteAdapter:
        return lambda _operation, _path: PreparedExecuteRouter(
            None,
            pixi=Path("unused-test-launcher"),
            system_env=Path("/usr/bin/env"),
            runtime_env=runtime_env,
            command_environment=lambda _root, _witness: IdentityCommandEnvironment(),
            sensitive_values=tuple(runtime_env.values()),
        )


def _assignment(
    definition: type[OperationDefinition] | type[CompositeDefinition] = _CommandMetric,
    *,
    params: dict[str, Any] | None = None,
) -> Assignment:
    plan = compile_definition(definition, params=params or {})
    identity = AssignmentIdentity(step_run_id="a" * 32, unit_index=0)
    boundary = OccurrenceGraph(boundary_invocation=identity.assignment_id).project(
        {}, {}
    )
    return Assignment(
        AssignmentAttempt(
            assignment=identity,
            attempt=0,
            generation=0,
            recipe_digest=plan.recipe.digest,
            software_digest="c" * 64,
            input_context_digest=boundary.input_context_digest,
        ),
        plan,
        {},
    )


def _run(
    tmp_path: Path,
    sink: Callable[[DeliveryDiagnostics], None],
    *,
    assignment: Assignment | None = None,
) -> SealedDelivery:
    def seal(success: SuccessfulDelivery, result: PlanExecution) -> SealedDelivery:
        return stage_retained_result(
            str(tmp_path / "staging"),
            success,
            result.projection,
            result.retained_artifacts,
            fs=LocalFileSystem(),
        )

    return execute_assignment(
        assignment or _assignment(),
        _DirectSoftware(),
        seal,
        working_root=tmp_path / "work",
        runtime_env={"DIAGNOSTIC_VALUE": _SECRET},
        diagnostic_sink=sink,
    )


@pytest.mark.parametrize("composed", [False, True])
def test_delivery_captures_commands_before_cleanup_and_keeps_only_declared_outputs(
    tmp_path: Path, composed: bool
) -> None:
    captured: list[DeliveryDiagnostics] = []
    assignment = _assignment(_TwoCalls if composed else _CommandMetric)
    receipt = _run(
        tmp_path,
        captured.append,
        assignment=assignment,
    )
    assert len(captured) == 1
    diagnostic = captured[0]
    assert diagnostic.delivery == receipt.delivery
    assert diagnostic.seal_digest == receipt.seal_digest
    assert diagnostic.outcome == "success"
    assert diagnostic.cleanup == "closed"
    assert diagnostic.omitted_invocations == 0
    assert _SECRET not in diagnostic.model_dump_json()
    assert len(diagnostic.invocations) == (2 if composed else 1)
    assert len({item.invocation_id for item in diagnostic.invocations}) == len(
        diagnostic.invocations
    )
    if composed:
        assert [item.call_path for item in diagnostic.invocations] == [
            ("discarded",),
            ("retained",),
        ]
    for item in diagnostic.invocations:
        assert item.succeeded
        assert set(item.timings) == {"setup", "preprocess", "execute", "postprocess"}
        assert item.duration_seconds >= 0
        assert item.tool_output is not None
        assert "<redacted>" in item.tool_output
        assert item.command_recording.status == "complete"
        assert len(item.command_recording.commands) == 1
        command = item.command_recording.commands[0]
        assert command.outcome == "succeeded"
        assert command.requested_argv[-2] == "<redacted>"
        assert not Path(command.cwd).exists()
    assert not list((tmp_path / "work").rglob("artisan-session-*"))
    with capture_retained_delivery(
        receipt, {}, expected_recipe=assignment.plan.recipe, fs=LocalFileSystem()
    ) as retained:
        assert len(retained.artifacts) == 1
        assert len(retained.projection.occurrences) == 1
        assert retained.projection.exports[0].role == "metrics"


@pytest.mark.parametrize("phase", ["preprocess", "postprocess"])
def test_delivery_failure_preserves_phase_redacts_secrets_and_never_seals(
    tmp_path: Path, phase: str
) -> None:
    captured: list[DeliveryDiagnostics] = []
    with pytest.raises(AssignmentExecutionError) as error:
        _run(
            tmp_path,
            captured.append,
            assignment=_assignment(params={"failure": phase}),
        )
    assert error.value.phase == phase
    diagnostic = captured[0]
    assert diagnostic.outcome == "science"
    assert error.value.delivery == diagnostic.delivery
    assert diagnostic.phase == phase
    assert diagnostic.seal_digest is None
    assert diagnostic.cleanup == "closed"
    assert len(diagnostic.invocations) == 1
    assert not diagnostic.invocations[0].succeeded
    assert diagnostic.invocations[0].phase == phase
    assert _SECRET not in diagnostic.model_dump_json()
    assert _SECRET not in str(error.value)
    assert diagnostic.timestamp_start.utcoffset() is not None
    text = recording._failure_log(diagnostic).decode("utf-8")
    assert _SECRET not in text
    assert "<redacted>" in text
    assert diagnostic.delivery.delivery_id in text
    assert f"Phase: {phase}" in text
    assert not (tmp_path / "staging").exists()


@pytest.mark.parametrize("nested", [False, True])
def test_closed_structured_failure_preserves_cause_and_redacts_fields(
    tmp_path: Path, nested: bool
) -> None:
    captured: list[DeliveryDiagnostics] = []
    definition = _NestedStructuredFailure if nested else _StructuredFailure
    with pytest.raises(AssignmentExecutionError) as raised:
        _run(tmp_path, captured.append, assignment=_assignment(definition))
    diagnostic = captured[0]
    assert raised.value.delivery == diagnostic.delivery
    assert diagnostic.outcome == "science"
    assert diagnostic.cleanup == "closed"
    assert diagnostic.seal_digest is None
    assert diagnostic.call_path == (("failure",) if nested else ())
    assert diagnostic.error_envelope is not None
    assert diagnostic.error_envelope["code"] == "synthetic_failure"
    assert diagnostic.error_envelope["recovery_hint"] == "CHECK_INPUT"
    assert diagnostic.error_envelope["cause"]["type"] == "ValueError"
    assert diagnostic.error_envelope["suggestions"] == ["<redacted>"]
    assert diagnostic.invocations[0].error_envelope == diagnostic.error_envelope
    assert _SECRET not in diagnostic.model_dump_json()
    assert _SECRET not in recording._failure_log(diagnostic).decode()


def test_later_sensitive_values_redact_structured_cause_and_suggestions() -> None:
    secret = "later-structured-private-value"
    envelope = ArtisanError(
        "synthetic_failure", secret, error_type="validation", suggestions=[secret]
    ).to_dict()
    envelope["cause"] = {"type": "ValueError", "message": secret, "traceback": secret}
    failure = AssignmentExecutionError("authored failure", error_envelope=envelope)
    recorder = recording.DeliveryRecorder(sensitive_values=())
    with (
        pytest.raises(AssignmentExecutionError),
        recorder.invocation(_CommandMetric(), "first", ("first",)) as observed,
    ):
        observed.cleanup = "closed"
        raise failure
    assert secret in recorder.invocations[0].model_dump_json()
    with recorder.invocation(_CommandMetric(), "second", ("second",)) as observed:
        observed.cleanup = "closed"
        commands = current_recorder()
        assert commands is not None
        commands.add_sensitive_data(secret)
    recorder.cleanup = "closed"
    diagnostic = recorder.finish(
        DeliveryIdentity.new(_assignment().attempt),
        started=time.perf_counter(),
        failure=failure,
    )
    assert secret not in diagnostic.model_dump_json()
    assert diagnostic.error_envelope["cause"]["message"] == "<redacted>"
    assert diagnostic.invocations[0].error_envelope["suggestions"] == ["<redacted>"]


@pytest.mark.parametrize("payload", ["ü" * 20_000, '\\"' * 20_000])
def test_oversized_structured_error_omits_only_envelope(payload: str) -> None:
    envelope = ArtisanError(
        "synthetic_failure", payload, error_type="validation"
    ).to_dict()
    failure = AssignmentExecutionError("ordinary failure", error_envelope=envelope)
    recorder = recording.DeliveryRecorder(sensitive_values=())
    with (
        pytest.raises(AssignmentExecutionError),
        recorder.invocation(_CommandMetric(), "child", ()) as observed,
    ):
        observed.cleanup = "closed"
        raise failure
    recorder.cleanup = "closed"
    diagnostic = recorder.finish(
        DeliveryIdentity.new(_assignment().attempt),
        started=time.perf_counter(),
        failure=failure,
    )
    assert diagnostic.error_envelope is None
    assert diagnostic.invocations[0].error_envelope is None
    assert "ordinary failure" in diagnostic.error
    assert b"ordinary failure" in recording._failure_log(diagnostic)
    assert diagnostic.omitted_invocations == 0
    encoded = diagnostic.model_dump()
    for invalid in ({"code": "missing-required-fields"}, envelope):
        with pytest.raises(ValueError):
            DeliveryDiagnostics.model_validate({**encoded, "error_envelope": invalid})


def test_unqualified_command_failure_does_not_claim_a_scientific_exit(
    tmp_path: Path,
) -> None:
    captured: list[DeliveryDiagnostics] = []
    with pytest.raises(ExecuteFailure):
        _run(
            tmp_path,
            captured.append,
            assignment=_assignment(params={"failure": "execute"}),
        )
    diagnostic = captured[0]
    assert diagnostic.outcome == "infrastructure"
    assert diagnostic.phase is None
    assert diagnostic.seal_digest is None
    child = diagnostic.invocations[0]
    assert not child.succeeded
    assert child.command_recording.commands[0].outcome == "failed"
    assert child.command_recording.commands[0].returncode == 1
    assert _SECRET not in diagnostic.model_dump_json()


@pytest.mark.parametrize("science_failure", [False, True])
@pytest.mark.parametrize("logger_failure", [False, True])
def test_diagnostic_publication_failure_never_replaces_actual_result(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    science_failure: bool,
    logger_failure: bool,
) -> None:
    def fail(*args: object, **kwargs: object) -> None:
        raise OSError(_SECRET)

    if logger_failure:
        monkeypatch.setattr(recording._LOGGER, "warning", fail)
    assignment = _assignment(
        params={"failure": "preprocess" if science_failure else "none"}
    )
    if science_failure:
        with pytest.raises(AssignmentExecutionError) as error:
            _run(tmp_path, fail, assignment=assignment)
        assert error.value.phase == "preprocess"
        assert error.value.delivery is not None
        assert error.value.delivery.attempt == assignment.attempt
    else:
        assert (
            _run(tmp_path, fail, assignment=assignment).delivery.attempt
            == assignment.attempt
        )
    assert _SECRET not in caplog.text


@pytest.mark.parametrize("unknown", [False, True])
def test_unresolved_invocation_never_reads_mutable_tool_output(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, unknown: bool
) -> None:
    recorder = recording.DeliveryRecorder(sensitive_values=())
    log = tmp_path / "command.log"
    log.write_text("observed output")
    error = CommandOwnershipError() if unknown else CancelledError("cancelled")
    if unknown:

        def forbidden(*args: object, **kwargs: object) -> None:
            pytest.fail("Unresolved command ownership read mutable tool output")

        monkeypatch.setattr(recording, "_read_tool_output", forbidden)
    with (
        pytest.raises(type(error)),
        recorder.invocation(_CommandMetric(), "child", ("child",)) as observed,
    ):
        observed.log_path = str(log)
        observed.cleanup = "unknown" if unknown else "closed"
        raise error
    diagnostic = recorder.finish(
        DeliveryIdentity.new(_assignment().attempt),
        started=time.perf_counter(),
        failure=error,
    )
    assert diagnostic.outcome == ("ownership" if unknown else "cancelled")
    child = diagnostic.invocations[0]
    assert child.cleanup == ("unknown" if unknown else "closed")
    assert child.command_recording.status == ("unavailable" if unknown else "complete")
    assert child.tool_output == (None if unknown else "observed output")


def test_later_sensitive_values_redact_previous_child_output(tmp_path: Path) -> None:
    secret = "later-discovered-private-value"
    log = tmp_path / "command.log"
    log.write_text(secret)
    recorder = recording.DeliveryRecorder(sensitive_values=())
    with recorder.invocation(_CommandMetric(), "first", ("first",)) as observed:
        observed.log_path = str(log)
        observed.cleanup = "closed"
    assert recorder.invocations[0].tool_output == secret
    with recorder.invocation(_CommandMetric(), "second", ("second",)) as observed:
        observed.cleanup = "closed"
        commands = current_recorder()
        assert commands is not None
        commands.add_sensitive_data(secret)
    diagnostic = recorder.finish(
        DeliveryIdentity.new(_assignment().attempt),
        started=time.perf_counter(),
        failure=CancelledError("cancelled"),
    )
    assert secret not in diagnostic.model_dump_json()
    assert diagnostic.invocations[0].tool_output == "<redacted>"


def test_diagnostic_budget_reports_omitted_invocations(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(recording, "MAX_DIAGNOSTIC_BYTES", 0)
    recorder = recording.DeliveryRecorder(sensitive_values=())
    with recorder.invocation(_CommandMetric(), "child", ()) as observed:
        observed.cleanup = "closed"
    assert recorder.invocations == []
    assert recorder.omitted == 1


def test_optional_collector_setup_failure_preserves_authored_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail(*args: object, **kwargs: object) -> None:
        msg = "collector setup"
        raise ValueError(msg)

    monkeypatch.setattr(recording, "configuration_recorder", fail)
    recorder = recording.DeliveryRecorder(sensitive_values=())
    with (
        pytest.raises(RuntimeError, match="original"),
        recorder.invocation(_CommandMetric(), "child", ()),
    ):
        msg = "original"
        raise RuntimeError(msg)
    assert recorder.omitted == 1
    assert current_recorder() is None


def test_authored_failure_with_unresolved_child_never_reads_logs_or_commands(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def unknown(error: BaseException | None = None) -> bool:
        closed = close_owned_scope(error)
        return closed if error is None else False

    def forbidden(*args: object, **kwargs: object) -> None:
        pytest.fail("Unresolved child captured final mutable command evidence")

    monkeypatch.setattr("artisan.execution.session.close_command_scope", unknown)
    monkeypatch.setattr("artisan.execution.assignment.close_command_scope", unknown)
    monkeypatch.setattr(recording, "_read_tool_output", forbidden)
    monkeypatch.setattr(CommandRecorder, "snapshot", forbidden)
    captured: list[DeliveryDiagnostics] = []
    with pytest.raises(CommandOwnershipError):
        _run(
            tmp_path,
            captured.append,
            assignment=_assignment(params={"failure": "preprocess"}),
        )
    diagnostic = captured[0]
    assert diagnostic.outcome == "ownership"
    assert diagnostic.phase == "preprocess"
    assert diagnostic.cleanup == "unknown"
    assert diagnostic.invocations[0].cleanup == "unknown"
    assert diagnostic.invocations[0].tool_output is None
    assert diagnostic.invocations[0].command_recording.status == "unavailable"


@pytest.mark.parametrize("cancelled", [False, True])
def test_unresolved_session_keeps_original_infrastructure_or_cancellation_outcome(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, cancelled: bool
) -> None:
    error = CancelledError("cancelled") if cancelled else ValueError("seal failure")

    def fail(*args: object, **kwargs: object) -> None:
        raise error

    def unknown(failure: BaseException | None = None) -> bool:
        closed = close_owned_scope(failure)
        return closed if failure is None else False

    monkeypatch.setattr(sys.modules[__name__], "stage_retained_result", fail)
    monkeypatch.setattr("artisan.execution.session.close_command_scope", unknown)
    captured: list[DeliveryDiagnostics] = []
    with pytest.raises(type(error)) as caught:
        _run(tmp_path, captured.append)
    assert caught.value is error
    diagnostic = captured[0]
    assert diagnostic.outcome == ("cancelled" if cancelled else "infrastructure")
    assert diagnostic.cleanup == "unknown"
    assert diagnostic.invocations[0].cleanup == "closed"
    assert diagnostic.seal_digest is None


@pytest.mark.parametrize("authored_failure", [False, True])
def test_collector_teardown_cannot_replace_authored_outcome(
    monkeypatch: pytest.MonkeyPatch, authored_failure: bool
) -> None:
    capture = recording.capture_commands

    @contextmanager
    def failing_capture(operation: OperationDefinition) -> Iterator[CommandRecorder]:
        with capture(operation) as commands:
            yield commands
        msg = "optional collector teardown"
        raise OSError(msg)

    monkeypatch.setattr(recording, "capture_commands", failing_capture)
    recorder = recording.DeliveryRecorder(sensitive_values=())
    expected = (
        pytest.raises(RuntimeError, match="authored")
        if authored_failure
        else nullcontext()
    )
    with expected, recorder.invocation(_CommandMetric(), "child", ()) as observed:
        observed.cleanup = "closed"
        if authored_failure:
            msg = "authored failure"
            raise RuntimeError(msg)
    assert recorder.omitted == 1
    assert recorder.invocations == []
    assert current_recorder() is None


def test_late_redaction_covers_secrets_across_log_and_error_cuts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    secret = "private-prefix\nprivate-middle\nprivate-suffix"
    log = tmp_path / "command.log"
    # The first incomplete line is discarded; a multiline secret still crosses
    # the remaining left edge and becomes known only in the following child.
    monkeypatch.setattr(
        recording,
        "_read_tool_output",
        lambda _: "[truncated]\npartial-line\nprivate-middle\nprivate-suffix\nend",
    )
    monkeypatch.setattr(
        recording, "format_error", lambda _: "message " + secret + " tail"
    )
    monkeypatch.setattr(
        recording,
        "MAX_DIAGNOSTIC_MESSAGE",
        len("message private-prefix") + len("\n[truncated]"),
    )
    recorder = recording.DeliveryRecorder(sensitive_values=())
    with (
        pytest.raises(RuntimeError),
        recorder.invocation(_CommandMetric(), "first", ()) as observed,
    ):
        observed.cleanup = "closed"
        observed.log_path = str(log)
        msg = "authored error"
        raise RuntimeError(msg)
    assert "private-middle" in recorder.invocations[0].tool_output
    assert "private-prefix" in recorder.invocations[0].error
    with recorder.invocation(_CommandMetric(), "second", ()) as observed:
        observed.cleanup = "closed"
        commands = current_recorder()
        assert commands is not None
        commands.add_sensitive_data((secret, "private-suffix", "private-prefix"))
    diagnostic = recorder.finish(
        DeliveryIdentity.new(_assignment().attempt),
        started=time.perf_counter(),
        failure=CancelledError("cancelled"),
    )
    encoded = diagnostic.model_dump_json()
    assert "private-prefix" not in encoded
    assert "private-middle" not in encoded
    assert "private-suffix" not in encoded
    assert "[truncated]" in encoded
