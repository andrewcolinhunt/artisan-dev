"""Acceptance tests for actual, bounded, credential-safe command evidence."""

from __future__ import annotations

import sys
from concurrent.futures import ThreadPoolExecutor
from contextvars import copy_context
from unittest.mock import Mock, patch

import pytest
from pydantic import ValidationError

from artisan.execution.recording.commands import (
    CommandRecorder,
    capture_commands,
    invocation_scope,
)
from artisan.schemas.execution.command_record import (
    MAX_COMMAND_RECORDING_BYTES,
    CommandRecord,
    CommandRecording,
)
from artisan.schemas.operation_config.environment_spec import (
    ApptainerEnvironmentSpec,
    DockerEnvironmentSpec,
    LocalEnvironmentSpec,
    PixiEnvironmentSpec,
)
from artisan.utils.external_tools import ExternalToolError, run_command

pytestmark = [
    pytest.mark.filterwarnings("ignore::ResourceWarning"),
    pytest.mark.filterwarnings("ignore::pytest.PytestUnraisableExceptionWarning"),
]


def _record(invocation: int = 0, sequence: int = 0, arg: str = "tool") -> CommandRecord:
    return CommandRecord(
        invocation=invocation,
        sequence=sequence,
        location="endpoint",
        requested_argv=[arg],
        argv=[arg],
        cwd="/worker",
        tool=None,
        environment={
            "type": "LocalEnvironmentSpec",
            "identity": {},
            "variable_names": [],
        },
        outcome="succeeded",
        returncode=0,
        redacted_fields=[],
        required_environment=[],
        launch_seconds=None,
    )


def _recording(*commands: CommandRecord, omitted: int = 0) -> CommandRecording:
    return CommandRecording(
        status="partial" if omitted else "complete",
        commands=list(commands),
        missing_invocations=[],
        omitted_commands=omitted,
        omitted_missing_invocations=0,
        unavailable_reason=None,
    )


@pytest.mark.parametrize("stream", [False, True])
def test_actual_boundary_and_failure_outcomes(tmp_path, stream):
    environment = LocalEnvironmentSpec()
    with capture_commands() as recorder, invocation_scope():
        run_command(
            environment,
            [sys.executable, "-c", "print('ok')"],
            cwd=str(tmp_path),
            stream_output=stream,
        )
        with pytest.raises(ExternalToolError):
            run_command(
                environment,
                [sys.executable, "-c", "raise SystemExit(4)"],
                stream_output=stream,
            )
        with pytest.raises(FileNotFoundError):
            run_command(environment, ["/does-not-exist-command"], stream_output=stream)
    commands = recorder.snapshot().commands
    assert [(c.invocation, c.sequence, c.outcome, c.returncode) for c in commands] == [
        (0, 0, "succeeded", 0),
        (0, 1, "failed", 4),
        (0, 2, "launch_failed", None),
    ]
    assert commands[0].cwd == str(tmp_path)
    assert commands[0].argv == commands[0].requested_argv


def test_log_open_and_post_exit_write_boundaries(tmp_path):
    with capture_commands() as recorder:
        with pytest.raises(FileNotFoundError):
            run_command(
                LocalEnvironmentSpec(),
                [sys.executable, "-c", "pass"],
                stream_output=True,
                log_path=str(tmp_path / "missing" / "log"),
            )
        assert recorder.snapshot() == CommandRecording.empty()
        with pytest.raises(IsADirectoryError):
            run_command(
                LocalEnvironmentSpec(),
                [sys.executable, "-c", "pass"],
                log_path=str(tmp_path),
            )
    assert recorder.snapshot().commands[0].outcome == "succeeded"


@pytest.mark.parametrize("stream", [False, True])
def test_interruption_finalizes_and_preserves_cleanup(stream):
    process = Mock(returncode=-15)
    process.communicate.side_effect = KeyboardInterrupt
    process.stdout = iter(["line\n"])
    process.wait.side_effect = KeyboardInterrupt
    with (
        capture_commands() as recorder,
        patch("artisan.utils.external_tools.subprocess.Popen", return_value=process),
        patch("artisan.utils.external_tools._kill_process_group") as kill,
        pytest.raises(KeyboardInterrupt),
    ):
        run_command(LocalEnvironmentSpec(), ["tool"], stream_output=stream)
    kill.assert_called_once_with(process)
    assert recorder.snapshot().commands[0].outcome == "interrupted"


def test_redaction_preserves_successful_live_result_and_failure_diagnostics(
    monkeypatch,
):
    monkeypatch.setenv("CUSTOM_AUTH_TOKEN", "inherited-secret")
    secret = "opaque-secret"
    environment = LocalEnvironmentSpec(env={"INNOCENT": "custom-secret"})
    argv = [
        sys.executable,
        "-c",
        "import sys; print(sys.argv[1:])",
        secret,
        "custom-secret",
    ]
    with capture_commands() as recorder:
        result = run_command(environment, argv, sensitive_values=(secret,))
        with pytest.raises(ExternalToolError) as failed:
            run_command(
                environment,
                [
                    sys.executable,
                    "-c",
                    "import sys; print('opaque-secret inherited-secret'); sys.exit(2)",
                ],
                sensitive_values=(secret,),
            )
    assert result.args == argv
    assert secret in result.stdout
    assert secret not in str(failed.value)
    assert "inherited-secret" not in str(failed.value)
    payload = recorder.snapshot().model_dump_json()
    assert secret not in payload
    assert "custom-secret" not in payload
    assert "INNOCENT" in recorder.snapshot().commands[0].required_environment


def test_standalone_sensitive_values_redact_only_diagnostics():
    secret = "standalone-opaque"
    with pytest.raises(ExternalToolError) as failed:
        run_command(
            LocalEnvironmentSpec(),
            [
                sys.executable,
                "-c",
                "import sys; print(sys.argv[1]); sys.exit(2)",
                secret,
            ],
            sensitive_values=(secret,),
        )
    assert secret not in str(failed.value)
    assert secret not in " ".join(failed.value.command)


@pytest.mark.parametrize(
    ("environment", "identity"),
    [
        (LocalEnvironmentSpec(venv_path="/venv"), {"venv_path": "/venv"}),
        (
            DockerEnvironmentSpec(
                image="example:1", env={"CONFIG": "container-secret"}
            ),
            {"image": "example:1"},
        ),
        (ApptainerEnvironmentSpec(image="image.sif"), {"image": "image.sif"}),
        (
            PixiEnvironmentSpec(
                pixi_environment="tools", manifest_path="/project/pixi.toml"
            ),
            {"pixi_environment": "tools", "manifest_path": "/project/pixi.toml"},
        ),
    ],
)
def test_environment_wrapping_and_identity(environment, identity):
    process = Mock(returncode=0)
    process.communicate.return_value = ("", "")
    with (
        capture_commands() as recorder,
        patch(
            "artisan.utils.external_tools.subprocess.Popen", return_value=process
        ) as launch,
    ):
        run_command(environment, ["tool", "input"], cwd="/work")
    command = recorder.snapshot().commands[0]
    assert command.environment.identity == identity
    assert command.environment.type == type(environment).__name__
    assert command.requested_argv == ["tool", "input"]
    assert "container-secret" not in recorder.snapshot().model_dump_json()
    if isinstance(environment, DockerEnvironmentSpec):
        assert "CONFIG=container-secret" in launch.call_args.args[0]
        assert "CONFIG=<redacted>" in command.argv


def test_structured_arguments_uri_capabilities_and_short_secrets():
    argv = [
        "tool",
        "--TOKEN",
        "flag-secret",
        "--credential=equals-secret",
        '{"api-key":"json-secret","nested":["https://user:pass@host/p?q=cap#frag"]}',
        "KEY=https://user:pass@host/p?q=cap#frag",
        "text https://host/p?q=cap trailing",
    ]
    recorder = CommandRecorder()
    attempt = recorder.begin(LocalEnvironmentSpec(), argv, argv, "/work", ())
    attempt.finish("succeeded", 0)
    encoded = recorder.snapshot().model_dump_json()
    for value in (
        "flag-secret",
        "equals-secret",
        "json-secret",
        "user:pass",
        "q=cap",
        "#frag",
    ):
        assert value not in encoded
    assert "KEY=https://host/p" in recorder.snapshot().commands[0].argv
    recorder = CommandRecorder()
    attempt = recorder.begin(
        LocalEnvironmentSpec(env={"ODD": "x"}),
        ["tool", "x"],
        ["tool", "x"],
        "/work",
        (),
    )
    attempt.finish("succeeded", 0)
    assert recorder.snapshot().commands[0].argv[-1] == "<redacted>"


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("invocation", True),
        ("sequence", -1),
        ("argv", [3]),
        ("launch_seconds", float("nan")),
        ("returncode", 1),
    ],
)
def test_strict_command_validation(field, value):
    payload = _record().model_dump()
    payload[field] = value
    with pytest.raises(ValidationError):
        CommandRecord.model_validate(payload)


def test_recording_requires_all_fields_order_and_truthful_status():
    with pytest.raises(ValidationError):
        CommandRecording(status="complete", commands=[])
    for payload in [
        {
            **CommandRecording.empty().model_dump(),
            "commands": [_record(1).model_dump(), _record(0).model_dump()],
        },
        {**CommandRecording.empty().model_dump(), "omitted_commands": 1},
        {**CommandRecording.empty().model_dump(), "omitted_commands": True},
    ]:
        with pytest.raises(ValidationError):
            CommandRecording.model_validate(payload)
    assert CommandRecording.unavailable().status == "unavailable"


def test_whole_multibyte_prefix_is_identical_for_shuffled_completions():
    evidence = {
        0: _recording(_record(arg="é" * 210_000)),
        1: _recording(_record(arg="b" * 210_000)),
        2: _recording(_record(arg="later-short")),
    }
    recordings = []
    for order in [(0, 1, 2), (2, 1, 0), (1, 2, 0)]:
        recorder = CommandRecorder()
        for invocation in order:
            recorder.merge(evidence[invocation], invocation)
        recording = recorder.snapshot()
        assert len(recording.model_dump_json().encode()) <= MAX_COMMAND_RECORDING_BYTES
        recordings.append(recording)
    assert recordings[0] == recordings[1] == recordings[2]
    assert [c.invocation for c in recordings[0].commands] == [0]
    assert recordings[0].omitted_commands == 2


def test_remote_truncation_closes_prefix_even_when_later_records_fit():
    first = _recording(_record(arg="first"), omitted=4)
    later = _recording(_record(arg="later"))
    results = []
    for order in [(first, later), (later, first)]:
        recorder = CommandRecorder()
        for remote in order:
            recorder.merge(remote, 0 if remote is first else 1)
        results.append(recorder.snapshot())
    assert results[0] == results[1]
    assert results[0].omitted_commands == 5
    assert [c.argv for c in results[0].commands] == [["first"]]


def test_missing_markers_and_counter_growth_respect_entire_payload(monkeypatch):
    # A smaller byte budget exercises delimiter/status/counter growth cheaply.
    monkeypatch.setattr(
        "artisan.execution.recording.commands.MAX_COMMAND_RECORDING_BYTES", 350
    )
    recorder = CommandRecorder()
    for invocation in range(125):
        recorder.missing(invocation, "transport_failure")
    recording = recorder.snapshot()
    assert len(recording.model_dump_json().encode()) <= 350
    assert recording.status == "partial"
    assert (
        len(recording.missing_invocations) + recording.omitted_missing_invocations
        == 125
    )
    assert [m.invocation for m in recording.missing_invocations] == list(
        range(len(recording.missing_invocations))
    )


def test_scopes_and_dispatch_contexts_are_isolated():
    def work(slot):
        with invocation_scope(invocation=slot):
            run_command(LocalEnvironmentSpec(), [sys.executable, "-c", "pass"])
            run_command(LocalEnvironmentSpec(), [sys.executable, "-c", "pass"])

    with capture_commands() as recorder:
        slots = recorder.reserve(3)
        with ThreadPoolExecutor(max_workers=3) as pool:
            futures = [
                pool.submit(copy_context().run, work, slot) for slot in reversed(slots)
            ]
            for future in futures:
                future.result()
    assert [(c.invocation, c.sequence) for c in recorder.snapshot().commands] == [
        (i, j) for i in range(3) for j in range(2)
    ]
    with capture_commands() as fresh:
        assert fresh.snapshot() == CommandRecording.empty()


def test_missing_markers_reach_the_real_one_mib_limit():
    recorder = CommandRecorder()
    for invocation in range(25_000):
        recorder.missing(invocation, "transport_failure")
    recording = recorder.snapshot()
    assert len(recording.model_dump_json().encode()) <= MAX_COMMAND_RECORDING_BYTES
    assert recording.omitted_missing_invocations > 0
    assert (
        len(recording.missing_invocations) + recording.omitted_missing_invocations
        == 25_000
    )


def test_custom_auth_prefix_and_parent_remote_redaction(monkeypatch):
    from artisan.operations.examples import WaitTool
    from artisan.schemas.operation_config.compute import (
        ComputeProvider,
        ModalComputeConfig,
    )

    monkeypatch.setenv("CUSTOM_TOKEN_ID", "custom-id")
    monkeypatch.setenv("CUSTOM_TOKEN_SECRET", "custom-secret")
    operation = WaitTool(
        compute_provider=ComputeProvider(
            active="modal", modal=ModalComputeConfig(auth_secret="CUSTOM")
        )
    )
    recorder = CommandRecorder(operation)
    recorder.merge(_recording(_record(arg="custom-id/custom-secret")), 3)
    command = recorder.snapshot().commands[0]
    assert command.argv == ["<redacted>/<redacted>"]
    assert {"CUSTOM_TOKEN_ID", "CUSTOM_TOKEN_SECRET"} <= set(
        command.required_environment
    )
    assert "$.argv[0]" in command.redacted_fields


def test_wrapper_failure_does_not_claim_an_attempt_and_scrubs_diagnostics(monkeypatch):
    from artisan.execution.recording.commands import sanitize_diagnostic

    def fail(*args):
        msg = "wrapping failed for opaque-value"
        raise ValueError(msg)

    monkeypatch.setattr(LocalEnvironmentSpec, "wrap_command", fail)
    with capture_commands() as recorder:
        with pytest.raises(ValueError) as caught:
            run_command(
                LocalEnvironmentSpec(), ["tool"], sensitive_values=("opaque-value",)
            )
        assert "opaque-value" not in sanitize_diagnostic(str(caught.value))
    assert recorder.snapshot() == CommandRecording.empty()


def test_single_character_secret_keeps_structural_fields_valid():
    with capture_commands() as recorder:
        run_command(
            LocalEnvironmentSpec(env={"ODD": "e"}), [sys.executable, "-c", "pass"]
        )
    recording = recorder.snapshot()
    assert recording.commands[0].outcome == "succeeded"
    assert "<redacted>" in recording.commands[0].argv[0]


def test_execute_as_tool_records_its_actual_generated_command(tmp_path):
    from fixtures.endpoint_ops import FlagTool

    from artisan.execution.compute.invoke import invoke_op_work
    from artisan.schemas.specs.input_models import ExecuteInput

    process = Mock(returncode=0)
    process.communicate.return_value = ("", "")
    operation = FlagTool()
    with (
        capture_commands(operation) as recorder,
        patch(
            "artisan.utils.external_tools.subprocess.Popen", return_value=process
        ) as launch,
    ):
        invoke_op_work(operation, ExecuteInput(execute_dir=str(tmp_path), inputs={}))
    command = recorder.snapshot().commands[0]
    assert command.argv == launch.call_args.args[0]
    assert command.argv[:3] == ["artisan", "op", "run"]


def test_oversized_wire_string_is_checked_in_bounded_chunks(monkeypatch):
    from artisan.schemas.execution import command_record

    encode = command_record.to_json
    encoded_lengths = []

    def tracked_encode(value):
        if isinstance(value, str):
            encoded_lengths.append(len(value))
        return encode(value)

    monkeypatch.setattr(command_record, "to_json", tracked_encode)
    with pytest.raises(ValueError, match="byte limit"):
        command_record.check_recording_size({"argv": ["é" * 1_000_000]})
    assert max(encoded_lengths) <= 4096
    assert sum(encoded_lengths) < 1_000_000


def test_completed_and_direct_invocations_release_sequence_bookkeeping(monkeypatch):
    monkeypatch.setattr(
        "artisan.execution.recording.commands.MAX_COMMAND_RECORDING_BYTES", 350
    )
    with capture_commands() as recorder:
        for _ in range(1000):
            with invocation_scope():
                first = recorder.begin(
                    LocalEnvironmentSpec(), ["tool"], ["tool"], "/work", ()
                )
                first.finish("succeeded", 0)
                with invocation_scope():
                    second = recorder.begin(
                        LocalEnvironmentSpec(), ["tool"], ["tool"], "/work", ()
                    )
                    second.finish("succeeded", 0)
                assert list(recorder._sequences.values()) == [2]
            assert recorder._sequences == {}
            direct = recorder.begin(
                LocalEnvironmentSpec(), ["tool"], ["tool"], "/work", ()
            )
            direct.finish("succeeded", 0)
            assert recorder._sequences == {}
    recording = recorder.snapshot()
    assert recording.omitted_commands == 3000
    assert recording.commands == []


def test_unchanged_json_argument_preserves_whitespace_without_false_redactions():
    argument = '{ "ordinary": "text", "list": [1, 2] }'
    recorder = CommandRecorder()
    attempt = recorder.begin(
        LocalEnvironmentSpec(), ["tool", argument], ["tool", argument], "/work", ()
    )
    attempt.finish("succeeded", 0)
    command = recorder.snapshot().commands[0]
    assert command.requested_argv[-1] == argument
    assert command.argv[-1] == argument
    assert command.redacted_fields == []


def test_remote_environment_hints_allow_duplicates_and_parent_augmentation():
    command = _record(arg="parent-secret").model_copy(
        update={"required_environment": ["REMOTE", "REMOTE"]}
    )
    recorder = CommandRecorder()
    recorder.add_environment({"PARENT": "parent-secret"})
    recorder.merge(_recording(command), 0)
    retained = recorder.snapshot().commands[0]
    assert retained.required_environment == ["PARENT", "REMOTE"]
    assert retained.argv == ["<redacted>"]
    assert retained.redacted_fields == ["$.argv[0]", "$.requested_argv[0]"]
