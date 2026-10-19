"""Fixed deployment negotiation, strict wire conformance, and cache guards."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any, ClassVar
from unittest.mock import AsyncMock, MagicMock

import httpx
import jsonschema
import pytest
from fastapi.testclient import TestClient
from fixtures.endpoint_ops import FlagTool, GpuTool, PlainTool
from pydantic import BaseModel, Field, SecretStr, ValidationError

from artisan.errors import ArtisanError
from artisan.execution.recording.commands import capture_commands
from artisan.execution.tool_endpoint import client as client_mod
from artisan.execution.tool_endpoint import deploy as deploy_mod
from artisan.execution.tool_endpoint import spec as spec_mod
from artisan.execution.tool_endpoint.client import call_endpoint
from artisan.execution.tool_endpoint.deploy import build_app
from artisan.execution.tool_endpoint.protocol import (
    CommandContract,
    SchemaResponse,
    ToolManifest,
    ToolRequest,
)
from artisan.execution.tool_endpoint.server import run_tool_request
from artisan.execution.tool_endpoint.spec import (
    command_contract,
    validate_deployment_overrides,
)
from artisan.operations.examples import WaitTool
from artisan.orchestration.engine.step_executor import execute_step
from artisan.orchestration.pipeline_manager import PipelineManager
from artisan.schemas.execution.command_record import CommandRecording
from artisan.schemas.operation_config.environment_spec import LocalEnvironmentSpec
from artisan.schemas.operation_config.environments import Environments
from artisan.schemas.operation_config.tool_spec import ToolSpec
from artisan.schemas.orchestration.step_lifecycle import StepStatus
from artisan.schemas.orchestration.step_overrides import StepOverrides
from artisan.schemas.specs.input_models import ExecuteInput
from artisan.utils.hashing import (
    compute_execution_spec_id,
    compute_step_spec_id,
    effective_config_payload,
)


class CredentialContractTool(PlainTool):
    name: ClassVar[str] = "credential_contract_test"

    class Params(BaseModel):
        token: str = Field(description="Synthetic credential-named value.")
        credential: SecretStr = Field(description="Synthetic typed secret.")

    params: Params


class DefaultCredentialContractTool(CredentialContractTool):
    name: ClassVar[str] = "default_credential_contract_test"

    class Params(CredentialContractTool.Params):
        pass

    params: Params = Params(
        token="synthetic-token", credential=SecretStr("synthetic-typed-secret")
    )
    tool: ToolSpec = ToolSpec(executable="/tools/synthetic-typed-secret")
    environments: Environments = Environments(
        local=LocalEnvironmentSpec(
            env={"ROOT": "synthetic-typed-secret"}, public_env=("ROOT",)
        )
    )


def _op(**updates: Any) -> PlainTool:
    op = PlainTool()
    modal = op.compute_provider.modal.model_copy(
        update={"endpoint_url": "https://test.invalid", **updates}
    )
    op.compute_provider = op.compute_provider.model_copy(
        update={"active": "modal", "modal": modal}
    )
    return op


def _schema(contract: CommandContract) -> dict[str, Any]:
    return SchemaResponse(
        command_contract=contract,
        operation=contract.operation_identity.name,
        operation_identity=contract.operation_identity,
        debug_capture_supported=True,
    ).model_dump(mode="json")


def _http(monkeypatch: pytest.MonkeyPatch, handler: Any) -> None:
    client_class = httpx.Client
    monkeypatch.setattr(
        client_mod.httpx,
        "Client",
        lambda **kwargs: client_class(**kwargs, transport=httpx.MockTransport(handler)),
    )


def test_contract_required_params_and_python_mode() -> None:
    assert command_contract(GpuTool).tool_parts == ["bash"]
    assert command_contract(FlagTool).kind == "python_function"
    assert command_contract(FlagTool).tool_parts == []


@pytest.mark.parametrize(
    "tool",
    [
        ToolSpec(executable="other"),
        ToolSpec(executable="bash", interpreter="python -u"),
        ToolSpec(executable="bash", subcommand="infer"),
    ],
)
def test_contract_each_tool_field_changes_identity(tool: ToolSpec) -> None:
    assert command_contract(PlainTool(tool=tool)) != command_contract(PlainTool)


def test_contract_normalizes_with_tool_parts() -> None:
    a = PlainTool(tool=ToolSpec(executable="bash", interpreter="python   -u"))
    b = PlainTool(tool=ToolSpec(executable="bash", interpreter="python -u"))
    assert command_contract(a) == command_contract(b)


def test_contract_unavailable_source_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    identity = command_contract(PlainTool).operation_identity.model_copy(
        update={"module_digest": None}
    )
    monkeypatch.setattr(spec_mod, "operation_identity", lambda _: identity)
    with pytest.raises(ValueError, match="unavailable"):
        command_contract(PlainTool)


@pytest.mark.parametrize(
    "executable",
    [
        "https://user:password@tool.invalid/run",
        "https://:password@tool.invalid/run",
        "--token=secret-marker",
    ],
)
def test_contract_rejects_sensitive_parts(executable: str) -> None:
    with pytest.raises(ValueError) as error:
        command_contract(PlainTool(tool=ToolSpec(executable=executable)))
    assert executable not in str(error.value)


@pytest.mark.parametrize("owner", ["local", "modal"])
def test_contract_public_environment_preserves_known_secret_precedence(
    owner: str,
) -> None:
    root = "/srv/shared"
    operation = PlainTool(tool=ToolSpec(executable=f"{root}/bin/tool"))
    if owner == "local":
        operation.environments.local = LocalEnvironmentSpec(
            env={"ROOT": root}, public_env=("ROOT",)
        )
    else:
        operation.compute_provider.modal = type(operation.compute_provider.modal)(
            **{
                **operation.compute_provider.modal.model_dump(),
                "env": {"ROOT": root},
                "public_env": ("ROOT",),
            }
        )
    assert command_contract(operation).tool_parts == [f"{root}/bin/tool"]
    with capture_commands(operation) as recorder:
        recorder.add_sensitive_data(root)
        with pytest.raises(ValueError, match="sensitive"):
            command_contract(operation)


def test_public_annotation_hash_preserves_modal_checked_policy() -> None:
    operation = _op(env={"ROOT": "/srv/models"})
    before = effective_config_payload(operation)
    operation.compute_provider.modal.public_env = ("ROOT",)
    annotated = effective_config_payload(operation)
    assert annotated == before
    assert annotated["endpoint_contract_policy"] == "fixed_command_v1"
    operation.compute_provider.modal.env["ROOT"] = "/srv/other"
    assert effective_config_payload(operation) != before
    operation.compute_provider.active = "local"
    assert "endpoint_contract_policy" not in effective_config_payload(operation)


@pytest.mark.parametrize("secret", ["synthetic-token", "synthetic-typed-secret"])
def test_contract_rejects_public_parameter_alias_without_secret_io(
    monkeypatch: pytest.MonkeyPatch, secret: str
) -> None:
    from artisan.execution.recording import commands

    def unexpected_secret_read(*args: Any, **kwargs: Any) -> None:
        pytest.fail("contract checking must not load endpoint credentials")

    monkeypatch.setattr(commands, "env_or_dotenv", unexpected_secret_read)
    operation = CredentialContractTool(
        params={"token": "synthetic-token", "credential": "synthetic-typed-secret"},
        tool=ToolSpec(executable=f"/tools/{secret}"),
    )
    operation.environments.local = LocalEnvironmentSpec(
        env={"ROOT": secret}, public_env=("ROOT",)
    )
    with pytest.raises(ValueError, match="sensitive") as error:
        command_contract(operation)
    assert secret not in str(error.value)


def test_contract_class_defaults_reject_public_secret_alias_without_construction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from artisan.execution.recording import commands

    def unexpected_call(*args: Any, **kwargs: Any) -> None:
        pytest.fail(
            "contract checking must not construct operations or load credentials"
        )

    monkeypatch.setattr(commands, "env_or_dotenv", unexpected_call)
    monkeypatch.setattr(DefaultCredentialContractTool, "__init__", unexpected_call)
    with pytest.raises(ValueError, match="sensitive") as error:
        command_contract(DefaultCredentialContractTool)
    assert "synthetic-typed-secret" not in str(error.value)


@pytest.mark.parametrize(
    "mutation",
    [
        lambda v: v.update(extra=True),
        lambda v: v.update(kind="unknown"),
        lambda v: v.update(tool_parts=[]),
        lambda v: v.update(kind="python_function"),
        lambda v: v.update(tool_parts=[1]),
        lambda v: v["operation_identity"].update(module_digest=None),
        lambda v: v["operation_identity"].update(extra=True),
    ],
)
def test_contract_model_and_slim_schema_agree(mutation: Any) -> None:
    value = command_contract(PlainTool).model_dump(mode="json")
    schema = CommandContract.model_json_schema()
    jsonschema.validate(value, schema)
    mutation(value)
    with pytest.raises(ValidationError):
        CommandContract.model_validate(value)
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(value, schema)


def test_contract_byte_bound(monkeypatch: pytest.MonkeyPatch) -> None:
    from artisan.execution.tool_endpoint import transport

    value = command_contract(PlainTool).model_dump(mode="json")
    monkeypatch.setattr(transport, "MAX_INLINE_BYTES", 1)
    with pytest.raises(ValueError, match="byte limit"):
        CommandContract.model_validate(value)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("image", "different"),
        ("image_registry_secret", "private"),
        ("secrets", ["private"]),
        ("volumes", {"/mnt": "volume"}),
        ("env", {"TOKEN": "secret-marker"}),
        ("local_python_sources", ["other"]),
        ("retries", 9),
        ("min_containers", 2),
        ("max_containers", 3),
        ("scaledown_window", 30),
    ],
)
def test_deployment_overrides_fail_without_values(field: str, value: Any) -> None:
    with pytest.raises(ArtisanError) as error:
        validate_deployment_overrides(_op(**{field: value}))
    assert field in str(error.value)
    assert "secret-marker" not in str(error.value)


def test_client_controls_and_inactive_modal_remain_supported() -> None:
    validate_deployment_overrides(
        _op(
            endpoint_url="https://another.invalid",
            poll_interval=1,
            max_concurrent_calls=2,
        )
    )
    op = _op(image="unused")
    op.compute_provider.active = "local"
    validate_deployment_overrides(op)
    with pytest.raises(ArtisanError):
        validate_deployment_overrides(op, endpoint_call=True)


@pytest.mark.parametrize("mode", ["old", "mismatch", "invalid"])
def test_normal_handshake_rejects_before_pack_or_submit(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, mode: str
) -> None:
    contract = command_contract(PlainTool)
    schema = _schema(contract)
    if mode == "old":
        schema.pop("command_contract")
    elif mode == "invalid":
        schema["command_contract"]["tool_parts"] = [1]
    else:
        schema["command_contract"]["tool_parts"] = ["other"]
    calls = []

    def handle(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        return httpx.Response(200, json=schema)

    _http(monkeypatch, handle)
    packed = MagicMock(side_effect=AssertionError("must check before packaging"))
    monkeypatch.setattr(client_mod, "_pack_request_inputs", packed)
    with pytest.raises(ArtisanError):
        call_endpoint(_op(), ExecuteInput(inputs={}, execute_dir=str(tmp_path)))
    assert calls == ["/schema"]
    packed.assert_not_called()


def test_matching_override_and_params_sent_after_handshake(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    op = WaitTool(params=WaitTool.Params(seconds=7), tool=ToolSpec(executable="bash"))
    op.compute_provider.modal.endpoint_url = "https://test.invalid"
    contract = command_contract(op)
    calls = []

    def handle(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        if request.url.path == "/schema":
            return httpx.Response(200, json=_schema(contract))
        if request.url.path == "/submit":
            body = request.read().decode()
            from urllib.parse import parse_qs

            fields = parse_qs(body)
            assert json.loads(fields["params"][0]) == {"seconds": 7}
            assert (
                CommandContract.model_validate_json(
                    fields["expected_command_contract"][0]
                )
                == contract
            )
            return httpx.Response(200, json={"call_id": "one"})
        manifest = ToolManifest(
            command_contract=contract,
            operation_identity=contract.operation_identity,
            debug_capture=None,
            command_recording=CommandRecording.empty(),
        )
        return httpx.Response(
            200, json={"status": "done", "manifest": manifest.model_dump(mode="json")}
        )

    _http(monkeypatch, handle)
    call_endpoint(op, ExecuteInput(inputs={}, execute_dir=str(tmp_path)))
    assert calls == ["/schema", "/submit", "/result"]


def test_worker_drift_rejected_before_launch(monkeypatch: pytest.MonkeyPatch) -> None:
    request = ToolRequest(expected_command_contract=command_contract(PlainTool))
    monkeypatch.setattr(
        "artisan.execution.tool_endpoint.server.instantiate_op",
        lambda *args: PlainTool(tool=ToolSpec(executable="different")),
    )
    invoke = MagicMock(side_effect=AssertionError("must not launch"))
    monkeypatch.setattr("artisan.execution.tool_endpoint.server.invoke_op_work", invoke)
    result = run_tool_request(PlainTool, request)
    assert result.manifest.error.code == "tool_endpoint_misconfigured"
    assert result.manifest.command_contract is None
    assert result.manifest.command_recording.commands == []
    invoke.assert_not_called()


def test_failed_construction_has_no_verified_contract() -> None:
    result = run_tool_request(
        GpuTool, ToolRequest(expected_command_contract=command_contract(GpuTool))
    )
    assert result.manifest.error is not None
    assert result.manifest.command_contract is None


def test_successful_manifest_requires_matching_contract() -> None:
    contract = command_contract(PlainTool)
    fields = {
        "operation_identity": contract.operation_identity,
        "debug_capture": None,
        "command_recording": CommandRecording.empty(),
    }
    with pytest.raises(ValueError, match="requires"):
        ToolManifest(**fields)
    with pytest.raises(ValueError, match="disagrees"):
        ToolManifest(command_contract=command_contract(WaitTool), **fields)


@pytest.mark.parametrize("mode", ["missing", "mismatch", "type", "extra"])
def test_submit_contract_checked_before_read_or_spawn(
    monkeypatch: pytest.MonkeyPatch, mode: str
) -> None:
    modal = MagicMock()
    monkeypatch.setattr(deploy_mod, "import_modal", lambda: modal)
    build_app(PlainTool)
    web = modal.asgi_app.return_value.call_args.args[0]()
    worker = modal.App.return_value.function.return_value.return_value
    worker.spawn.aio = AsyncMock(return_value=SimpleNamespace(object_id="one"))
    contract = command_contract(PlainTool).model_dump(mode="json")
    if mode == "mismatch":
        contract["tool_parts"] = ["secret-marker"]
    elif mode == "type":
        contract["tool_parts"] = [1]
    elif mode == "extra":
        contract["extra"] = "secret-marker"
    data = (
        {} if mode == "missing" else {"expected_command_contract": json.dumps(contract)}
    )
    response = TestClient(web).post("/submit", data=data)
    assert response.status_code == (409 if mode == "mismatch" else 422)
    assert "secret-marker" not in response.text
    worker.spawn.aio.assert_not_called()
    modal.App.assert_called_once()  # only explicit build_app above


def test_old_policy_misses_both_cache_ids_and_preserves_local_shape() -> None:
    op = _op()
    checked = effective_config_payload(op)
    old = {
        key: value
        for key, value in checked.items()
        if key != "endpoint_contract_policy"
    }
    old_entries = {
        compute_execution_spec_id(op.name, {}, {}, old): "unchecked",
        compute_step_spec_id(op.name, 0, {}, {}, old): "unchecked",
    }
    assert compute_execution_spec_id(op.name, {}, {}, checked) not in old_entries
    assert compute_step_spec_id(op.name, 0, {}, {}, checked) not in old_entries
    op.compute_provider.active = "local"
    local = effective_config_payload(op)
    assert "endpoint_contract_policy" not in local
    assert set(local) == set(old)


def test_invalid_override_rejected_before_execution_cache(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lookup = MagicMock(return_value=object())
    monkeypatch.setattr(
        "artisan.orchestration.engine.step_executor.check_cache_for_batch", lookup
    )
    with pytest.raises(ArtisanError):
        execute_step(_op(image="different"), {}, StepOverrides(), MagicMock())
    lookup.assert_not_called()


def test_invalid_override_rejected_before_whole_step_cache(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    pipeline = PipelineManager.create(
        name="modal-cache-guard",
        delta_root=str(tmp_path / "delta"),
        staging_root=str(tmp_path / "staging"),
        working_root=str(tmp_path / "working"),
    )
    lookup = MagicMock(return_value=object())
    monkeypatch.setattr(pipeline._step_tracker, "check_cache", lookup)
    result = pipeline.run(
        PlainTool, compute_provider=_op(image="different").compute_provider
    )
    assert result.status == StepStatus.FAILED
    lookup.assert_not_called()
    pipeline.finalize()


def test_checked_execution_cache_needs_no_endpoint(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from artisan.orchestration.engine.inputs import PreparedInputs
    from artisan.orchestration.runners import Runner
    from artisan.schemas.execution.cache_result import CacheHit

    pipeline = PipelineManager.create(
        name="checked-execution-cache",
        delta_root=str(tmp_path / "delta"),
        staging_root=str(tmp_path / "staging"),
        working_root=str(tmp_path / "working"),
    )
    operation = _op()
    checked_id = compute_execution_spec_id(
        operation.name, {}, {}, effective_config_payload(operation)
    )
    source = CacheHit(execution_run_id="c" * 32, execution_spec_id=checked_id)
    lookup = MagicMock(
        side_effect=lambda key, *args, **kwargs: source if key == checked_id else None
    )
    monkeypatch.setattr(
        "artisan.orchestration.engine.step_executor.check_cache_for_batch", lookup
    )
    endpoint = MagicMock(side_effect=AssertionError("historical hit needs no endpoint"))
    monkeypatch.setattr(client_mod.httpx, "Client", endpoint)
    result = execute_step(
        operation,
        PreparedInputs({}, {}, None, {}),
        StepOverrides(),
        Runner.LOCAL,
        config=pipeline.config,
    )
    assert result.status == StepStatus.SUCCEEDED
    assert result.succeeded_count == 1
    lookup.assert_called_once()
    endpoint.assert_not_called()
    pipeline.finalize()


def test_checked_whole_step_cache_retains_source_without_endpoint(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from artisan.orchestration.engine.step_tracker import _WholeStepCacheHit
    from artisan.schemas.orchestration.step_lifecycle import StepDisposition
    from artisan.schemas.orchestration.step_result import StepResult

    pipeline = PipelineManager.create(
        name="checked-whole-cache",
        delta_root=str(tmp_path / "delta"),
        staging_root=str(tmp_path / "staging"),
        working_root=str(tmp_path / "working"),
    )
    operation = _op()
    checked_id = compute_step_spec_id(
        operation.name, 0, {}, {}, effective_config_payload(operation)
    )
    source = _WholeStepCacheHit(
        result=StepResult(
            step_name="source",
            step_number=0,
            status=StepStatus.SUCCEEDED,
            disposition=StepDisposition.EXECUTED,
            output_roles=frozenset({"output"}),
            output_types={"output": "data"},
            step_run_id="a" * 32,
        ),
        source_step_run_id="a" * 32,
        execution_run_ids=("c" * 32,),
    )
    monkeypatch.setattr(
        pipeline._step_tracker,
        "check_cache",
        lambda key, *args: source if key == checked_id else None,
    )
    commit = MagicMock(side_effect=lambda *args, **kwargs: kwargs["result"])
    monkeypatch.setattr(pipeline, "_commit_whole_step_reuse", commit)
    endpoint = MagicMock(side_effect=AssertionError("historical hit needs no endpoint"))
    monkeypatch.setattr(client_mod.httpx, "Client", endpoint)
    result = pipeline.run(
        PlainTool, compute_provider=operation.compute_provider, compact=False
    )
    assert result.disposition == StepDisposition.CACHE_HIT
    assert result.step_run_id != source.source_step_run_id
    assert commit.call_args.args[1] == source.execution_run_ids
    endpoint.assert_not_called()


def test_contract_rejects_known_environment_value() -> None:
    operation = _op()
    operation.environments.current().env = {"TOKEN": "private-command-marker"}
    operation.tool = ToolSpec(executable="private-command-marker")
    with pytest.raises(ValueError, match="sensitive") as error:
        command_contract(operation)
    assert "private-command-marker" not in str(error.value)
