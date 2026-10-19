"""Flattened deploy spec for an operation's tool endpoint.

``endpoint_spec`` reads only class-level defaults — it never instantiates
the operation (params arrive per request). Deliberately modal-free: the
CLI resolves image refs and container metadata from here without
importing the ``modal`` SDK; ``deploy.build_app`` consumes the same spec
to construct the Modal app.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from artisan.errors import ArtisanError, ErrorCode
from artisan.execution.recording.commands import CommandRecorder, current_recorder
from artisan.execution.tool_endpoint.protocol import CommandContract
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.registry.resolve import operation_identity
from artisan.registry.schemas import params_schema_for
from artisan.schemas.operation_config.compute import (
    ComputeProvider,
    ModalComputeConfig,
)
from artisan.schemas.operation_config.compute_resources import ComputeResources


class EndpointSpec(BaseModel):
    """Flattened class-level deploy config for one tool endpoint."""

    op_module: str
    op_qualname: str
    name: str
    image: str
    image_registry_secret: str | None
    env: dict[str, str]
    volumes: dict[str, str]
    secrets: list[str]
    retries: int
    min_containers: int
    max_containers: int | None
    scaledown_window: int | None
    local_python_sources: list[str]
    gpu: str | None
    cpu: float | None
    memory_mb: int | None
    timeout: int | None
    params_schema: dict[str, Any] = Field(default_factory=dict)
    description: str = ""
    input_roles: dict[str, dict[str, Any]] = Field(default_factory=dict)
    data_policy: dict[str, list[str]] = Field(default_factory=dict)


def endpoint_spec(op_cls: type[OperationDefinition]) -> EndpointSpec:
    """Flatten the op's class-level tool + compute config into a deploy spec.

    Args:
        op_cls: The registered operation class to deploy.

    Returns:
        The deploy spec read from class-level field defaults, including the
        op's ``Params`` JSON schema (from the registry's canonical
        ``params_schema_for`` builder — the same schema the registry/MCP
        plane serves) for boundary validation. A parameterless operation's
        schema accepts only an empty object and rejects extra keys.

    Raises:
        ValueError: If the op is not a command op (ToolSpec +
            execute_command, or execute_as_tool=True) or declares no
            ``compute_provider.modal`` config.
    """
    if not op_cls.declares_command_execute():
        msg = (
            f"{op_cls.__name__} is not a command op — deploying an endpoint "
            "requires a ToolSpec + execute_command(), or execute_as_tool=True"
        )
        raise ValueError(msg)
    provider = op_cls.model_fields["compute_provider"].default
    modal_cfg = provider.modal if isinstance(provider, ComputeProvider) else None
    if not isinstance(modal_cfg, ModalComputeConfig):
        msg = (
            f"{op_cls.__name__} declares no compute_provider.modal config — "
            "set one with the worker image to deploy"
        )
        raise ValueError(msg)
    try:
        modal_cfg = ModalComputeConfig.model_validate(
            modal_cfg.model_dump(mode="python", warnings=False)
        )
    except (TypeError, ValueError) as exc:
        msg = "class-default modal endpoint configuration is invalid"
        raise ValueError(msg) from exc
    resources = op_cls.model_fields["compute_resources"].default
    if not isinstance(resources, ComputeResources):
        resources = ComputeResources()
    params_schema = params_schema_for(op_cls)
    if modal_cfg.output_store is not None:
        try:
            modal_cfg.data_policy.authorize_output(modal_cfg.output_store)
        except ValueError as exc:
            msg = "class-default output_store is outside the endpoint data policy"
            raise ValueError(msg) from exc
    return EndpointSpec(
        op_module=op_cls.__module__,
        op_qualname=op_cls.__qualname__,
        name=op_cls.name,
        image=modal_cfg.image,
        image_registry_secret=modal_cfg.image_registry_secret,
        env=modal_cfg.env,
        volumes=modal_cfg.volumes,
        secrets=modal_cfg.secrets,
        retries=modal_cfg.retries,
        min_containers=modal_cfg.min_containers,
        max_containers=modal_cfg.max_containers,
        scaledown_window=modal_cfg.scaledown_window,
        local_python_sources=modal_cfg.local_python_sources,
        gpu=resources.gpu,
        cpu=resources.cpu,
        memory_mb=resources.memory_gb * 1024 if resources.memory_gb else None,
        timeout=resources.timeout,
        params_schema=params_schema,
        description=op_cls.description,
        # str(role) — role keys may be StrEnum members; an enum instance in
        # the baked dict would drag its artisan-defined class into the
        # endpoint closure at cloudpickle time
        input_roles={
            str(role): {"required": s.required, "description": s.description}
            for role, s in op_cls.inputs.items()
        },
        data_policy=modal_cfg.data_policy.model_dump(mode="json"),
    )


_DEPLOYMENT_FIELDS = (
    "image",
    "image_registry_secret",
    "secrets",
    "volumes",
    "env",
    "local_python_sources",
    "retries",
    "min_containers",
    "max_containers",
    "scaledown_window",
    "data_policy",
)


def command_contract(
    operation: type[OperationDefinition] | OperationDefinition,
) -> CommandContract:
    """Describe a class or instance without constructing required Params.

    Raises:
        ValueError: The command is unavailable or contains sensitive values.
    """
    op_cls = operation if isinstance(operation, type) else type(operation)
    tool = (
        op_cls.model_fields["tool"].default
        if isinstance(operation, type)
        else operation.tool
    )
    if not op_cls.declares_command_execute():
        msg = "operation does not declare a command"
        raise ValueError(msg)
    if (tool is None) != op_cls.execute_as_tool:
        msg = "command kind and ToolSpec declaration disagree"
        raise ValueError(msg)
    parts = tool.parts() if tool is not None else []
    recorder = CommandRecorder()
    current = current_recorder()
    if current is not None:
        recorder.add_sensitive_data(current.sensitive_values)
    provider = (
        op_cls.model_fields["compute_provider"].default
        if isinstance(operation, type)
        else operation.compute_provider
    )
    modal = getattr(provider, "modal", None)
    recorder.add_environment(getattr(modal, "env", {}))
    environments = (
        op_cls.model_fields["environments"].default
        if isinstance(operation, type)
        else operation.environments
    )
    environment = getattr(environments, getattr(environments, "active", ""), None)
    recorder.add_environment(getattr(environment, "env", {}))
    # Use the same URI, flag, and known-value policy as command evidence.
    if recorder._argv(parts) != parts:
        msg = "command declaration contains sensitive values"
        raise ValueError(msg)
    return CommandContract(
        operation_identity=operation_identity(op_cls),
        kind="python_function" if op_cls.execute_as_tool else "tool",
        tool_parts=parts,
    )


def validate_deployment_overrides(
    operation: OperationDefinition, *, endpoint_call: bool = False
) -> None:
    """Reject ignored Modal infrastructure overrides before any cache lookup.

    This check reads declarations only; it performs no discovery or secret I/O.
    Client routing, polling, output selection, and inactive providers are excluded.
    """
    if not endpoint_call and operation.compute_provider.active != "modal":
        return
    defaults = type(operation).model_fields["compute_provider"].default
    declared = defaults.modal if isinstance(defaults, ComputeProvider) else None
    actual = operation.compute_provider.modal
    field = None
    if declared is None or actual is None:
        field = "compute_provider.modal"
    else:
        field = next(
            (
                name
                for name in _DEPLOYMENT_FIELDS
                if getattr(actual, name) != getattr(declared, name)
            ),
            None,
        )
    resources = type(operation).model_fields["compute_resources"].default
    if field is None and operation.compute_resources != resources:
        field = "compute_resources"
    if field is not None:
        raise ArtisanError(
            code=ErrorCode.TOOL_ENDPOINT_MISCONFIGURED,
            message=f"Modal deployment field {field} differs from the class declaration; change the declaration and explicitly deploy or select the compatible endpoint",
            error_type="config",
            operation_name=operation.name,
            recovery_hint="CHECK_INPUT",
        )
