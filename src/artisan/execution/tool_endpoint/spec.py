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

from artisan.operations.base.operation_definition import OperationDefinition
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


def endpoint_spec(op_cls: type[OperationDefinition]) -> EndpointSpec:
    """Flatten the op's class-level tool + compute config into a deploy spec.

    Args:
        op_cls: The registered operation class to deploy.

    Returns:
        The deploy spec read from class-level field defaults, including the
        op's ``Params`` JSON schema (from the registry's canonical
        ``params_schema_for`` builder — the same schema the registry/MCP
        plane serves) for boundary validation. A parameter-less op gets the
        empty-``Params`` shape ``{"type": "object", "title": "Params",
        "properties": {}}``, not ``{}``.

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
    resources = op_cls.model_fields["compute_resources"].default
    if not isinstance(resources, ComputeResources):
        resources = ComputeResources()
    params_schema = params_schema_for(op_cls)
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
    )
