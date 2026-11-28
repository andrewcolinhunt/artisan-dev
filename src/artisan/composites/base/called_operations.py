"""Capture configuration-selected creator calls without candidate parameters."""

from __future__ import annotations

import json
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, cast

from pydantic import BaseModel
from pydantic_core import to_jsonable_python

from artisan.composites.base.snapshots import (
    input_contracts,
    output_contracts,
    parameter_class,
)
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.schemas.composites.plan import (
    CalledOperationContract,
    ConstantArtifact,
    DefinitionIdentity,
    ExecutionContract,
    InvocationPolicy,
    RoleBinding,
    ToolContract,
)
from artisan.schemas.enums import GroupByStrategy
from artisan.schemas.execution.batch_strategy import BatchStrategy
from artisan.schemas.operation_config.environment import Environment
from artisan.schemas.operation_config.tool_spec import ToolSpec
from artisan.schemas.specs.input_defaults import capture_input_defaults
from artisan.utils.hashing import canonical_json_bytes


@dataclass(frozen=True)
class CalledOperation:
    """Select a creator and fixed settings before candidate inputs exist.

    Args:
        definition: Trusted imported creator operation class.
        params: Fixed parameter fields. Request parameters cannot override them.
    """

    definition: type[OperationDefinition]
    params: Mapping[str, object] | BaseModel | None = None


@dataclass(frozen=True)
class CapturedCalledOperation:
    """Live code paired with a detached configuration and command declaration."""

    definition: type[OperationDefinition]
    contract: CalledOperationContract
    environment: Environment | None


def capture_called_operations(
    selections: Mapping[str, CalledOperation] | None,
) -> tuple[CapturedCalledOperation, ...]:
    """Freeze selections without instantiating incomplete parameter models."""
    return tuple(
        _capture(key, item) for key, item in sorted((selections or {}).items())
    )


def _default(definition: type[OperationDefinition], name: str) -> object:
    field = definition.model_fields[name]
    if field.is_required() or field.default_factory is not None:
        msg = f"Configured calls require a concrete {name} declaration"
        raise ValueError(msg)
    return field.default


def _capture(key: str, item: object) -> CapturedCalledOperation:
    if not isinstance(item, CalledOperation):
        msg = "Configured selections must contain CalledOperation values"
        raise TypeError(msg)
    definition: object = item.definition
    if not isinstance(definition, type) or not issubclass(
        definition, OperationDefinition
    ):
        msg = "Configured calls require creator operation classes"
        raise TypeError(msg)
    if definition._kind() != "creator" or definition.runtime_defined_inputs:
        msg = "Configured calls require statically declared creator inputs"
        raise ValueError(msg)
    tool = cast(ToolSpec | None, _default(definition, "tool"))
    environment = cast(Environment | None, _default(definition, "environment"))
    mode: Literal["native", "command", "python_adapter"] = (
        "python_adapter"
        if definition.execute_as_tool
        else "command"
        if definition.declares_command_execute()
        else "native"
    )
    if mode == "native" and environment is not None:
        msg = "Native operations use their caller's Python environment"
        raise ValueError(msg)
    fixed, defaults, schema = _parameters(definition, item.params)
    batch = cast(BatchStrategy, _default(definition, "batch_strategy"))
    contract = CalledOperationContract(
        key=key,
        calls_operations=definition.calls_operations,
        definition=DefinitionIdentity(
            kind="creator",
            name=definition.name,
            version=definition.version,
            module=definition.__module__,
            qualname=definition.__qualname__,
        ),
        execution=ExecutionContract(
            mode=mode,
            tool=None if tool is None else ToolContract(**tool.model_dump()),
        ),
        inputs=input_contracts(definition),
        outputs=output_contracts(definition),
        policy=InvocationPolicy(
            group_by=cast(GroupByStrategy | None, _default(definition, "group_by")),
            independent_input_streams=definition.independent_input_streams,
            per_artifact_dispatch=definition.per_artifact_dispatch,
            artifacts_per_unit=batch.artifacts_per_unit,
            max_artifacts_per_unit=batch.max_artifacts_per_unit,
            shared_input_roles=definition.shared_input_roles,
        ),
        default_inputs=tuple(
            RoleBinding(
                role=role,
                sources=(),
                constants=tuple(
                    ConstantArtifact(
                        artifact_id=cast(str, artifact.artifact_id),
                        artifact_type=artifact.artifact_type,
                    )
                    for artifact in artifacts
                ),
            )
            for role, artifacts in sorted(
                capture_input_defaults(definition.inputs, ()).items()
            )
        ),
        fixed_params_json=_json(fixed),
        defaults_json=_json(defaults),
        parameter_schema_json=_json(schema),
        cacheable=definition.cacheable,
    )
    return CapturedCalledOperation(definition, contract, environment)


def _json(value: object) -> str:
    return canonical_json_bytes(to_jsonable_python(value)).decode()


def _parameters(
    definition: type[OperationDefinition],
    params: Mapping[str, object] | BaseModel | None,
) -> tuple[dict[str, object], dict[str, object], dict[str, object]]:
    model = parameter_class(definition)
    if isinstance(params, BaseModel):
        if type(params) is not model:
            msg = "Fixed settings must use the selected operation's Params"
            raise TypeError(msg)
        fixed = params.model_dump(mode="json", by_alias=False)
    else:
        fixed = dict(params or {})
    if model is None:
        if fixed:
            msg = "Selected operation declares no Params"
            raise ValueError(msg)
        return {}, {}, {}
    if fixed.keys() - model.model_fields.keys():
        msg = "Fixed settings contain unknown parameter fields"
        raise ValueError(msg)
    defaults = {}
    for name, field in model.model_fields.items():
        if name in fixed or field.is_required():
            continue
        if field.default_factory is not None:
            msg = f"Supply an explicit fixed value for factory parameter {name!r}"
            raise ValueError(msg)
        defaults[name] = field.default
    return fixed, defaults, model.model_json_schema()


def request_parameters(
    captured: CapturedCalledOperation,
    params: Mapping[str, object] | BaseModel | None,
) -> dict[str, object]:
    """Combine request values with frozen defaults, preserving fixed settings."""
    values = (
        params.model_dump(mode="python", exclude_unset=True, by_alias=False)
        if isinstance(params, BaseModel)
        else dict(params or {})
    )
    fixed = json.loads(captured.contract.fixed_params_json)
    if values.keys() & fixed.keys():
        msg = "Request parameters cannot override configured fixed settings"
        raise ValueError(msg)
    return {**json.loads(captured.contract.defaults_json), **fixed, **values}


def environment_key(
    definition: type[OperationDefinition],
    environment: Environment | None,
) -> tuple[str, str] | None:
    """Describe the declaration relative to its installed package when possible."""
    if environment is None:
        return None
    root = sys.modules[definition.__module__.partition(".")[0]]
    package_file = getattr(root, "__file__", None)
    manifest = environment.manifest
    if isinstance(manifest, Path) and package_file is not None:
        package = Path(package_file).parent
        if manifest.is_relative_to(package):
            return environment.name, "package:" + str(manifest.relative_to(package))
    return environment.name, str(manifest)
