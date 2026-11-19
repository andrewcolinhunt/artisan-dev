"""Capture and restore deterministic definition parameters and execution defaults."""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Literal, cast

from pydantic import BaseModel, JsonValue, SecretStr

from artisan.composites.base.composite_definition import CompositeDefinition
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.schemas.composites.plan import (
    DefinitionContract,
    DefinitionIdentity,
    ExecutionContract,
    InputContract,
    InvocationPolicy,
    MixedOutputContract,
    OutputContract,
    OutputRoleContract,
    ToolContract,
)

type Definition = type[OperationDefinition] | type[CompositeDefinition]
type Instance = OperationDefinition | CompositeDefinition


def definition_snapshot(instance: Instance) -> DefinitionContract:
    """Freeze scientific defaults without code, transport or environment objects."""
    definition = type(instance)
    composite = isinstance(instance, CompositeDefinition)
    return DefinitionContract(
        definition=DefinitionIdentity(
            kind="composite" if composite else "creator",
            name=definition.name,
            version=definition.version,
            module=definition.__module__,
            qualname=definition.__qualname__,
        ),
        params_json=parameter_snapshot(instance),
        inputs=input_contracts(definition),
        outputs=output_contracts(definition),
        policy=invocation_policy(instance),
        execution=execution_contract(instance),
        cacheable=definition.cacheable,
    )


def input_contracts(definition: Definition) -> tuple[InputContract, ...]:
    """Freeze declared scientific input behavior without descriptive prose."""
    return tuple(
        InputContract(role=role, **spec.model_dump(exclude={"description"}))
        for role, spec in definition.inputs.items()
    )


def output_contracts(definition: Definition) -> tuple[OutputRoleContract, ...]:
    """Freeze declared output membership and every exact parent namespace."""
    outputs: list[OutputRoleContract] = []
    for role, spec in definition.outputs.items():
        if (
            issubclass(definition, CompositeDefinition)
            and spec.derives_from is not None
        ):
            msg = "Composite outputs derive lineage from their calls; leave derives_from unset"
            raise ValueError(msg)
        if spec.derives_from is not None and len(spec.derives_from) == 2:
            outputs.append(
                MixedOutputContract(
                    role=role,
                    artifact_type=spec.artifact_type,
                    required=spec.required,
                    input_parent_roles=tuple(spec.derives_from["inputs"]),
                    output_parent_roles=tuple(spec.derives_from["outputs"]),
                )
            )
            continue
        kind, roles = (
            next(iter(spec.derives_from.items())) if spec.derives_from else (None, ())
        )
        outputs.append(
            OutputContract(
                role=role,
                artifact_type=spec.artifact_type,
                required=spec.required,
                parent_kind=cast("Literal['inputs', 'outputs'] | None", kind),
                parent_roles=tuple(roles),
            )
        )
    return tuple(outputs)


def parameter_class(definition: Definition) -> type[BaseModel] | None:
    """Reject split or malformed parameter declarations before compiling a plan."""
    if issubclass(
        definition, CompositeDefinition
    ) and definition.model_fields.keys() - (
        CompositeDefinition.model_fields.keys() | {"params"}
    ):
        msg = f"{definition.__name__} must declare scientific fields inside Params"
        raise TypeError(msg)
    model = getattr(definition, "Params", None)
    field = definition.model_fields.get("params")
    if model is None and field is None:
        return None
    if (
        not isinstance(model, type)
        or not issubclass(model, BaseModel)
        or field is None
        or field.annotation is not model
    ):
        msg = f"{definition.__name__} must pair its exact nested Params model and params field"
        raise TypeError(msg)
    declared_model = "Params" in definition.__dict__
    declared_field = "params" in definition.__dict__.get("__annotations__", {})
    if declared_model != declared_field:
        msg = f"{definition.__name__} must redefine both Params and params, or inherit both"
        raise TypeError(msg)
    return model


def instantiate_parameters(definition: Definition, params: object) -> Instance:
    """Validate caller parameters and materialize defaults once."""
    model = parameter_class(definition)
    if params is None:
        instance = definition()
        if model is not None and type(getattr(instance, "params", None)) is not model:
            msg = (
                f"{definition.__name__}.params default must use its exact Params model"
            )
            raise TypeError(msg)
        return instance
    if model is None:
        if params != {}:
            msg = f"Definition {definition.name!r} declares no Params"
            raise ValueError(msg)
        return definition()
    if isinstance(params, BaseModel):
        if type(params) is not model:
            msg = f"Parameters must use {definition.__name__}.Params"
            raise TypeError(msg)
        payload = params.model_dump(mode="python", round_trip=True, by_alias=False)
    elif isinstance(params, Mapping):
        payload = dict(params)
    else:
        msg = "Parameters must be a mapping or the exact Params model"
        raise TypeError(msg)
    validated = model.model_validate(payload, extra="forbid", by_name=True)
    if isinstance(params, BaseModel) and validated != params:
        msg = "Parameters must round-trip without changing their typed values"
        raise ValueError(msg)
    return definition.model_validate({"params": validated})


def parameter_snapshot(instance: Instance, *, allow_private: bool = False) -> str:
    """Capture exact Params; private curator snapshots remain in live memory only."""
    params = getattr(instance, "params", None)
    if params is None:
        return "{}"
    json_value = params.model_dump(mode="json", round_trip=True, by_alias=False)
    encoded = _ordered_json(
        _canonical_unordered(params, json_value, allow_private=allow_private)
    )
    restored = type(params).model_validate_json(encoded, extra="forbid", by_name=True)
    if restored != params:
        msg = "Parameters must round-trip through JSON without changing their typed values"
        raise ValueError(msg)
    return encoded


def _ordered_json(value: JsonValue) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _canonical_unordered(
    value: object, encoded: JsonValue, *, allow_private: bool = False
) -> JsonValue:
    if allow_private and isinstance(value, SecretStr):
        return value.get_secret_value()
    if isinstance(value, BaseModel):
        if (
            not isinstance(encoded, dict)
            or encoded.keys() - type(value).model_fields.keys()
        ):
            msg = "Parameter models must serialize with their declared field names"
            raise ValueError(msg)
        return {
            key: _canonical_unordered(
                getattr(value, key), item, allow_private=allow_private
            )
            for key, item in encoded.items()
        }
    if isinstance(value, (set, frozenset)):
        if not isinstance(encoded, list) or any(
            isinstance(item, (dict, list)) for item in encoded
        ):
            msg = "Unordered parameters must serialize as scalar-item lists; use an ordered sequence for custom encodings"
            raise ValueError(msg)
        return sorted(encoded, key=_ordered_json)
    if isinstance(value, dict):
        if (
            any(not isinstance(key, str) for key in value)
            or not isinstance(encoded, dict)
            or encoded.keys() - value.keys()
        ):
            msg = "Parameter mappings must preserve string keys for canonical JSON snapshots"
            raise ValueError(msg)
        return {
            key: _canonical_unordered(value.get(key), item, allow_private=allow_private)
            for key, item in encoded.items()
        }
    if isinstance(value, (tuple, list)):
        if not isinstance(encoded, list) or len(value) != len(encoded):
            msg = "Parameter sequences must preserve their items in JSON"
            raise ValueError(msg)
        return [
            _canonical_unordered(original, item, allow_private=allow_private)
            for original, item in zip(value, encoded, strict=True)
        ]
    if isinstance(encoded, (dict, list)):
        msg = "Structured parameters must use Pydantic models or built-in containers"
        raise ValueError(msg)
    return encoded


def execution_contract(instance: Instance) -> ExecutionContract:
    """Freeze only the execution adapter and command prefix."""
    if isinstance(instance, CompositeDefinition):
        return ExecutionContract(mode="composite")
    mode: Literal["native", "command", "python_adapter"] = (
        "python_adapter"
        if instance.execute_as_tool
        else "command"
        if instance.is_command_op()
        else "native"
    )
    tool = instance.tool
    return ExecutionContract(
        mode=mode,
        tool=None if tool is None else ToolContract(**tool.model_dump()),
    )


def invocation_policy(instance: Instance) -> InvocationPolicy:
    """Capture scientific invocation defaults without transport packing."""
    return InvocationPolicy(
        group_by=instance.group_by,
        independent_input_streams=instance.independent_input_streams
        if isinstance(instance, OperationDefinition)
        else False,
        per_artifact_dispatch=instance.per_artifact_dispatch
        if isinstance(instance, OperationDefinition)
        else True,
        artifacts_per_unit=instance.batch_strategy.artifacts_per_unit,
        max_artifacts_per_unit=instance.batch_strategy.max_artifacts_per_unit,
    )
