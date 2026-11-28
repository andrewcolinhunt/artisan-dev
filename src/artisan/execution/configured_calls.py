"""Invoke configured creators with software handed down by the command owner."""

from __future__ import annotations

import hashlib
import json
import os
import sys
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from uuid import uuid4

from pydantic import BaseModel

from artisan.composites.base.called_operations import (
    CalledOperation,
    capture_called_operations,
    environment_key,
    request_parameters,
)
from artisan.composites.base.compiled import CompiledPlan
from artisan.composites.base.compiler import Definition, compile_definition
from artisan.composites.base.snapshots import execution_contract, invocation_policy
from artisan.execution.compute.native_routing import native_router
from artisan.execution.compute.prepared import CommandEnvironment, PreparedExecuteRouter
from artisan.execution.environments.observed_worker import (
    observe_current_worker,
)
from artisan.execution.runtime_compatibility import verify_runtime_compatibility
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.schemas.execution.called_operations import ConfiguredCallScope
from artisan.utils.command_supervisor import configured_call_context

_current: ContextVar[ConfiguredCallScope | None] = ContextVar(
    "configured_calls", default=None
)


@dataclass(frozen=True)
class ConfiguredCommandEnvironment:
    """Add an inert handoff to an existing managed command environment."""

    environment: CommandEnvironment
    payload: dict[str, Any]
    scratch: Path
    _configured_calls: dict[str, Any] = field(default_factory=dict, init=False)

    def __getattr__(self, name: str) -> Any:
        return getattr(self.environment, name)

    def wrap_command(self, command: list[str], cwd: str | None = None) -> list[str]:
        root = self.scratch
        root.mkdir(parents=True, exist_ok=True)
        path = root / f"configured-calls-{uuid4().hex}.json"
        data = json.dumps(self.payload, sort_keys=True, separators=(",", ":")).encode()
        with path.open("xb") as stream:
            os.fchmod(stream.fileno(), 0o600)
            stream.write(data)
        self._configured_calls.update(
            {
                "path": str(path.absolute()),
                "sha256": hashlib.sha256(data).hexdigest(),
                "size": len(data),
            }
        )
        return self.environment.wrap_command(command, cwd)

    def prepare_env(self) -> dict[str, str]:
        return self.environment.prepare_env()


@contextmanager
def configured_calls(scope: ConfiguredCallScope) -> Iterator[None]:
    """Bind native phases to the same selection delivered to their commands."""
    token = _current.set(scope)
    try:
        yield
    finally:
        _current.reset(token)


def current_call_scope() -> ConfiguredCallScope | None:
    """Use a bound scope or retrieve one from the authenticated native owner."""
    if scope := _current.get():
        return scope
    reference = configured_call_context()
    if reference is None:
        return None
    if set(reference) != {"path", "sha256", "size"}:
        msg = "Invalid configured call handoff"
        raise ValueError(msg)
    path = Path(reference["path"])
    if not path.is_absolute() or path.stat().st_size != reference["size"]:
        msg = "Configured call handoff is missing or changed"
        raise ValueError(msg)
    data = path.read_bytes()
    if hashlib.sha256(data).hexdigest() != reference["sha256"]:
        msg = "Configured call handoff content changed"
        raise ValueError(msg)
    return ConfiguredCallScope.model_validate_json(data)


def compile_configured_call(
    scope: ConfiguredCallScope,
    definition: Definition,
    key: str,
    params: Mapping[str, object] | BaseModel | None,
) -> CompiledPlan:
    """Validate trusted code and real parameters before invoking authored phases."""
    if not issubclass(definition, OperationDefinition):
        msg = "Configured calls require creator operation classes"
        raise TypeError(msg)
    matches = [item for item in scope.operations if item.contract.key == key]
    if not matches:
        msg = f"Unknown configured operation key {key!r}"
        raise ValueError(msg)
    selected = matches[0]
    captured = capture_called_operations(
        {
            key: CalledOperation(
                definition, json.loads(selected.contract.fixed_params_json)
            )
        }
    )[0]
    if (
        captured.contract != selected.contract
        or environment_key(captured.definition, captured.environment)
        != selected.environment_key
    ):
        msg = "Configured operation definition or software contract changed"
        raise ValueError(msg)
    if Path(sys.prefix).resolve() != scope.caller_prefix.resolve():
        msg = "Configured call is running in a different Python environment"
        raise ValueError(msg)
    verify_runtime_compatibility(scope.caller_compatibility)
    plan = compile_definition(definition, params=request_parameters(captured, params))
    instance = plan.instantiate()
    assert isinstance(instance, OperationDefinition)
    if (
        execution_contract(instance) != selected.contract.execution
        or invocation_policy(instance) != selected.contract.policy
        or instance.environment != captured.environment
    ):
        msg = "Request parameters changed the configured execution contract"
        raise ValueError(msg)
    return plan


def configured_router(scope: ConfiguredCallScope, key: str) -> PreparedExecuteRouter:
    """Reuse the selected native target without any software lookup."""
    selected = next(item for item in scope.operations if item.contract.key == key)
    target = selected.target or scope.caller_target
    child = scope
    if target is not None:
        compatibility = target.compatibility
        if compatibility is not None:
            child = scope.model_copy(
                update={
                    "caller_prefix": target.prefix,
                    "caller_target": target,
                    "caller_compatibility": compatibility,
                }
            )
    observed = None
    if target is None and selected.contract.execution.mode != "native":
        observed = observe_current_worker(
            tuple(item.name for item in scope.caller_compatibility.packages)
        )
    return native_router(
        observed or target,
        pixi=scope.pixi,
        system_env=scope.system_env,
        runtime_env=scope.runtime_env,
        witness=scope.witness,
        gpu=scope.gpu,
        call_context=child.model_dump(mode="json"),
    )
