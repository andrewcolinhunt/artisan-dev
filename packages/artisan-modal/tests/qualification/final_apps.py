"""Qualification-only ID observation before pinned SDK image/deployment work."""

from __future__ import annotations

import asyncio
import contextlib
import functools
import importlib.metadata
import inspect
import json
import os
import re
import threading
from collections.abc import Iterator
from contextvars import ContextVar
from pathlib import Path
from typing import Any

_REGISTRY: ContextVar[Path | None] = ContextVar("qualification_apps", default=None)

APP_ID = re.compile(r"ap-[A-Za-z0-9]+")
_SIGNATURES = {
    "_init_local_app_new": (
        "client",
        "description",
        "tags",
        "app_state",
        "environment_name",
        "interactive",
    ),
    "_init_local_app_from_name": ("client", "name", "tags", "environment_name"),
}


def write_json(path: Path, value: object) -> None:
    """Publish only bounded primitive evidence; a failed write is never ignored."""
    encoded = json.dumps(value, indent=2, sort_keys=True).encode() + b"\n"
    if len(encoded) > 1024 * 1024:
        message = "Qualification checkpoint exceeds one MiB"
        raise ValueError(message)
    temporary = path.with_suffix(path.suffix + ".writing")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(descriptor, "wb") as stream:
        stream.write(encoded)
        stream.flush()
        os.fsync(stream.fileno())
    temporary.replace(path)


def check_sdk_hooks() -> None:
    from modal import runner

    if importlib.metadata.version("modal") != "1.4.1":
        message = "Qualification App observation requires the pinned Modal SDK"
        raise ValueError(message)
    for name, names in _SIGNATURES.items():
        function = getattr(runner, name)
        parameters = inspect.signature(function).parameters
        if (
            not inspect.iscoroutinefunction(function)
            or tuple(parameters) != names
            or any(
                value.kind != inspect.Parameter.POSITIONAL_OR_KEYWORD
                for value in parameters.values()
            )
        ):
            message = "Pinned Modal App creation signature changed"
            raise ValueError(message)


@contextlib.contextmanager
def observe_apps(path: Path) -> Iterator[None]:
    """Record exact returned IDs; never inspect or persist SDK requests or clients."""
    from modal import runner

    check_sdk_hooks()
    if path.exists():
        message = "Preserve existing App ownership evidence"
        raise FileExistsError(message)
    state: dict[str, Any] = {"app_ids": [], "pending_creations": 0}
    guard = threading.Lock()
    write_json(path, state)
    originals = {name: getattr(runner, name) for name in _SIGNATURES}

    def tracked(original: Any) -> Any:
        @functools.wraps(original)
        async def initialize(*args: Any, **kwargs: Any) -> Any:
            with guard:
                if len(state["app_ids"]) + state["pending_creations"] >= 4:
                    message = "Qualification reached its four-App creation bound"
                    raise ValueError(message)
                state["pending_creations"] += 1
                write_json(path, state)
            value = await original(*args, **kwargs)
            app_id = value.app_id
            if not isinstance(app_id, str) or APP_ID.fullmatch(app_id) is None:
                message = "SDK returned an invalid App identity"
                raise ValueError(message)
            with guard:
                if app_id not in state["app_ids"]:
                    state["app_ids"].append(app_id)
                state["pending_creations"] -= 1
                write_json(path, state)
            return value

        return initialize

    token = _REGISTRY.set(path)
    try:
        for name, original in originals.items():
            setattr(runner, name, tracked(original))
        yield
    finally:
        for name, original in originals.items():
            setattr(runner, name, original)
        _REGISTRY.reset(token)


def recorded_apps() -> Path:
    """Return the active observed registry; absent ownership never authorizes RPCs."""
    path = _REGISTRY.get()
    if path is None:
        message = "App cleanup requires an active recorded ownership scope"
        raise ValueError(message)
    return path


def app_ids(path: Path) -> tuple[str, ...]:
    if not path.exists():
        return ()
    if path.stat().st_size > 1024 * 1024:
        message = "Recorded App inventory exceeds one MiB"
        raise ValueError(message)
    value = json.loads(path.read_bytes())
    identifiers = value["app_ids"]
    if (
        not isinstance(identifiers, list)
        or len(identifiers) > 4
        or any(
            not isinstance(item, str) or APP_ID.fullmatch(item) is None
            for item in identifiers
        )
        or len(set(identifiers)) != len(identifiers)
    ):
        message = "Invalid recorded qualification App inventory"
        raise ValueError(message)
    return tuple(identifiers)


async def app_control(
    registry: Path, action: str, app_id: str
) -> list[dict[str, str | int]]:
    """Use bounded pinned SDK RPCs for one recorded App, without importing its CLI."""
    if action not in {"list", "stop"} or app_id not in app_ids(registry):
        message = "App control requires a fixed action and an exact recorded identity"
        raise ValueError(message)
    check_sdk_hooks()
    from modal._object import _get_environment_name
    from modal.client import _Client
    from modal.environments import ensure_env
    from modal_proto import api_pb2

    async with asyncio.timeout(8):
        client = await _Client.from_env()
        if action == "stop":
            await client.stub.AppStop(
                api_pb2.AppStopRequest(
                    app_id=app_id, source=api_pb2.APP_STOP_SOURCE_CLI
                ),
                timeout=5,
                retry=None,
            )
            return []
        response = await client.stub.AppList(
            api_pb2.AppListRequest(
                environment_name=_get_environment_name(ensure_env(None)) or ""
            ),
            timeout=5,
            retry=None,
        )
        return [
            {
                "App ID": row.app_id,
                "State": "stopped"
                if row.state == api_pb2.APP_STATE_STOPPED
                else "other",
                "Tasks": row.n_running_tasks,
            }
            for row in response.apps
            if row.app_id == app_id
        ]
