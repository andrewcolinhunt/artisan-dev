"""Private exact-container lifecycle, driven by the native owner's event loop."""

from __future__ import annotations

import json
import os
import re
import time
from typing import Any

from artisan.utils._command_supervisor import CLEANUP_SECONDS, TERM_SECONDS, Command

_TERMINAL = {"closed", "unknown", "launch_failed"}


class DockerCommand(Command):
    """Retain the container before admission and fence delayed starts on removal."""

    def __init__(
        self,
        record: Command,
        argv: list[str],
        create: list[str],
        env: dict[str, str],
        cwd: str | None,
        fds: list[int],
    ) -> None:
        super().__init__(
            record.request_id,
            record.client,
            scope=record.scope,
            parent=record.parent,
            client_channel=record.client_channel,
        )
        self.state = "running"
        self.stage = "resolve"
        self.container_id: str | None = None
        self.jobs: dict[str, Command] = {}
        self.readers: dict[str, int] = {}
        self.output: dict[str, bytearray] = {}
        self.fds = [os.dup(fd) for fd in fds]
        self.env = env
        self.cwd = cwd
        self.create_argv = create
        self.fallback_argv = argv
        self.prefix: list[str] = []
        self.result: int | None = None
        self.removed = False
        self.stopped = False
        self._reserved: dict[int, str] = {}

    def begin(self) -> None:
        """Launch only after the owner has recorded this container obligation."""
        if self.env.get("DOCKER_HOST") and not self.env.get("DOCKER_CONTEXT"):
            self._resolved(self.env["DOCKER_HOST"])
        else:
            context = (
                [self.env["DOCKER_CONTEXT"]] if self.env.get("DOCKER_CONTEXT") else []
            )
            self._job(
                "resolve",
                [
                    "docker",
                    "context",
                    "inspect",
                    *context,
                    "--format",
                    "{{json .Endpoints.docker}}",
                ],
            )

    def owned_commands(self) -> tuple[Command, ...]:
        return tuple(self.jobs.values())

    def _job(self, name: str, argv: list[str], *, foreground: bool = False) -> None:
        """Reuse native identity reservation, observation and reaping for every CLI."""
        child = Command(f"{self.request_id}:{name}", self.client)
        self.jobs[name] = child
        with open(os.devnull, "r+b") as null:
            descriptors = [null.fileno()] * 3
            if foreground:
                descriptors = self.fds
            else:
                reader, writer = os.pipe()
                os.set_blocking(reader, False)
                self.readers[name] = reader
                self.output[name] = bytearray()
                descriptors = [null.fileno(), writer, null.fileno()]
                if name == "create":
                    descriptors[2] = self.fds[2]
            try:
                child.launch(argv, self.env, self.cwd, descriptors)
                if child.process is not None and not child.reaped:
                    self._reserved[child.pid] = child.request_id
            finally:
                if not foreground:
                    os.close(writer)
        if foreground:
            self.process = child.process
            self.launch_seconds = child.launch_seconds
            self._release_fds()

    def _resolved(self, endpoint: str) -> None:
        if not endpoint.startswith("unix://"):
            self.external = True
            self.stage = "fallback"
            self._job("fallback", self.fallback_argv, foreground=True)
            self.create_argv = self.fallback_argv = []
            return
        self.prefix = ["docker", "--host", endpoint]
        self.env = dict(self.env)
        self.env.pop("DOCKER_CONTEXT", None)
        self.env.pop("DOCKER_HOST", None)
        self.stage = "create"
        self._job("create", [*self.prefix, *self.create_argv[1:]])
        self.create_argv = self.fallback_argv = []

    def _release_fds(self) -> None:
        for descriptor in self.fds:
            os.close(descriptor)
        self.fds.clear()

    def _drain(self) -> None:
        for name, descriptor in list(self.readers.items()):
            try:
                data = os.read(descriptor, 65536)
            except BlockingIOError:
                continue
            if not data:
                os.close(descriptor)
                del self.readers[name]
            elif len(self.output[name]) + len(data) > 65536:
                self._unknown()
                return
            else:
                self.output[name].extend(data)

    def _finished(self, name: str) -> bool:
        return self.jobs[name].state in _TERMINAL and name not in self.readers

    def _successful(self, name: str) -> bool:
        child = self.jobs[name]
        return (
            child.state == "closed"
            and child.process is not None
            and child.process.returncode == 0
        )

    def stop(
        self,
        deadline: float,
        *,
        immediate: bool = False,
        accepted_at: float | None = None,
    ) -> None:
        if self.state in _TERMINAL:
            return
        accepted_at = time.monotonic() if accepted_at is None else accepted_at
        self.deadline = min(self.deadline or deadline, deadline)
        self.kill_at = min(
            self.kill_at or deadline,
            accepted_at + (0 if immediate else TERM_SECONDS),
            deadline,
        )
        self.state = "stopping"
        self.stopped = True
        if self.stage == "fallback":
            self.jobs["fallback"].stop(
                self.deadline, immediate=immediate, accepted_at=accepted_at
            )

    def _unknown(self) -> None:
        self.state = "unknown"
        deadline = self.deadline or time.monotonic()
        for child in self.jobs.values():
            child.stop(deadline, immediate=True)
            child.tick(self._reserved)
        self._release_fds()
        for descriptor in self.readers.values():
            os.close(descriptor)
        self.readers.clear()

    def _resolve_done(self) -> None:
        if not self._successful("resolve"):
            self._unknown()
            return
        try:
            endpoint = json.loads(self.output["resolve"])["Host"]
            if not isinstance(endpoint, str):
                message = "Invalid Docker endpoint"
                raise ValueError(message)
        except (ValueError, KeyError, TypeError):
            self._unknown()
            return
        if self.stopped:
            self.state = "closed"
            self._release_fds()
        else:
            self._resolved(endpoint)

    def _create_done(self) -> None:
        identity = (
            bytes(self.output["create"]).strip().decode("ascii", errors="replace")
        )
        if (
            not self._successful("create")
            or re.fullmatch(r"[0-9a-f]{64}", identity) is None
        ):
            self._unknown()
            return
        self.container_id = identity
        self.stage = "execute"
        if not self.stopped:
            self._job(
                "start", [*self.prefix, "start", "--attach", identity], foreground=True
            )
        else:
            self._release_fds()

    def _remove(self, *, force: bool) -> None:
        if "remove" not in self.jobs:
            assert self.container_id is not None
            self._job(
                "remove",
                [
                    *self.prefix,
                    "rm",
                    *(["--force"] if force else []),
                    "--volumes",
                    self.container_id,
                ],
            )

    def _cancel_tick(self, now: float) -> None:
        assert self.container_id is not None
        if "term" not in self.jobs and "remove" not in self.jobs:
            self._job(
                "term", [*self.prefix, "kill", "--signal", "TERM", self.container_id]
            )
        if self.kill_at is not None and now >= self.kill_at:
            self._remove(force=True)

    def _natural_tick(self) -> None:
        if not self._finished("start"):
            return
        start = self.jobs["start"]
        if start.state != "closed" or start.process is None:
            self.observation_error = True
            self.stop(time.monotonic() + CLEANUP_SECONDS, immediate=True)
            return
        self.result = start.process.returncode
        self.deadline = self.deadline or time.monotonic() + CLEANUP_SECONDS
        if "inspect" not in self.jobs:
            assert self.container_id is not None
            self._job(
                "inspect",
                [
                    *self.prefix,
                    "inspect",
                    "--format",
                    "{{json .State}}",
                    self.container_id,
                ],
            )
        elif self._finished("inspect"):
            try:
                state = json.loads(self.output["inspect"])
                terminal = (
                    self._successful("inspect")
                    and state["Status"] in {"exited", "dead"}
                    and state["Running"] is False
                    and state["Restarting"] is False
                    and state["ExitCode"] == self.result
                )
            except (ValueError, KeyError, TypeError):
                terminal = False
            if terminal:
                self._remove(force=False)
            else:
                self.observation_error |= self.result == 0
                self.stop(self.deadline, immediate=True)

    def _removal_tick(self) -> None:
        if "remove" not in self.jobs or not self._finished("remove"):
            return
        if not self._successful("remove"):
            self._unknown()
            return
        # Moby fences start with RemovalInProgress/Dead and waits for termination
        # before removal succeeds. A missing container alone is never this proof.
        self.removed = True
        for child in self.jobs.values():
            child.stop(
                self.deadline or time.monotonic() + CLEANUP_SECONDS, immediate=True
            )
        if all(
            child.state in {"closed", "launch_failed"} for child in self.jobs.values()
        ):
            self.state = "unknown" if self.observation_error else "closed"
            self._release_fds()

    def tick(self, reserved: dict[int, str]) -> None:
        self._reserved = reserved
        for child in self.jobs.values():
            child.tick(reserved)
        self._drain()
        if self.state in _TERMINAL:
            return
        now = time.monotonic()
        if self.deadline is not None and now >= self.deadline:
            self._unknown()
            return
        if self.stage == "resolve" and self._finished("resolve"):
            self._resolve_done()
        elif self.stage == "create" and self._finished("create"):
            self._create_done()
        elif self.stage == "fallback":
            child = self.jobs["fallback"]
            self.state = child.state
            self.reaped = child.reaped
            self.deadline = child.deadline
        elif self.stage == "execute":
            if self.stopped:
                self._cancel_tick(now)
            else:
                self._natural_tick()
            self._removal_tick()

    def snapshot(self) -> dict[str, Any]:
        snapshot = super().snapshot()
        if self.result is not None:
            snapshot["returncode"] = self.result
        return snapshot
