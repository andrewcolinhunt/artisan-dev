"""Artisan command-line interface.

The repo's console entry point (``[project.scripts]``). Subcommands:

- ``artisan modal deploy <op>`` — deploy a tool op's Modal endpoint.
- ``artisan op image <op>`` — print the container image ref an op runs
  in (``--json`` for the full container view). Modal-free: external
  harnesses resolve "which image runs op X" from the op's config.
- ``artisan op run <module:Qualname>`` — run an ``execute_as_tool``
  op's Python body. The framework-generated command: ``run_command``
  spawns it locally, the endpoint worker spawns it in containers, and
  RL harnesses invoke it directly via ``docker run``.
- ``artisan docker build <op>`` — build the op's image from its
  conventional Dockerfile, tagged with the ref the config declares.
- ``artisan op list`` / ``artisan op describe <op>`` — registry
  discovery for agents and humans (``--json`` for machine output).
- ``artisan runs`` / ``artisan failures`` / ``artisan provenance`` —
  read persisted run history, failure envelopes, and provenance edges
  from a Delta root (``--delta-root`` or ``ARTISAN_DELTA_ROOT``).

Heavy artisan imports are deferred into the command functions so
``--help`` and argument errors stay fast. Under ``--json``, handled
failures serialize as ``ArtisanError.to_dict()`` envelopes on stdout
with exit code 1 — the CLI is a machine-read boundary per the
error-envelope policy.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from artisan.errors import ArtisanError
    from artisan.operations.base.operation_definition import OperationDefinition

_CONTAINER_VIEW_FIELDS = (
    "image",
    "env",
    "volumes",
    "secrets",
    "gpu",
    "cpu",
    "memory_mb",
    "timeout",
)
"""``op image --json`` fields: what running the container anywhere needs.

Deliberately excludes endpoint-deploy concerns (op module/qualname,
params schema, description/input_roles, retries/scaling).
"""


def main(argv: Sequence[str] | None = None) -> int:
    """Entry point for the ``artisan`` console script."""
    parser = _build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args))


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="artisan", description="Artisan pipeline framework CLI"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    modal_parser = sub.add_parser("modal", help="Modal tool-endpoint commands")
    modal_sub = modal_parser.add_subparsers(dest="modal_command", required=True)
    deploy = modal_sub.add_parser(
        "deploy", help="Deploy an operation's tool endpoint to Modal"
    )
    deploy.add_argument(
        "operation", help="Registered operation name (e.g. 'wait_tool')"
    )
    deploy.add_argument(
        "--overlay",
        action="append",
        metavar="PKG",
        help=(
            "Dev mode: mount a local Python package source onto the worker "
            "image, shadowing the baked version (repeatable). Production "
            "deploys bake code into the image instead."
        ),
    )
    deploy.set_defaults(func=_modal_deploy)

    op_parser = sub.add_parser("op", help="Operation commands")
    op_sub = op_parser.add_subparsers(dest="op_command", required=True)

    op_list = op_sub.add_parser("list", help="List registered operations")
    op_list.add_argument(
        "--kind", choices=["creator", "curator"], help="Restrict to one op kind"
    )
    op_list.add_argument(
        "--query", help="Substring match on name or description (case-insensitive)"
    )
    op_list.add_argument("--json", action="store_true", help="Emit JSON")
    op_list.set_defaults(func=_op_list)

    describe = op_sub.add_parser(
        "describe", help="Full metadata for one operation (schemas, examples)"
    )
    describe.add_argument("operation", help="Registered operation name")
    describe.add_argument("--json", action="store_true", help="Emit JSON")
    describe.set_defaults(func=_op_describe)

    image = op_sub.add_parser(
        "image", help="Print the container image ref an operation runs in"
    )
    image.add_argument("operation", help="Registered operation name")
    image.add_argument(
        "--json",
        action="store_true",
        help="Emit the container view (image, env, volumes, secrets, hardware)",
    )
    image.set_defaults(func=_op_image)

    run = op_sub.add_parser(
        "run",
        help=(
            "Run an execute_as_tool op's Python body (the framework-generated "
            "command behind such ops)"
        ),
    )
    run.add_argument(
        "target", help="Op class as module:Qualname (e.g. 'mypkg.ops:EmbedSequences')"
    )
    run.add_argument("--params", default="{}", help="Nested Params model as JSON")
    run.add_argument(
        "--inputs", default="{}", help="Prepared inputs as JSON (role -> path(s))"
    )
    run.add_argument(
        "--execute-dir",
        default=None,
        help="Directory for output files (defaults to the working directory)",
    )
    run.set_defaults(func=_op_run)

    docker_parser = sub.add_parser("docker", help="Container image commands")
    docker_sub = docker_parser.add_subparsers(dest="docker_command", required=True)
    build = docker_sub.add_parser(
        "build",
        help=(
            "Build an operation's image from docker/<image-name>/Dockerfile, "
            "tagged with the ref its config declares (run from the repo root)"
        ),
    )
    build.add_argument("operation", help="Registered operation name")
    build.set_defaults(func=_docker_build)

    runs = sub.add_parser("runs", help="List persisted pipeline runs")
    _add_store_args(runs)
    runs.set_defaults(func=_runs)

    failures = sub.add_parser(
        "failures", help="Failure report — one row per failed execution"
    )
    _add_store_args(failures)
    failures.add_argument("--run", help="Filter to one pipeline run id")
    failures.set_defaults(func=_failures)

    provenance = sub.add_parser(
        "provenance", help="Provenance edges around one artifact"
    )
    provenance.add_argument("artifact_id", help="Artifact to walk from")
    _add_store_args(provenance)
    provenance.add_argument(
        "--direction",
        choices=["backward", "forward"],
        default="backward",
        help="Walk toward ancestors (backward) or descendants (forward)",
    )
    provenance.add_argument(
        "--depth", type=int, default=3, help="Maximum hops from the artifact"
    )
    provenance.set_defaults(func=_provenance)

    return parser


def _add_store_args(parser: argparse.ArgumentParser) -> None:
    """Add the shared flags for commands that read a Delta root."""
    parser.add_argument(
        "--delta-root",
        default=None,
        help="Delta Lake root (falls back to ARTISAN_DELTA_ROOT)",
    )
    parser.add_argument("--json", action="store_true", help="Emit JSON")


def _resolve_op_cls(operation: str) -> type[OperationDefinition] | None:
    """Resolve a registered op class; print the registry error on a miss."""
    from artisan.operations.base.operation_definition import OperationDefinition
    from artisan.registry.discovery import discover

    discover()
    try:
        return OperationDefinition.get(operation)
    except KeyError as exc:
        sys.stderr.write(f"{exc.args[0] if exc.args else exc}\n")
        return None


def _emit(args: argparse.Namespace, payload_fn: Callable[[], Any]) -> int:
    """Run a command body; serialize the result, or the error envelope.

    ``FileNotFoundError`` from store-reading bodies wraps into a
    ``CHECK_INPUT`` envelope: an agent pointing at an empty or wrong
    root is a recoverable input error, not a bug. Other non-Artisan
    exceptions propagate as tracebacks.
    """
    from artisan.errors import ArtisanError, ErrorCode

    try:
        payload = payload_fn()
    except FileNotFoundError as exc:
        err = ArtisanError(
            ErrorCode.STORE_NOT_FOUND,
            str(exc),
            error_type="io",
            hint="no Delta tables at this root — check --delta-root / ARTISAN_DELTA_ROOT",
            recovery_hint="CHECK_INPUT",
        )
        err.__cause__ = exc
        return _emit_error(args, err)
    except ArtisanError as exc:
        return _emit_error(args, exc)
    sys.stdout.write(_render(payload, json_mode=args.json) + "\n")
    return 0


def _emit_error(args: argparse.Namespace, exc: ArtisanError) -> int:
    """Write the failure: envelope JSON on stdout under ``--json``, else stderr."""
    if args.json:
        sys.stdout.write(json.dumps(exc.to_dict()) + "\n")  # machine boundary
    else:
        sys.stderr.write(f"{exc}\n")
    return 1


def _render(payload: Any, *, json_mode: bool) -> str:
    """Serialize a command payload for stdout.

    JSON mode: Pydantic model → its dump; list of models →
    ``{"items": [...]}``; DataFrame → ``{"items": to_dicts()}``
    (``default=str`` covers Datetime columns). Human mode: DataFrames
    render as Polars tables, registry summaries as aligned lines,
    models as indented JSON.
    """
    import polars as pl
    from pydantic import BaseModel

    if json_mode:
        if isinstance(payload, pl.DataFrame):
            data: Any = {"items": payload.to_dicts()}
        elif isinstance(payload, BaseModel):
            data = payload.model_dump()
        else:
            data = {"items": [item.model_dump() for item in payload]}
        return json.dumps(data, default=str)
    if isinstance(payload, pl.DataFrame):
        return str(payload)
    if isinstance(payload, BaseModel):
        return json.dumps(payload.model_dump(), indent=2, default=str)
    lines = [f"{s.name:<32} {s.kind:<8} {s.description}" for s in payload]
    return "\n".join(lines) if lines else "(no operations registered)"


def _require_delta_root(args: argparse.Namespace) -> str:
    """Resolve the Delta root from ``--delta-root`` or the environment."""
    root = args.delta_root or os.environ.get("ARTISAN_DELTA_ROOT")
    if not root:
        from artisan.errors import ArtisanError, ErrorCode

        raise ArtisanError(
            ErrorCode.DELTA_ROOT_UNSET,
            "no Delta root given",
            error_type="config",
            hint="pass --delta-root or set ARTISAN_DELTA_ROOT",
            recovery_hint="CHECK_INPUT",
        )
    return root


def _op_list(args: argparse.Namespace) -> int:
    """List registered operations (``OperationSummary`` rows)."""

    def payload() -> Any:
        from artisan import registry

        registry.discover()
        return registry.list_operations(kind=args.kind, query=args.query)

    return _emit(args, payload)


def _op_describe(args: argparse.Namespace) -> int:
    """Full ``OperationMetadata`` for one op (examples included)."""

    def payload() -> Any:
        from artisan import registry

        registry.discover()
        return registry.describe(args.operation)

    return _emit(args, payload)


def _runs(args: argparse.Namespace) -> int:
    """List persisted pipeline runs from the steps table."""

    def payload() -> Any:
        from artisan.orchestration.run_history import list_runs

        return list_runs(_require_delta_root(args))

    return _emit(args, payload)


def _failures(args: argparse.Namespace) -> int:
    """Failure report with deserialized envelope columns."""

    def payload() -> Any:
        from artisan.visualization.inspect import inspect_failures

        return inspect_failures(
            _require_delta_root(args), pipeline_run_id=args.run
        )

    return _emit(args, payload)


def _provenance(args: argparse.Namespace) -> int:
    """Bounded provenance edge list around one artifact."""

    def payload() -> Any:
        from artisan.provenance import provenance_edges

        return provenance_edges(
            _require_delta_root(args),
            args.artifact_id,
            direction=args.direction,
            depth=args.depth,
        )

    return _emit(args, payload)


def _modal_deploy(args: argparse.Namespace) -> int:
    """Resolve the op, build its app, and deploy it as a persistent Modal app."""
    from artisan.execution.tool_endpoint.deploy import build_app

    op_cls = _resolve_op_cls(args.operation)
    if op_cls is None:
        return 1
    app = build_app(op_cls, overlay=args.overlay)
    app.deploy()
    sys.stdout.write(f"Deployed artisan-tool-{op_cls.name}\n")
    return 0


def _op_image(args: argparse.Namespace) -> int:
    """Print the op's image ref, or the ``--json`` container view."""
    from artisan.execution.tool_endpoint.spec import endpoint_spec

    op_cls = _resolve_op_cls(args.operation)
    if op_cls is None:
        return 1
    try:
        spec = endpoint_spec(op_cls)
    except ValueError as exc:
        sys.stderr.write(f"{exc}\n")
        return 1
    if args.json:
        view = {field: getattr(spec, field) for field in _CONTAINER_VIEW_FIELDS}
        sys.stdout.write(json.dumps(view, indent=2) + "\n")
    else:
        sys.stdout.write(f"{spec.image}\n")
    return 0


def _file_shaped(inputs: dict[str, Any]) -> dict[str, list[str]]:
    """Normalize wire-delivered inputs to the list shape preprocess produced.

    A bare string is a single file (the endpoint protocol's
    one-file-per-role shape, wrapped str → [str]); lists of paths pass
    through. This is what makes ``execute_function``'s view identical
    in-process, under the local shim, and behind the endpoint worker.

    Args:
        inputs: Parsed ``--inputs`` JSON (role → path or paths).

    Returns:
        Role → list-of-paths.

    Raises:
        ValueError: When a value is neither a path nor a list of paths.
    """
    normalized: dict[str, list[str]] = {}
    for role, value in inputs.items():
        if isinstance(value, str):
            normalized[role] = [value]
        elif isinstance(value, list) and all(isinstance(v, str) for v in value):
            normalized[role] = value
        else:
            msg = (
                f"input {role!r} is {type(value).__name__}, not a file path — "
                "execute_as_tool ops receive role -> path(s); scalars belong "
                "in Params"
            )
            raise ValueError(msg)
    return normalized


def _op_run(args: argparse.Namespace) -> int:
    """Run an execute_as_tool op's ``execute_function`` — the recursion leaf of the shim.

    Resolves the class by ``module:Qualname`` (no registry discovery),
    rebuilds the op from the params JSON, and calls the Python body
    directly. The log path points at a throwaway tempfile so nothing
    pollutes output collection; logging that matters goes to stdout,
    which the parent ``run_command`` captures to the unit log.
    """
    from artisan.execution.tool_endpoint.server import instantiate_op, resolve_op
    from artisan.schemas.specs.input_models import ExecuteInput

    module, sep, qualname = args.target.partition(":")
    if not sep or not module or not qualname:
        sys.stderr.write(f"target {args.target!r} is not module:Qualname\n")
        return 1
    try:
        op_cls = resolve_op(module, qualname)
        normalized = _file_shaped(json.loads(args.inputs))
        op = instantiate_op(op_cls, json.loads(args.params))
    except (ImportError, AttributeError, TypeError, ValueError) as exc:
        sys.stderr.write(f"{exc}\n")
        return 1
    fd, log_path = tempfile.mkstemp(prefix="artisan-op-run-", suffix=".log")
    os.close(fd)
    result = op.execute_function(
        ExecuteInput(
            execute_dir=args.execute_dir or os.getcwd(),
            inputs=normalized,
            log_path=log_path,
        )
    )
    if result is not None:
        sys.stderr.write(
            f"{op_cls.__name__}.execute_function returned "
            f"{type(result).__name__} — execute_as_tool ops must write files "
            "to execute_dir and return None\n"
        )
        return 1
    return 0


def _docker_build(args: argparse.Namespace) -> int:
    """Build the op's image from its conventional Dockerfile."""
    from artisan.execution.tool_endpoint.docker import build_image

    op_cls = _resolve_op_cls(args.operation)
    if op_cls is None:
        return 1
    try:
        ref = build_image(op_cls, Path.cwd())
    except (ValueError, FileNotFoundError) as exc:
        sys.stderr.write(f"{exc}\n")
        return 1
    except subprocess.CalledProcessError as exc:
        sys.stderr.write(f"docker build failed with exit code {exc.returncode}\n")
        return 1
    sys.stdout.write(f"{ref}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
