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

Heavy artisan imports are deferred into the command functions so
``--help`` and argument errors stay fast.
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
    from collections.abc import Sequence

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

    return parser


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
