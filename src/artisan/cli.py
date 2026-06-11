"""Artisan command-line interface.

The repo's console entry point (``[project.scripts]``). Subcommands:

- ``artisan modal deploy <op>`` — deploy a tool op's Modal endpoint.
- ``artisan op image <op>`` — print the container image ref an op runs
  in (``--json`` for the full container view). Modal-free: external
  harnesses resolve "which image runs op X" from the op's config.
- ``artisan docker build <op>`` — build the op's image from its
  conventional Dockerfile, tagged with the ref the config declares.

Heavy artisan imports are deferred into the command functions so
``--help`` and argument errors stay fast.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import TYPE_CHECKING

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
    deploy.set_defaults(func=_modal_deploy)

    op_parser = sub.add_parser("op", help="Operation inspection commands")
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
    app = build_app(op_cls)
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
