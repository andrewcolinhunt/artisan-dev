"""Artisan command-line interface.

The repo's first console entry point (``[project.scripts]``). One command
so far: ``artisan modal deploy <operation>`` — deploy a tool op's Modal
endpoint.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence


def main(argv: Sequence[str] | None = None) -> int:
    """Entry point for the ``artisan`` console script."""
    parser = _build_parser()
    args = parser.parse_args(argv)
    return _modal_deploy(args.operation)


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
        "operation", help="Registered operation name (e.g. 'echo_tool')"
    )
    return parser


def _modal_deploy(operation: str) -> int:
    """Resolve the op, build its app, and deploy it as a persistent Modal app."""
    from artisan.execution.tool_endpoint.deploy import build_app
    from artisan.operations.base.operation_definition import OperationDefinition
    from artisan.registry.discovery import discover

    discover()
    try:
        op_cls = OperationDefinition.get(operation)
    except KeyError as exc:
        sys.stderr.write(f"{exc.args[0] if exc.args else exc}\n")
        return 1
    app = build_app(op_cls)
    app.deploy()
    sys.stdout.write(f"Deployed artisan-tool-{op_cls.name}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
