"""Artisan command-line interface.

Expose operation discovery and execution, store inspection, diagnostic boundary
replay, and selected-publication repair. Providers own software preparation.
Use ``artisan --help`` and subcommand help for available commands and flags.

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
import sys
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

from artisan._cli_output import _diagnostics_to_stderr

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from artisan.errors import ArtisanError
    from artisan.operations.base.operation_definition import OperationDefinition
    from artisan.schemas.execution.storage_config import StorageConfig


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

    execution = sub.add_parser("execution", help="Execution diagnostics")
    execution_sub = execution.add_subparsers(dest="execution_command", required=True)
    replay = execution_sub.add_parser(
        "replay", help="Replay one committed execution unit"
    )
    replay.add_argument("execution_run_id")
    replay.add_argument("--operation", required=True, metavar="MODULE:CLASS")
    replay.add_argument(
        "--definition", action="append", default=[], metavar="MODULE:CLASS"
    )
    _add_store_args(replay)
    replay.add_argument("--debug-root", required=True)
    replay.add_argument("--staging-root")
    replay.add_argument("--local-runner", action="store_true")
    replay.add_argument(
        "--supply", action="append", default=[], metavar="POINTER=ENV_NAME"
    )
    replay.add_argument(
        "--runtime",
        action="append",
        default=[],
        metavar="KEY=ENV_NAME",
        help="Supply a captured runtime value from an environment variable",
    )
    replay.add_argument("--allow-code-change", action="store_true")
    replay.set_defaults(func=_execution_replay)

    store_parser = sub.add_parser("store", help="Persisted store commands")
    store_sub = store_parser.add_subparsers(dest="store_command", required=True)
    repair = store_sub.add_parser(
        "repair", help="Inspect store integrity or repair staged commits"
    )
    _add_store_args(repair)
    repair.add_argument("--staging-root", required=True, help="Worker staging root")
    repair.add_argument(
        "--apply",
        action="store_true",
        help="Audit and finish already selected immutable publications",
    )
    repair.add_argument(
        "--preserve-staging",
        action="store_true",
        help="Keep staged files after verified commitment",
    )
    repair.set_defaults(func=_store_repair)

    _add_inspection_commands(sub)
    return parser


def _add_inspection_commands(sub: Any) -> None:
    """Register local evidence interfaces without loading readers."""
    inspect = sub.add_parser("inspect", help="Discover and query bounded evidence")
    commands = inspect.add_subparsers(dest="inspection_command", required=True)
    listing = commands.add_parser("list", help="List installed inspection contracts")
    describe = commands.add_parser("describe", help="Show one inspection's schemas")
    describe.add_argument("capability")
    for command in (listing, describe):
        command.add_argument("--json", action="store_true", help="Emit JSON")
        command.set_defaults(func=_inspection_command)
    run = commands.add_parser("run", help="Run one bounded inspection")
    run.add_argument("capability")
    run.add_argument("--request", metavar="FILE|-", help="JSON object (default: {})")
    _add_store_args(run)
    run.set_defaults(func=_inspection_command)

    guide = sub.add_parser("guide", help="Read installed operating guidance")
    guides = guide.add_subparsers(dest="inspection_command", required=True)
    guide_list = guides.add_parser("list", help="List installed guidance topics")
    show = guides.add_parser("show", help="Read one operating guide topic")
    show.add_argument("guide_id", choices=["pipeline-operate"])
    show.add_argument("--topic", default="workflow")
    for command in (guide_list, show):
        command.add_argument("--json", action="store_true", help="Emit JSON")
        command.set_defaults(func=_inspection_command)

    evidence = sub.add_parser("evidence", help="Save complete evidence reports")
    exports = evidence.add_subparsers(dest="inspection_command", required=True)
    export = exports.add_parser(
        "export", help="Export one complete assessment or comparison"
    )
    export.add_argument("capability", choices=["assess_outcomes", "compare_runs"])
    export.add_argument("--request", required=True, metavar="FILE|-")
    export.add_argument("--output", required=True, metavar="DIRECTORY")
    export.add_argument("--expected-evidence-id")
    _add_store_args(export)
    export.set_defaults(func=_inspection_command)


def _inspection_command(args: argparse.Namespace) -> int:
    """Enter the isolated machine-response boundary only after parsing."""
    from artisan._inspection_cli import run_command

    return run_command(args)


def _add_store_args(parser: argparse.ArgumentParser) -> None:
    """Add shared store-location and JSON-output flags."""
    parser.add_argument(
        "--delta-root",
        default=None,
        help="Delta Lake root (falls back to ARTISAN_DELTA_ROOT)",
    )
    parser.add_argument("--json", action="store_true", help="Emit JSON")
    parser.add_argument("--files-root", help="Configured managed artifact carrier root")
    parser.add_argument("--storage-config", type=Path, help="StorageConfig JSON file")
    parser.add_argument(
        "--load-module",
        action="append",
        default=[],
        metavar="MODULE",
        help="Load an installed trusted application before reading the store (repeatable)",
    )


def _emit(args: argparse.Namespace, payload_fn: Callable[[], Any]) -> int:
    """Run a command body; serialize the result, or the error envelope.

    ``FileNotFoundError`` from store-reading bodies wraps into a
    ``CHECK_INPUT`` envelope: an agent pointing at an empty or wrong
    root is a recoverable input error, not a bug. Other non-Artisan
    exceptions propagate as tracebacks.
    """
    from artisan.errors import ArtisanError, ErrorCode

    try:
        with _diagnostics_to_stderr():
            if hasattr(args, "load_module"):
                from artisan.registry.discovery import bootstrap_applications
                from artisan.utils.application_modules import (
                    resolve_application_modules,
                )

                args.application_modules = resolve_application_modules(args.load_module)
                bootstrap_applications(
                    args.application_modules, purpose="CLI store access"
                )
            payload = payload_fn()
            rendered = _render(payload, json_mode=args.json)
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
    sys.stdout.write(rendered + "\n")
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
            data = payload.model_dump(mode="json")
        else:
            data = {"items": [item.model_dump(mode="json") for item in payload]}
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

        return list_runs(
            _require_delta_root(args),
            files_root=args.files_root,
            storage=_storage_config(args),
        )

    return _emit(args, payload)


def _failures(args: argparse.Namespace) -> int:
    """Failure report with deserialized envelope columns."""

    def payload() -> Any:
        from artisan.visualization.inspect import inspect_failures

        storage = _storage_config(args)
        return inspect_failures(
            _require_delta_root(args),
            pipeline_run_id=args.run,
            files_root=args.files_root,
            fs=storage.filesystem(),
            storage_options=storage.delta_storage_options(),
        )

    return _emit(args, payload)


def _provenance(args: argparse.Namespace) -> int:
    """Bounded provenance edge list around one artifact."""

    def payload() -> Any:
        from artisan.provenance import provenance_edges

        storage = _storage_config(args)
        return provenance_edges(
            _require_delta_root(args),
            args.artifact_id,
            direction=args.direction,
            depth=args.depth,
            files_root=args.files_root,
            storage=storage,
        )

    return _emit(args, payload)


def _execution_replay(args: argparse.Namespace) -> int:
    """Emit one diagnostic boundary result using explicit trusted definitions."""
    from artisan.errors import ArtisanError, ErrorCode
    from artisan.orchestration import replay_execution
    from artisan.orchestration.runners import LocalRunner
    from artisan.registry.resolve import resolve_definition
    from artisan.schemas.orchestration.step_lifecycle import StepStatus

    outcome = None

    def payload() -> Any:
        nonlocal outcome
        storage = _storage_config(args)
        if not storage.is_local and args.files_root is None:
            raise ArtisanError(
                ErrorCode.REPLAY_CONFIGURATION_INVALID,
                "Nonlocal replay requires --files-root",
                error_type="config",
            )
        replacements = _replay_replacements(args.supply, pointers=True)
        runtime = _replay_replacements(args.runtime, pointers=False)
        try:
            operation = resolve_definition(args.operation)
            definitions = tuple(resolve_definition(item) for item in args.definition)
        except (ImportError, AttributeError, TypeError, ValueError):
            raise ArtisanError(
                ErrorCode.REPLAY_DEPENDENCY_UNAVAILABLE,
                "Explicit replay definitions could not be loaded",
                error_type="config",
            ) from None
        outcome = replay_execution(
            args.execution_run_id,
            delta_root=_require_delta_root(args),
            files_root=args.files_root,
            storage=storage,
            operation=operation,
            definitions=definitions,
            application_modules=args.application_modules,
            step_runner=LocalRunner() if args.local_runner else None,
            runtime_replacements=runtime,
            configuration_replacements=replacements,
            allow_code_change=args.allow_code_change,
            working_root=Path(args.debug_root).resolve(),
            staging_root=args.staging_root,
        )
        return outcome

    code = _emit(args, payload)
    if code == 0 and outcome is not None:
        return int(
            outcome.step_result.status != StepStatus.SUCCEEDED
            or outcome.diagnostic_status != "complete"
        )
    return code


def _replay_replacements(values: Sequence[str], *, pointers: bool) -> dict[str, Any]:
    from artisan.errors import ArtisanError, ErrorCode

    replacements = {}
    for supplied in values:
        key, separator, variable = supplied.partition("=")
        if (
            not separator
            or not key
            or not variable
            or key in replacements
            or (pointers and not key.startswith("/"))
        ):
            raise ArtisanError(
                ErrorCode.REPLAY_CONFIGURATION_INVALID,
                "Replacements require unique POINTER=ENV_NAME or KEY=ENV_NAME entries",
                error_type="config",
            )
        if variable not in os.environ:
            raise ArtisanError(
                ErrorCode.REPLAY_REPLACEMENT_REQUIRED,
                "A replacement environment variable is unset",
                error_type="config",
            )
        value = os.environ[variable]
        if pointers:
            try:
                replacements[key] = json.loads(value)
            except ValueError:
                replacements[key] = value
        else:
            replacements[key] = value
    return replacements


def _storage_config(args: argparse.Namespace) -> StorageConfig:
    from fsspec.utils import get_protocol

    from artisan.schemas.execution.storage_config import StorageConfig

    if args.storage_config is not None:
        return StorageConfig.model_validate_json(args.storage_config.read_text())
    protocol = get_protocol(_require_delta_root(args))
    return StorageConfig(protocol="file" if protocol == "local" else protocol)


def _store_repair(args: argparse.Namespace) -> int:
    """Inspect evidence or finish already selected immutable publications."""
    result: dict[str, Any] = {}

    def payload() -> Any:
        from artisan.storage.io.repair import repair_store

        storage = _storage_config(args)
        report = repair_store(
            delta_root=_require_delta_root(args),
            staging_root=args.staging_root,
            fs=storage.filesystem(),
            storage_options=storage.delta_storage_options(),
            apply=args.apply,
            preserve_staging=args.preserve_staging,
            files_root=args.files_root,
        )
        result["report"] = report
        return report

    exit_code = _emit(args, payload)
    report = result.get("report")
    if exit_code == 0 and report is not None and report.unresolved:
        return 1
    return exit_code


def _file_shaped(inputs: dict[str, Any]) -> dict[str, list[str]]:
    """Normalize wire-delivered inputs to the list shape preprocess produced.

    A bare string is a single file (the endpoint protocol's
    one-file-per-role shape, wrapped str → [str]); lists of paths pass
    through. This is what makes ``execute_function``'s view identical
    in-process, under the local command adapter, and behind the endpoint worker.

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
    """Establish inherited command ownership before importing operation code."""
    from artisan.utils.command_supervisor import (
        CommandOwnershipError,
        attachment_sensitive_values,
        managed_invocation,
    )
    from artisan.utils.external_tools import redact_values
    from artisan.utils.traceback import format_error

    sensitive_values = attachment_sensitive_values()
    try:
        with managed_invocation():
            sensitive_values = (*sensitive_values, *attachment_sensitive_values())
            return _op_run_managed(args)
    except CommandOwnershipError:
        sys.stderr.write("Managed command ownership could not be confirmed\n")
        return 1
    except Exception as error:
        sys.stderr.write(redact_values(format_error(error), sensitive_values) + "\n")
        return 1


def _op_run_managed(args: argparse.Namespace) -> int:
    """Run an execute_as_tool op's function through the command adapter.

    Resolves the class by ``module:Qualname`` (no registry discovery),
    rebuilds the op from the params JSON, and calls the Python body
    directly. The log path points at a throwaway tempfile so nothing
    pollutes output collection; logging that matters goes to stdout,
    which the parent ``run_command`` captures to the unit log.
    """
    from artisan.composites.base.snapshots import instantiate_parameters
    from artisan.registry.resolve import resolve_operation
    from artisan.schemas.specs.input_models import ExecuteInput
    from artisan.utils.command_supervisor import (
        attachment_sensitive_values,
        close_command_scope,
    )
    from artisan.utils.external_tools import redact_values

    module, sep, qualname = args.target.partition(":")
    if not sep or not module or not qualname:
        sys.stderr.write(
            redact_values(
                f"target {args.target!r} is not module:Qualname\n",
                attachment_sensitive_values(),
            )
        )
        return 1
    try:
        op_cls = resolve_operation(f"{module}:{qualname}")
        normalized = _file_shaped(json.loads(args.inputs))
        op = cast(
            "OperationDefinition",
            instantiate_parameters(op_cls, json.loads(args.params)),
        )
    except (ImportError, AttributeError, TypeError, ValueError) as exc:
        sys.stderr.write(redact_values(f"{exc}\n", attachment_sensitive_values()))
        return 1
    fd, log_path = tempfile.mkstemp(prefix="artisan-op-run-", suffix=".log")
    os.close(fd)
    try:
        result = op.execute_function(
            ExecuteInput(
                execute_dir=args.execute_dir or os.getcwd(),
                inputs=normalized,
                log_path=log_path,
            )
        )
    except BaseException as error:
        if close_command_scope(error):
            Path(log_path).unlink(missing_ok=True)
        raise
    else:
        close_command_scope()
        Path(log_path).unlink(missing_ok=True)
    if result is not None:
        sys.stderr.write(
            redact_values(
                f"{op_cls.__name__}.execute_function returned "
                f"{type(result).__name__} — execute_as_tool ops must write files "
                "to execute_dir and return None\n",
                attachment_sensitive_values(),
            )
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
