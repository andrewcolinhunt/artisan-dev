"""Local adapters for shared inspection, installed guidance and evidence exports."""

from __future__ import annotations

import json
import os
import sys
from contextlib import contextmanager, redirect_stdout
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    import argparse
    from collections.abc import Iterator

    from pydantic import BaseModel


@contextmanager
def _diagnostics_to_stderr() -> Iterator[None]:
    """Keep Python and native application import chatter off machine stdout."""
    sys.stdout.flush()
    saved = os.dup(1)
    try:
        os.dup2(2, 1)
        with redirect_stdout(sys.stderr):
            yield
            sys.stdout.flush()
    finally:
        os.dup2(saved, 1)
        os.close(saved)


def run_command(args: argparse.Namespace) -> int:
    """Emit exactly one validated payload, leaving unexpected defects visible."""
    with _diagnostics_to_stderr():
        from artisan.errors import ArtisanError
        from artisan.inspection._boundary import error_payload
        from artisan.inspection._evidence import encode_json, ensure_fits, json_ready

        status = 0
        try:
            result = _dispatch(args)
            payload = json_ready(result)
            ensure_fits(payload)
        except ArtisanError as exc:
            status = 1
            payload = error_payload(exc)
        encoded = encode_json(payload).decode("utf-8")
    if args.json:
        sys.stdout.write(encoded + "\n")
    elif status:
        sys.stderr.write(json.dumps(payload, ensure_ascii=False, indent=2) + "\n")
    elif args.command == "guide" and args.inspection_command == "show":
        sys.stdout.write(
            f"Artisan {payload['artisan_version']} | {payload['origin']} | "
            f"{payload['digest_algorithm']}:{payload['content_digest']}\n\n"
            + payload["markdown"]
        )
    else:
        sys.stdout.write(json.dumps(payload, ensure_ascii=False, indent=2) + "\n")
    return status


def _dispatch(args: argparse.Namespace) -> BaseModel:
    if args.command == "guide":
        from artisan.inspection import list_operating_guides, read_operating_guide

        if args.inspection_command == "list":
            return list_operating_guides()
        return read_operating_guide(args.topic)

    from artisan.inspection import describe_inspection, list_inspections

    if args.command == "inspect":
        if args.inspection_command == "list":
            return list_inspections()
        if args.inspection_command == "describe":
            return describe_inspection(args.capability)
        return _inspect(args)
    return _export(args)


def _request(path: str | None) -> dict[str, Any]:
    from artisan.inspection._boundary import invalid_input
    from artisan.inspection._evidence import decode_json_object

    if path is None:
        return {}
    try:
        data = sys.stdin.buffer.read() if path == "-" else Path(path).read_bytes()
        return decode_json_object(data)
    except (OSError, ValueError):
        invalid_input("request", "Provide a readable UTF-8 JSON object request")


def _store_context(args: argparse.Namespace) -> str:
    from artisan.inspection._boundary import require_delta_root
    from artisan.registry.discovery import bootstrap_applications
    from artisan.utils.application_modules import resolve_application_modules

    root = require_delta_root(args.delta_root or os.environ.get("ARTISAN_DELTA_ROOT"))
    modules = resolve_application_modules(args.load_module)
    bootstrap_applications(modules, purpose="CLI store access")
    return root


def _inspect(args: argparse.Namespace) -> BaseModel:
    from artisan.inspection import inspect_evidence
    from artisan.inspection.api import validate_request
    from artisan.inspection.catalog import definition

    arguments = _request(args.request)
    validate_request(args.capability, arguments)
    if definition(args.capability).requires_store:
        return inspect_evidence(
            args.capability, arguments, delta_root=_store_context(args)
        )
    from artisan.registry.discovery import discover
    from artisan.utils.application_modules import resolve_application_modules

    discovery = discover(
        extra_modules=list(resolve_application_modules(args.load_module)),
        include_environment=False,
    )
    return inspect_evidence(args.capability, arguments, discovery=discovery)


def _export(args: argparse.Namespace) -> BaseModel:
    from artisan.inspection import export_evidence
    from artisan.inspection._boundary import invalid_input, store_not_found
    from artisan.inspection.api import validate_request
    from artisan.inspection.exports import preflight_destination
    from artisan.inspection.requests import AssessmentRequest, ComparisonRequest
    from artisan.visualization import (
        OutcomeAssessment,
        OutcomeSelection,
        RunComparison,
        assess_outcomes,
        compare_runs,
    )

    arguments = _request(args.request)
    if {"cursor", "limit", "view"}.intersection(arguments):
        invalid_input(
            "request", "Exports require a full report request without page controls"
        )
    request = validate_request(args.capability, arguments)
    destination = preflight_destination(args.output)
    root = _store_context(args)
    report: OutcomeAssessment | RunComparison
    try:
        if args.capability == "assess_outcomes":
            assessment = cast("AssessmentRequest", request)
            selection = OutcomeSelection.model_validate(
                {
                    name: getattr(assessment, name)
                    for name in OutcomeSelection.model_fields
                }
            )
            report = assess_outcomes(root, selection=selection, spec=assessment.spec)
        else:
            comparison = cast("ComparisonRequest", request)
            report = compare_runs(
                root,
                baseline=comparison.baseline,
                candidate=comparison.candidate,
                spec=comparison.spec,
            )
    except FileNotFoundError as exc:
        raise store_not_found() from exc
    return export_evidence(
        report, destination, expected_evidence_id=args.expected_evidence_id
    )
