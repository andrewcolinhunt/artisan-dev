"""Explicit complete local report exports, with passive integrity verification."""

from __future__ import annotations

import re
import stat
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

from fsspec.implementations.local import LocalFileSystem

from artisan import __version__
from artisan.errors import ArtisanError, ErrorCode, StoreIntegrityError
from artisan.inspection._evidence import decode_json_object, encode_json, ensure_fits
from artisan.inspection.export_models import (
    AssessmentCollectionCounts,
    AssessmentMetadata,
    ComparisonCollectionCounts,
    ComparisonMetadata,
    EvidenceExportManifest,
    EvidenceExportReceipt,
    ExportFileIdentity,
)
from artisan.inspection.outcomes import assessment_payload, comparison_payload
from artisan.storage.io.publication import publish_immutable_bytes
from artisan.utils.hashing import compute_content_digest
from artisan.visualization.comparison import RunComparison
from artisan.visualization.outcomes import OutcomeAssessment

type EvidenceReport = OutcomeAssessment | RunComparison
_REPORT_MODELS: dict[str, type[OutcomeAssessment] | type[RunComparison]] = {
    "assessment": OutcomeAssessment,
    "comparison": RunComparison,
}
_BUNDLE_FILES = {"report.json", "schema.json", "manifest.json"}
_RECOVERY = (
    "If a manifest exists, try load_evidence_export to verify the visible bundle. "
    "Inspect or remove your incomplete output explicitly, or choose a new directory."
)


def _export_error(code: str) -> ArtisanError:
    messages = {
        ErrorCode.EVIDENCE_EXPORT_INVALID: "Invalid evidence report or local export destination",
        ErrorCode.EVIDENCE_EXPORT_FORMAT_UNSUPPORTED: "Evidence export format or report version is unsupported",
        ErrorCode.EVIDENCE_EXPORT_INTEGRITY_FAILED: "Evidence export is incomplete or inconsistent",
        ErrorCode.EVIDENCE_EXPORT_FAILED: "Evidence export file I/O failed",
    }
    invalid = code == ErrorCode.EVIDENCE_EXPORT_INVALID
    return ArtisanError(
        code,
        messages[code],
        error_type="validation" if invalid else "io",
        hint=(
            "Supply a supported report and a new local directory with an existing parent."
            if invalid
            else _RECOVERY
        ),
        recovery_hint="CHECK_INPUT",
    )


def _local_path(destination: object) -> Path:
    if not isinstance(destination, (str, Path)) or re.match(
        r"^[A-Za-z][A-Za-z0-9+.-]*:", str(destination)
    ):
        raise _export_error(ErrorCode.EVIDENCE_EXPORT_INVALID)
    try:
        return Path(destination).expanduser()
    except (ValueError, RuntimeError) as exc:
        raise _export_error(ErrorCode.EVIDENCE_EXPORT_INVALID) from exc


def preflight_destination(destination: str | Path) -> Path:
    """Resolve an existing parent and reject every existing destination entry."""
    path = _local_path(destination)
    try:
        parent = path.parent.resolve(strict=True)
        if not parent.is_dir():
            raise _export_error(ErrorCode.EVIDENCE_EXPORT_INVALID)
        path = parent / path.name
        try:
            path.lstat()
        except FileNotFoundError:
            return path
    except (FileNotFoundError, NotADirectoryError, ValueError) as exc:
        raise _export_error(ErrorCode.EVIDENCE_EXPORT_INVALID) from exc
    except OSError as exc:
        raise _export_error(ErrorCode.EVIDENCE_EXPORT_FAILED) from exc
    raise _export_error(ErrorCode.EVIDENCE_EXPORT_INVALID)


def _check_versions(value: dict[str, Any]) -> None:
    for name in ("format_version", "reader_version", "evaluator_version"):
        if name in value and value[name] != "1":
            raise _export_error(ErrorCode.EVIDENCE_EXPORT_FORMAT_UNSUPPORTED)
    if "report_kind" in value and value["report_kind"] not in _REPORT_MODELS:
        raise _export_error(ErrorCode.EVIDENCE_EXPORT_FORMAT_UNSUPPORTED)
    for name in ("report_metadata", "baseline_assessment", "candidate_assessment"):
        if isinstance(value.get(name), dict):
            _check_versions(value[name])


def _validated_report(
    raw: bytes, model: type[OutcomeAssessment] | type[RunComparison]
) -> EvidenceReport:
    value = decode_json_object(raw)
    _check_versions(value)
    report = model.model_validate_json(raw)
    if encode_json(report) != encode_json(value):
        msg = "Report does not survive a complete typed JSON round trip"
        raise ValueError(msg)
    return report


def _metadata(report: EvidenceReport) -> AssessmentMetadata | ComparisonMetadata:
    model = (
        AssessmentMetadata
        if isinstance(report, OutcomeAssessment)
        else ComparisonMetadata
    )
    fields = set(model.model_fields) - {"baseline_assessment", "candidate_assessment"}
    values = report.model_dump(mode="python", include=fields)
    if isinstance(report, RunComparison):
        for side in ("baseline_assessment", "candidate_assessment"):
            nested = getattr(report, side)
            values[side] = _metadata(nested) if nested is not None else None
    return model.model_validate(values)


def _counts(
    report: EvidenceReport,
) -> AssessmentCollectionCounts | ComparisonCollectionCounts:
    if isinstance(report, OutcomeAssessment):
        return AssessmentCollectionCounts(observations=len(report.observations))
    return ComparisonCollectionCounts(
        pairs=len(report.pairs),
        unmatched=len(report.unmatched),
        invalid=len(report.invalid),
        baseline_assessment_observations=(
            len(report.baseline_assessment.observations)
            if report.baseline_assessment is not None
            else None
        ),
        candidate_assessment_observations=(
            len(report.candidate_assessment.observations)
            if report.candidate_assessment is not None
            else None
        ),
    )


def _identity(data: bytes) -> ExportFileIdentity:
    return ExportFileIdentity(digest=compute_content_digest(data), size_bytes=len(data))


def _receipt(
    report: EvidenceReport,
    destination: Path,
    manifest: EvidenceExportManifest,
    raw: bytes,
) -> EvidenceExportReceipt:
    compact = EvidenceExportReceipt(
        **manifest.model_dump(
            include={
                "format_version",
                "report_kind",
                "report_metadata",
                "collection_counts",
                "report_complete",
            }
        ),
        destination=str(destination),
        manifest_path=str(destination / "manifest.json"),
        manifest_digest=compute_content_digest(raw),
        summary=None,
        summary_status="omitted_size_limit",
    )
    ensure_fits(compact.model_dump(mode="python"))
    try:
        summary = (
            assessment_payload(report, "", "summary", 50, None)
            if isinstance(report, OutcomeAssessment)
            else comparison_payload(report, "", "summary", 50, None)
        )
        complete = EvidenceExportReceipt(
            **compact.model_dump(exclude={"summary", "summary_status"}),
            summary=summary,
            summary_status="included",
        )
        ensure_fits(complete.model_dump(mode="python"))
    except ArtisanError as exc:
        if exc.code != ErrorCode.EVIDENCE_RESPONSE_TOO_LARGE:
            raise
        return compact
    return complete


def export_evidence(
    report: EvidenceReport,
    destination: str | Path,
    *,
    expected_evidence_id: str | None = None,
) -> EvidenceExportReceipt:
    """Save one complete captured assessment or comparison without store access.

    Args:
        report: Complete result from the assessment or comparison reader.
        destination: New local directory whose parent already exists.
        expected_evidence_id: Optional previously observed evidence identity.
            Assessment criteria have a separate spec identity.

    Returns:
        A receipt within the shared JSON budget after all files are published.

    Raises:
        ArtisanError: The input, destination or expected identity is invalid,
            mandatory receipt is too large, or file publication fails. A failed
            publication may leave files for explicit recovery; it is never retried.
    """
    path = preflight_destination(destination)
    if type(report) not in _REPORT_MODELS.values() or (
        expected_evidence_id is not None and not isinstance(expected_evidence_id, str)
    ):
        raise _export_error(ErrorCode.EVIDENCE_EXPORT_INVALID)
    try:
        raw = encode_json(report)
        captured = _validated_report(raw, type(report))
    except (TypeError, ValueError) as exc:
        raise _export_error(ErrorCode.EVIDENCE_EXPORT_INVALID) from exc
    if (
        expected_evidence_id is not None
        and captured.evidence_id != expected_evidence_id
    ):
        raise ArtisanError(
            ErrorCode.EVIDENCE_VIEW_CHANGED,
            "Selected evidence differs from the expected export identity",
            error_type="validation",
            hint="Inspect the current report and explicitly choose which evidence to export.",
            recovery_hint="CHECK_INPUT",
        )
    contents: dict[Literal["report.json", "schema.json"], bytes] = {
        "report.json": raw,
        "schema.json": encode_json(
            type(captured).model_json_schema(mode="serialization")
        ),
    }
    manifest = EvidenceExportManifest(
        report_kind="assessment"
        if isinstance(captured, OutcomeAssessment)
        else "comparison",
        artisan_version=__version__,
        exported_at=datetime.now(UTC),
        report_metadata=_metadata(captured),
        collection_counts=_counts(captured),
        files={name: _identity(data) for name, data in contents.items()},
    )
    files: dict[str, bytes] = dict(contents.items())
    files["manifest.json"] = encode_json(manifest)
    receipt = _receipt(captured, path, manifest, files["manifest.json"])
    try:
        path.mkdir()
    except FileExistsError as exc:
        raise _export_error(ErrorCode.EVIDENCE_EXPORT_INVALID) from exc
    except OSError as exc:
        raise _export_error(ErrorCode.EVIDENCE_EXPORT_FAILED) from exc
    try:
        for name, data in files.items():
            publish_immutable_bytes(LocalFileSystem(), str(path / name), data)
        if _read_bundle(path) != files:
            raise _export_error(ErrorCode.EVIDENCE_EXPORT_INTEGRITY_FAILED)
    except (OSError, StoreIntegrityError) as exc:
        raise _export_error(ErrorCode.EVIDENCE_EXPORT_FAILED) from exc
    return receipt


def _read_bundle(path: Path) -> dict[str, bytes]:
    if not stat.S_ISDIR(path.lstat().st_mode):
        raise _export_error(ErrorCode.EVIDENCE_EXPORT_INTEGRITY_FAILED)
    entries = list(path.iterdir())
    if {entry.name for entry in entries} != _BUNDLE_FILES or any(
        not stat.S_ISREG(entry.lstat().st_mode) for entry in entries
    ):
        raise _export_error(ErrorCode.EVIDENCE_EXPORT_INTEGRITY_FAILED)
    return {entry.name: entry.read_bytes() for entry in entries}


def _validate_schema(
    data: bytes, model: type[OutcomeAssessment] | type[RunComparison]
) -> None:
    schema = decode_json_object(data)
    if (
        schema.get("type") != "object"
        or schema.get("title") != model.__name__
        or not isinstance(schema.get("properties"), dict)
        or ("$defs" in schema and not isinstance(schema["$defs"], dict))
    ):
        msg = "Malformed archival report schema"
        raise ValueError(msg)


def load_evidence_export(destination: str | Path) -> EvidenceReport:
    """Verify and load a complete local bundle without reading its source store.

    Args:
        destination: Directory containing exactly the three regular bundle files.

    Returns:
        The complete existing assessment or comparison model.

    Raises:
        ArtisanError: Files cannot be read, format versions are unsupported,
            or the bundle is incomplete, malformed or inconsistent. Verification
            proves current file consistency, not authenticity or prior durability.
    """
    try:
        files = _read_bundle(_local_path(destination))
        value = decode_json_object(files["manifest.json"])
        _check_versions(value)
        manifest = EvidenceExportManifest.model_validate_json(files["manifest.json"])
        # UTC timestamps may use either Z or +00:00 without changing their meaning.
        if encode_json(manifest) != encode_json(
            {**value, "exported_at": manifest.exported_at}
        ):
            msg = "Manifest fields are incomplete"
            raise ValueError(msg)
        for name, identity in manifest.files.items():
            if _identity(files[name]) != identity:
                msg = "Export file bytes disagree with the manifest"
                raise ValueError(msg)
        model = _REPORT_MODELS[manifest.report_kind]
        _validate_schema(files["schema.json"], model)
        report = _validated_report(files["report.json"], model)
        if (
            encode_json(_metadata(report)) != encode_json(manifest.report_metadata)
            or _counts(report) != manifest.collection_counts
        ):
            msg = "Report metadata disagrees with the manifest"
            raise ValueError(msg)
        return report
    except (TypeError, ValueError) as exc:
        raise _export_error(ErrorCode.EVIDENCE_EXPORT_INTEGRITY_FAILED) from exc
    except OSError as exc:
        raise _export_error(ErrorCode.EVIDENCE_EXPORT_FAILED) from exc
