"""Complete local bundles retain exact evidence and fail closed on partial writes."""

from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier
from typing import Any

import pytest
from fixtures.outcome_store import append_metric_attempt

from artisan.errors import ArtisanError, ErrorCode
from artisan.inspection import exports
from artisan.inspection._evidence import MAX_PAYLOAD_BYTES, encode_json, ensure_fits
from artisan.inspection.export_models import EvidenceExportReceipt
from artisan.inspection.outcomes import assessment_payload, comparison_payload
from artisan.schemas import MetricArtifact
from artisan.storage.io import publication
from artisan.utils.hashing import compute_content_digest
from artisan.visualization.comparison import (
    ComparisonMetric,
    ComparisonSpec,
    MetadataKeyMatch,
    compare_runs,
)
from artisan.visualization.outcomes import (
    AssessmentCheck,
    AssessmentSpec,
    OutcomeAssessment,
    OutcomeSelection,
    assess_outcomes,
)


def _metric(value: Any, name: str, key: Any) -> MetricArtifact:
    return MetricArtifact.draft(
        {"score": value}, name, 0, metadata={"case": key}
    ).finalize()


@pytest.fixture(scope="module")
def reports(tmp_path_factory: pytest.TempPathFactory) -> dict[str, Any]:
    root = tmp_path_factory.mktemp("export-source") / "store"
    baseline = append_metric_attempt(
        root,
        [
            _metric(9007199254740993, "exact.json", 1),
            _metric(None, "missing.json", "missing"),
            _metric("bad", "invalid.json", False),
            _metric(0.79999, "unmatched.json", "left"),
        ],
        pipeline_run_id="baseline",
    )
    candidate = append_metric_attempt(
        root,
        [
            _metric(9007199254740995, "exact.json", 1),
            _metric(1.1, "missing.json", "missing"),
            _metric(0.80001, "unmatched.json", "right"),
        ],
        pipeline_run_id="candidate",
    )
    unavailable = append_metric_attempt(
        root, [], pipeline_run_id="failed", status="failed"
    )
    duplicate = append_metric_attempt(
        root,
        [_metric(2, "first.json", 1), _metric(3, "second.json", 1)],
        pipeline_run_id="duplicate",
    )
    spec = AssessmentSpec(
        label="Keep all criterion evidence",
        checks=[
            AssessmentCheck(
                check_id="score", metric_path=["score"], operator="ge", threshold=0.8
            )
        ],
        expected_count=4,
    )
    comparison_spec = ComparisonSpec(
        label="Exact comparison",
        matching=MetadataKeyMatch(key="case"),
        metrics=[
            ComparisonMetric(
                metric_id="score",
                metric_path=["score"],
                direction="higher",
                measurement_id="score",
                measurement_version="1",
                unit="unitless",
            )
        ],
        assessment=spec,
    )
    return {
        "assessment": assess_outcomes(str(root), baseline, spec),
        "unavailable": assess_outcomes(str(root), unavailable, spec),
        "comparison": compare_runs(str(root), baseline, candidate, comparison_spec),
        "ambiguous": compare_runs(str(root), duplicate, candidate, comparison_spec),
        "unavailable_comparison": compare_runs(
            str(root), unavailable, candidate, comparison_spec
        ),
        "changed_spec": assess_outcomes(
            str(root),
            baseline,
            AssessmentSpec(
                label="Different criteria",
                checks=[
                    AssessmentCheck(
                        check_id="score",
                        metric_path=["score"],
                        operator="ge",
                        threshold=0.9,
                    )
                ],
                expected_count=0,
            ),
        ),
    }


@pytest.fixture
def assessment(reports: dict[str, Any]) -> OutcomeAssessment:
    return reports["assessment"].model_copy(deep=True)


def _bundle(tmp_path: Path, report: Any) -> Path:
    path = tmp_path / "export"
    exports.export_evidence(report, path)
    return path


def _rewrite(path: Path, name: str, data: bytes) -> None:
    (path / name).write_bytes(data)
    if name != "manifest.json":
        manifest = json.loads((path / "manifest.json").read_bytes())
        manifest["files"][name]["digest"] = compute_content_digest(data)
        manifest["files"][name]["size_bytes"] = len(data)
        (path / "manifest.json").write_bytes(encode_json(manifest))


@pytest.mark.parametrize(
    "kind",
    ["assessment", "unavailable", "comparison", "ambiguous", "unavailable_comparison"],
)
def test_export_complete_round_trip(
    tmp_path: Path, reports: dict[str, Any], kind: str
) -> None:
    report = reports[kind]
    path = tmp_path / "export"
    receipt = exports.export_evidence(
        report, path, expected_evidence_id=report.evidence_id
    )
    loaded = exports.load_evidence_export(path)
    manifest_bytes = (path / "manifest.json").read_bytes()
    manifest = json.loads(manifest_bytes)
    assert type(loaded) is type(report)
    assert (
        encode_json(loaded)
        == encode_json(report)
        == (path / "report.json").read_bytes()
    )
    assert {item.name for item in path.iterdir()} == {
        "report.json",
        "schema.json",
        "manifest.json",
    }
    assert receipt.manifest_digest == compute_content_digest(manifest_bytes)
    assert receipt.destination == str(path.resolve())
    assert receipt.manifest_path == str(path.resolve() / "manifest.json")
    assert (
        receipt.model_dump(mode="json")["report_metadata"]
        == manifest["report_metadata"]
    )
    assert (
        receipt.model_dump(mode="json")["collection_counts"]
        == manifest["collection_counts"]
    )
    assert receipt.report_complete
    assert manifest["report_complete"]
    assert manifest["capture_consistency"] == "reader_defined"
    projector = (
        assessment_payload
        if isinstance(report, OutcomeAssessment)
        else comparison_payload
    )
    assert receipt.summary == projector(report, "", "summary", 50, None)
    assert receipt.summary["follow_up"] is None
    assert len(encode_json(receipt)) <= MAX_PAYLOAD_BYTES
    if kind == "assessment":
        values = [item.checks[0].value for item in loaded.observations]
        assert 9007199254740993 in values
        assert 0.79999 in values
        assert None in values
    elif kind == "unavailable":
        assert receipt.report_metadata.observation_count is None
        assert receipt.collection_counts.observations == 0
    elif kind == "comparison":
        assert loaded.baseline_assessment.spec == report.spec.assessment
        assert len(loaded.baseline_assessment.observations) == 4
        assert len(loaded.candidate_assessment.observations) == 3
        assert receipt.collection_counts.invalid == 1
        assert receipt.collection_counts.unmatched == 2
    elif kind == "ambiguous":
        assert loaded.comparison_status == "ambiguous"
        assert loaded.coverage.ambiguous_key_count == 1
    elif kind == "unavailable_comparison":
        assert loaded.baseline.observation_count is None
        assert receipt.collection_counts.baseline_assessment_observations == 0


def test_export_absent_nested_assessments_have_null_counts(
    tmp_path: Path, reports: dict[str, Any]
) -> None:
    report = reports["comparison"].model_copy(deep=True)
    report.baseline_assessment = report.candidate_assessment = None
    report.assessment_population = None
    receipt = exports.export_evidence(report, tmp_path / "export")
    assert receipt.collection_counts.baseline_assessment_observations is None
    assert receipt.report_metadata.candidate_assessment is None
    assert exports.load_evidence_export(tmp_path / "export") == report


def test_export_oversized_report_is_complete(
    tmp_path: Path, assessment: OutcomeAssessment
) -> None:
    assessment.observations[0].original_name = 'é"\\' * MAX_PAYLOAD_BYTES
    assert len(encode_json(assessment)) > MAX_PAYLOAD_BYTES
    receipt = exports.export_evidence(assessment, tmp_path / "export")
    assert receipt.summary_status == "included"
    assert len(encode_json(receipt)) <= MAX_PAYLOAD_BYTES
    assert exports.load_evidence_export(tmp_path / "export") == assessment


def test_export_same_evidence_different_criteria(
    tmp_path: Path, reports: dict[str, Any]
) -> None:
    original, changed = reports["assessment"], reports["changed_spec"]
    assert original.evidence_id == changed.evidence_id
    assert original.spec_id != changed.spec_id
    receipt = exports.export_evidence(
        changed, tmp_path / "export", expected_evidence_id=original.evidence_id
    )
    assert receipt.report_metadata.spec_id == changed.spec_id
    assert receipt.report_metadata.evidence_id == original.evidence_id


def test_export_expected_identity_mismatch_precedes_writes(
    tmp_path: Path, assessment: OutcomeAssessment
) -> None:
    with pytest.raises(ArtisanError) as caught:
        exports.export_evidence(
            assessment, tmp_path / "export", expected_evidence_id="old"
        )
    assert caught.value.code == ErrorCode.EVIDENCE_VIEW_CHANGED
    assert "page" not in caught.value.envelope.hint
    assert not (tmp_path / "export").exists()


def test_export_mandatory_receipt_overflow_precedes_writes(
    tmp_path: Path, assessment: OutcomeAssessment
) -> None:
    assessment.reason = "x" * MAX_PAYLOAD_BYTES
    with pytest.raises(ArtisanError) as caught:
        exports.export_evidence(assessment, tmp_path / "export")
    assert caught.value.code == ErrorCode.EVIDENCE_RESPONSE_TOO_LARGE
    assert not (tmp_path / "export").exists()


def test_export_summary_projection_overflow_is_omitted(
    tmp_path: Path, reports: dict[str, Any]
) -> None:
    report = reports["comparison"].model_copy(deep=True)
    report.metrics[0].reason = "x" * MAX_PAYLOAD_BYTES
    receipt = exports.export_evidence(report, tmp_path / "export")
    assert receipt.summary is None
    assert receipt.summary_status == "omitted_size_limit"
    assert exports.load_evidence_export(tmp_path / "export") == report


def test_export_combined_receipt_overflow_is_omitted(
    tmp_path: Path, assessment: OutcomeAssessment
) -> None:
    assessment.reason = "x" * (MAX_PAYLOAD_BYTES // 2)
    assert (
        len(encode_json(assessment_payload(assessment, "", "summary", 50, None)))
        < MAX_PAYLOAD_BYTES
    )
    receipt = exports.export_evidence(assessment, tmp_path / "export")
    assert receipt.summary is None
    assert receipt.summary_status == "omitted_size_limit"
    assert exports.load_evidence_export(tmp_path / "export") == assessment


@pytest.mark.parametrize("excess", [0, 1])
def test_export_receipt_exact_json_byte_boundary(
    tmp_path: Path,
    assessment: OutcomeAssessment,
    monkeypatch: pytest.MonkeyPatch,
    excess: int,
) -> None:
    def oversized(*args: Any) -> dict[str, Any]:
        return ensure_fits({"value": "x" * MAX_PAYLOAD_BYTES})

    monkeypatch.setattr(exports, "assessment_payload", oversized)
    compact = exports.export_evidence(assessment, tmp_path / "probe")
    fields = compact.model_dump(exclude={"summary", "summary_status"})
    fields.update(
        destination=str(tmp_path / "trial"),
        manifest_path=str(tmp_path / "trial" / "manifest.json"),
    )
    empty = EvidenceExportReceipt(
        **fields, summary={"value": ""}, summary_status="included"
    )
    remaining = MAX_PAYLOAD_BYTES - len(encode_json(empty)) + excess
    value = 'é"\\' * (remaining // 10) + "x" * (remaining % 10)
    summary = {"value": value}
    monkeypatch.setattr(exports, "assessment_payload", lambda *args: summary)
    receipt = exports.export_evidence(assessment, tmp_path / "trial")
    if excess:
        assert receipt.summary_status == "omitted_size_limit"
        assert receipt.summary is None
    else:
        assert receipt.summary == summary
        assert len(encode_json(receipt)) == MAX_PAYLOAD_BYTES


@pytest.mark.parametrize(
    "failure",
    [
        ValueError("projection defect"),
        ArtisanError(
            ErrorCode.EVIDENCE_ITEM_TOO_LARGE, "wrong failure", error_type="validation"
        ),
    ],
)
def test_export_projection_defects_are_not_suppressed(
    tmp_path: Path,
    assessment: OutcomeAssessment,
    monkeypatch: pytest.MonkeyPatch,
    failure: Exception,
) -> None:
    def fail(*args: Any) -> dict[str, Any]:
        raise failure

    monkeypatch.setattr(exports, "assessment_payload", fail)
    with pytest.raises(type(failure)) as caught:
        exports.export_evidence(assessment, tmp_path / "export")
    assert caught.value is failure
    assert not (tmp_path / "export").exists()


def test_export_uses_captured_report_and_never_reads_store(
    tmp_path: Path, assessment: OutcomeAssessment, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = assessment.model_copy(deep=True)

    def mutate_caller(report: OutcomeAssessment, *args: Any) -> dict[str, Any]:
        assessment.reason = "caller changed the source object"
        assessment.observations.clear()
        return assessment_payload(report, *args)

    def no_reader(*args: Any, **kwargs: Any) -> None:
        pytest.fail("Export or load attempted to query source evidence")

    monkeypatch.setattr(exports, "assessment_payload", mutate_caller)
    monkeypatch.setattr("artisan.visualization.outcomes.assess_outcomes", no_reader)
    monkeypatch.setattr(
        "artisan.visualization.outcomes.read_metric_observations", no_reader
    )
    monkeypatch.setattr(
        "artisan.visualization.outcomes._evaluate_observations", no_reader
    )
    monkeypatch.setattr("artisan.visualization.comparison.compare_runs", no_reader)
    receipt = exports.export_evidence(assessment, tmp_path / "export")
    assert exports.load_evidence_export(tmp_path / "export") == original
    assert receipt.collection_counts.observations == len(original.observations)
    assert receipt.report_metadata.reason == original.reason


@pytest.mark.parametrize(
    "kind", ["file", "directory", "live_symlink", "dangling_symlink"]
)
def test_export_rejects_every_existing_destination(
    tmp_path: Path, assessment: OutcomeAssessment, kind: str
) -> None:
    path = tmp_path / "export"
    if kind == "file":
        path.write_bytes(b"owned")
    elif kind == "directory":
        path.mkdir()
        (path / "owned").write_bytes(b"owned")
    else:
        target = tmp_path / "target"
        if kind == "live_symlink":
            target.mkdir()
        path.symlink_to(target)
    with pytest.raises(ArtisanError) as caught:
        exports.export_evidence(assessment, path)
    assert caught.value.code == ErrorCode.EVIDENCE_EXPORT_INVALID
    assert path.is_symlink() if "symlink" in kind else path.exists()


@pytest.mark.parametrize(
    "destination",
    [
        "missing/export",
        "file:///tmp/export",
        "s3://bucket/export",
        "https://example.test/export",
    ],
)
def test_export_rejects_invalid_destination_before_writes(
    tmp_path: Path,
    assessment: OutcomeAssessment,
    monkeypatch: pytest.MonkeyPatch,
    destination: str,
) -> None:
    monkeypatch.chdir(tmp_path)
    with pytest.raises(ArtisanError) as caught:
        exports.export_evidence(assessment, destination)
    assert caught.value.code == ErrorCode.EVIDENCE_EXPORT_INVALID
    assert not list(tmp_path.iterdir())


def test_export_resolves_only_parent_symlink(
    tmp_path: Path, assessment: OutcomeAssessment
) -> None:
    parent = tmp_path / "actual"
    parent.mkdir()
    link = tmp_path / "linked"
    link.symlink_to(parent)
    receipt = exports.export_evidence(assessment, link / "export")
    assert receipt.destination == str(parent / "export")


@pytest.mark.parametrize("report", [None, {}, "report"])
def test_export_rejects_unsupported_report(tmp_path: Path, report: Any) -> None:
    with pytest.raises(ArtisanError) as caught:
        exports.export_evidence(report, tmp_path / "export")
    assert caught.value.code == ErrorCode.EVIDENCE_EXPORT_INVALID
    assert not (tmp_path / "export").exists()


def test_export_two_writers_reserve_exclusively(
    tmp_path: Path, assessment: OutcomeAssessment, monkeypatch: pytest.MonkeyPatch
) -> None:
    barrier = Barrier(2)
    build_receipt = exports._receipt

    def synchronize(*args: Any) -> EvidenceExportReceipt:
        receipt = build_receipt(*args)
        barrier.wait(timeout=10)
        return receipt

    def write() -> EvidenceExportReceipt | ArtisanError:
        try:
            return exports.export_evidence(assessment, tmp_path / "export")
        except ArtisanError as exc:
            return exc

    monkeypatch.setattr(exports, "_receipt", synchronize)
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: write(), range(2)))
    assert sum(isinstance(result, EvidenceExportReceipt) for result in results) == 1
    assert [result.code for result in results if isinstance(result, ArtisanError)] == [
        ErrorCode.EVIDENCE_EXPORT_INVALID
    ]
    assert exports.load_evidence_export(tmp_path / "export") == assessment


@pytest.mark.parametrize("failed_file", ["report.json", "schema.json", "manifest.json"])
def test_export_publication_failure_leaves_owned_files(
    tmp_path: Path,
    assessment: OutcomeAssessment,
    monkeypatch: pytest.MonkeyPatch,
    failed_file: str,
) -> None:
    publish = exports.publish_immutable_bytes
    calls = []

    def fail(fs: Any, path: str, data: bytes) -> None:
        calls.append(Path(path).name)
        if Path(path).name == failed_file:
            msg = "private destination /secret and credentials"
            raise OSError(msg)
        publish(fs, path, data)

    monkeypatch.setattr(exports, "publish_immutable_bytes", fail)
    with pytest.raises(ArtisanError) as caught:
        exports.export_evidence(assessment, tmp_path / "export")
    assert caught.value.code == ErrorCode.EVIDENCE_EXPORT_FAILED
    assert "secret" not in str(caught.value)
    assert calls == ["report.json", "schema.json", "manifest.json"][: len(calls)]
    assert calls[-1] == failed_file
    assert (tmp_path / "export").is_dir()
    assert not (tmp_path / "export" / "manifest.json").exists()
    with pytest.raises(ArtisanError) as load_error:
        exports.load_evidence_export(tmp_path / "export")
    assert load_error.value.code == ErrorCode.EVIDENCE_EXPORT_INTEGRITY_FAILED


def test_export_post_manifest_link_flush_failure_remains_failure(
    tmp_path: Path, assessment: OutcomeAssessment, monkeypatch: pytest.MonkeyPatch
) -> None:
    directory = tmp_path / "export"
    sync = publication._sync_directory_chain

    def fail_after_manifest(path: str) -> None:
        if (directory / "manifest.json").exists():
            msg = "directory flush failed"
            raise OSError(msg)
        sync(path)

    monkeypatch.setattr(publication, "_sync_directory_chain", fail_after_manifest)
    with pytest.raises(ArtisanError) as caught:
        exports.export_evidence(assessment, directory)
    assert caught.value.code == ErrorCode.EVIDENCE_EXPORT_FAILED
    assert (directory / "manifest.json").exists()
    assert exports.load_evidence_export(directory) == assessment
    with pytest.raises(ArtisanError) as retry:
        exports.export_evidence(assessment, directory)
    assert retry.value.code == ErrorCode.EVIDENCE_EXPORT_INVALID


@pytest.mark.parametrize(
    "change",
    [
        "missing_manifest",
        "missing_report",
        "extra_file",
        "nested_directory",
        "temporary",
        "symlink_file",
        "symlink_root",
    ],
)
def test_load_requires_exact_regular_bundle_files(
    tmp_path: Path, assessment: OutcomeAssessment, change: str
) -> None:
    path = _bundle(tmp_path, assessment)
    if change.startswith("missing"):
        (
            path / ("manifest.json" if change == "missing_manifest" else "report.json")
        ).unlink()
    elif change == "extra_file":
        (path / "other").write_bytes(b"extra")
    elif change == "nested_directory":
        (path / "nested").mkdir()
    elif change == "temporary":
        (path / ("report.json.tmp-" + "a" * 32)).write_bytes(b"temporary")
    elif change == "symlink_file":
        target = tmp_path / "report.json"
        (path / "report.json").rename(target)
        (path / "report.json").symlink_to(target)
    else:
        alias = tmp_path / "alias"
        alias.symlink_to(path)
        path = alias
    with pytest.raises(ArtisanError) as caught:
        exports.load_evidence_export(path)
    assert caught.value.code == ErrorCode.EVIDENCE_EXPORT_INTEGRITY_FAILED


@pytest.mark.parametrize("name", ["report.json", "schema.json"])
def test_load_rejects_changed_file_bytes(
    tmp_path: Path, assessment: OutcomeAssessment, name: str
) -> None:
    path = _bundle(tmp_path, assessment)
    with (path / name).open("ab") as stream:
        stream.write(b" ")
    with pytest.raises(ArtisanError) as caught:
        exports.load_evidence_export(path)
    assert caught.value.code == ErrorCode.EVIDENCE_EXPORT_INTEGRITY_FAILED


@pytest.mark.parametrize("name", ["manifest.json", "report.json", "schema.json"])
@pytest.mark.parametrize(
    "data",
    [b'{"x":1,"x":2}', b'{"x":NaN}', b'{"x":Infinity}', b'{"x":1e999}', b"[]", b"\xff"],
)
def test_load_rejects_non_strict_json(
    tmp_path: Path, assessment: OutcomeAssessment, name: str, data: bytes
) -> None:
    path = _bundle(tmp_path, assessment)
    _rewrite(path, name, data)
    with pytest.raises(ArtisanError) as caught:
        exports.load_evidence_export(path)
    assert caught.value.code == ErrorCode.EVIDENCE_EXPORT_INTEGRITY_FAILED


@pytest.mark.parametrize(
    ("field", "value"),
    [("type", "array"), ("title", "OtherReport"), ("properties", []), ("$defs", [])],
)
def test_load_rejects_malformed_archival_schema(
    tmp_path: Path, assessment: OutcomeAssessment, field: str, value: Any
) -> None:
    path = _bundle(tmp_path, assessment)
    schema = json.loads((path / "schema.json").read_bytes())
    schema[field] = value
    _rewrite(path, "schema.json", encode_json(schema))
    with pytest.raises(ArtisanError) as caught:
        exports.load_evidence_export(path)
    assert caught.value.code == ErrorCode.EVIDENCE_EXPORT_INTEGRITY_FAILED


def test_load_accepts_harmless_schema_and_package_version_differences(
    tmp_path: Path, assessment: OutcomeAssessment
) -> None:
    path = _bundle(tmp_path, assessment)
    schema = json.loads((path / "schema.json").read_bytes())
    schema["description"] = "Generated by another compatible schema generator"
    _rewrite(path, "schema.json", encode_json(schema))
    manifest = json.loads((path / "manifest.json").read_bytes())
    manifest["artisan_version"] = "another-package-version"
    _rewrite(path, "manifest.json", encode_json(manifest))
    assert exports.load_evidence_export(path) == assessment


@pytest.mark.parametrize(
    "change",
    [
        "evidence_id",
        "spec_id",
        "selection",
        "counts",
        "nested_id",
        "nested_selection",
        "nested_count",
        "coverage",
        "report_complete",
        "extra_field",
        "file_map",
    ],
)
def test_load_rejects_manifest_disagreement(
    tmp_path: Path, reports: dict[str, Any], change: str
) -> None:
    path = _bundle(tmp_path, reports["comparison"])
    manifest = json.loads((path / "manifest.json").read_bytes())
    metadata = manifest["report_metadata"]
    if change in {"evidence_id", "spec_id"}:
        metadata[change] = "changed"
    elif change == "selection":
        metadata["baseline"]["selection"]["step_run_id"] = "a" * 32
    elif change == "counts":
        manifest["collection_counts"]["pairs"] += 1
    elif change == "nested_id":
        metadata["baseline_assessment"]["evidence_id"] = "changed"
    elif change == "nested_selection":
        metadata["baseline_assessment"]["selection"]["step_run_id"] = "a" * 32
    elif change == "nested_count":
        manifest["collection_counts"]["baseline_assessment_observations"] += 1
    elif change == "coverage":
        metadata["coverage"]["matched_pair_count"] += 1
    elif change == "report_complete":
        manifest["report_complete"] = False
    elif change == "extra_field":
        manifest["unexpected"] = "value"
    else:
        manifest["files"]["../external.json"] = manifest["files"].pop("report.json")
    _rewrite(path, "manifest.json", encode_json(manifest))
    with pytest.raises(ArtisanError) as caught:
        exports.load_evidence_export(path)
    assert caught.value.code == ErrorCode.EVIDENCE_EXPORT_INTEGRITY_FAILED


@pytest.mark.parametrize(
    "change",
    [
        "format_version",
        "report_kind",
        "reader_version",
        "evaluator_version",
        "report_version",
        "nested_version",
    ],
)
def test_load_rejects_unsupported_versions(
    tmp_path: Path, reports: dict[str, Any], change: str
) -> None:
    path = _bundle(tmp_path, reports["comparison"])
    name = (
        "report.json"
        if change in {"report_version", "nested_version"}
        else "manifest.json"
    )
    value = json.loads((path / name).read_bytes())
    if change in {"format_version", "report_kind"}:
        value[change] = "unsupported"
    elif change in {"reader_version", "evaluator_version"}:
        value["report_metadata"][change] = "unsupported"
    elif change == "report_version":
        value["reader_version"] = "unsupported"
    else:
        value["baseline_assessment"]["evaluator_version"] = "unsupported"
    _rewrite(path, name, encode_json(value))
    with pytest.raises(ArtisanError) as caught:
        exports.load_evidence_export(path)
    assert caught.value.code == ErrorCode.EVIDENCE_EXPORT_FORMAT_UNSUPPORTED


@pytest.mark.parametrize(
    "change", ["missing_default", "extra_field", "wrong_enum", "coerced_count"]
)
def test_load_rejects_lossy_or_invalid_report(
    tmp_path: Path, assessment: OutcomeAssessment, change: str
) -> None:
    path = _bundle(tmp_path, assessment)
    value = json.loads((path / "report.json").read_bytes())
    if change == "missing_default":
        value.pop("reason")
    elif change == "extra_field":
        value["other"] = 1
    elif change == "wrong_enum":
        value["execution_status"] = "invented"
    else:
        value["observation_count"] = "4"
    _rewrite(path, "report.json", encode_json(value))
    with pytest.raises(ArtisanError) as caught:
        exports.load_evidence_export(path)
    assert caught.value.code == ErrorCode.EVIDENCE_EXPORT_INTEGRITY_FAILED


def test_load_file_io_failure_is_sanitized(
    tmp_path: Path, assessment: OutcomeAssessment, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = _bundle(tmp_path, assessment)

    def fail(path: Path) -> bytes:
        msg = "private secret"
        raise PermissionError(msg)

    monkeypatch.setattr(Path, "read_bytes", fail)
    with pytest.raises(ArtisanError) as caught:
        exports.load_evidence_export(path)
    assert caught.value.code == ErrorCode.EVIDENCE_EXPORT_FAILED
    assert "secret" not in str(caught.value)


def test_load_reads_each_verified_file_once(
    tmp_path: Path, assessment: OutcomeAssessment, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = _bundle(tmp_path, assessment)
    read = Path.read_bytes
    calls = []

    def record(file: Path) -> bytes:
        calls.append(file.name)
        return read(file)

    monkeypatch.setattr(Path, "read_bytes", record)
    assert exports.load_evidence_export(path) == assessment
    assert sorted(calls) == ["manifest.json", "report.json", "schema.json"]


def test_load_accepts_equivalent_utc_timestamp(
    tmp_path: Path, assessment: OutcomeAssessment
) -> None:
    path = _bundle(tmp_path, assessment)
    manifest = json.loads((path / "manifest.json").read_bytes())
    manifest["exported_at"] = manifest["exported_at"].replace("+00:00", "Z")
    _rewrite(path, "manifest.json", encode_json(manifest))
    assert exports.load_evidence_export(path) == assessment


def test_export_rejects_nonfinite_report_before_writes(
    tmp_path: Path, assessment: OutcomeAssessment
) -> None:
    assessment.checks[0].coverage = float("nan")
    with pytest.raises(ArtisanError) as caught:
        exports.export_evidence(assessment, tmp_path / "export")
    assert caught.value.code == ErrorCode.EVIDENCE_EXPORT_INVALID
    assert not (tmp_path / "export").exists()


def test_export_verifies_published_files_before_returning_receipt(
    tmp_path: Path, assessment: OutcomeAssessment, monkeypatch: pytest.MonkeyPatch
) -> None:
    publish = exports.publish_immutable_bytes

    def corrupt(fs: Any, path: str, data: bytes) -> None:
        publish(fs, path, data)
        if Path(path).name == "manifest.json":
            (Path(path).parent / "report.json").write_bytes(b"changed")

    monkeypatch.setattr(exports, "publish_immutable_bytes", corrupt)
    with pytest.raises(ArtisanError) as caught:
        exports.export_evidence(assessment, tmp_path / "export")
    assert caught.value.code == ErrorCode.EVIDENCE_EXPORT_INTEGRITY_FAILED


@pytest.mark.parametrize("kind", ["assessment", "comparison"])
def test_cli_export_reads_complete_report_once(
    tmp_path: Path,
    reports: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
    capfd: pytest.CaptureFixture[str],
    kind: str,
) -> None:
    from artisan.cli import main

    report = reports[kind]
    capability = "assess_outcomes" if kind == "assessment" else "compare_runs"
    query = {"spec": report.spec.model_dump(mode="json")}
    if kind == "assessment":
        query.update(
            report.selection.model_dump(
                mode="json", include=set(OutcomeSelection.model_fields)
            )
        )
    else:
        query.update(
            {
                side: getattr(report, side).selection.model_dump(
                    mode="json", include=set(OutcomeSelection.model_fields)
                )
                for side in ("baseline", "candidate")
            }
        )
    request = tmp_path / "request.json"
    request.write_bytes(encode_json(query))
    calls = []

    def reader(root: str, **kwargs: Any) -> Any:
        calls.append((root, kwargs))
        print("incidental reader diagnostic")
        return report

    def no_dispatcher(*args: Any, **kwargs: Any) -> None:
        pytest.fail("Export called the bounded live dispatcher")

    monkeypatch.setattr("artisan._inspection_cli._store_context", lambda args: "source")
    monkeypatch.setattr(f"artisan.visualization.{capability}", reader)
    monkeypatch.setattr("artisan.inspection.inspect_evidence", no_dispatcher)
    assert (
        main(
            [
                "evidence",
                "export",
                capability,
                "--request",
                str(request),
                "--output",
                str(tmp_path / "export"),
                "--json",
            ]
        )
        == 0
    )
    output = capfd.readouterr()
    receipt = EvidenceExportReceipt.model_validate_json(output.out)
    assert output.out == encode_json(receipt).decode() + "\n"
    assert "incidental reader diagnostic" in output.err
    assert len(calls) == 1
    assert calls[0][0] == "source"
    assert calls[0][1]["spec"] == report.spec
    assert exports.load_evidence_export(receipt.destination) == report


def test_cli_export_preflights_destination_before_source_access(
    tmp_path: Path,
    assessment: OutcomeAssessment,
    monkeypatch: pytest.MonkeyPatch,
    capfd: pytest.CaptureFixture[str],
) -> None:
    from artisan.cli import main

    request = tmp_path / "request.json"
    request.write_bytes(
        encode_json(
            {
                **assessment.selection.model_dump(
                    mode="json", include=set(OutcomeSelection.model_fields)
                ),
                "spec": assessment.spec.model_dump(mode="json"),
            }
        )
    )
    output = tmp_path / "existing"
    output.mkdir()

    def no_source(*args: Any, **kwargs: Any) -> None:
        pytest.fail(
            "Existing destination triggered application bootstrap or source access"
        )

    monkeypatch.setattr("artisan._inspection_cli._store_context", no_source)
    assert (
        main(
            [
                "evidence",
                "export",
                "assess_outcomes",
                "--request",
                str(request),
                "--output",
                str(output),
                "--json",
            ]
        )
        == 1
    )
    payload = json.loads(capfd.readouterr().out)
    assert payload["code"] == ErrorCode.EVIDENCE_EXPORT_INVALID
