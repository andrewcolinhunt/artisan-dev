"""Delivery sidecars are bounded exact diagnostics, separate from result authority."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from fixtures.retained_readers import Backend
from fixtures.retained_results import retained_case

from artisan.errors import StoreIntegrityError
from artisan.execution.recording import delivery_diagnostics as recording
from artisan.schemas.execution.assignment import DeliveryIdentity
from artisan.schemas.execution.delivery_diagnostics import DeliveryDiagnostics
from artisan.storage.core.store_format import initialize_retained_store
from artisan.utils.hashing import canonical_json_bytes


def _diagnostic(tmp_path: Path) -> DeliveryDiagnostics:
    return DeliveryDiagnostics(
        timestamp_start=datetime(2024, 1, 1, tzinfo=UTC),
        delivery=retained_case(tmp_path, kind="empty").completion().delivery,
        seal_digest="a" * 32,
        outcome="success",
        cleanup="closed",
        duration_seconds=0.1,
        invocations=(),
        omitted_invocations=0,
        phase=None,
        call_path=(),
        error=None,
    )


def test_delivery_sidecar_requires_exact_owner_and_seal(
    backend_fs: Backend, tmp_path: Path
) -> None:
    fs, _, root = backend_fs
    root = f"{root}/diagnostic-store"
    initialize_retained_store(root, fs)
    diagnostic = _diagnostic(tmp_path)
    assert (
        recording.read_delivery_diagnostics(diagnostic.delivery, delta_root=root, fs=fs)
        is None
    )
    recording.publish_delivery_diagnostics(diagnostic, delta_root=root, fs=fs)
    assert (
        recording.read_delivery_diagnostics(
            diagnostic.delivery,
            delta_root=root,
            fs=fs,
            expected_seal_digest=diagnostic.seal_digest,
        )
        == diagnostic
    )
    # Publishing observations creates neither acceptance nor persistent child nodes.
    files = fs.find(root)
    assert len(files) == 2
    assert any(path.endswith("execution.json") for path in files)
    assert any(path.endswith("store.json") for path in files)
    other = DeliveryIdentity.new(diagnostic.delivery.attempt)
    assert recording.read_delivery_diagnostics(other, delta_root=root, fs=fs) is None
    with pytest.raises(StoreIntegrityError, match="exact owner"):
        recording.read_delivery_diagnostics(
            diagnostic.delivery, delta_root=root, fs=fs, expected_seal_digest="b" * 32
        )


@pytest.mark.parametrize(
    "mutation", ["owner", "encoding", "schema", "bool_version", "field", "size"]
)
def test_delivery_sidecar_rejects_changed_or_malformed_evidence(
    backend_fs: Backend,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
) -> None:
    fs, _, root = backend_fs
    root = f"{root}/diagnostic-store"
    initialize_retained_store(root, fs)
    diagnostic = _diagnostic(tmp_path)
    recording.publish_delivery_diagnostics(diagnostic, delta_root=root, fs=fs)
    value = diagnostic.model_dump(mode="json")
    if mutation == "owner":
        value["delivery"] = DeliveryIdentity.new(
            diagnostic.delivery.attempt
        ).model_dump(mode="json")
    elif mutation == "schema":
        value["schema_version"] = 2
    elif mutation == "bool_version":
        value["schema_version"] = True
    elif mutation == "field":
        value["surprise"] = True
    encoded = canonical_json_bytes(value)
    if mutation == "encoding":
        encoded = json.dumps(value, indent=2).encode()
    if mutation == "size":
        monkeypatch.setattr(recording, "MAX_DIAGNOSTIC_BYTES", len(encoded) - 1)
    with fs.open(recording._path(root, diagnostic.delivery), "wb") as stream:
        stream.write(encoded)
    with pytest.raises(StoreIntegrityError):
        recording.read_delivery_diagnostics(diagnostic.delivery, delta_root=root, fs=fs)


def test_oversized_delivery_diagnostics_are_not_published(
    backend_fs: Backend, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fs, _, root = backend_fs
    root = f"{root}/diagnostic-store"
    initialize_retained_store(root, fs)
    diagnostic = _diagnostic(tmp_path)
    monkeypatch.setattr(recording, "MAX_DIAGNOSTIC_BYTES", 1)
    with pytest.raises(ValueError, match="byte limit"):
        recording.publish_delivery_diagnostics(diagnostic, delta_root=root, fs=fs)
    assert not fs.exists(recording._path(root, diagnostic.delivery))


@pytest.mark.parametrize(
    ("outcome", "cleanup", "expected"),
    [
        ("science", "closed", True),
        ("science", "unknown", False),
        ("infrastructure", "closed", False),
        ("ownership", "unknown", False),
    ],
)
def test_only_closed_science_materializes_bounded_failure_log(
    tmp_path: Path,
    outcome: str,
    cleanup: str,
    expected: bool,
) -> None:
    from fsspec.implementations.local import LocalFileSystem

    from artisan.utils.log_paths import failure_log_relative_path

    fs = LocalFileSystem()
    delta = str(tmp_path / "store")
    initialize_retained_store(delta, fs)
    diagnostic = DeliveryDiagnostics.model_validate(
        {
            **_diagnostic(tmp_path).model_dump(),
            "seal_digest": None,
            "outcome": outcome,
            "cleanup": cleanup,
            "error": "sanitized scientific error",
            "phase": "execute",
        }
    )
    recording.publish_delivery_diagnostics(diagnostic, delta_root=delta, fs=fs)
    path = (
        tmp_path
        / "logs"
        / "failures"
        / failure_log_relative_path(
            diagnostic.delivery.delivery_id, diagnostic.timestamp_start
        )
    )
    assert path.exists() is expected
    if expected:
        assert "sanitized scientific error" in path.read_text()
        assert path.stat().st_size <= recording.MAX_DIAGNOSTIC_BYTES
        recording.publish_delivery_diagnostics(diagnostic, delta_root=delta, fs=fs)


def test_failure_log_write_error_keeps_original_diagnostic(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fsspec.implementations.local import LocalFileSystem

    fs = LocalFileSystem()
    delta = str(tmp_path / "store")
    initialize_retained_store(delta, fs)
    diagnostic = DeliveryDiagnostics.model_validate(
        {
            **_diagnostic(tmp_path).model_dump(),
            "seal_digest": None,
            "outcome": "science",
            "error": "scientific error",
            "phase": "execute",
        }
    )
    publish = recording.publish_immutable_bytes

    def fail_log(fs, path: str, content: bytes) -> None:
        if path.endswith(".log"):
            message = "unavailable optional log"
            raise OSError(message)
        publish(fs, path, content)

    monkeypatch.setattr(recording, "publish_immutable_bytes", fail_log)
    with pytest.raises(OSError, match="optional log"):
        recording.publish_delivery_diagnostics(diagnostic, delta_root=delta, fs=fs)
    assert (
        recording.read_delivery_diagnostics(
            diagnostic.delivery,
            delta_root=delta,
            fs=fs,
        )
        == diagnostic
    )


def test_failure_log_cap_preserves_utf8(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    diagnostic = DeliveryDiagnostics.model_validate(
        {
            **_diagnostic(tmp_path).model_dump(),
            "seal_digest": None,
            "outcome": "science",
            "error": "💥" * 1000,
            "phase": "execute",
        }
    )
    monkeypatch.setattr(recording, "MAX_DIAGNOSTIC_BYTES", 301)
    content = recording._failure_log(diagnostic)
    assert len(content) <= 301
    assert content.decode("utf-8").endswith("[truncated]")
