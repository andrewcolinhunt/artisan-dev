"""Worker compatibility checks metadata while transport retains payload integrity."""

from __future__ import annotations

import csv
import io
import json
import sys
from importlib.metadata import PathDistribution
from pathlib import Path
from typing import IO, Any

import pytest

from artisan.errors import ApplicationRegistrationError
from artisan.execution.runtime_compatibility import (
    capture_distribution_metadata,
    capture_runtime_compatibility,
    select_runtime_compatibility,
    verify_runtime_compatibility,
)
from artisan.execution.worker_bootstrap import (
    PreparedProcessCall,
    execute_worker_call,
    serialize_worker_call,
)
from artisan.schemas.execution.worker_transport import (
    TRANSPORT_PACKAGES,
    DistributionMetadata,
    WorkerTransportHeader,
)


def _distribution(
    root: Path,
    name: str,
    package: str,
    *,
    editable: bool = False,
    version: str = "1.0",
) -> PathDistribution:
    info = root / f"{name.replace('-', '_')}-{version}.dist-info"
    info.mkdir(parents=True)
    (info / "METADATA").write_text(f"Name: {name}\nVersion: {version}\n")
    module = root / package / "__init__.py"
    module.parent.mkdir()
    module.write_text("raise AssertionError('package imported during measurement')\n")
    record = io.StringIO()
    writer = csv.writer(record)
    writer.writerow([str(module.relative_to(root)), "", ""])
    writer.writerow([str((info / "METADATA").relative_to(root)), "", ""])
    writer.writerow([str((info / "RECORD").relative_to(root)), "", ""])
    (info / "RECORD").write_text(record.getvalue())
    if editable:
        (info / "direct_url.json").write_text(
            json.dumps({"dir_info": {"editable": True}, "url": "file:///source"})
        )
    return PathDistribution(info)


@pytest.fixture
def installed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    root = tmp_path / "lib" / "python3.12" / "site-packages"
    distributions = {
        "dexterity-artisan": _distribution(root, "dexterity-artisan", "artisan"),
        "cloudpickle": _distribution(root, "cloudpickle", "cloudpickle"),
        "pydantic": _distribution(root, "pydantic", "pydantic"),
        "pydantic-core": _distribution(root, "pydantic-core", "pydantic_core"),
        "example": _distribution(root, "example", "example"),
    }
    monkeypatch.setattr(
        "artisan.execution.runtime_compatibility.importlib.metadata.distribution",
        distributions.__getitem__,
    )
    monkeypatch.setattr(
        "artisan.execution.runtime_compatibility.importlib.metadata.distributions",
        lambda: tuple(distributions.values()),
    )
    return root


@pytest.fixture
def metadata_paths(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> tuple[Path, Path]:
    primary = tmp_path / "primary"
    fallback = tmp_path / "fallback"
    fallback.mkdir()
    for name in TRANSPORT_PACKAGES:
        _distribution(primary, name, name.replace("-", "_"))
    monkeypatch.setattr(sys, "path", [str(primary), str(fallback)])
    return primary, fallback


@pytest.mark.parametrize("reverse", [False, True])
def test_all_metadata_uses_named_resolution_across_search_paths(
    metadata_paths: tuple[Path, Path], reverse: bool
) -> None:
    primary, fallback = metadata_paths
    _distribution(primary, "Example_Package", "example_package", version="2.0")
    _distribution(fallback, "example-package", "example_package", version="1.0")
    _distribution(fallback, "fallback-only", "fallback_only")
    if reverse:
        sys.path.reverse()

    compatibility = capture_runtime_compatibility()

    assert compatibility == capture_runtime_compatibility(
        [*TRANSPORT_PACKAGES, "example-package", "fallback-only"]
    )
    assert capture_distribution_metadata("example-package").version == (
        "1.0" if reverse else "2.0"
    )
    verify_runtime_compatibility(compatibility)


def test_all_metadata_uses_named_resolution_without_overlapping_packages(
    metadata_paths: tuple[Path, Path],
) -> None:
    primary, fallback = metadata_paths
    _distribution(primary, "example", "example")
    _distribution(fallback, "fallback-only", "fallback_only")

    assert capture_runtime_compatibility() == capture_runtime_compatibility(
        [*TRANSPORT_PACKAGES, "example", "fallback-only"]
    )


@pytest.mark.parametrize("root_index", [0, 1])
def test_all_metadata_rejects_conflicting_versions_in_one_installation(
    metadata_paths: tuple[Path, Path], root_index: int
) -> None:
    _distribution(metadata_paths[0], "example", "example")
    root = metadata_paths[root_index]
    _distribution(root, "example", "example_second", version="2.0")
    if root_index:
        _distribution(root, "example", "example_third", version="3.0")

    with pytest.raises(ValueError, match="conflicting versions"):
        capture_runtime_compatibility()


def test_all_metadata_accepts_repeated_installation_discovery(
    metadata_paths: tuple[Path, Path], tmp_path: Path
) -> None:
    primary, _ = metadata_paths
    alias = tmp_path / "alias"
    alias.symlink_to(primary, target_is_directory=True)
    sys.path.extend([str(primary), str(alias)])

    assert capture_runtime_compatibility() == capture_runtime_compatibility(
        TRANSPORT_PACKAGES
    )


@pytest.mark.parametrize(
    ("metadata", "expected_errors"),
    [
        ("Version: 1.0\n", (KeyError, ValueError)),
        ("Name: invalid/name\nVersion: 1.0\n", (ValueError,)),
        ("Name: example\n", (KeyError, ValueError)),
    ],
)
@pytest.mark.parametrize("root_index", [0, 1])
@pytest.mark.filterwarnings(
    "ignore:Implicit None on return values is deprecated:DeprecationWarning"
)
def test_all_metadata_rejects_malformed_visible_metadata(
    metadata_paths: tuple[Path, Path],
    metadata: str,
    expected_errors: tuple[type[Exception], ...],
    root_index: int,
) -> None:
    primary, fallback = metadata_paths
    _distribution(primary, "example", "example")
    _distribution(fallback, "example", "example")
    (metadata_paths[root_index] / "example-1.0.dist-info/METADATA").write_text(metadata)

    # Missing fields raise KeyError with the metadata backport, or fail validation.
    with pytest.raises(expected_errors):
        capture_runtime_compatibility()


def test_all_metadata_rejects_missing_required_package(
    metadata_paths: tuple[Path, Path],
) -> None:
    primary, _ = metadata_paths
    (primary / "cloudpickle-1.0.dist-info").rename(primary / "unrecognized")

    with pytest.raises(ValueError, match="requires packages"):
        capture_runtime_compatibility()


def test_metadata_capture_and_verification_never_read_installed_payloads(
    installed: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    original_open = Path.open

    def metadata_only(path: Path, *args: Any, **kwargs: Any) -> IO[Any]:
        if path.is_relative_to(installed) and path.suffix in {".py", ".pyc"}:
            pytest.fail("Compatibility read installed package payload")
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", metadata_only)
    monkeypatch.setattr(
        Path,
        "rglob",
        lambda *args: pytest.fail("Compatibility traversed package files"),
    )
    compatibility = capture_runtime_compatibility()
    assert {package.name for package in compatibility.packages} == {
        "example",
        *TRANSPORT_PACKAGES,
    }
    verify_runtime_compatibility(compatibility)
    assert capture_distribution_metadata("example") == DistributionMetadata(
        name="example", version="1.0"
    )


def test_installed_byte_changes_are_outside_metadata_contract(installed: Path) -> None:
    compatibility = capture_runtime_compatibility()
    (installed / "example/__init__.py").write_text("CHANGED = True\n")
    (installed / "example/extra.py").write_text("ADDED = True\n")
    verify_runtime_compatibility(compatibility)
    assert capture_runtime_compatibility() == compatibility


def test_all_metadata_supports_later_selection_without_recapture(
    installed: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    available = capture_runtime_compatibility()
    monkeypatch.setattr(
        "artisan.execution.runtime_compatibility.importlib.metadata.distributions",
        lambda: pytest.fail("Selection enumerated installed packages again"),
    )
    selected = select_runtime_compatibility(available, TRANSPORT_PACKAGES)
    assert "example" not in {package.name for package in selected.packages}
    assert (
        select_runtime_compatibility(available, (*TRANSPORT_PACKAGES, "example"))
        == available
    )
    with pytest.raises(ValueError, match="lacks required packages"):
        select_runtime_compatibility(available, (*TRANSPORT_PACKAGES, "missing"))


def test_metadata_is_normalized_and_immutable(installed: Path) -> None:
    package = DistributionMetadata(name="Example_Package", version="1.0")
    assert package.name == "example-package"
    with pytest.raises(ValueError, match="frozen"):
        package.version = "2.0"
    with pytest.raises(ValueError, match="Extra inputs"):
        DistributionMetadata.model_validate(
            {"name": "example", "version": "1.0", "files": []}
        )


@pytest.mark.parametrize("change", ["python", "package-version", "payload"])
def test_worker_rejects_mismatch_before_deserialization(
    installed: Path, monkeypatch: pytest.MonkeyPatch, change: str
) -> None:
    compatibility = capture_runtime_compatibility(["example", *TRANSPORT_PACKAGES])
    call = serialize_worker_call(
        lambda value: value + 2, compatibility=compatibility, args=(5,)
    )
    if change == "python":
        bad_runtime = compatibility.model_copy(update={"python_version": "0.0.0"})
        call = PreparedProcessCall(
            call.header.model_copy(update={"compatibility": bad_runtime}), call.payload
        )
    elif change == "package-version":
        metadata = installed / "example-1.0.dist-info" / "METADATA"
        metadata.write_text("Name: example\nVersion: 2.0\n")
    else:
        call = PreparedProcessCall(call.header, call.payload + b"changed")

    def forbidden(_payload: bytes) -> None:
        pytest.fail("Mismatched worker reached live-object deserialization")

    monkeypatch.setattr("cloudpickle.loads", forbidden)
    with pytest.raises(ValueError, match="Worker|worker"):
        execute_worker_call(call)


def test_qualified_worker_executes_trusted_call_and_closed_header(
    installed: Path,
) -> None:
    compatibility = capture_runtime_compatibility(TRANSPORT_PACKAGES)
    verify_runtime_compatibility(compatibility)
    call = serialize_worker_call(
        lambda value, *, scale: value * scale,
        compatibility=compatibility,
        args=(3,),
        kwargs={"scale": 4},
    )
    parsed = WorkerTransportHeader.model_validate_json(call.header.model_dump_json())
    assert parsed == call.header
    assert execute_worker_call(PreparedProcessCall(parsed, call.payload)) == 12
    with pytest.raises(ValueError, match="Extra inputs"):
        WorkerTransportHeader.model_validate(
            {**call.header.model_dump(), "module": "unsafe"}
        )
    with pytest.raises(ValueError, match="schema version must be an integer"):
        WorkerTransportHeader.model_validate(
            {**call.header.model_dump(), "schema_version": True}
        )


def test_runtime_requires_unique_serializer_and_framework_packages(
    installed: Path,
) -> None:
    with pytest.raises(ValueError, match="Duplicate"):
        capture_runtime_compatibility(
            ["dexterity-artisan", "dexterity_artisan", "cloudpickle"]
        )
    with pytest.raises(ValueError, match="requires"):
        capture_runtime_compatibility(["example"])


def test_controller_rejects_version_mismatch_before_serialization(
    installed: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    compatibility = capture_runtime_compatibility(TRANSPORT_PACKAGES)
    (installed / "cloudpickle-1.0.dist-info/METADATA").write_text(
        "Name: cloudpickle\nVersion: 2.0\n"
    )

    def forbidden(_value: object) -> None:
        pytest.fail("Mismatched controller reached live-object serialization")

    monkeypatch.setattr("cloudpickle.dumps", forbidden)
    with pytest.raises(ValueError, match="differs from prepared metadata"):
        serialize_worker_call(lambda: None, compatibility=compatibility)


def test_decode_failure_cannot_claim_scientific_authority(
    installed: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from artisan.execution.exceptions import AssignmentExecutionError

    compatibility = capture_runtime_compatibility(TRANSPORT_PACKAGES)
    call = serialize_worker_call(lambda: None, compatibility=compatibility)

    def fail(_payload: bytes) -> None:
        message = "model reconstruction failed"
        raise AssignmentExecutionError(message, phase="execute")

    monkeypatch.setattr("cloudpickle.loads", fail)
    with pytest.raises(RuntimeError, match="deserialization failed") as raised:
        execute_worker_call(call)
    assert not isinstance(raised.value, AssignmentExecutionError)


@pytest.mark.parametrize(
    "change",
    [
        "raw-pickle",
        "truncated-prefix",
        "truncated-body",
        "extra",
        "string",
        "prefix-hash",
    ],
)
def test_live_envelope_rejects_before_application_import_or_unpickle(
    installed: Path, monkeypatch: pytest.MonkeyPatch, change: str
) -> None:
    import hashlib

    from artisan.execution.worker_bootstrap import _ENVELOPE_PREFIX

    compatibility = capture_runtime_compatibility(TRANSPORT_PACKAGES)
    call = serialize_worker_call(lambda: None, compatibility=compatibility)
    encoded = b'{"application_modules":[]}'
    body = b"unpickle-canary"
    if change == "raw-pickle":
        payload = b"\x80\x05unframed-call"
    elif change == "truncated-prefix":
        payload = _ENVELOPE_PREFIX + b"\x00\x00"
    else:
        if change == "extra":
            encoded = b'{"application_modules":[],"import":"untrusted"}'
        elif change == "string":
            encoded = b'{"application_modules":"untrusted"}'
        elif change == "prefix-hash":
            encoded = b'{"application_modules":["json"]}'
        elif change == "truncated-body":
            body = b""
        payload = _ENVELOPE_PREFIX + len(encoded).to_bytes(4, "big") + encoded + body
    header = (
        call.header
        if change == "prefix-hash"
        else call.header.model_copy(
            update={"payload_sha256": hashlib.sha256(payload).hexdigest()}
        )
    )
    monkeypatch.setattr(
        "artisan.execution.worker_bootstrap.bootstrap_worker_applications",
        lambda *a, **kw: pytest.fail("Malformed envelope reached application import"),
    )
    monkeypatch.setattr(
        "cloudpickle.loads",
        lambda *a, **kw: pytest.fail("Malformed envelope reached live deserialization"),
    )
    expected = ApplicationRegistrationError if change == "string" else ValueError
    with pytest.raises(expected):
        execute_worker_call(PreparedProcessCall(header, payload))


@pytest.mark.parametrize("stage", ["unpickle", "call"])
def test_live_application_scope_covers_decode_and_call_then_resets(
    installed: Path, monkeypatch: pytest.MonkeyPatch, stage: str
) -> None:
    from artisan.execution.application_bootstrap import bootstrap_worker_applications

    compatibility = capture_runtime_compatibility(TRANSPORT_PACKAGES)

    def fail() -> None:
        with pytest.raises(ValueError, match="live call selection"):
            bootstrap_worker_applications((), qualified_modules=(), compatibility=None)
        message = "synthetic live failure"
        raise RuntimeError(message)

    call = serialize_worker_call(
        fail, compatibility=compatibility, application_modules=("json",)
    )
    with monkeypatch.context() as patch:
        if stage == "unpickle":
            patch.setattr("cloudpickle.loads", lambda _: fail())
        with pytest.raises(RuntimeError):
            execute_worker_call(call)
    # The next independently selected unit must not inherit failed-call authority.
    bootstrap_worker_applications((), qualified_modules=(), compatibility=None)
