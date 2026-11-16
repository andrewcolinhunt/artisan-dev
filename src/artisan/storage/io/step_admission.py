"""Immutable finite step admission, independently bound by lifecycle state."""

from __future__ import annotations

from fsspec import AbstractFileSystem

from artisan.errors import StoreIntegrityError
from artisan.schemas.execution.step_admission import StepAdmission
from artisan.storage.core.store_format import assert_retained_store
from artisan.storage.io.publication import publish_immutable_bytes
from artisan.utils.hashing import canonical_json_bytes
from artisan.utils.path import uri_join


def publish_step_admission(
    admission: StepAdmission, *, delta_root: str, fs: AbstractFileSystem
) -> str:
    """Publish immutable membership; the caller must bind its digest durably."""
    assert_retained_store(delta_root, fs)
    admission = StepAdmission.model_validate(admission)
    publish_immutable_bytes(
        fs,
        _path(delta_root, admission.step_run_id),
        canonical_json_bytes(admission.model_dump(mode="json")),
    )
    return admission.digest


def read_step_admission(
    step_run_id: str,
    expected_digest: str,
    *,
    delta_root: str,
    fs: AbstractFileSystem,
) -> StepAdmission:
    """Read exact finite membership, never discover authority from a file listing."""
    assert_retained_store(delta_root, fs)
    with fs.open(_path(delta_root, step_run_id), "rb", cache_type="none") as stream:
        encoded = stream.read()
    admission = StepAdmission.model_validate_json(encoded)
    if (
        admission.step_run_id != step_run_id
        or admission.digest != expected_digest
        or canonical_json_bytes(admission.model_dump(mode="json")) != encoded
    ):
        msg = "Step admission differs from its controller-bound identity"
        raise StoreIntegrityError(msg)
    return admission


def _path(delta_root: str, step_run_id: str) -> str:
    if len(step_run_id) != 32 or any(
        char not in "0123456789abcdef" for char in step_run_id
    ):
        msg = "Step admission requires a canonical step identity"
        raise ValueError(msg)
    return uri_join(delta_root, "_artisan", "step-admissions", f"{step_run_id}.json")
