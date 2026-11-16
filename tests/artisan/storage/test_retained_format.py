"""The retained domain cannot relabel historical or unknown storage."""

from __future__ import annotations

import pytest

from artisan.errors import IncompatibleStoreError
from artisan.storage.core.store_format import (
    assert_retained_store,
    initialize_retained_store,
)


def test_retained_domain_requires_explicit_idempotent_initialization(
    backend_fs,
) -> None:
    fs, _, root = backend_fs
    candidate = f"{root}/candidate"
    with pytest.raises(IncompatibleStoreError, match="no domain manifest"):
        assert_retained_store(candidate, fs)
    assert not fs.exists(candidate)
    initialize_retained_store(candidate, fs)
    initialize_retained_store(candidate, fs)
    assert_retained_store(candidate, fs)


def test_retained_domain_rejects_legacy_and_unlabeled_roots(backend_fs) -> None:
    fs, _, root = backend_fs
    legacy = f"{root}/legacy"
    fs.makedirs(f"{legacy}/_artisan", exist_ok=True)
    with fs.open(f"{legacy}/_artisan/store.json", "wb") as stream:
        stream.write(b'{"store_format":9,"artifact_identity":1,"cache_identity":3}')
    with pytest.raises(IncompatibleStoreError, match="outside"):
        initialize_retained_store(legacy, fs)
    candidate = f"{root}/unlabeled"
    fs.makedirs(candidate, exist_ok=True)
    with fs.open(f"{candidate}/unowned.json", "wb") as stream:
        stream.write(b"{}")
    with pytest.raises(IncompatibleStoreError, match="nonempty"):
        initialize_retained_store(candidate, fs)
