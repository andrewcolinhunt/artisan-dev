"""Public analysis readers preserve explicit filesystem and carrier-root binding."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
from fsspec import AbstractFileSystem
from fsspec.implementations.local import LocalFileSystem

from artisan.inspection.api import inspect_evidence
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.storage.io.retained_store import (
    create_retained_store,
    open_configured_store,
)
from artisan.visualization.inspect import inspect_pipeline


class UnopenedCloud(AbstractFileSystem):
    protocol = "s3"

    def exists(self, path: str, **kwargs: Any) -> bool:
        pytest.fail("Invalid configuration must reject before I/O")


@pytest.mark.parametrize("extra", [{"fs": LocalFileSystem()}, {"storage_options": {}}])
def test_configured_open_rejects_two_storage_spellings_before_io(
    extra: dict[str, object],
) -> None:
    with (
        patch("artisan.storage.io.retained_store.open_retained_store") as opened,
        pytest.raises(ValueError, match="not both"),
    ):
        open_configured_store("/unused", storage=StorageConfig(), **extra)
    opened.assert_not_called()


def test_nonlocal_reader_requires_explicit_files_root_before_io() -> None:
    with pytest.raises(ValueError, match="explicit files_root"):
        inspect_pipeline("s3://synthetic/delta", fs=UnopenedCloud())


def test_configured_open_preserves_exact_fs_options_and_custom_root() -> None:
    fs = UnopenedCloud()
    options = {"AWS_REGION": "synthetic-region"}
    with patch("artisan.storage.io.retained_store.open_retained_store") as opened:
        open_configured_store(
            "s3://synthetic/delta",
            files_root="s3://carriers/root",
            fs=fs,
            storage_options=options,
        )
    opened.assert_called_once_with(
        "s3://synthetic/delta", "s3://carriers/root", fs=fs, storage_options=options
    )


def test_bounded_inspection_forwards_storage_as_call_context(tmp_path: Path) -> None:
    root, files = tmp_path / "delta", tmp_path / "custom-carriers"
    storage = StorageConfig()
    create_retained_store(str(root), str(files), fs=storage.filesystem())
    from artisan.storage.io import retained_store

    opened = retained_store.open_configured_store
    with patch.object(retained_store, "open_configured_store", wraps=opened) as tracked:
        result = inspect_evidence(
            "list_runs",
            {},
            delta_root=str(root),
            files_root=str(files),
            storage=storage,
        )
    assert result.items == []
    tracked.assert_called_once_with(str(root), files_root=str(files), storage=storage)
    assert "storage" not in result.model_dump()
    assert "files_root" not in result.model_dump()


def test_inspection_first_import_does_not_cycle_through_timing() -> None:
    subprocess.run(
        [
            sys.executable,
            "-c",
            "from artisan.inspection.api import inspect_evidence; "
            "from artisan.visualization import PipelineTimings",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
