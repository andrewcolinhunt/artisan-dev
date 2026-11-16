"""The supported domain rejects historical bytes and validates registered tables."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, ClassVar
from unittest.mock import Mock

import polars as pl
import pytest
from fsspec.implementations.local import LocalFileSystem

from artisan.errors import IncompatibleStoreError, StoreIntegrityError
from artisan.operations.curator.ingest_pipeline_step import IngestPipelineStep
from artisan.orchestration import PipelineManager
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.storage import ArtifactStore
from artisan.storage.core.store_format import (
    STORE_MANIFEST_PATH,
    assert_retained_store,
    initialize_retained_store,
)
from artisan.storage.io.repair import repair_store
from artisan.storage.io.retained_store import create_retained_store, open_retained_store


@pytest.fixture
def list_score_type(monkeypatch: pytest.MonkeyPatch) -> type[ArtifactTypeDef]:
    """Register a domain artifact without leaking registry entries or attributes."""
    monkeypatch.setattr(ArtifactTypeDef, "_registry", dict(ArtifactTypeDef._registry))
    monkeypatch.setattr(ArtifactTypes, "_registry", dict(ArtifactTypes._registry))
    monkeypatch.setattr(ArtifactTypes, "TEST_LIST_SCORE", None, raising=False)

    class ListScoreArtifact(Artifact):
        artifact_type: str = "test_list_score"
        score: float
        site_values: list[float]
        pair_values: list[list[float]]
        POLARS_SCHEMA: ClassVar[dict[str, Any]] = {
            "artifact_id": pl.String,
            "origin_step_number": pl.Int32,
            "score": pl.Float32,
            "site_values": pl.List(pl.Float32),
            "pair_values": pl.List(pl.List(pl.Float32)),
            "metadata": pl.String,
        }

    class ListScoreTypeDef(ArtifactTypeDef):
        key = "test_list_score"
        table_path = "artifacts/test_list_scores"
        model = ListScoreArtifact

    return ListScoreTypeDef


def test_registered_list_artifact_store_initializes_and_reopens(
    tmp_path: Path, list_score_type: type[ArtifactTypeDef]
) -> None:
    fs = LocalFileSystem()
    root, files = str(tmp_path / "delta"), str(tmp_path / "files")
    create_retained_store(root, files, fs=fs)
    open_retained_store(root, files, fs=fs)
    ArtifactStore(root, fs=fs)
    create_retained_store(root, files, fs=fs)
    schema = pl.scan_delta(f"{root}/{list_score_type.table_path}").collect_schema()
    assert schema["score"] == pl.Float32
    assert schema["site_values"] == pl.List(pl.Float32)
    assert schema["pair_values"] == pl.List(pl.List(pl.Float32))


@pytest.mark.parametrize(
    "wrong_type", [pl.List(pl.List(pl.Float64)), pl.List(pl.Float32)]
)
def test_registered_list_artifact_rejects_wrong_schema(
    tmp_path: Path, list_score_type: type[ArtifactTypeDef], wrong_type: object
) -> None:
    fs = LocalFileSystem()
    root, files = str(tmp_path / "delta"), str(tmp_path / "files")
    create_retained_store(root, files, fs=fs)
    schema = {
        **list_score_type.polars_schema(),
        "pair_values": wrong_type,
        "logical_commit_id": pl.String,
    }
    pl.DataFrame(schema=schema).write_delta(
        f"{root}/{list_score_type.table_path}",
        mode="overwrite",
        delta_write_options={"schema_mode": "overwrite"},
    )
    with pytest.raises(StoreIntegrityError):
        open_retained_store(root, files, fs=fs)
    with pytest.raises(StoreIntegrityError):
        ArtifactStore(root, fs=fs)


def test_manifest_round_trip_for_initialized_store(tmp_path: Path) -> None:
    fs = LocalFileSystem()
    root, files = str(tmp_path / "delta"), str(tmp_path / "files")
    create_retained_store(root, files, fs=fs)
    assert_retained_store(root, fs)
    assert (tmp_path / "delta" / STORE_MANIFEST_PATH).read_bytes() == (
        b'{"domain":"artisan.retained-store","schema_version":1}'
    )
    assert fs.exists(f"{root}/artifacts/file_refs")
    assert fs.exists(f"{root}/artifacts/csvs")


def test_manifest_alone_does_not_authorize_reading_missing_tables(
    tmp_path: Path,
) -> None:
    fs = LocalFileSystem()
    root = str(tmp_path / "delta")
    initialize_retained_store(root, fs)
    with pytest.raises(StoreIntegrityError, match="missing required table"):
        ArtifactStore(root, fs=fs)


@pytest.mark.parametrize(
    "content",
    [
        None,
        b"not-json",
        b"{}",
        b'{"domain":"artisan.retained-store","schema_version":true}',
    ],
)
def test_missing_malformed_and_unsupported_manifests_fail_without_mutation(
    tmp_path: Path, content: bytes | None
) -> None:
    fs = LocalFileSystem()
    root = tmp_path / "delta"
    path = root / STORE_MANIFEST_PATH
    if content is not None:
        path.parent.mkdir(parents=True)
        path.write_bytes(content)
    with pytest.raises(IncompatibleStoreError):
        assert_retained_store(str(root), fs)
    assert path.read_bytes() == content if content is not None else not root.exists()


def test_unlabeled_nonempty_root_cannot_be_initialized(tmp_path: Path) -> None:
    root = tmp_path / "delta"
    root.mkdir()
    (root / "legacy.bin").write_bytes(b"legacy")
    with pytest.raises(IncompatibleStoreError, match="nonempty"):
        create_retained_store(str(root), str(tmp_path / "files"), fs=LocalFileSystem())
    assert list(root.iterdir()) == [root / "legacy.bin"]


@pytest.mark.parametrize("previous_format", [5, 6, 7, 8, 9])
def test_previous_manifest_rejected_even_with_current_tables(
    tmp_path: Path, previous_format: int
) -> None:
    fs = LocalFileSystem()
    root, files = str(tmp_path / "delta"), str(tmp_path / "files")
    create_retained_store(root, files, fs=fs)
    path = tmp_path / "delta" / STORE_MANIFEST_PATH
    previous = json.dumps(
        {"store_format": previous_format, "artifact_identity": 1, "cache_identity": 3}
    ).encode()
    path.write_bytes(previous)
    with pytest.raises(IncompatibleStoreError, match="outside"):
        open_retained_store(root, files, fs=fs)
    assert path.read_bytes() == previous


@pytest.mark.parametrize(
    "action", ["open", "initialize", "resume", "repair", "report", "import"]
)
def test_historical_root_rejected_without_changing_store_or_staging(
    tmp_path: Path, action: str
) -> None:
    root, staging = tmp_path / "delta", tmp_path / "staging"
    legacy = {
        root
        / STORE_MANIFEST_PATH: b'{"store_format":9,"artifact_identity":1,"cache_identity":3}',
        root / "artifacts/data/old.parquet": b"old table content",
        staging / "old/data.parquet": b"old staged content",
    }
    for path, content in legacy.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
    paths_before = set(tmp_path.rglob("*"))
    fs = LocalFileSystem()
    actions = {
        "open": lambda: ArtifactStore(str(root), fs=fs),
        "initialize": lambda: create_retained_store(
            str(root), str(tmp_path / "files"), fs=fs
        ),
        "resume": lambda: PipelineManager.resume(str(root), str(staging)),
        "repair": lambda: repair_store(
            delta_root=str(root), staging_root=str(staging), fs=fs, apply=True
        ),
        "report": lambda: repair_store(
            delta_root=str(root), staging_root=str(staging), fs=fs
        ),
        "import": lambda: IngestPipelineStep(
            params={
                "source_delta_root": str(root),
                "source_run_id": "old-run",
                "source_step": 0,
            }
        ).execute_curator({}, 0, Mock()),
    }
    with pytest.raises(IncompatibleStoreError, match="outside"):
        actions[action]()
    assert set(tmp_path.rglob("*")) == paths_before
    assert {path: path.read_bytes() for path in legacy} == legacy
