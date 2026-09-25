"""Tests for the coordinated release store boundary."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, ClassVar

import polars as pl
import pytest
from deltalake import DeltaTable
from fsspec.implementations.local import LocalFileSystem

from artisan.errors import IncompatibleStoreError
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.storage.core.artifact_store import ArtifactStore
from artisan.storage.core.store_format import (
    STORE_MANIFEST,
    STORE_MANIFEST_PATH,
    _delta_type,
    assert_store_format,
    prepare_store_initialization,
    publish_store_manifest,
)
from artisan.storage.io.commit import DeltaCommitter
from artisan.storage.io.staging import StagingManager


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


@pytest.mark.parametrize(
    ("dtype", "expected"),
    [
        (pl.Float32, "float"),
        (
            pl.List(pl.Float32),
            {"type": "array", "elementType": "float", "containsNull": True},
        ),
        (
            pl.List(pl.List(pl.Float32)),
            {
                "type": "array",
                "elementType": {
                    "type": "array",
                    "elementType": "float",
                    "containsNull": True,
                },
                "containsNull": True,
            },
        ),
        (
            pl.List(pl.String),
            {"type": "array", "elementType": "string", "containsNull": True},
        ),
    ],
)
def test_delta_type_float32_and_lists(dtype: object, expected: object) -> None:
    assert _delta_type(dtype) == expected


def test_delta_type_deep_lists_matches_delta_schema(tmp_path: Path) -> None:
    dtype = pl.List(pl.List(pl.List(pl.Int64)))
    table_path = str(tmp_path / "deep_lists")
    pl.DataFrame(schema={"values": dtype}).write_delta(table_path)
    schema = json.loads(DeltaTable(table_path).schema().to_json())

    assert _delta_type(dtype) == schema["fields"][0]["type"]


@pytest.mark.parametrize(
    "dtype",
    [pl.Struct({"value": pl.Float32}), pl.Array(pl.Float32, 2), pl.List(pl.Int16)],
)
def test_delta_type_rejects_unsupported_types(dtype: object) -> None:
    with pytest.raises(TypeError, match="unsupported physical schema type"):
        _delta_type(dtype)


def test_registered_list_artifact_store_initializes_and_reopens(
    tmp_path: Path, list_score_type: type[ArtifactTypeDef]
) -> None:
    fs = LocalFileSystem()
    root = str(tmp_path / "delta")
    committer = DeltaCommitter(
        root, StagingManager(str(tmp_path / "staging"), fs), fs=fs
    )
    committer.initialize_tables()

    assert_store_format(root, fs)
    ArtifactStore(root, fs=fs)
    committer.initialize_tables()
    schema = json.loads(
        DeltaTable(f"{root}/{list_score_type.table_path}").schema().to_json()
    )
    types = {field["name"]: field["type"] for field in schema["fields"]}
    assert types["score"] == "float"
    assert types["site_values"] == {
        "type": "array",
        "elementType": "float",
        "containsNull": True,
    }
    assert types["pair_values"] == {
        "type": "array",
        "elementType": {
            "type": "array",
            "elementType": "float",
            "containsNull": True,
        },
        "containsNull": True,
    }


@pytest.mark.parametrize(
    "wrong_type",
    [pl.List(pl.List(pl.Float64)), pl.List(pl.Float32)],
    ids=["wrong-element-type", "wrong-nesting-depth"],
)
def test_registered_list_artifact_rejects_wrong_schema(
    tmp_path: Path, list_score_type: type[ArtifactTypeDef], wrong_type: object
) -> None:
    fs = LocalFileSystem()
    root = str(tmp_path / "delta")
    DeltaCommitter(
        root, StagingManager(str(tmp_path / "staging"), fs), fs=fs
    ).initialize_tables()
    schema = {
        **list_score_type.polars_schema(),
        "pair_values": wrong_type,
        "logical_commit_id": pl.String,
    }
    pl.DataFrame(schema=schema).write_delta(
        f"{root}/{list_score_type.table_path}",
        mode="overwrite",
        delta_write_options={
            "schema_mode": "overwrite",
            "partition_by": ["origin_step_number"],
        },
    )

    with pytest.raises(IncompatibleStoreError, match="test_list_scores.*has schema"):
        assert_store_format(root, fs)
    with pytest.raises(IncompatibleStoreError, match="test_list_scores.*has schema"):
        ArtifactStore(root, fs=fs)


def test_manifest_round_trip_for_initialized_store(tmp_path) -> None:
    fs = LocalFileSystem()
    root = str(tmp_path / "delta")
    DeltaCommitter(
        root,
        StagingManager(str(tmp_path / "staging"), fs),
        fs=fs,
    ).initialize_tables()

    assert_store_format(root, fs)
    with fs.open(f"{root}/{STORE_MANIFEST_PATH}", "r") as stream:
        assert json.load(stream) == STORE_MANIFEST


def test_initialize_empty_root_publishes_manifest_last(tmp_path) -> None:
    fs = LocalFileSystem()
    root = str(tmp_path / "delta")
    staging = StagingManager(str(tmp_path / "staging"), fs)
    committer = DeltaCommitter(root, staging, fs=fs)

    committer.initialize_tables()

    assert_store_format(root, fs)


def test_current_manifest_without_required_tables_fails(tmp_path) -> None:
    fs = LocalFileSystem()
    root = str(tmp_path / "delta")
    publish_store_manifest(root, fs)

    with pytest.raises(IncompatibleStoreError, match="missing table"):
        assert_store_format(root, fs)


def test_malformed_cache_reuse_table_fails(tmp_path) -> None:
    fs = LocalFileSystem()
    root = str(tmp_path / "delta")
    DeltaCommitter(
        root,
        StagingManager(str(tmp_path / "staging"), fs),
        fs=fs,
    ).initialize_tables()
    fs.rm(f"{root}/orchestration/cache_reuse", recursive=True)
    fs.makedirs(f"{root}/orchestration/cache_reuse", exist_ok=True)

    with pytest.raises(IncompatibleStoreError, match="malformed table"):
        assert_store_format(root, fs)


def test_wrong_cache_reuse_schema_fails(tmp_path) -> None:
    fs = LocalFileSystem()
    root = str(tmp_path / "delta")
    DeltaCommitter(
        root,
        StagingManager(str(tmp_path / "staging"), fs),
        fs=fs,
    ).initialize_tables()
    fs.rm(f"{root}/orchestration/cache_reuse", recursive=True)
    pl.DataFrame(schema={"cached_execution_run_id": pl.String}).write_delta(
        f"{root}/orchestration/cache_reuse"
    )

    with pytest.raises(IncompatibleStoreError, match="has schema"):
        assert_store_format(root, fs)


@pytest.mark.parametrize(
    "content",
    [
        None,
        "not-json",
        json.dumps({"store_format": 2, "artifact_identity": 1, "cache_identity": 2}),
        json.dumps({"store_format": 4}),
    ],
)
def test_missing_malformed_and_unsupported_manifests_fail(tmp_path, content) -> None:
    fs = LocalFileSystem()
    root = str(tmp_path / "delta")
    if content is not None:
        fs.makedirs(f"{root}/_artisan", exist_ok=True)
        with fs.open(f"{root}/{STORE_MANIFEST_PATH}", "w") as stream:
            stream.write(content)

    with pytest.raises(IncompatibleStoreError, match="Use a new Delta root"):
        assert_store_format(root, fs)


def test_nonempty_legacy_root_cannot_be_initialized(tmp_path) -> None:
    fs = LocalFileSystem()
    root = str(tmp_path / "delta")
    fs.makedirs(root, exist_ok=True)
    with fs.open(f"{root}/legacy.bin", "wb") as stream:
        stream.write(b"legacy")

    with pytest.raises(IncompatibleStoreError, match="not empty"):
        prepare_store_initialization(root, fs)


def test_previous_manifest_rejected_even_with_current_tables(tmp_path) -> None:
    fs = LocalFileSystem()
    root = str(tmp_path / "delta")
    DeltaCommitter(
        root,
        StagingManager(str(tmp_path / "staging"), fs),
        fs=fs,
    ).initialize_tables()
    previous_manifest = {**STORE_MANIFEST, "store_format": 5}
    with fs.open(f"{root}/{STORE_MANIFEST_PATH}", "w") as stream:
        json.dump(previous_manifest, stream)

    with pytest.raises(IncompatibleStoreError, match="found .*store_format.*5"):
        assert_store_format(root, fs)
    # Rejection must not rewrite an old manifest to bless inferred history.
    with fs.open(f"{root}/{STORE_MANIFEST_PATH}") as stream:
        assert json.load(stream) == previous_manifest
