"""Tests for ConsolidateAppendables curator."""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import polars as pl
import pytest
from fsspec import AbstractFileSystem
from fsspec.implementations.local import LocalFileSystem

from artisan.operations.curator.consolidate_appendables import (
    ConsolidateAppendables,
)
from artisan.schemas.artifact.appendable import AppendableArtifact
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.utils.hashing import compute_content_hash


@pytest.fixture(
    params=[
        pytest.param("local"),
        pytest.param("s3", marks=pytest.mark.integration),
    ]
)
def backend_fs(request, tmp_path, s3_fs):
    """Yield ``(fs, StorageConfig, uri_prefix)`` for both backends.

    Module-level so ``TestConsolidateBasicExecution`` and
    ``TestConsolidateAppendablesBackendParametrized`` share the same
    parametrization. Inlined here because this test file lives outside
    ``tests/artisan/storage/`` (where the shared fixture is defined);
    only ``s3_fs`` (from the root ``tests/conftest.py``) is needed to
    stay in scope. The ``s3`` param carries the ``integration`` marker
    so ``test-unit`` stays MinIO-free.
    """
    if request.param == "local":
        return LocalFileSystem(), StorageConfig(), str(tmp_path)
    return s3_fs


def _df(artifact_ids: list[str]) -> pl.DataFrame:
    """Build a curator-style DataFrame from artifact IDs."""
    return pl.DataFrame({"artifact_id": artifact_ids})


def _make_appendable_artifact(
    record_id: str,
    external_path: str,
    step_number: int = 0,
) -> AppendableArtifact:
    """Create a finalized AppendableArtifact."""
    line = json.dumps({"record_id": record_id, "values": {"x": 1.0}}, sort_keys=True)
    art = AppendableArtifact.draft(
        record_id=record_id,
        content_hash=compute_content_hash(line.encode()),
        size_bytes=len(line.encode()),
        step_number=step_number,
        external_path=external_path,
        original_name=record_id,
    )
    art.finalize()
    return art


def _write_jsonl(fs: AbstractFileSystem, path: str, records: list[dict]) -> None:
    """Write a JSONL file via fsspec (works for both local + s3)."""
    with fs.open(path, "w") as f:
        for rec in records:
            f.write(json.dumps(rec, sort_keys=True) + "\n")


def _mock_store_with_appendables(
    artifacts: dict[str, AppendableArtifact],
    files_root: str | None,
    fs: AbstractFileSystem,
) -> MagicMock:
    """Create a mock ArtifactStore with get_artifacts_by_type returning artifacts."""
    store = MagicMock()
    store.files_root = files_root
    store._fs = fs
    store.get_artifacts_by_type.return_value = artifacts
    return store


class TestConsolidateBasicExecution:
    """Tests for basic consolidation behavior.

    Runs against both local and s3 backends via the module-level
    ``backend_fs`` fixture — verifies the consolidation codepath
    (worker-file read, combined-file write via ``fs.open``) works on
    both filesystems.
    """

    def test_concatenates_worker_files(self, backend_fs) -> None:
        fs, _storage, root = backend_fs
        # Set up two worker JSONL files
        worker_a = f"{root}/workers/a/records.jsonl"
        worker_b = f"{root}/workers/b/records.jsonl"
        fs.makedirs(f"{root}/workers/a", exist_ok=True)
        fs.makedirs(f"{root}/workers/b", exist_ok=True)
        _write_jsonl(fs, worker_a, [{"record_id": "rec_0", "values": {"x": 1}}])
        _write_jsonl(fs, worker_b, [{"record_id": "rec_1", "values": {"x": 2}}])

        art_0 = _make_appendable_artifact("rec_0", worker_a)
        art_1 = _make_appendable_artifact("rec_1", worker_b)
        artifacts = {art_0.artifact_id: art_0, art_1.artifact_id: art_1}

        files_root = f"{root}/files"
        fs.makedirs(files_root, exist_ok=True)
        store = _mock_store_with_appendables(artifacts, files_root, fs)
        inputs = {"records": _df(list(artifacts.keys()))}

        op = ConsolidateAppendables()
        op.execute_curator(inputs, step_number=5, artifact_store=store)

        combined = f"{files_root}/5/combined.jsonl"
        assert fs.exists(combined)
        with fs.open(combined, "r") as f:
            lines = f.read().strip().split("\n")
        assert len(lines) == 2

    def test_output_artifacts_point_to_combined_file(self, backend_fs) -> None:
        fs, _storage, root = backend_fs
        worker = f"{root}/worker.jsonl"
        _write_jsonl(fs, worker, [{"record_id": "rec_0", "values": {}}])

        art = _make_appendable_artifact("rec_0", worker)
        artifacts = {art.artifact_id: art}

        files_root = f"{root}/files"
        fs.makedirs(files_root, exist_ok=True)
        store = _mock_store_with_appendables(artifacts, files_root, fs)
        inputs = {"records": _df(list(artifacts.keys()))}

        op = ConsolidateAppendables()
        result = op.execute_curator(inputs, step_number=3, artifact_store=store)

        expected_path = f"{files_root}/3/combined.jsonl"
        for draft in result.artifacts["records"]:
            assert draft.external_path == expected_path

    def test_record_count_preserved(self, backend_fs) -> None:
        fs, _storage, root = backend_fs
        worker = f"{root}/worker.jsonl"
        _write_jsonl(
            fs,
            worker,
            [
                {"record_id": "rec_0", "values": {}},
                {"record_id": "rec_1", "values": {}},
            ],
        )

        art_0 = _make_appendable_artifact("rec_0", worker)
        art_1 = _make_appendable_artifact("rec_1", worker)
        artifacts = {art_0.artifact_id: art_0, art_1.artifact_id: art_1}

        files_root = f"{root}/files"
        fs.makedirs(files_root, exist_ok=True)
        store = _mock_store_with_appendables(artifacts, files_root, fs)
        inputs = {"records": _df(list(artifacts.keys()))}

        op = ConsolidateAppendables()
        result = op.execute_curator(inputs, step_number=0, artifact_store=store)

        assert len(result.artifacts["records"]) == 2

    def test_new_artifact_ids(self, backend_fs) -> None:
        """Consolidated artifacts get new IDs because external_path changed."""
        fs, _storage, root = backend_fs
        worker = f"{root}/worker.jsonl"
        _write_jsonl(fs, worker, [{"record_id": "rec_0", "values": {}}])

        art = _make_appendable_artifact("rec_0", worker)
        artifacts = {art.artifact_id: art}

        files_root = f"{root}/files"
        fs.makedirs(files_root, exist_ok=True)
        store = _mock_store_with_appendables(artifacts, files_root, fs)
        inputs = {"records": _df(list(artifacts.keys()))}

        op = ConsolidateAppendables()
        result = op.execute_curator(inputs, step_number=1, artifact_store=store)

        # Finalize the draft to get an ID, then compare
        draft = result.artifacts["records"][0]
        draft.finalize()
        assert draft.artifact_id != art.artifact_id


class TestConsolidateErrorHandling:
    """Tests for error conditions.

    Stays local-only (no backend parametrization) — ``test_raises_without_files_root``
    asserts only a ``ValueError`` without touching the filesystem.
    """

    def test_raises_without_files_root(self) -> None:
        store = MagicMock()
        store.files_root = None

        op = ConsolidateAppendables()
        with pytest.raises(ValueError, match="files_root required"):
            op.execute_curator(
                {"records": _df(["abc" * 10 + "ab"])},
                step_number=0,
                artifact_store=store,
            )


class TestConsolidateClassAttributes:
    """Tests for operation class configuration."""

    def test_has_correct_name(self) -> None:
        assert ConsolidateAppendables.name == "consolidate_appendables"

    def test_has_records_input_spec(self) -> None:
        assert "records" in ConsolidateAppendables.inputs
        assert ConsolidateAppendables.inputs["records"].artifact_type == "appendable"

    def test_has_records_output_spec(self) -> None:
        assert "records" in ConsolidateAppendables.outputs
        assert ConsolidateAppendables.outputs["records"].artifact_type == "appendable"

    def test_output_lineage_traces_to_input(self) -> None:
        spec = ConsolidateAppendables.outputs["records"]
        assert spec.infer_lineage_from == {"inputs": ["records"]}


class TestConsolidateAppendablesBackendParametrized:
    """Smoke test: consolidate JSONL workers into a combined file on each backend.

    Kept as a higher-level end-to-end smoke alongside the promoted
    ``TestConsolidateBasicExecution`` class, which now covers the same
    backends at a finer granularity.
    """

    def test_consolidates_worker_jsonl_files(self, backend_fs) -> None:
        """Two worker JSONL files concatenate into one combined.jsonl."""
        fs, _, root = backend_fs

        # Write two per-worker JSONL files via the parametrized fs in
        # text mode — this is the S3-sensitive path the curator uses for
        # the combined output write below.
        worker_a = f"{root}/workers/a/records.jsonl"
        worker_b = f"{root}/workers/b/records.jsonl"
        fs.makedirs(f"{root}/workers/a", exist_ok=True)
        fs.makedirs(f"{root}/workers/b", exist_ok=True)
        with fs.open(worker_a, "w") as f:
            f.write(json.dumps({"record_id": "rec_0", "values": {"x": 1}}) + "\n")
        with fs.open(worker_b, "w") as f:
            f.write(json.dumps({"record_id": "rec_1", "values": {"x": 2}}) + "\n")

        art_0 = _make_appendable_artifact("rec_0", worker_a)
        art_1 = _make_appendable_artifact("rec_1", worker_b)
        artifacts = {art_0.artifact_id: art_0, art_1.artifact_id: art_1}

        files_root = f"{root}/files"
        store = MagicMock()
        store.files_root = files_root
        store._fs = fs
        store.get_artifacts_by_type.return_value = artifacts

        op = ConsolidateAppendables()
        result = op.execute_curator(
            {"records": _df(list(artifacts.keys()))},
            step_number=5,
            artifact_store=store,
        )

        combined = f"{files_root}/5/combined.jsonl"
        assert fs.exists(combined)
        with fs.open(combined, "r") as f:
            lines = f.read().strip().split("\n")
        assert len(lines) == 2

        # Output drafts point to the combined file on both backends.
        for draft in result.artifacts["records"]:
            assert draft.external_path == combined
