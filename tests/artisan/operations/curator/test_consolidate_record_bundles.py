"""Tests for ConsolidateRecordBundles curator."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

import polars as pl
import pytest

from artisan.operations.curator.consolidate_record_bundles import (
    ConsolidateRecordBundles,
)
from artisan.schemas.artifact.record_bundle import RecordBundleArtifact
from artisan.utils.hashing import compute_content_hash


def _df(artifact_ids: list[str]) -> pl.DataFrame:
    """Build a curator-style DataFrame from artifact IDs."""
    return pl.DataFrame({"artifact_id": artifact_ids})


def _make_bundle_artifact(
    record_id: str,
    external_path: str,
    step_number: int = 0,
) -> RecordBundleArtifact:
    """Create a finalized RecordBundleArtifact."""
    line = json.dumps({"record_id": record_id, "values": {"x": 1.0}}, sort_keys=True)
    art = RecordBundleArtifact.draft(
        record_id=record_id,
        content_hash=compute_content_hash(line.encode()),
        size_bytes=len(line.encode()),
        step_number=step_number,
        external_path=external_path,
        original_name=record_id,
    )
    art.finalize()
    return art


def _write_jsonl(path: Path, records: list[dict]) -> None:
    with path.open("w") as f:
        for rec in records:
            f.write(json.dumps(rec, sort_keys=True) + "\n")


def _mock_store_with_bundles(
    artifacts: dict[str, RecordBundleArtifact],
    files_root: Path | None,
) -> MagicMock:
    """Create a mock ArtifactStore with get_artifacts_by_type returning artifacts."""
    store = MagicMock()
    store.files_root = files_root
    store.get_artifacts_by_type.return_value = artifacts
    return store


class TestConsolidateBasicExecution:
    """Tests for basic consolidation behavior."""

    def test_concatenates_worker_files(self, tmp_path: Path) -> None:
        # Set up two worker JSONL files
        worker_a = tmp_path / "workers" / "a" / "records.jsonl"
        worker_b = tmp_path / "workers" / "b" / "records.jsonl"
        worker_a.parent.mkdir(parents=True)
        worker_b.parent.mkdir(parents=True)
        _write_jsonl(worker_a, [{"record_id": "rec_0", "values": {"x": 1}}])
        _write_jsonl(worker_b, [{"record_id": "rec_1", "values": {"x": 2}}])

        art_0 = _make_bundle_artifact("rec_0", str(worker_a))
        art_1 = _make_bundle_artifact("rec_1", str(worker_b))
        artifacts = {art_0.artifact_id: art_0, art_1.artifact_id: art_1}

        files_root = tmp_path / "files"
        files_root.mkdir()
        store = _mock_store_with_bundles(artifacts, files_root)
        inputs = {"records": _df(list(artifacts.keys()))}

        op = ConsolidateRecordBundles()
        result = op.execute_curator(inputs, step_number=5, artifact_store=store)

        combined = files_root / "5" / "combined.jsonl"
        assert combined.exists()
        lines = combined.read_text().strip().split("\n")
        assert len(lines) == 2

    def test_output_artifacts_point_to_combined_file(self, tmp_path: Path) -> None:
        worker = tmp_path / "worker.jsonl"
        _write_jsonl(worker, [{"record_id": "rec_0", "values": {}}])

        art = _make_bundle_artifact("rec_0", str(worker))
        artifacts = {art.artifact_id: art}

        files_root = tmp_path / "files"
        files_root.mkdir()
        store = _mock_store_with_bundles(artifacts, files_root)
        inputs = {"records": _df(list(artifacts.keys()))}

        op = ConsolidateRecordBundles()
        result = op.execute_curator(inputs, step_number=3, artifact_store=store)

        expected_path = str(files_root / "3" / "combined.jsonl")
        for draft in result.artifacts["records"]:
            assert draft.external_path == expected_path

    def test_record_count_preserved(self, tmp_path: Path) -> None:
        worker = tmp_path / "worker.jsonl"
        _write_jsonl(
            worker,
            [
                {"record_id": "rec_0", "values": {}},
                {"record_id": "rec_1", "values": {}},
            ],
        )

        art_0 = _make_bundle_artifact("rec_0", str(worker))
        art_1 = _make_bundle_artifact("rec_1", str(worker))
        artifacts = {art_0.artifact_id: art_0, art_1.artifact_id: art_1}

        files_root = tmp_path / "files"
        files_root.mkdir()
        store = _mock_store_with_bundles(artifacts, files_root)
        inputs = {"records": _df(list(artifacts.keys()))}

        op = ConsolidateRecordBundles()
        result = op.execute_curator(inputs, step_number=0, artifact_store=store)

        assert len(result.artifacts["records"]) == 2

    def test_new_artifact_ids(self, tmp_path: Path) -> None:
        """Consolidated artifacts get new IDs because external_path changed."""
        worker = tmp_path / "worker.jsonl"
        _write_jsonl(worker, [{"record_id": "rec_0", "values": {}}])

        art = _make_bundle_artifact("rec_0", str(worker))
        artifacts = {art.artifact_id: art}

        files_root = tmp_path / "files"
        files_root.mkdir()
        store = _mock_store_with_bundles(artifacts, files_root)
        inputs = {"records": _df(list(artifacts.keys()))}

        op = ConsolidateRecordBundles()
        result = op.execute_curator(inputs, step_number=1, artifact_store=store)

        # Finalize the draft to get an ID, then compare
        draft = result.artifacts["records"][0]
        draft.finalize()
        assert draft.artifact_id != art.artifact_id


class TestConsolidateErrorHandling:
    """Tests for error conditions."""

    def test_raises_without_files_root(self) -> None:
        store = MagicMock()
        store.files_root = None

        op = ConsolidateRecordBundles()
        with pytest.raises(ValueError, match="files_root required"):
            op.execute_curator(
                {"records": _df(["abc" * 10 + "ab"])},
                step_number=0,
                artifact_store=store,
            )


class TestConsolidateClassAttributes:
    """Tests for operation class configuration."""

    def test_has_correct_name(self) -> None:
        assert ConsolidateRecordBundles.name == "consolidate_record_bundles"

    def test_has_records_input_spec(self) -> None:
        assert "records" in ConsolidateRecordBundles.inputs
        assert (
            ConsolidateRecordBundles.inputs["records"].artifact_type == "record_bundle"
        )

    def test_has_records_output_spec(self) -> None:
        assert "records" in ConsolidateRecordBundles.outputs
        assert (
            ConsolidateRecordBundles.outputs["records"].artifact_type == "record_bundle"
        )

    def test_output_lineage_traces_to_input(self) -> None:
        spec = ConsolidateRecordBundles.outputs["records"]
        assert spec.infer_lineage_from == {"inputs": ["records"]}
