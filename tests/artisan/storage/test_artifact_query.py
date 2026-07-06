"""Tests for the artifact_index reference scan."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import polars as pl
import pytest

from artisan.schemas.enums import TablePath
from artisan.storage.core.artifact_query import ArtifactRef, query_artifacts
from artisan.storage.core.table_schemas import ARTIFACT_INDEX_SCHEMA, STEPS_SCHEMA


def _seed_index(root: Path, entries: list[tuple[str, str, int]]) -> None:
    """Write an artifact_index from (id, type, origin_step_number) triples."""
    df = pl.DataFrame(
        {
            "artifact_id": [e[0] for e in entries],
            "artifact_type": [e[1] for e in entries],
            "origin_step_number": [e[2] for e in entries],
            "metadata": ['{"note": "x"}'] * len(entries),
        },
        schema=ARTIFACT_INDEX_SCHEMA,
    )
    df.write_delta(str(root / TablePath.ARTIFACT_INDEX))


def _seed_steps(root: Path, run_id: str, numbers: list[int]) -> None:
    """Write minimal completed step rows so run filtering resolves numbers."""
    rows = [
        {
            "step_run_id": f"{run_id}-{n}",
            "step_spec_id": f"spec-{n}",
            "pipeline_run_id": run_id,
            "step_number": n,
            "step_name": f"step{n}",
            "status": "completed",
            "operation_class": "DataGenerator",
            "params_json": "{}",
            "input_refs_json": "{}",
            "compute_backend": "local",
            "compute_options_json": "{}",
            "output_roles_json": "[]",
            "output_types_json": "[]",
            "total_count": 1,
            "succeeded_count": 1,
            "failed_count": 0,
            "timestamp": datetime(2026, 7, 1, tzinfo=UTC),
            "duration_seconds": 1.0,
            "error": None,
            "dispatch_error": None,
            "commit_error": None,
            "metadata": "{}",
        }
        for n in numbers
    ]
    pl.DataFrame(rows, schema=STEPS_SCHEMA).write_delta(str(root / TablePath.STEPS))


A, B, C = "a" * 32, "b" * 32, "c" * 32


class TestQueryArtifacts:
    def test_all_refs(self, tmp_path) -> None:
        _seed_index(tmp_path, [(A, "data", 1), (B, "metric", 2)])
        refs = query_artifacts(str(tmp_path))
        assert {r.artifact_id for r in refs} == {A, B}
        assert all(isinstance(r, ArtifactRef) for r in refs)

    def test_type_filter(self, tmp_path) -> None:
        _seed_index(tmp_path, [(A, "data", 1), (B, "metric", 2)])
        refs = query_artifacts(str(tmp_path), artifact_type="metric")
        assert [r.artifact_id for r in refs] == [B]
        assert refs[0].artifact_type == "metric"
        assert refs[0].origin_step_number == 2

    def test_run_filter_via_step_numbers(self, tmp_path) -> None:
        _seed_index(tmp_path, [(A, "data", 1), (B, "data", 2), (C, "data", 9)])
        _seed_steps(tmp_path, "run-x", [1, 2])  # run-x owns steps 1 and 2 only
        refs = query_artifacts(str(tmp_path), pipeline_run_id="run-x")
        assert {r.artifact_id for r in refs} == {A, B}
        assert all(r.pipeline_run_id == "run-x" for r in refs)

    def test_refs_carry_metadata_not_payload(self, tmp_path) -> None:
        _seed_index(tmp_path, [(A, "data", 1)])
        ref = query_artifacts(str(tmp_path))[0]
        assert ref.metadata == {"note": "x"}
        # No content / payload field exists on the ref.
        assert "content" not in ref.model_dump()

    def test_missing_index_raises(self, tmp_path) -> None:
        with pytest.raises(FileNotFoundError):
            query_artifacts(str(tmp_path))
