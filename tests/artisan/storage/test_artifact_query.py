"""Tests for the artifact_index reference scan."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import polars as pl
import pytest
from fixtures.store_format import publish_test_store
from fsspec.implementations.local import LocalFileSystem

from artisan.schemas.enums import TablePath
from artisan.storage.core.artifact_query import ArtifactRef, query_artifacts
from artisan.storage.core.table_schemas import (
    ARTIFACT_INDEX_SCHEMA,
    CACHE_REUSE_SCHEMA,
    EXECUTION_EDGES_SCHEMA,
    EXECUTIONS_SCHEMA,
    STEPS_SCHEMA,
)
from artisan.utils.hashing import digest_utf8


@pytest.fixture(autouse=True)
def _format_root(tmp_path) -> None:
    publish_test_store(str(tmp_path), LocalFileSystem())


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


def _seed_run_outputs(root: Path, run_id: str, entries: list[tuple[int, str]]) -> None:
    """Write completed attempts, their executions, and exact output edges."""
    rows = [
        {
            "step_run_id": digest_utf8(f"{run_id}:{n}:step"),
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
        for n, _ in entries
    ]
    pl.DataFrame(rows, schema=STEPS_SCHEMA).write_delta(
        str(root / TablePath.STEPS), mode="append"
    )
    execution_rows = [
        {
            "execution_run_id": digest_utf8(f"{run_id}:{n}:execution"),
            "execution_spec_id": f"execution-spec-{n}",
            "step_run_id": digest_utf8(f"{run_id}:{n}:step"),
            "origin_step_number": n,
            "operation_name": f"step{n}",
            "params": "{}",
            "user_overrides": "{}",
            "timestamp_start": datetime(2026, 7, 1, tzinfo=UTC),
            "timestamp_end": datetime(2026, 7, 1, tzinfo=UTC),
            "source_worker": 0,
            "compute_backend": "local",
            "success": True,
            "error": None,
            "error_envelope": None,
            "tool_output": None,
            "worker_log": None,
            "metadata": "{}",
        }
        for n, _ in entries
    ]
    pl.DataFrame(execution_rows, schema=EXECUTIONS_SCHEMA).write_delta(
        str(root / TablePath.EXECUTIONS), mode="append"
    )
    edge_rows = [
        {
            "execution_run_id": digest_utf8(f"{run_id}:{n}:execution"),
            "direction": "output",
            "role": "output",
            "artifact_id": artifact_id,
        }
        for n, artifact_id in entries
    ]
    pl.DataFrame(edge_rows, schema=EXECUTION_EDGES_SCHEMA).write_delta(
        str(root / TablePath.EXECUTION_EDGES), mode="append"
    )


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

    def test_run_filter_via_exact_execution_membership(self, tmp_path) -> None:
        _seed_index(tmp_path, [(A, "data", 1), (B, "data", 2), (C, "data", 9)])
        _seed_run_outputs(tmp_path, "run-x", [(1, A), (2, B)])
        refs = query_artifacts(str(tmp_path), pipeline_run_id="run-x")
        assert {r.artifact_id for r in refs} == {A, B}
        assert all(r.pipeline_run_id == "run-x" for r in refs)
        assert {r.current_step_number for r in refs} == {1, 2}

    def test_run_filter_isolates_same_steps_and_projects_cached_origin(
        self, tmp_path
    ) -> None:
        _seed_index(tmp_path, [(A, "data", 1), (B, "data", 7), (C, "data", 7)])
        _seed_run_outputs(tmp_path, "source-run", [(1, A)])
        _seed_run_outputs(tmp_path, "current-run", [(7, B)])
        _seed_run_outputs(tmp_path, "other-run", [(7, C)])
        pl.DataFrame(
            [
                {
                    "current_step_run_id": digest_utf8("current-run:7:step"),
                    "cached_execution_run_id": digest_utf8("source-run:1:execution"),
                }
            ],
            schema=CACHE_REUSE_SCHEMA,
        ).write_delta(str(tmp_path / TablePath.CACHE_REUSE), mode="append")

        refs = query_artifacts(str(tmp_path), pipeline_run_id="current-run")
        cached = next(ref for ref in refs if ref.artifact_id == A)

        assert {ref.artifact_id for ref in refs} == {A, B}
        assert cached.origin_step_number == 1
        assert cached.current_step_number == 7
        assert C not in {ref.artifact_id for ref in refs}

    def test_refs_carry_metadata_not_payload(self, tmp_path) -> None:
        _seed_index(tmp_path, [(A, "data", 1)])
        ref = query_artifacts(str(tmp_path))[0]
        assert ref.metadata == {"note": "x"}
        # No content / payload field exists on the ref.
        assert "content" not in ref.model_dump()

    def test_missing_index_raises(self, tmp_path) -> None:
        with pytest.raises(FileNotFoundError):
            query_artifacts(str(tmp_path))
