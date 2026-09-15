"""Tests for artisan_get_provenance_graph and the lineage resource."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import polars as pl
from fixtures.store_format import commit_test_tables, publish_test_store
from fsspec.implementations.local import LocalFileSystem

from artisan.schemas.enums import TablePath
from artisan.storage.core.table_schemas import ARTIFACT_EDGES_SCHEMA

A, B, C = "a" * 32, "b" * 32, "c" * 32


def _seed_edges(root: Path, pairs: list[tuple[str, str]]) -> None:
    n = len(pairs)
    commit_test_tables(
        str(root),
        str(root / "staging"),
        LocalFileSystem(),
        {
            TablePath.ARTIFACT_EDGES.value: pl.DataFrame(
                {
                    "execution_run_id": ["run"] * n,
                    "source_artifact_id": [p[0] for p in pairs],
                    "target_artifact_id": [p[1] for p in pairs],
                    "source_artifact_type": ["data"] * n,
                    "target_artifact_type": ["data"] * n,
                    "source_role": ["input"] * n,
                    "target_role": ["output"] * n,
                    "group_id": [None] * n,
                    "step_boundary": [True] * n,
                },
                schema=ARTIFACT_EDGES_SCHEMA,
            )
        },
        step_run_id="seed-provenance-edges",
        operation_name="seed_provenance",
    )


class TestProvenanceTool:
    def test_backward_walk(self, make_app, invoke, tmp_path) -> None:
        _seed_edges(tmp_path, [(A, B), (B, C)])
        app = make_app(delta_root=tmp_path)
        result = invoke(app, "artisan_get_provenance_graph", {"artifact_id": C})
        assert result["direction"] == "backward"
        assert result["edges"] == [
            {"source_artifact_id": B, "target_artifact_id": C},
            {"source_artifact_id": A, "target_artifact_id": B},
        ]
        assert result["truncated"] is False

    def test_forward_depth_truncation(self, make_app, invoke, tmp_path) -> None:
        _seed_edges(tmp_path, [(A, B), (B, C)])
        app = make_app(delta_root=tmp_path)
        result = invoke(
            app,
            "artisan_get_provenance_graph",
            {"artifact_id": A, "direction": "forward", "depth": 1},
        )
        assert result["edges"] == [{"source_artifact_id": A, "target_artifact_id": B}]
        assert result["truncated"] is True

    def test_missing_table_degrades_to_empty(self, make_app, invoke, tmp_path) -> None:
        publish_test_store(str(tmp_path), LocalFileSystem())
        app = make_app(delta_root=tmp_path)
        result = invoke(app, "artisan_get_provenance_graph", {"artifact_id": A})
        assert result["edges"] == []

    def test_rejects_unknown_direction(self, make_app, invoke_result, tmp_path) -> None:
        result = invoke_result(
            make_app(delta_root=tmp_path),
            "artisan_get_provenance_graph",
            {"artifact_id": A, "direction": "sideways"},
        )
        assert result.is_error is True

    def test_rejects_out_of_range_depth(self, make_app, invoke, tmp_path) -> None:
        app = make_app(delta_root=tmp_path)
        for depth in (0, 11):
            result = invoke(
                app,
                "artisan_get_provenance_graph",
                {"artifact_id": A, "depth": depth},
            )
            assert result["code"] == "param_type_mismatch"
            assert result["field"] == "depth"


class TestLineageResource:
    def test_returns_dot_source(self, make_app, read_resource, seeded_run) -> None:
        app = make_app(delta_root=seeded_run.delta_root)
        content = read_resource(app, f"artisan://lineage/run/{seeded_run.run_id}")
        assert "digraph" in content.text

    def test_unavailable_graph_is_sanitized(self, make_app, read_resource) -> None:
        content = read_resource(make_app(), "artisan://lineage/run/missing")

        assert "Lineage is unavailable" in content.text
        assert "Traceback" not in content.text

    def test_passes_resource_run_to_macro_graph(
        self, make_app, read_resource, monkeypatch, tmp_path
    ) -> None:
        from artisan.visualization.graph import macro

        seen: list[str] = []

        def build_graph(_root: str, *, pipeline_run_id: str) -> SimpleNamespace:
            seen.append(pipeline_run_id)
            return SimpleNamespace(source="digraph pipeline {}")

        monkeypatch.setattr(macro, "build_macro_graph", build_graph)

        content = read_resource(
            make_app(delta_root=tmp_path), "artisan://lineage/run/requested-run"
        )

        assert "digraph pipeline" in content.text
        assert seen == ["requested-run"]

    def test_oversized_graph_returns_bounded_dot(
        self, make_app, read_resource, monkeypatch, tmp_path
    ) -> None:
        from artisan.visualization.graph import macro

        monkeypatch.setattr(
            macro,
            "build_macro_graph",
            lambda _root, *, pipeline_run_id: SimpleNamespace(source="x" * 100_000),
        )

        content = read_resource(
            make_app(delta_root=tmp_path), "artisan://lineage/run/large-run"
        )

        assert "exceeds the MCP resource limit" in content.text
        assert len(content.text) < 1_000
