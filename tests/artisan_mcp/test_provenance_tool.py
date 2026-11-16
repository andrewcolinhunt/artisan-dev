"""Tests for artisan_get_provenance_graph and the lineage resource."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from fixtures.retained_inspection import commit_inspection_attempt
from fixtures.store_format import publish_test_store
from fsspec.implementations.local import LocalFileSystem

from artisan.schemas import CSVArtifact

_CHAIN = tuple(
    CSVArtifact.draft(
        f"value\n{index}\n".encode(), f"chain-{index}.csv", index
    ).finalize()
    for index in range(3)
)
A, B, C = (artifact.artifact_id for artifact in _CHAIN)


def _seed_edges(root: Path, pairs: list[tuple[str, str]]) -> None:
    assert pairs == [(A, B), (B, C)]
    for index, artifact in enumerate(_CHAIN):
        commit_inspection_attempt(
            root,
            number=index,
            outputs={"data": [artifact]},
            inputs={} if not index else {"source": [_CHAIN[index - 1]]},
        )


class TestProvenanceTool:
    def test_backward_walk(self, make_app, invoke, tmp_path) -> None:
        _seed_edges(tmp_path, [(A, B), (B, C)])
        app = make_app(delta_root=tmp_path)
        result = invoke(app, "artisan_get_provenance_graph", {"artifact_id": C})
        assert result["direction"] == "backward"
        assert result["edges"]["items"] == [
            {"source_artifact_id": B, "target_artifact_id": C},
            {"source_artifact_id": A, "target_artifact_id": B},
        ]
        assert result["traversal_truncated"] is False

    def test_forward_depth_truncation(self, make_app, invoke, tmp_path) -> None:
        _seed_edges(tmp_path, [(A, B), (B, C)])
        app = make_app(delta_root=tmp_path)
        result = invoke(
            app,
            "artisan_get_provenance_graph",
            {"artifact_id": A, "direction": "forward", "depth": 1},
        )
        assert result["edges"]["items"] == [
            {"source_artifact_id": A, "target_artifact_id": B}
        ]
        assert result["traversal_truncated"] is True

    def test_valid_empty_store_returns_no_edges(
        self, make_app, invoke, tmp_path
    ) -> None:
        publish_test_store(str(tmp_path), LocalFileSystem())
        app = make_app(delta_root=tmp_path)
        result = invoke(app, "artisan_get_provenance_graph", {"artifact_id": A})
        assert result["edges"]["items"] == []

    def test_rejects_unknown_direction(self, make_app, invoke_result, tmp_path) -> None:
        result = invoke_result(
            make_app(delta_root=tmp_path),
            "artisan_get_provenance_graph",
            {"artifact_id": A, "direction": "sideways"},
        )
        assert result.is_error is True

    def test_rejects_out_of_range_depth(
        self, make_app, invoke_result, tmp_path
    ) -> None:
        app = make_app(delta_root=tmp_path)
        for depth in (0, 11):
            result = invoke_result(
                app,
                "artisan_get_provenance_graph",
                {"artifact_id": A, "depth": depth},
            )
            assert result.is_error is True
            assert result.structured_content is None


class TestLineageResource:
    def test_returns_dot_source(self, make_app, read_resource, seeded_run) -> None:
        app = make_app(delta_root=seeded_run.delta_root)
        content = read_resource(app, f"artisan://lineage/run/{seeded_run.run_id}")
        assert "digraph" in content.text

    def test_unavailable_graph_is_sanitized(self, make_app, read_resource) -> None:
        content = read_resource(make_app(), "artisan://lineage/run/missing")

        assert "Macro lineage unavailable" in content.text
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

        assert "response exceeds the byte budget" in content.text
        assert len(content.text) < 1_000


def test_wide_provenance_pages_preserve_depth_omission(monkeypatch):
    import artisan.provenance
    from artisan.inspection.provenance import provenance_payload
    from artisan.provenance.reader import ProvenanceEdges

    edges = [
        {"source_artifact_id": f"parent-{index:03d}", "target_artifact_id": C}
        for index in range(175)
    ]
    result = ProvenanceEdges(
        artifact_id=C,
        direction="backward",
        depth=1,
        edges=list(reversed(edges)),
        truncated=True,
    )
    monkeypatch.setattr(
        artisan.provenance, "provenance_edges", lambda *args, **kwargs: result
    )
    seen = []
    cursor = None
    while True:
        payload = provenance_payload("/store", C, depth=1, limit=37, cursor=cursor)
        assert payload["traversal_truncated"] is True
        assert payload["edges"]["total_items"] == len(edges)
        seen.extend(payload["edges"]["items"])
        cursor = payload["edges"]["next_cursor"]
        if cursor is None:
            assert payload["edges"]["has_more"] is False
            break
    assert seen == edges
