"""Macro layout is pure; public acquisition uses exact retained attempts."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Literal

import graphviz
import polars as pl
import pytest
from fixtures.retained_inspection import commit_inspection_attempt, inspection_store

from artisan.visualization.graph import build_macro_graph, render_macro_graph
from artisan.visualization.graph._styles import render_graph
from artisan.visualization.graph.macro import _build_macro_graph, _parse_input_refs


def _ref(number: int, role: str) -> dict[str, object]:
    return {"source_step": number, "step_run_id": f"attempt-{number}", "role": role}


def _frame(*, passthrough: bool = False, multiple: bool = False) -> pl.DataFrame:
    """Supply valid rendering inputs, independently of acceptance authority."""
    return pl.DataFrame(
        [
            {
                "step_run_id": "attempt-0",
                "step_number": 0,
                "step_name": "Ingest",
                "input_refs_json": "{}",
                "output_roles_json": '["file"]',
                "output_types_json": '{"file":"file_ref"}',
            },
            {
                "step_run_id": "attempt-1",
                "step_number": 1,
                "step_name": "Filter" if passthrough else "Transform",
                "input_refs_json": json.dumps({"input": [_ref(0, "file")]}),
                "output_roles_json": '["data"]',
                "output_types_json": json.dumps(
                    {"data": None if passthrough else "csv"}
                ),
            },
            {
                "step_run_id": "attempt-2",
                "step_number": 2,
                "step_name": "Merge" if multiple else "Metrics",
                "input_refs_json": json.dumps(
                    {
                        "input": [_ref(0, "file"), _ref(1, "data")]
                        if multiple
                        else [_ref(1, "data")]
                    }
                ),
                "output_roles_json": '["metrics"]',
                "output_types_json": '{"metrics":"metric"}',
            },
        ]
    )


@pytest.mark.parametrize(
    ("encoded", "expected"),
    [
        (json.dumps({"data": [_ref(1, "data")]}), [(1, "data", "attempt-1")]),
        (
            json.dumps({"data": [_ref(0, "file"), _ref(1, "data")]}),
            [(0, "file", "attempt-0"), (1, "data", "attempt-1")],
        ),
        (
            json.dumps({"data": [_ref(0, "file")], "config": ["artifact-id"]}),
            [(0, "file", "attempt-0")],
        ),
        (json.dumps({"paths": ["/data/input.csv"]}), []),
        ("null", []),
        ("", []),
    ],
)
def test_parse_input_refs_keeps_exact_attempts(
    encoded: str, expected: list[tuple[int, str, str]]
) -> None:
    assert _parse_input_refs(encoded) == expected


def test_empty_pipeline_has_no_real_nodes() -> None:
    graph = _build_macro_graph(_frame().clear())
    assert isinstance(graph, graphviz.Digraph)
    assert "digraph" in graph.source
    assert "exec_0" not in graph.source


@pytest.mark.parametrize(
    "expected",
    [
        "(0) Ingest",
        "(1) Transform",
        "(2) Metrics",
        "data_0_file",
        "data_1_data",
        "data_2_metrics",
        "->",
        "fillcolor=lightgray",
        '"#',
        "rankdir=LR",
        "_anchor_exec_",
        "style=invis",
    ],
)
def test_macro_layout_preserves_nodes_roles_and_columns(expected: str) -> None:
    assert expected in _build_macro_graph(_frame()).source


def test_passthrough_node_uses_passthrough_colour() -> None:
    assert "#E0F0FF" in _build_macro_graph(_frame(passthrough=True)).source


def test_multiple_inputs_keep_two_incoming_edges() -> None:
    lines = _build_macro_graph(_frame(multiple=True)).source.splitlines()
    assert (
        len(
            [
                line
                for line in lines
                if "->" in line and "exec_2" in line and "invis" not in line
            ]
        )
        == 3
    )


def test_prior_attempt_reference_never_points_to_current_output() -> None:
    rows = _frame().with_columns(
        pl.when(pl.col("step_number") == 0)
        .then(pl.lit("new-attempt"))
        .otherwise(pl.col("step_run_id"))
        .alias("step_run_id")
    )
    graph = _build_macro_graph(rows).source
    assert '"data_0_attempt-0_file" -> exec_1' in graph
    assert "data_0_file -> exec_1" not in graph
    assert "file (prior attempt)" in graph


@pytest.mark.parametrize("format", ["svg", "png"])
def test_render_preserves_format_and_creates_directories(
    tmp_path: Path, format: Literal["svg", "png"]
) -> None:
    path = render_graph(
        _build_macro_graph(_frame()), str(tmp_path / "nested" / "macro"), format
    )
    assert isinstance(path, str)
    assert Path(path).is_file()
    assert path.endswith(f".{format}")
    if format == "png":
        assert Path(path).read_bytes()[:8] == b"\x89PNG\r\n\x1a\n"


def test_public_macro_selects_current_run_and_exact_prior_reference(
    tmp_path: Path,
) -> None:
    root = tmp_path / "delta"
    first, _ = commit_inspection_attempt(
        root,
        run_id="one",
        name="old",
        outputs={"data": []},
        output_types={"data": "csv"},
    )
    commit_inspection_attempt(
        root,
        run_id="one",
        number=1,
        name="consumer",
        input_refs={
            "input": [{"source_step": 0, "step_run_id": first, "role": "data"}]
        },
    )
    commit_inspection_attempt(
        root,
        run_id="one",
        name="new",
        outputs={"data": []},
        output_types={"data": "csv"},
    )
    commit_inspection_attempt(root, run_id="other", name="unrelated")
    source = build_macro_graph(str(root), pipeline_run_id="one").source
    assert "(0) new" in source
    assert "consumer" in source
    assert "unrelated" not in source
    assert f"data_0_{first}_data -> exec_1" in source
    assert "data_0_data -> exec_1" not in source
    assert "unrelated" in build_macro_graph(str(root)).source


def test_public_macro_masks_previous_success_with_pending_attempt(
    tmp_path: Path,
) -> None:
    root = tmp_path / "delta"
    commit_inspection_attempt(root, name="previous")
    commit_inspection_attempt(root, name="pending", status="pending")
    source = build_macro_graph(str(root)).source
    assert "previous" not in source
    assert "pending" not in source


def test_public_render_accepts_initialized_empty_store(tmp_path: Path) -> None:
    root = tmp_path / "delta"
    inspection_store(root)
    output = render_macro_graph(str(root), str(tmp_path / "graph"))
    assert Path(output).is_file()
