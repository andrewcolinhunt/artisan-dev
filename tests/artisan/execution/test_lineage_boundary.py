"""Protect operation ownership of matching and artifact-edge declarations."""

from __future__ import annotations

import ast
from pathlib import Path

_SOURCE_ROOT = Path(__file__).resolve().parents[3] / "src" / "artisan"


def test_generic_execution_cannot_import_the_author_matcher() -> None:
    for path in (_SOURCE_ROOT / "execution").rglob("*.py"):
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                assert node.module != "artisan.operations.lineage", path
            if isinstance(node, ast.Import):
                assert all(
                    alias.name != "artisan.operations.lineage" for alias in node.names
                ), path
            if isinstance(node, (ast.Name, ast.Attribute)):
                name = node.id if isinstance(node, ast.Name) else node.attr
                assert name != "match_outputs_to_inputs_by_stem", path


def test_artifact_edges_only_constructed_from_explicit_pairs() -> None:
    constructors = {
        "ArtifactProvenanceEdge": {"execution/lineage/enrich.py"},
        "SourceTargetPair": {
            "execution/lineage/builder.py",
            "operations/curator/declare_lineage.py",
        },
    }
    for path in _SOURCE_ROOT.rglob("*.py"):
        relative = path.relative_to(_SOURCE_ROOT).as_posix()
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            name = (
                node.func.id
                if isinstance(node.func, ast.Name)
                else (node.func.attr if isinstance(node.func, ast.Attribute) else None)
            )
            if name in constructors:
                assert relative in constructors[name], (relative, node.lineno, name)


def test_removed_inference_channels_do_not_return() -> None:
    removed = {
        "capture_lineage_metadata",
        "build_filesystem_match_map",
        "augment_match_map_from_artifacts",
        "derive_human_names",
        "output_pair_map",
        "_hydrate_inputs_for_lineage",
        "build_config_reference_edges",
    }
    for path in _SOURCE_ROOT.rglob("*.py"):
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if isinstance(node, ast.Name):
                assert node.id not in removed, (path, node.lineno)
            elif isinstance(node, ast.Attribute):
                assert node.attr not in removed, (path, node.lineno)
            elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                assert node.name not in removed, (path, node.lineno)
