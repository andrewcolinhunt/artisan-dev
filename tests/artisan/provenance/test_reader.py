"""Bounded provenance reads over real retained direct and collapsed relations."""

from __future__ import annotations

from pathlib import Path

import pytest
from fixtures.retained_inspection import commit_inspection_attempt, inspection_store

from artisan.provenance import provenance_edges
from artisan.schemas import CSVArtifact


@pytest.fixture(scope="module")
def chain(tmp_path_factory: pytest.TempPathFactory) -> tuple[Path, list[str]]:
    root = tmp_path_factory.mktemp("retained-provenance") / "delta"
    artifacts = [
        CSVArtifact.draft(
            f"value\n{index}\n".encode(), f"data-{index}.csv", index
        ).finalize()
        for index in range(4)
    ]
    for index, artifact in enumerate(artifacts):
        commit_inspection_attempt(
            root,
            number=index,
            inputs={} if not index else {"source": [artifacts[index - 1]]},
            outputs={"data": [artifact]},
        )
    return root, [artifact.artifact_id for artifact in artifacts]


def test_backward_full_chain(chain: tuple[Path, list[str]]) -> None:
    root, (a, b, c, d) = chain
    result = provenance_edges(str(root), d, direction="backward", depth=3)
    assert result.edges == [
        {"source_artifact_id": c, "target_artifact_id": d},
        {"source_artifact_id": b, "target_artifact_id": c},
        {"source_artifact_id": a, "target_artifact_id": b},
    ]
    assert result.truncated is False
    assert (result.artifact_id, result.direction, result.depth) == (d, "backward", 3)


def test_backward_depth_bound_truncates(chain: tuple[Path, list[str]]) -> None:
    root, (_, _, c, d) = chain
    result = provenance_edges(str(root), d, direction="backward", depth=1)
    assert result.edges == [{"source_artifact_id": c, "target_artifact_id": d}]
    assert result.truncated is True


def test_forward_walk(chain: tuple[Path, list[str]]) -> None:
    root, (a, b, c, _) = chain
    result = provenance_edges(str(root), a, direction="forward", depth=2)
    assert result.edges == [
        {"source_artifact_id": a, "target_artifact_id": b},
        {"source_artifact_id": b, "target_artifact_id": c},
    ]
    assert result.truncated is True
    assert result.direction == "forward"


def test_unknown_artifact_is_empty(chain: tuple[Path, list[str]]) -> None:
    root, _ = chain
    result = provenance_edges(str(root), "f" * 32)
    assert result.edges == []
    assert result.truncated is False


def test_initialized_empty_table_is_empty(tmp_path: Path) -> None:
    inspection_store(tmp_path)
    result = provenance_edges(str(tmp_path), "a" * 32)
    assert result.edges == []
    assert result.truncated is False
