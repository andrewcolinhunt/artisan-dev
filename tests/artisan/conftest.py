"""Root conftest for artisan tests."""

from __future__ import annotations

from pathlib import Path

import pytest


@pytest.fixture
def seed_artifact_edges():
    """Return a seeder writing an artifact_edges table from id pairs."""

    def _seed(root: Path, pairs: list[tuple[str, str]]) -> None:
        import polars as pl

        from artisan.schemas.enums import TablePath
        from artisan.storage.core.table_schemas import ARTIFACT_EDGES_SCHEMA

        n = len(pairs)
        df = pl.DataFrame(
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
        df.write_delta(str(root / TablePath.ARTIFACT_EDGES))

    return _seed
