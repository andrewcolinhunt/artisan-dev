"""Pytest configuration and shared fixtures for integration tests."""

from __future__ import annotations

import csv
import io
import os
import random
from pathlib import Path

import polars as pl
import pytest
from prefect.testing.utilities import prefect_test_harness


@pytest.fixture(scope="session", autouse=True)
def _prefect_harness():
    """Activate Prefect test harness and bridge PREFECT_API_URL to os.environ.

    The test harness provides an ephemeral Prefect server. We use session scope
    so each xdist worker shares a single server (instead of one per module),
    and a longer timeout to handle concurrent startup under load.
    """
    with prefect_test_harness(server_startup_timeout=60):
        from prefect.settings import PREFECT_API_URL

        url = PREFECT_API_URL.value()
        os.environ["PREFECT_API_URL"] = url
        yield
        os.environ.pop("PREFECT_API_URL", None)


@pytest.fixture
def pipeline_env(tmp_path: Path) -> dict[str, Path]:
    """Create isolated pipeline environment with Delta Lake directories.

    Args:
        tmp_path: Pytest-provided temporary directory.

    Returns:
        Dictionary with delta_root, staging_root, and working_root paths.
    """
    delta_root = tmp_path / "delta"
    staging_root = tmp_path / "staging"
    working_root = tmp_path / "working"

    delta_root.mkdir()
    staging_root.mkdir()
    working_root.mkdir()

    return {
        "delta_root": delta_root,
        "staging_root": staging_root,
        "working_root": working_root,
    }


def _make_csv(rows: int = 5, seed: int = 42) -> bytes:
    """Generate test CSV content with id, x, y, z, score columns.

    Args:
        rows: Number of data rows.
        seed: Random seed for reproducibility.

    Returns:
        CSV content as bytes.
    """
    rng = random.Random(seed)
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["id", "x", "y", "z", "score"])
    for i in range(rows):
        writer.writerow(
            [
                i,
                round(rng.uniform(0.0, 10.0), 4),
                round(rng.uniform(0.0, 10.0), 4),
                round(rng.uniform(0.0, 10.0), 4),
                round(rng.uniform(0.0, 1.0), 4),
            ]
        )
    return buf.getvalue().encode("utf-8")


@pytest.fixture
def sample_csv_files(tmp_path: Path) -> list[Path]:
    """Create 3 sample CSV files with unique content for testing.

    Each file uses a different seed so content hashes are unique.

    Args:
        tmp_path: Pytest-provided temporary directory.

    Returns:
        List of paths to CSV files.
    """
    files = []
    for i in range(3):
        path = tmp_path / f"data_{i}.csv"
        path.write_bytes(_make_csv(rows=5, seed=100 + i))
        files.append(path)
    return files


# =============================================================================
# Assertion Helper Functions
# =============================================================================


def read_table(delta_root: Path, table_name: str) -> pl.DataFrame:
    """Read a Delta Lake table, return empty DataFrame if not exists.

    Args:
        delta_root: Root directory for Delta Lake tables.
        table_name: Name of the table to read.

    Returns:
        Polars DataFrame with table contents, or empty DataFrame.
    """
    table_path = delta_root / table_name
    if not table_path.exists():
        return pl.DataFrame()
    return pl.read_delta(str(table_path))


def count_artifacts_by_step(delta_root: Path, step_number: int) -> int:
    """Count artifacts produced by a specific step.

    Queries: artifact_index WHERE origin_step_number = step_number

    Args:
        delta_root: Root directory for Delta Lake tables.
        step_number: Step number to count artifacts for.

    Returns:
        Number of artifacts produced by the step.
    """
    df_index = read_table(delta_root, "artifacts/index")
    if df_index.is_empty():
        return 0
    return df_index.filter(pl.col("origin_step_number") == step_number).height


def count_artifacts_by_type(delta_root: Path, artifact_type: str) -> int:
    """Count artifacts of a specific type.

    Queries: artifact_index WHERE artifact_type = artifact_type

    Args:
        delta_root: Root directory for Delta Lake tables.
        artifact_type: Artifact type to count.

    Returns:
        Number of artifacts of the specified type.
    """
    df_index = read_table(delta_root, "artifacts/index")
    if df_index.is_empty():
        return 0
    return df_index.filter(pl.col("artifact_type") == artifact_type).height


def get_execution_outputs(delta_root: Path, step_number: int, role: str) -> list[str]:
    """Get output artifact IDs for a step/role.

    Steps:
    1. Query executions for execution_run_id WHERE origin_step_number = step_number
    2. Query execution_edges for artifact_id WHERE execution_run_id IN (...)
       AND direction = 'output' AND role = role

    Args:
        delta_root: Root directory for Delta Lake tables.
        step_number: Step number to query.
        role: Output role name.

    Returns:
        List of artifact IDs.
    """
    df_exec = read_table(delta_root, "orchestration/executions")
    df_prov = read_table(delta_root, "provenance/execution_edges")

    if df_exec.is_empty() or df_prov.is_empty():
        return []

    exec_ids = df_exec.filter(pl.col("origin_step_number") == step_number)[
        "execution_run_id"
    ].to_list()

    return df_prov.filter(
        pl.col("execution_run_id").is_in(exec_ids)
        & (pl.col("direction") == "output")
        & (pl.col("role") == role)
    )["artifact_id"].to_list()


def get_execution_inputs(delta_root: Path, step_number: int, role: str) -> list[str]:
    """Get input artifact IDs for a step/role.

    Same as get_execution_outputs but with direction = 'input'.

    Args:
        delta_root: Root directory for Delta Lake tables.
        step_number: Step number to query.
        role: Input role name.

    Returns:
        List of artifact IDs.
    """
    df_exec = read_table(delta_root, "orchestration/executions")
    df_prov = read_table(delta_root, "provenance/execution_edges")

    if df_exec.is_empty() or df_prov.is_empty():
        return []

    exec_ids = df_exec.filter(pl.col("origin_step_number") == step_number)[
        "execution_run_id"
    ].to_list()

    return df_prov.filter(
        pl.col("execution_run_id").is_in(exec_ids)
        & (pl.col("direction") == "input")
        & (pl.col("role") == role)
    )["artifact_id"].to_list()


def count_executions_by_step(delta_root: Path, step_number: int) -> int:
    """Count execution records for a specific step.

    Queries: executions WHERE origin_step_number = step_number
    Used for batch processing validation.

    Args:
        delta_root: Root directory for Delta Lake tables.
        step_number: Step number to count executions for.

    Returns:
        Number of execution records for the step.
    """
    df_exec = read_table(delta_root, "orchestration/executions")
    if df_exec.is_empty():
        return 0
    return df_exec.filter(pl.col("origin_step_number") == step_number).height
