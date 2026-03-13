"""Pytest configuration and shared fixtures for integration tests."""

from __future__ import annotations

import csv
import os
import re
from enum import StrEnum
from pathlib import Path
from typing import Any, ClassVar

import polars as pl
import pytest
from fixtures.csv import make_csv
from prefect.testing.utilities import prefect_test_harness
from pydantic import BaseModel, Field

from artisan.operations.base.operation_definition import OperationDefinition
from artisan.schemas import ArtifactResult
from artisan.schemas.artifact.data import DataArtifact
from artisan.schemas.execution.execution_config import ExecutionConfig
from artisan.schemas.operation_config.resource_config import ResourceConfig
from artisan.schemas.specs.input_models import (
    ExecuteInput,
    PostprocessInput,
    PreprocessInput,
)
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec


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
        path.write_bytes(make_csv(rows=5, seed=100 + i))
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


def get_artifact_edges(delta_root: Path, source_id: str) -> list[str]:
    """Get target artifact IDs linked from a source artifact.

    Args:
        delta_root: Root directory for Delta Lake tables.
        source_id: Source artifact ID.

    Returns:
        List of target artifact IDs.
    """
    df = read_table(delta_root, "provenance/artifact_edges")
    if df.is_empty():
        return []
    return df.filter(pl.col("source_artifact_id") == source_id)[
        "target_artifact_id"
    ].to_list()


def get_step_status(delta_root: Path, step_number: int) -> str | None:
    """Get the latest status for a step.

    Args:
        delta_root: Root directory for Delta Lake tables.
        step_number: Step number to query.

    Returns:
        Latest status string, or None if not found.
    """
    df = read_table(delta_root, "orchestration/steps")
    if df.is_empty():
        return None
    filtered = df.filter(pl.col("step_number") == step_number).sort(
        "timestamp", descending=True
    )
    if filtered.is_empty():
        return None
    return filtered["status"][0]


def get_failed_executions(delta_root: Path, step_number: int) -> int:
    """Count failed executions for a step.

    Args:
        delta_root: Root directory for Delta Lake tables.
        step_number: Step number to query.

    Returns:
        Number of failed executions.
    """
    df_exec = read_table(delta_root, "orchestration/executions")
    if df_exec.is_empty():
        return 0
    return df_exec.filter(
        (pl.col("origin_step_number") == step_number) & (pl.col("success") == False)  # noqa: E712
    ).height


def get_successful_executions(delta_root: Path, step_number: int) -> int:
    """Count successful executions for a step.

    Args:
        delta_root: Root directory for Delta Lake tables.
        step_number: Step number to query.

    Returns:
        Number of successful executions.
    """
    df_exec = read_table(delta_root, "orchestration/executions")
    if df_exec.is_empty():
        return 0
    return df_exec.filter(
        (pl.col("origin_step_number") == step_number) & (pl.col("success") == True)  # noqa: E712
    ).height


@pytest.fixture
def dual_pipeline_env(tmp_path: Path) -> dict[str, dict[str, Path]]:
    """Create two isolated pipeline environments for cross-pipeline tests.

    Args:
        tmp_path: Pytest-provided temporary directory.

    Returns:
        Dict with keys "a" and "b", each containing delta_root,
        staging_root, and working_root paths.
    """
    envs = {}
    for label in ("a", "b"):
        base = tmp_path / label
        delta = base / "delta"
        staging = base / "staging"
        working = base / "working"
        delta.mkdir(parents=True)
        staging.mkdir()
        working.mkdir()
        envs[label] = {
            "delta_root": delta,
            "staging_root": staging,
            "working_root": working,
        }
    return envs


# =============================================================================
# Shared Test Operations
# =============================================================================


class FailingTransformer(OperationDefinition):
    """Transform CSV datasets with controllable failure injection.

    Used by error handling and cache policy tests.
    """

    name = "failing_transformer"
    description = "Transform CSV datasets with controllable failures"

    class InputRole(StrEnum):
        DATASET = "dataset"

    inputs: ClassVar[dict[str, InputSpec]] = {
        InputRole.DATASET: InputSpec(
            artifact_type="data",
            required=True,
            description="Input CSV dataset",
        ),
    }

    class OutputRole(StrEnum):
        DATASET = "dataset"

    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.DATASET: OutputSpec(
            artifact_type="data",
            description="Transformed CSV dataset",
            infer_lineage_from={"inputs": ["dataset"]},
        ),
    }

    class Params(BaseModel):
        fail_on_index: int = Field(
            default=-1, description="Fail on dataset with this index (-1 = never)"
        )
        fail_on_all: bool = Field(default=False, description="Fail on all datasets")

    params: Params = Params()
    resources: ResourceConfig = ResourceConfig(time_limit="00:10:00")
    execution: ExecutionConfig = ExecutionConfig(job_name="failing_transformer")

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        """Extract materialized paths from input artifacts."""
        return {
            role: [a.materialized_path for a in artifacts]
            for role, artifacts in inputs.input_artifacts.items()
        }

    def execute(self, inputs: ExecuteInput) -> dict[str, Any]:
        """Transform CSV (prepend marker line) with controllable failure injection."""
        output_dir = inputs.execute_dir
        output_dir.mkdir(parents=True, exist_ok=True)

        dataset_input = inputs.inputs.get("dataset")
        if dataset_input is None:
            raise ValueError("No dataset input provided")

        if isinstance(dataset_input, (str, Path)):
            input_files = [Path(dataset_input)]
        else:
            input_files = [Path(f) for f in dataset_input]

        for input_path in input_files:
            input_path = Path(input_path)
            stem = input_path.stem
            match = re.search(r"dataset_(\d+)", stem)
            index = int(match.group(1)) if match else -1

            if self.params.fail_on_all:
                raise ValueError(f"Intentional failure on {stem}")
            if index == self.params.fail_on_index:
                raise ValueError(f"Intentional failure on index {index}")

            # Scale numeric columns by 1.1 to produce different content
            with input_path.open() as f:
                reader = csv.DictReader(f)
                headers = list(reader.fieldnames or [])
                rows = list(reader)

            out_path = output_dir / f"{stem}_0.csv"
            with out_path.open("w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=headers)
                writer.writeheader()
                for row in rows:
                    new_row = dict(row)
                    for col in headers:
                        if col in ("x", "y", "z", "score"):
                            new_row[col] = round(float(row[col]) * 1.1, 4)
                    writer.writerow(new_row)

        return {}

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        """Build DataArtifact drafts from output CSV files."""
        drafts: list[DataArtifact] = []
        for file_path in inputs.file_outputs:
            if file_path.suffix == ".csv":
                drafts.append(
                    DataArtifact.draft(
                        content=file_path.read_bytes(),
                        original_name=file_path.name,
                        step_number=inputs.step_number,
                    )
                )
        return ArtifactResult(
            success=True,
            artifacts={"dataset": drafts},
            metadata={"operation": "failing_transformer"},
        )
