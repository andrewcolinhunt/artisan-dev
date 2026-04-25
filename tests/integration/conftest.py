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
from fsspec import AbstractFileSystem
from prefect.testing.utilities import prefect_test_harness
from pydantic import BaseModel, Field

from artisan.operations.base.operation_definition import OperationDefinition
from artisan.schemas import ArtifactResult
from artisan.schemas.artifact.data import DataArtifact
from artisan.schemas.execution.batch_strategy import BatchStrategy
from artisan.schemas.operation_config.runner_resources import RunnerResources
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
def pipeline_env(tmp_path: Path) -> dict[str, str]:
    """Create isolated pipeline environment with Delta Lake directories.

    Args:
        tmp_path: Pytest-provided temporary directory.

    Returns:
        Dictionary with delta_root, staging_root, and working_root as strings.
    """
    delta_root = tmp_path / "delta"
    staging_root = tmp_path / "staging"
    working_root = tmp_path / "working"

    delta_root.mkdir()
    staging_root.mkdir()
    working_root.mkdir()

    return {
        "delta_root": str(delta_root),
        "staging_root": str(staging_root),
        "working_root": str(working_root),
    }


@pytest.fixture
def s3_pipeline_env(tmp_path: Path, s3_fs) -> dict:
    """Cloud-backed pipeline environment on the per-test MinIO bucket.

    Mirrors :func:`pipeline_env` but returns ``delta_root``,
    ``staging_root``, and ``files_root`` as ``s3://`` URIs plus the
    populated ``StorageConfig``. ``working_root`` stays local — it
    holds sandboxes and (cloud-derived) failure logs that
    ``recorder._write_failure_log`` writes with ``os.makedirs``.

    Args:
        tmp_path: Pytest-provided temporary directory (used for working_root).
        s3_fs: ``(fs, storage_config, uri_prefix)`` from the session MinIO.

    Returns:
        Dict with ``delta_root``, ``staging_root``, ``files_root``,
        ``working_root``, and ``storage`` (the ``StorageConfig``).
    """
    fs, storage, uri_prefix = s3_fs
    working_root = tmp_path / "working"
    working_root.mkdir()
    return {
        "delta_root": f"{uri_prefix}/delta",
        "staging_root": f"{uri_prefix}/staging",
        "files_root": f"{uri_prefix}/files",
        "working_root": str(working_root),
        "storage": storage,
        "fs": fs,
        "uri_prefix": uri_prefix,
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


def read_table(
    delta_root: str,
    table_name: str,
    *,
    fs: AbstractFileSystem | None = None,
    storage_options: dict | None = None,
) -> pl.DataFrame:
    """Read a Delta Lake table, return empty DataFrame if not exists.

    ``fs`` and ``storage_options`` travel as a pair. For cloud
    ``delta_root`` (URI), callers must pass ``fs``; ``storage_options``
    should be ``env["storage"].delta_storage_options()`` from the
    fixture, which may be ``None`` for production IAM/env-var auth.
    Local callers pass nothing and behave identically to today.

    Args:
        delta_root: Root directory or URI for Delta Lake tables.
        table_name: Name of the table to read.
        fs: fsspec filesystem. Required when ``delta_root`` is a URI.
        storage_options: Delta-rs storage options forwarded to
            ``pl.read_delta``. ``None`` is valid on cloud URIs — delta-rs
            will read credentials from env vars / IAM role.

    Returns:
        Polars DataFrame with table contents, or empty DataFrame when
        the table doesn't exist.

    Raises:
        ValueError: When ``delta_root`` is a URI but ``fs`` is None.
    """
    if "://" in delta_root:
        if fs is None:
            msg = f"fs required for cloud delta_root {delta_root!r}"
            raise ValueError(msg)
        table_uri = f"{delta_root.rstrip('/')}/{table_name.lstrip('/')}"
        if not fs.exists(table_uri):
            return pl.DataFrame()
    else:
        table_uri = os.path.join(delta_root, table_name)
        if not os.path.exists(table_uri):
            return pl.DataFrame()
    return pl.read_delta(table_uri, storage_options=storage_options)


def count_artifacts_by_step(
    delta_root: str,
    step_number: int,
    *,
    fs: AbstractFileSystem | None = None,
    storage_options: dict | None = None,
) -> int:
    """Count artifacts produced by a specific step.

    Queries: artifact_index WHERE origin_step_number = step_number

    Args:
        delta_root: Root directory for Delta Lake tables.
        step_number: Step number to count artifacts for.
        fs: fsspec filesystem. Required when ``delta_root`` is a URI.
        storage_options: Delta-rs storage options.

    Returns:
        Number of artifacts produced by the step.
    """
    df_index = read_table(
        delta_root, "artifacts/index", fs=fs, storage_options=storage_options
    )
    if df_index.is_empty():
        return 0
    return df_index.filter(pl.col("origin_step_number") == step_number).height


def count_artifacts_by_type(
    delta_root: str,
    artifact_type: str,
    *,
    fs: AbstractFileSystem | None = None,
    storage_options: dict | None = None,
) -> int:
    """Count artifacts of a specific type.

    Queries: artifact_index WHERE artifact_type = artifact_type

    Args:
        delta_root: Root directory for Delta Lake tables.
        artifact_type: Artifact type to count.
        fs: fsspec filesystem. Required when ``delta_root`` is a URI.
        storage_options: Delta-rs storage options.

    Returns:
        Number of artifacts of the specified type.
    """
    df_index = read_table(
        delta_root, "artifacts/index", fs=fs, storage_options=storage_options
    )
    if df_index.is_empty():
        return 0
    return df_index.filter(pl.col("artifact_type") == artifact_type).height


def get_execution_outputs(
    delta_root: str,
    step_number: int,
    role: str,
    *,
    fs: AbstractFileSystem | None = None,
    storage_options: dict | None = None,
) -> list[str]:
    """Get output artifact IDs for a step/role.

    Steps:
    1. Query executions for execution_run_id WHERE origin_step_number = step_number
    2. Query execution_edges for artifact_id WHERE execution_run_id IN (...)
       AND direction = 'output' AND role = role

    Args:
        delta_root: Root directory for Delta Lake tables.
        step_number: Step number to query.
        role: Output role name.
        fs: fsspec filesystem. Required when ``delta_root`` is a URI.
        storage_options: Delta-rs storage options.

    Returns:
        List of artifact IDs.
    """
    df_exec = read_table(
        delta_root, "orchestration/executions", fs=fs, storage_options=storage_options
    )
    df_prov = read_table(
        delta_root,
        "provenance/execution_edges",
        fs=fs,
        storage_options=storage_options,
    )

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


def get_execution_inputs(
    delta_root: str,
    step_number: int,
    role: str,
    *,
    fs: AbstractFileSystem | None = None,
    storage_options: dict | None = None,
) -> list[str]:
    """Get input artifact IDs for a step/role.

    Same as get_execution_outputs but with direction = 'input'.

    Args:
        delta_root: Root directory for Delta Lake tables.
        step_number: Step number to query.
        role: Input role name.
        fs: fsspec filesystem. Required when ``delta_root`` is a URI.
        storage_options: Delta-rs storage options.

    Returns:
        List of artifact IDs.
    """
    df_exec = read_table(
        delta_root, "orchestration/executions", fs=fs, storage_options=storage_options
    )
    df_prov = read_table(
        delta_root,
        "provenance/execution_edges",
        fs=fs,
        storage_options=storage_options,
    )

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


def count_executions_by_step(
    delta_root: str,
    step_number: int,
    *,
    fs: AbstractFileSystem | None = None,
    storage_options: dict | None = None,
) -> int:
    """Count execution records for a specific step.

    Queries: executions WHERE origin_step_number = step_number
    Used for batch processing validation.

    Args:
        delta_root: Root directory for Delta Lake tables.
        step_number: Step number to count executions for.
        fs: fsspec filesystem. Required when ``delta_root`` is a URI.
        storage_options: Delta-rs storage options.

    Returns:
        Number of execution records for the step.
    """
    df_exec = read_table(
        delta_root, "orchestration/executions", fs=fs, storage_options=storage_options
    )
    if df_exec.is_empty():
        return 0
    return df_exec.filter(pl.col("origin_step_number") == step_number).height


def get_artifact_edges(
    delta_root: str,
    source_id: str,
    *,
    fs: AbstractFileSystem | None = None,
    storage_options: dict | None = None,
) -> list[str]:
    """Get target artifact IDs linked from a source artifact.

    Args:
        delta_root: Root directory for Delta Lake tables.
        source_id: Source artifact ID.
        fs: fsspec filesystem. Required when ``delta_root`` is a URI.
        storage_options: Delta-rs storage options.

    Returns:
        List of target artifact IDs.
    """
    df = read_table(
        delta_root,
        "provenance/artifact_edges",
        fs=fs,
        storage_options=storage_options,
    )
    if df.is_empty():
        return []
    return df.filter(pl.col("source_artifact_id") == source_id)[
        "target_artifact_id"
    ].to_list()


def get_step_status(
    delta_root: str,
    step_number: int,
    *,
    fs: AbstractFileSystem | None = None,
    storage_options: dict | None = None,
) -> str | None:
    """Get the latest status for a step.

    Args:
        delta_root: Root directory for Delta Lake tables.
        step_number: Step number to query.
        fs: fsspec filesystem. Required when ``delta_root`` is a URI.
        storage_options: Delta-rs storage options.

    Returns:
        Latest status string, or None if not found.
    """
    df = read_table(
        delta_root, "orchestration/steps", fs=fs, storage_options=storage_options
    )
    if df.is_empty():
        return None
    filtered = df.filter(pl.col("step_number") == step_number).sort(
        "timestamp", descending=True
    )
    if filtered.is_empty():
        return None
    return filtered["status"][0]


def get_failed_executions(
    delta_root: str,
    step_number: int,
    *,
    fs: AbstractFileSystem | None = None,
    storage_options: dict | None = None,
) -> int:
    """Count failed executions for a step.

    Args:
        delta_root: Root directory for Delta Lake tables.
        step_number: Step number to query.
        fs: fsspec filesystem. Required when ``delta_root`` is a URI.
        storage_options: Delta-rs storage options.

    Returns:
        Number of failed executions.
    """
    df_exec = read_table(
        delta_root, "orchestration/executions", fs=fs, storage_options=storage_options
    )
    if df_exec.is_empty():
        return 0
    return df_exec.filter(
        (pl.col("origin_step_number") == step_number) & (pl.col("success") == False)  # noqa: E712
    ).height


def get_successful_executions(
    delta_root: str,
    step_number: int,
    *,
    fs: AbstractFileSystem | None = None,
    storage_options: dict | None = None,
) -> int:
    """Count successful executions for a step.

    Args:
        delta_root: Root directory for Delta Lake tables.
        step_number: Step number to query.
        fs: fsspec filesystem. Required when ``delta_root`` is a URI.
        storage_options: Delta-rs storage options.

    Returns:
        Number of successful executions.
    """
    df_exec = read_table(
        delta_root, "orchestration/executions", fs=fs, storage_options=storage_options
    )
    if df_exec.is_empty():
        return 0
    return df_exec.filter(
        (pl.col("origin_step_number") == step_number) & (pl.col("success") == True)  # noqa: E712
    ).height


@pytest.fixture
def dual_pipeline_env(tmp_path: Path) -> dict[str, dict[str, str]]:
    """Create two isolated pipeline environments for cross-pipeline tests.

    Args:
        tmp_path: Pytest-provided temporary directory.

    Returns:
        Dict with keys "a" and "b", each containing delta_root,
        staging_root, and working_root as strings.
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
            "delta_root": str(delta),
            "staging_root": str(staging),
            "working_root": str(working),
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
    runner_resources: RunnerResources = RunnerResources(time_limit="00:10:00")
    batch_strategy: BatchStrategy = BatchStrategy(job_name="failing_transformer")

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        """Extract materialized paths and original names from input artifacts."""
        prepared = {}
        for role, artifacts in inputs.input_artifacts.items():
            prepared[role] = [a.materialized_path for a in artifacts]
            # Pass original names for index extraction (filenames are artifact_ids)
            prepared[f"{role}_names"] = [a.original_name for a in artifacts]
        return prepared

    def execute(self, inputs: ExecuteInput) -> dict[str, Any]:
        """Transform CSV (prepend marker line) with controllable failure injection."""
        output_dir = inputs.execute_dir
        os.makedirs(output_dir, exist_ok=True)

        dataset_input = inputs.inputs.get("dataset")
        if dataset_input is None:
            msg = "No dataset input provided"
            raise ValueError(msg)

        if isinstance(dataset_input, (str, Path)):
            input_files = [Path(dataset_input)]
        else:
            input_files = [Path(f) for f in dataset_input]

        # Use original names to extract dataset index for failure injection
        original_names = inputs.inputs.get("dataset_names", [])
        for file_idx, input_path in enumerate(input_files):
            input_path = Path(input_path)
            stem = input_path.stem

            # Extract index from original_name (e.g. "dataset_1" -> 1)
            orig_name = (
                original_names[file_idx] if file_idx < len(original_names) else ""
            )
            match = re.search(r"dataset_(\d+)", orig_name or "")
            index = int(match.group(1)) if match else -1

            if self.params.fail_on_all:
                msg = f"Intentional failure on {stem}"
                raise ValueError(msg)
            if index == self.params.fail_on_index:
                msg = f"Intentional failure on index {index}"
                raise ValueError(msg)

            # Scale numeric columns by 1.1 to produce different content
            with open(input_path) as f:
                reader = csv.DictReader(f)
                headers = list(reader.fieldnames or [])
                rows = list(reader)

            out_path = os.path.join(output_dir, f"{stem}_0.csv")
            with open(out_path, "w", newline="") as f:
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
            if file_path.endswith(".csv"):
                with open(file_path, "rb") as f:
                    content = f.read()
                drafts.append(
                    DataArtifact.draft(
                        content=content,
                        original_name=os.path.basename(file_path),
                        step_number=inputs.step_number,
                    )
                )
        return ArtifactResult(
            success=True,
            artifacts={"dataset": drafts},
            metadata={"operation": "failing_transformer"},
        )
