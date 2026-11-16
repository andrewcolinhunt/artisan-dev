"""Pytest configuration and shared fixtures for integration tests."""

from __future__ import annotations

import csv
import hashlib
import importlib.metadata
import json
import os
import re
import threading
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import replace
from enum import StrEnum
from pathlib import Path
from typing import Any, ClassVar

import polars as pl
import pytest
from fixtures.csv import make_csv
from fixtures.retained_execution import NativeSoftware
from fixtures.retained_pipeline import NativeCuratorSoftware, NativePipelineRunner
from fsspec import AbstractFileSystem
from pydantic import BaseModel, Field

from artisan.composites.base.compiled import CompiledPlan
from artisan.execution.admitted_worker import execute_admitted_assignment
from artisan.execution.assignment import Assignment
from artisan.execution.assignment_transport import (
    AdmittedAssignment,
    AssignmentDispatch,
)
from artisan.execution.composition import ExecuteAdapter
from artisan.execution.runtime_compatibility import capture_runtime_compatibility
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.operations.base.per_artifact import PerArtifact
from artisan.orchestration import PipelineManager
from artisan.orchestration.engine.retained_artifact_query import _step_exports
from artisan.orchestration.engine.retained_execution_query import _execution_membership
from artisan.orchestration.engine.retained_step_tracker import RetainedStepTracker
from artisan.schemas import ArtifactResult
from artisan.schemas.artifact.csv import CSVArtifact
from artisan.schemas.enums import FailurePolicy, TablePath
from artisan.schemas.execution.assignment import SealedDelivery
from artisan.schemas.execution.batch_strategy import BatchStrategy
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.schemas.execution.worker_transport import (
    TRANSPORT_PACKAGES,
    RuntimeCompatibility,
)
from artisan.schemas.operation_config.runner_resources import RunnerResources
from artisan.schemas.orchestration.step_state import StepState
from artisan.schemas.specs.input_models import (
    ExecuteInput,
    PostprocessInput,
    PreprocessInput,
)
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec
from artisan.storage.core.curator_view import CuratorView, capture_curator_view
from artisan.storage.core.execution_source import CuratorExecutionSource
from artisan.storage.io.retained_read import RetainedReadSnapshot
from artisan.storage.io.retained_store import open_configured_store


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


@contextmanager
def _step_read(
    delta_root: str,
    step_number: int,
    *,
    fs: AbstractFileSystem | None,
    storage_options: dict | None,
    files_root: str | None,
) -> Iterator[tuple[RetainedReadSnapshot, list[StepState]]]:
    """Select the latest fixture run's exact current attempt without hydration."""
    committer = open_configured_store(
        delta_root, files_root=files_root, fs=fs, storage_options=storage_options
    )
    with committer._read_snapshot() as read:
        tracker = RetainedStepTracker(committer, "")
        _, attempts = tracker._read_lifecycle(read)
        states = tracker._select_current_states(attempts)
        yield read, [state for state in states if state.step_number == step_number]


def load_artifact_edges(
    delta_root: str,
    target_ids: list[str] | set[str] | None,
    *,
    fs: AbstractFileSystem | None = None,
    storage_options: dict | None = None,
    files_root: str | None = None,
) -> pl.DataFrame:
    """Read declared ancestry from accepted projections and curator effects."""
    committer = open_configured_store(
        delta_root, files_root=files_root, fs=fs, storage_options=storage_options
    )
    with committer._read_snapshot() as read:
        view = CuratorView(
            committer, capture_curator_view(committer, _read=read), _read=read
        )
        edges = view.scan(TablePath.ARTIFACT_EDGES)
        if target_ids is not None:
            edges = edges.filter(pl.col("target_artifact_id").is_in(list(target_ids)))
        return edges.collect()


def get_execution_outputs(
    delta_root: str,
    step_number: int,
    role: str,
    *,
    fs: AbstractFileSystem | None = None,
    storage_options: dict | None = None,
    files_root: str | None = None,
) -> list[str]:
    """Read ordered current exports, including repeated and cached occurrences."""
    with _step_read(
        delta_root,
        step_number,
        fs=fs,
        storage_options=storage_options,
        files_root=files_root,
    ) as (read, states):
        return [
            item.artifact_id
            for state in states
            for item in _step_exports(read, state, role=role)
        ]


def get_execution_inputs(
    delta_root: str,
    step_number: int,
    role: str,
    *,
    fs: AbstractFileSystem | None = None,
    storage_options: dict | None = None,
    files_root: str | None = None,
) -> list[str]:
    """Read exact frozen input occurrences of current selected executions."""
    with _step_read(
        delta_root,
        step_number,
        fs=fs,
        storage_options=storage_options,
        files_root=files_root,
    ) as (read, states):
        ids = []
        for member in _execution_membership(read, states):
            if isinstance(member.source, CuratorExecutionSource):
                ids.extend(
                    item.artifact_id
                    for group in member.source.admission.inputs
                    if group.role == role
                    for item in group.members
                )
                continue
            boundary = member.source.admission.boundary
            inventory = {item.address: item for item in boundary.occurrences}
            ids.extend(
                inventory[address].artifact_id
                for group in boundary.boundary_inputs
                if group.role == role
                for address in group.occurrences
            )
        return ids


def count_executions_by_step(
    delta_root: str,
    step_number: int,
    *,
    fs: AbstractFileSystem | None = None,
    storage_options: dict | None = None,
    files_root: str | None = None,
) -> int:
    """Count actual selected outer deliveries, excluding current cache reuse."""
    with _step_read(
        delta_root,
        step_number,
        fs=fs,
        storage_options=storage_options,
        files_root=files_root,
    ) as (read, states):
        return sum(
            not member.cache_hit for member in _execution_membership(read, states)
        )


def get_artifact_edges(
    delta_root: str,
    source_id: str,
    *,
    fs: AbstractFileSystem | None = None,
    storage_options: dict | None = None,
    files_root: str | None = None,
) -> list[str]:
    """Read accepted content-level descendants without manufacturing physical rows."""
    committer = open_configured_store(
        delta_root, files_root=files_root, fs=fs, storage_options=storage_options
    )
    with committer._read_snapshot() as read:
        view = CuratorView(
            committer, capture_curator_view(committer, _read=read), _read=read
        )
        return (
            view.scan(TablePath.ARTIFACT_EDGES)
            .filter(pl.col("source_artifact_id") == source_id)
            .select("target_artifact_id")
            .collect()["target_artifact_id"]
            .to_list()
        )


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
    files_root: str | None = None,
) -> int:
    """Count failed terminal units; absence of worker diagnostics creates no source."""
    with _step_read(
        delta_root,
        step_number,
        fs=fs,
        storage_options=storage_options,
        files_root=files_root,
    ) as (_, states):
        return sum(state.failed_count for state in states)


def get_successful_executions(
    delta_root: str,
    step_number: int,
    *,
    fs: AbstractFileSystem | None = None,
    storage_options: dict | None = None,
    files_root: str | None = None,
) -> int:
    """Count successful selected units through their validated terminal results."""
    with _step_read(
        delta_root,
        step_number,
        fs=fs,
        storage_options=storage_options,
        files_root=files_root,
    ) as (_, states):
        return sum(state.succeeded_count for state in states)


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


def prepare_paired_files(inputs: PreprocessInput) -> dict[str, Any]:
    """Carry the operation's input IDs alongside its grouped file paths."""
    prepared = {
        role: PerArtifact([a.materialized_path for a in artifacts])
        for role, artifacts in inputs.input_artifacts.items()
    }
    roles = list(inputs.input_artifacts)
    prepared["source_ids"] = PerArtifact(
        [
            {
                role: [artifact.artifact_id]
                for role, artifact in zip(roles, group, strict=True)
            }
            for group in zip(*(inputs.input_artifacts[r] for r in roles), strict=True)
        ]
    )
    return prepared


def declared_file_result(
    inputs: PostprocessInput, role: str = "result"
) -> ArtifactResult:
    """Return drafts using only associations carried by the fixture operation."""
    result = ArtifactResult()
    for record in inputs.memory_outputs["records"]:
        path = Path(record["path"])
        result.add_artifact(
            role,
            CSVArtifact.draft(
                content=path.read_bytes(),
                original_name=record.get("name", path.name),
                step_number=inputs.step_number,
            ),
            sources=record["sources"],
        )
    return result


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
            artifact_type="csv",
            required=True,
            description="Input CSV dataset",
        ),
    }

    class OutputRole(StrEnum):
        DATASET = "dataset"

    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.DATASET: OutputSpec(
            artifact_type="csv",
            description="Transformed CSV dataset",
            derives_from={"inputs": ["dataset"]},
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
        prepared: dict[str, Any] = {}
        for role, artifacts in inputs.input_artifacts.items():
            prepared[role] = PerArtifact([a.materialized_path for a in artifacts])
            # Pass original names for index extraction (filenames are artifact_ids)
            prepared[f"{role}_names"] = PerArtifact(
                [a.original_name for a in artifacts]
            )
            prepared[f"{role}_ids"] = PerArtifact([a.artifact_id for a in artifacts])
        return prepared

    def execute_function(self, inputs: ExecuteInput) -> dict[str, Any]:
        """Scale numeric CSV columns with controllable failure injection."""
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
        records = []
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
            records.append(
                {
                    "path": out_path,
                    "name": f"{orig_name}_0.csv",
                    "sources": {"dataset": [inputs.inputs["dataset_ids"][file_idx]]},
                }
            )

        return {"records": records}

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        """Build CSVArtifact drafts from output CSV files."""
        return declared_file_result(inputs, "dataset")


class _DirectNativeSoftware(NativeSoftware):
    """Exercise the admitted worker without claiming installed software qualification."""

    @property
    def compatibility(self) -> RuntimeCompatibility:
        return _application_compatibility(self.application_modules)

    def verify(self, plan: CompiledPlan) -> None:
        plan.instantiate()

    def create_adapter(
        self,
        assignment: Assignment,
        *,
        working_root: Path,
        runtime_env: Mapping[str, str],
    ) -> ExecuteAdapter:
        from fixtures.commands import command_router

        return lambda _operation, _path: command_router()


class _DirectNativeCuratorSoftware(NativeCuratorSoftware):
    @property
    def compatibility(self) -> RuntimeCompatibility:
        return _application_compatibility(self.application_modules)


class _SerialNativePipelineRunner(NativePipelineRunner):
    """Keep the test process's command environment isolated across worker packs."""

    _science_lock = threading.Lock()

    def resolve(
        self,
        plan: CompiledPlan,
        runner_resources: RunnerResources,
        *,
        application_modules: tuple[str, ...] = (),
    ) -> NativeSoftware:
        software = super().resolve(plan, runner_resources)
        return replace(
            software,
            software_digest=_native_software_digest(application_modules),
            application_modules=application_modules,
        )

    def resolve_curator(
        self,
        operation: OperationDefinition,
        runner_resources: RunnerResources,
        *,
        application_modules: tuple[str, ...] = (),
    ) -> NativeCuratorSoftware:
        super().resolve_curator(operation, runner_resources)
        return _DirectNativeCuratorSoftware(
            software_digest=_native_software_digest(application_modules),
            application_modules=application_modules,
        )

    def _invoke(
        self, assignment: AdmittedAssignment, dispatch: AssignmentDispatch
    ) -> SealedDelivery:
        with self._science_lock:
            return execute_admitted_assignment(
                assignment,
                _DirectNativeSoftware(
                    software_digest=_native_software_digest(
                        dispatch.application_modules
                    ),
                    application_modules=dispatch.application_modules,
                ),
                dispatch,
                working_root=Path(dispatch.working_root),
            )


def _application_compatibility(
    application_modules: tuple[str, ...],
) -> RuntimeCompatibility:
    owners = importlib.metadata.packages_distributions()
    names = {
        owner
        for module in application_modules
        for owner in owners.get(module.partition(".")[0], ())
    }
    selected = tuple(sorted(names | set(TRANSPORT_PACKAGES)))
    return capture_runtime_compatibility(selected)


def _native_software_digest(application_modules: tuple[str, ...]) -> str:
    if not application_modules:
        return "c" * 64
    measured = _application_compatibility(application_modules).model_dump_json()
    payload = json.dumps([application_modules, measured]).encode()
    return hashlib.sha256(payload).hexdigest()


def create_native_pipeline(
    *,
    name: str,
    delta_root: str | Path,
    staging_root: str | Path,
    working_root: str | Path,
    failure_policy: FailurePolicy = FailurePolicy.CONTINUE,
    files_root: str | None = None,
    storage: StorageConfig | None = None,
    application_modules: tuple[str, ...] = (),
) -> PipelineManager:
    """Use the real public controller with explicit synthetic in-process software.

    Inline topology definitions exercise admission, grouping and publication here;
    installed worker and software qualification have separate real-run gates.
    """
    from artisan.orchestration import PipelineManager
    from artisan.schemas.execution.storage_config import StorageConfig
    from artisan.storage.io.retained_store import create_retained_store

    root = str(delta_root)
    files = files_root if files_root is not None else str(Path(root).parent / "files")
    selected_storage = storage if storage is not None else StorageConfig()
    committer = create_retained_store(
        root,
        files,
        fs=selected_storage.filesystem(),
        storage_options=selected_storage.delta_storage_options(),
    )
    return PipelineManager.create(
        name,
        root,
        str(staging_root),
        working_root=str(working_root),
        files_root=files,
        default_step_runner=_SerialNativePipelineRunner(committer),
        failure_policy=failure_policy,
        storage=selected_storage,
        application_modules=application_modules,
    )


@pytest.fixture
def native_pipeline(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Create real store/controller pipelines with synthetic in-process software.

    This fixture verifies admission/publication behavior, not installed worker
    qualification. Packs use one process-global command scope at a time.
    """
    from fixtures import retained_execution
    from fixtures.commands import command_router
    from fixtures.retained_pipeline import NativePipelineRunner

    from artisan.orchestration import PipelineManager
    from artisan.schemas.execution.storage_config import StorageConfig
    from artisan.storage.io.retained_store import create_retained_store

    monkeypatch.setattr(retained_execution, "LocalExecuteRouter", command_router)

    def create(name: str = "pipeline") -> PipelineManager:
        root = tmp_path / name
        committer = create_retained_store(
            str(root / "delta"), str(root / "files"), fs=StorageConfig().filesystem()
        )
        return PipelineManager.create(
            name,
            committer.delta_root,
            str(root / "staging"),
            files_root=committer.files_root,
            working_root=str(root / "working"),
            default_step_runner=NativePipelineRunner(committer),
        )

    return create
