"""Execution contexts snapshot identity and configured runtime resources."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from artisan.execution.context.builder import build_execution_context
from artisan.operations.examples.data_generator import DataGenerator
from artisan.schemas.execution.runtime_environment import RuntimeEnvironment
from artisan.schemas.execution.storage_config import StorageConfig


def test_build_execution_context_snapshots_runtime(tmp_path: Path) -> None:
    runtime = RuntimeEnvironment(
        delta_root=str(tmp_path / "delta"),
        staging_root=str(tmp_path / "staging"),
        files_root=str(tmp_path / "files"),
        worker_id=42,
        compute_backend_name="provider",
        shared_filesystem=True,
        storage=StorageConfig(
            options={"auto_mkdir": True}, delta_options={"AWS_REGION": "eu-west-1"}
        ),
    )
    operation = DataGenerator()
    started = datetime.now(UTC)
    context = build_execution_context(
        execution_run_id="a" * 32,
        execution_spec_id="b" * 32,
        step_number=3,
        timestamp_start=started,
        runtime_env=runtime,
        operation=operation,
        sandbox_path=str(tmp_path / "sandbox"),
        step_run_id="step-run",
    )
    assert context.worker_id == 42
    assert context.compute_backend == "provider"
    assert context.shared_filesystem is True
    assert context.execution_run_id == "a" * 32
    assert context.execution_spec_id == "b" * 32
    assert context.step_number == 3
    assert context.timestamp_start == started
    assert context.step_run_id == "step-run"
    assert context.sandbox_path == str(tmp_path / "sandbox")
    assert context.operation is operation
    assert context.staging_root == runtime.staging_root
    assert context.fs.auto_mkdir is True
    assert context.artifact_store.filesystem is context.fs
    assert context.artifact_store.base_path == runtime.delta_root
    assert context.artifact_store.files_root == runtime.files_root
    assert context.artifact_store._storage_options == {"AWS_REGION": "eu-west-1"}
