"""Shared fixtures and helpers for artisan example operation tests.

Provides utilities for testing operations with the lifecycle interface.
Infers artifact types from operation input specs.
"""

from __future__ import annotations

import csv
import io
import random
from pathlib import Path
from typing import Any

from artisan.schemas.artifact.data import DataArtifact  # noqa: F401 (registers DataTypeDef)
from artisan.schemas.artifact import Artifact, ArtifactTypeDef
from artisan.schemas import (
    ArtifactResult,
    ExecuteInput,
    PostprocessInput,
    PreprocessInput,
)

__all__ = [
    "make_csv",
    "run_inmemory_operation_lifecycle",
    "run_inmemory_operation_lifecycle_with_exception",
    "run_operation_lifecycle",
    "run_operation_lifecycle_with_exception",
]


def make_csv(rows: int = 5, seed: int = 42) -> bytes:
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
        writer.writerow([
            i,
            round(rng.uniform(0.0, 10.0), 4),
            round(rng.uniform(0.0, 10.0), 4),
            round(rng.uniform(0.0, 10.0), 4),
            round(rng.uniform(0.0, 1.0), 4),
        ])
    return buf.getvalue().encode("utf-8")


def _create_mock_artifact(
    content: bytes,
    name: str,
    path: Path,
    artifact_type: str,
    step_number: int = 0,
) -> Artifact:
    """Create a mock artifact of any registered type."""
    model_cls = ArtifactTypeDef.get_model(artifact_type)
    artifact = model_cls.draft(
        content=content,
        original_name=name,
        step_number=step_number,
    )
    finalized = artifact.finalize()
    return finalized.model_copy(update={"materialized_path": path})


def _resolve_input_artifacts(
    inputs: dict[str, Any],
    operation=None,
) -> dict[str, list[Artifact]]:
    """Convert raw inputs to artifact lists."""
    input_artifacts: dict[str, list[Artifact]] = {}
    for key, value in inputs.items():
        if operation is not None and hasattr(operation, "inputs") and key in operation.inputs:
            artifact_type = operation.inputs[key].artifact_type
        else:
            msg = f"Cannot infer artifact type for role '{key}': no operation spec provided"
            raise ValueError(msg)

        if isinstance(value, (str, Path)):
            path = Path(value)
            content = path.read_bytes() if path.exists() else b""
            input_artifacts[key] = [
                _create_mock_artifact(content, path.name, path, artifact_type)
            ]
        elif isinstance(value, list) and value and isinstance(value[0], (str, Path)):
            artifacts = []
            for p in value:
                path = Path(p)
                content = path.read_bytes() if path.exists() else b""
                artifacts.append(
                    _create_mock_artifact(content, path.name, path, artifact_type)
                )
            input_artifacts[key] = artifacts

    return input_artifacts


def _setup_dirs(
    output_dir: Path,
    preprocess_dir: Path | None,
    postprocess_dir: Path | None,
) -> tuple[Path, Path, Path, Path]:
    """Create standard lifecycle directories."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    if preprocess_dir is None:
        preprocess_dir = output_dir / "preprocess"
    preprocess_dir.mkdir(parents=True, exist_ok=True)
    if postprocess_dir is None:
        postprocess_dir = output_dir / "postprocess"
    postprocess_dir.mkdir(parents=True, exist_ok=True)
    execute_dir = output_dir / "execute"
    execute_dir.mkdir(parents=True, exist_ok=True)
    return output_dir, preprocess_dir, postprocess_dir, execute_dir


def run_operation_lifecycle(
    operation,
    inputs: dict[str, Any],
    output_dir: Path,
    preprocess_dir: Path | None = None,
    postprocess_dir: Path | None = None,
) -> ArtifactResult:
    """Run the full operation lifecycle: preprocess -> execute -> postprocess."""
    output_dir, preprocess_dir, postprocess_dir, execute_dir = _setup_dirs(
        output_dir, preprocess_dir, postprocess_dir
    )

    input_artifacts = _resolve_input_artifacts(inputs, operation)

    preprocess_input = PreprocessInput(
        input_artifacts=input_artifacts,
        preprocess_dir=preprocess_dir,
    )
    prepared_inputs = operation.preprocess(preprocess_input)

    execute_input = ExecuteInput(
        inputs=prepared_inputs,
        execute_dir=execute_dir,
    )
    raw_result = operation.execute(execute_input)

    new_files = [f for f in execute_dir.glob("**/*") if f.is_file()]

    postprocess_input = PostprocessInput(
        file_outputs=new_files,
        memory_outputs=raw_result,
        input_artifacts=input_artifacts,
        step_number=1,
        postprocess_dir=postprocess_dir,
    )
    return operation.postprocess(postprocess_input)


def run_operation_lifecycle_with_exception(
    operation,
    inputs: dict[str, Any],
    output_dir: Path,
    preprocess_dir: Path | None = None,
    postprocess_dir: Path | None = None,
) -> ArtifactResult:
    """Run lifecycle catching execute exceptions (matching executor behavior)."""
    output_dir, preprocess_dir, postprocess_dir, execute_dir = _setup_dirs(
        output_dir, preprocess_dir, postprocess_dir
    )

    input_artifacts = _resolve_input_artifacts(inputs, operation)

    preprocess_input = PreprocessInput(
        input_artifacts=input_artifacts,
        preprocess_dir=preprocess_dir,
    )
    prepared_inputs = operation.preprocess(preprocess_input)

    execute_input = ExecuteInput(
        inputs=prepared_inputs,
        execute_dir=execute_dir,
    )
    try:
        raw_result = operation.execute(execute_input)
    except Exception as e:
        return ArtifactResult(success=False, error=str(e))

    new_files = [f for f in execute_dir.glob("**/*") if f.is_file()]

    postprocess_input = PostprocessInput(
        file_outputs=new_files,
        memory_outputs=raw_result,
        input_artifacts=input_artifacts,
        step_number=1,
        postprocess_dir=postprocess_dir,
    )
    return operation.postprocess(postprocess_input)


def run_inmemory_operation_lifecycle(
    operation,
    input_artifacts: dict[str, list[Artifact]],
    output_dir: Path,
    preprocess_dir: Path | None = None,
    postprocess_dir: Path | None = None,
) -> ArtifactResult:
    """Run lifecycle for in-memory operations (materialize=False)."""
    output_dir, preprocess_dir, postprocess_dir, execute_dir = _setup_dirs(
        output_dir, preprocess_dir, postprocess_dir
    )

    preprocess_input = PreprocessInput(
        input_artifacts=input_artifacts,
        preprocess_dir=preprocess_dir,
    )
    prepared_inputs = operation.preprocess(preprocess_input)

    execute_input = ExecuteInput(
        inputs=prepared_inputs,
        execute_dir=execute_dir,
    )
    raw_result = operation.execute(execute_input)

    new_files = [f for f in execute_dir.glob("**/*") if f.is_file()]

    postprocess_input = PostprocessInput(
        file_outputs=new_files,
        memory_outputs=raw_result,
        input_artifacts=input_artifacts,
        step_number=1,
        postprocess_dir=postprocess_dir,
    )
    return operation.postprocess(postprocess_input)


def run_inmemory_operation_lifecycle_with_exception(
    operation,
    input_artifacts: dict[str, list[Artifact]],
    output_dir: Path,
    preprocess_dir: Path | None = None,
    postprocess_dir: Path | None = None,
) -> ArtifactResult:
    """Run in-memory lifecycle catching execute exceptions."""
    output_dir, preprocess_dir, postprocess_dir, execute_dir = _setup_dirs(
        output_dir, preprocess_dir, postprocess_dir
    )

    preprocess_input = PreprocessInput(
        input_artifacts=input_artifacts,
        preprocess_dir=preprocess_dir,
    )
    prepared_inputs = operation.preprocess(preprocess_input)

    execute_input = ExecuteInput(
        inputs=prepared_inputs,
        execute_dir=execute_dir,
    )
    try:
        raw_result = operation.execute(execute_input)
    except Exception as e:
        return ArtifactResult(success=False, error=str(e))

    new_files = [f for f in execute_dir.glob("**/*") if f.is_file()]

    postprocess_input = PostprocessInput(
        file_outputs=new_files,
        memory_outputs=raw_result,
        input_artifacts=input_artifacts,
        step_number=1,
        postprocess_dir=postprocess_dir,
    )
    return operation.postprocess(postprocess_input)
