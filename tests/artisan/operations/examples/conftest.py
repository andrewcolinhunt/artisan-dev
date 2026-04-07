"""Shared fixtures and helpers for artisan example operation tests.

Provides utilities for testing operations with the lifecycle interface.
Infers artifact types from operation input specs.
"""

from __future__ import annotations

import glob
import os
from pathlib import Path
from typing import Any

from artisan.schemas.artifact.data import DataArtifact  # noqa: F401 (registers DataTypeDef)
from artisan.schemas.artifact import Artifact
from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.schemas import (
    ArtifactResult,
    ExecuteInput,
    PostprocessInput,
    PreprocessInput,
)

__all__ = [
    "run_inmemory_operation_lifecycle",
    "run_inmemory_operation_lifecycle_with_exception",
    "run_operation_lifecycle",
    "run_operation_lifecycle_with_exception",
]


def _create_mock_artifact(
    content: bytes,
    name: str,
    path: str,
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
            path = str(value)
            if os.path.exists(path):
                with open(path, "rb") as fh:
                    content = fh.read()
            else:
                content = b""
            input_artifacts[key] = [
                _create_mock_artifact(content, os.path.basename(path), path, artifact_type)
            ]
        elif isinstance(value, list) and value and isinstance(value[0], (str, Path)):
            artifacts = []
            for p in value:
                path = str(p)
                if os.path.exists(path):
                    with open(path, "rb") as fh:
                        content = fh.read()
                else:
                    content = b""
                artifacts.append(
                    _create_mock_artifact(content, os.path.basename(path), path, artifact_type)
                )
            input_artifacts[key] = artifacts

    return input_artifacts


def _setup_dirs(
    output_dir: Path,
    preprocess_dir: Path | None,
    postprocess_dir: Path | None,
) -> tuple[str, str, str, str]:
    """Create standard lifecycle directories."""
    out = str(output_dir)
    os.makedirs(out, exist_ok=True)
    pre = str(preprocess_dir) if preprocess_dir is not None else os.path.join(out, "preprocess")
    os.makedirs(pre, exist_ok=True)
    post = str(postprocess_dir) if postprocess_dir is not None else os.path.join(out, "postprocess")
    os.makedirs(post, exist_ok=True)
    exe = os.path.join(out, "execute")
    os.makedirs(exe, exist_ok=True)
    return out, pre, post, exe


def run_operation_lifecycle(
    operation,
    inputs: dict[str, Any],
    output_dir: Path,
    preprocess_dir: Path | None = None,
    postprocess_dir: Path | None = None,
) -> ArtifactResult:
    """Run the full operation lifecycle: preprocess -> execute -> postprocess."""
    output_dir_str, preprocess_dir_str, postprocess_dir_str, execute_dir_str = _setup_dirs(
        output_dir, preprocess_dir, postprocess_dir
    )

    input_artifacts = _resolve_input_artifacts(inputs, operation)

    preprocess_input = PreprocessInput(
        input_artifacts=input_artifacts,
        preprocess_dir=preprocess_dir_str,
    )
    prepared_inputs = operation.preprocess(preprocess_input)

    execute_input = ExecuteInput(
        inputs=prepared_inputs,
        execute_dir=execute_dir_str,
    )
    raw_result = operation.execute(execute_input)

    new_files = [
        p for p in glob.glob(os.path.join(execute_dir_str, "**", "*"), recursive=True)
        if os.path.isfile(p)
    ]

    postprocess_input = PostprocessInput(
        file_outputs=new_files,
        memory_outputs=raw_result,
        input_artifacts=input_artifacts,
        step_number=1,
        postprocess_dir=postprocess_dir_str,
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
    output_dir_str, preprocess_dir_str, postprocess_dir_str, execute_dir_str = _setup_dirs(
        output_dir, preprocess_dir, postprocess_dir
    )

    input_artifacts = _resolve_input_artifacts(inputs, operation)

    preprocess_input = PreprocessInput(
        input_artifacts=input_artifacts,
        preprocess_dir=preprocess_dir_str,
    )
    prepared_inputs = operation.preprocess(preprocess_input)

    execute_input = ExecuteInput(
        inputs=prepared_inputs,
        execute_dir=execute_dir_str,
    )
    try:
        raw_result = operation.execute(execute_input)
    except Exception as e:
        return ArtifactResult(success=False, error=str(e))

    new_files = [
        p for p in glob.glob(os.path.join(execute_dir_str, "**", "*"), recursive=True)
        if os.path.isfile(p)
    ]

    postprocess_input = PostprocessInput(
        file_outputs=new_files,
        memory_outputs=raw_result,
        input_artifacts=input_artifacts,
        step_number=1,
        postprocess_dir=postprocess_dir_str,
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
    output_dir_str, preprocess_dir_str, postprocess_dir_str, execute_dir_str = _setup_dirs(
        output_dir, preprocess_dir, postprocess_dir
    )

    preprocess_input = PreprocessInput(
        input_artifacts=input_artifacts,
        preprocess_dir=preprocess_dir_str,
    )
    prepared_inputs = operation.preprocess(preprocess_input)

    execute_input = ExecuteInput(
        inputs=prepared_inputs,
        execute_dir=execute_dir_str,
    )
    raw_result = operation.execute(execute_input)

    new_files = [
        p for p in glob.glob(os.path.join(execute_dir_str, "**", "*"), recursive=True)
        if os.path.isfile(p)
    ]

    postprocess_input = PostprocessInput(
        file_outputs=new_files,
        memory_outputs=raw_result,
        input_artifacts=input_artifacts,
        step_number=1,
        postprocess_dir=postprocess_dir_str,
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
    output_dir_str, preprocess_dir_str, postprocess_dir_str, execute_dir_str = _setup_dirs(
        output_dir, preprocess_dir, postprocess_dir
    )

    preprocess_input = PreprocessInput(
        input_artifacts=input_artifacts,
        preprocess_dir=preprocess_dir_str,
    )
    prepared_inputs = operation.preprocess(preprocess_input)

    execute_input = ExecuteInput(
        inputs=prepared_inputs,
        execute_dir=execute_dir_str,
    )
    try:
        raw_result = operation.execute(execute_input)
    except Exception as e:
        return ArtifactResult(success=False, error=str(e))

    new_files = [
        p for p in glob.glob(os.path.join(execute_dir_str, "**", "*"), recursive=True)
        if os.path.isfile(p)
    ]

    postprocess_input = PostprocessInput(
        file_outputs=new_files,
        memory_outputs=raw_result,
        input_artifacts=input_artifacts,
        step_number=1,
        postprocess_dir=postprocess_dir_str,
    )
    return operation.postprocess(postprocess_input)
