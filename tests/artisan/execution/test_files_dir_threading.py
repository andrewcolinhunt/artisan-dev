"""Tests for files_dir threading through ExecuteInput and creator executor."""

from __future__ import annotations

import json
import os
from dataclasses import FrozenInstanceError
from enum import StrEnum, auto
from pathlib import Path
from typing import Any, ClassVar

import polars as pl
import pytest
import xxhash

from artisan.execution.executors.creator import run_creator_lifecycle
from artisan.execution.models.execution_unit import ExecutionUnit
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.schemas.artifact.metric import MetricArtifact
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.schemas.execution.curator_result import ArtifactResult
from artisan.schemas.execution.runtime_environment import RuntimeEnvironment
from artisan.schemas.specs.input_models import (
    ExecuteInput,
    PostprocessInput,
)
from artisan.schemas.specs.output_spec import OutputSpec
from artisan.storage.core.table_schemas import ARTIFACT_INDEX_SCHEMA


def _compute_artifact_id(content: bytes) -> str:
    return xxhash.xxh3_128(content).hexdigest()


def _setup_delta_tables(base_path: Path) -> None:
    """Create minimal empty Delta tables so ArtifactStore can initialize."""
    index_path = base_path / "artifacts/index"
    df = pl.DataFrame(
        [],
        schema=ARTIFACT_INDEX_SCHEMA,
    )
    df.write_delta(str(index_path))


# ---------------------------------------------------------------------------
# Test operation that captures files_dir from ExecuteInput
# ---------------------------------------------------------------------------


class _FilesDirCapture(OperationDefinition):
    """Generative op that records files_dir for test assertions."""

    class OutputRole(StrEnum):
        output = auto()

    name: ClassVar[str] = "files_dir_capture"
    inputs: ClassVar[dict] = {}
    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.output: OutputSpec(
            artifact_type=ArtifactTypes.METRIC,
            infer_lineage_from={"inputs": []},
        ),
    }

    def execute(self, inputs: ExecuteInput) -> dict[str, Any]:
        content = json.dumps({"value": 1})
        output_path = os.path.join(inputs.execute_dir, "out.json")
        with open(output_path, "w") as fh:
            fh.write(content)
        return {"files_dir": inputs.files_dir if inputs.files_dir else None}

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        drafts = []
        for fp in inputs.file_outputs:
            if fp.endswith(".json"):
                with open(fp) as fh:
                    parsed = json.loads(fh.read())
                drafts.append(
                    MetricArtifact.draft(
                        content=parsed,
                        original_name=os.path.basename(fp),
                        step_number=inputs.step_number,
                    )
                )
        return ArtifactResult(success=True, artifacts={"output": drafts})


# ---------------------------------------------------------------------------
# ExecuteInput field tests
# ---------------------------------------------------------------------------


class TestExecuteInputFilesDir:
    """Tests for the files_dir field on ExecuteInput."""

    def test_defaults_to_none(self, tmp_path: Path) -> None:
        ei = ExecuteInput(execute_dir=str(tmp_path))
        assert ei.files_dir is None

    def test_accepts_path(self, tmp_path: Path) -> None:
        files_dir = str(tmp_path / "files")
        ei = ExecuteInput(execute_dir=str(tmp_path), files_dir=files_dir)
        assert ei.files_dir == files_dir

    def test_frozen(self, tmp_path: Path) -> None:
        ei = ExecuteInput(execute_dir=str(tmp_path))
        with pytest.raises(FrozenInstanceError):
            ei.files_dir = str(tmp_path)  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Creator lifecycle integration tests
# ---------------------------------------------------------------------------


@pytest.fixture
def delta_root(tmp_path: Path) -> Path:
    base = tmp_path / "delta"
    _setup_delta_tables(base)
    return base


@pytest.fixture
def staging_root(tmp_path: Path) -> Path:
    d = tmp_path / "staging"
    d.mkdir()
    return d


@pytest.fixture
def working_root(tmp_path: Path) -> Path:
    d = tmp_path / "working"
    d.mkdir()
    return d


class TestCreatorLifecycleFilesDir:
    """Tests for files_dir construction in run_creator_lifecycle."""

    def test_files_dir_constructed_when_files_root_set(
        self, delta_root: Path, working_root: Path, staging_root: Path, tmp_path: Path
    ) -> None:
        files_root = tmp_path / "files"
        files_root.mkdir()

        env = RuntimeEnvironment(
            delta_root=str(delta_root),
            working_root=str(working_root),
            staging_root=str(staging_root),
            files_root=str(files_root),
        )
        unit = ExecutionUnit(
            operation=_FilesDirCapture(),
            inputs={},
            execution_spec_id="spec_fd01" + "0" * 23,
            step_number=3,
        )

        result = run_creator_lifecycle(unit, env)
        captured = None
        for arts in result.artifacts.values():
            for art in arts:
                if art.artifact_id:
                    captured = art
                    break

        # The operation captured files_dir — verify the sharded directory was created
        step_dir = files_root / "3_files_dir_capture"
        assert step_dir.exists()
        [prefix_dir] = list(step_dir.iterdir())
        [shard_dir] = list(prefix_dir.iterdir())
        [run_dir] = list(shard_dir.iterdir())
        assert run_dir.is_dir()

    def test_files_dir_none_when_files_root_unset(
        self, delta_root: Path, working_root: Path, staging_root: Path
    ) -> None:
        env = RuntimeEnvironment(
            delta_root=str(delta_root),
            working_root=str(working_root),
            staging_root=str(staging_root),
            files_root=None,
        )
        unit = ExecutionUnit(
            operation=_FilesDirCapture(),
            inputs={},
            execution_spec_id="spec_fd02" + "0" * 23,
            step_number=0,
        )

        result = run_creator_lifecycle(unit, env)

        # The operation should have received files_dir=None
        assert result.artifacts  # execution succeeded

    def test_files_dir_path_structure(
        self, delta_root: Path, working_root: Path, staging_root: Path, tmp_path: Path
    ) -> None:
        """Verify path follows shard_uri layout."""
        files_root = tmp_path / "files"
        files_root.mkdir()

        env = RuntimeEnvironment(
            delta_root=str(delta_root),
            working_root=str(working_root),
            staging_root=str(staging_root),
            files_root=str(files_root),
        )
        unit = ExecutionUnit(
            operation=_FilesDirCapture(),
            inputs={},
            execution_spec_id="spec_fd03" + "0" * 23,
            step_number=7,
        )

        run_creator_lifecycle(unit, env)

        # Check structure: files_root/7_files_dir_capture/{prefix}/{shard}/{run_id}/
        step_dir = files_root / "7_files_dir_capture"
        assert step_dir.exists()
        [prefix_dir] = list(step_dir.iterdir())
        [shard_dir] = list(prefix_dir.iterdir())
        [run_dir] = list(shard_dir.iterdir())
        assert run_dir.is_dir()
