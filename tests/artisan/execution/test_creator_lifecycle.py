"""End-to-end tests for artifact-ID materialization through the creator lifecycle.

Verifies: materialization with artifact_id filenames -> filesystem match map
-> lineage via match map -> name derivation -> correct original_name in output.
"""

from __future__ import annotations

import json
import os
from enum import StrEnum, auto
from pathlib import Path
from typing import Any, ClassVar

import polars as pl
import pytest
import xxhash

from artisan.execution.executors.creator import (
    LifecycleResult,
    run_creator_lifecycle,
)
from artisan.execution.models.execution_unit import ExecutionUnit
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.schemas.artifact.metric import MetricArtifact
from artisan.schemas.execution.curator_result import ArtifactResult
from artisan.schemas.execution.runtime_environment import RuntimeEnvironment
from artisan.schemas.specs.input_models import (
    ExecuteInput,
    PostprocessInput,
    PreprocessInput,
)
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec
from artisan.storage.core.table_schemas import ARTIFACT_INDEX_SCHEMA


def _compute_id(content: bytes) -> str:
    return xxhash.xxh3_128(content).hexdigest()


def _setup_delta(base_path: Path, metrics: list[dict], index: list[dict]) -> None:
    """Write Delta Lake tables for test input artifacts."""
    metrics_path = base_path / "artifacts/metrics"
    pl.DataFrame(metrics, schema=MetricArtifact.POLARS_SCHEMA).write_delta(
        str(metrics_path)
    )
    index_path = base_path / "artifacts/index"
    pl.DataFrame(index, schema=ARTIFACT_INDEX_SCHEMA).write_delta(str(index_path))


class _SuffixOp(OperationDefinition):
    """Test operation that reads inputs by materialized path and appends a suffix.

    The key behavior: the output filename preserves the input filename stem
    (which is now the artifact_id) plus a suffix. This allows the filesystem
    match map to connect outputs back to inputs.
    """

    class InputRole(StrEnum):
        source = auto()

    class OutputRole(StrEnum):
        output = auto()

    name: ClassVar[str] = "suffix_test"
    inputs: ClassVar[dict[str, InputSpec]] = {
        InputRole.source: InputSpec(artifact_type="metric", required=True),
    }
    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.output: OutputSpec(
            artifact_type="metric",
            infer_lineage_from={"inputs": ["source"]},
        ),
    }

    suffix: str = "_scored"

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        return {
            role: [a.materialized_path for a in artifacts]
            for role, artifacts in inputs.input_artifacts.items()
        }

    def execute(self, inputs: ExecuteInput) -> dict:
        source_paths = inputs.inputs["source"]
        for path in source_paths:
            with open(path) as fh:
                content = json.loads(fh.read())
            content["scored"] = True
            stem = os.path.splitext(os.path.basename(path))[0]
            out = os.path.join(inputs.execute_dir, f"{stem}{self.suffix}.json")
            with open(out, "w") as fh:
                fh.write(json.dumps(content))
        return {}

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        drafts = []
        for fp in inputs.file_outputs:
            if fp.endswith(".json"):
                with open(fp) as fh:
                    content = json.loads(fh.read())
                drafts.append(
                    MetricArtifact.draft(
                        content=content,
                        original_name=os.path.basename(fp),
                        step_number=inputs.step_number,
                    )
                )
        return ArtifactResult(success=True, artifacts={"output": drafts})


@pytest.fixture
def delta_with_named_input(tmp_path: Path):
    """Create a Delta root with a metric artifact that has a human name."""
    base = tmp_path / "delta"
    content = json.dumps({"value": 42}, sort_keys=True).encode("utf-8")
    aid = _compute_id(content)

    _setup_delta(
        base,
        metrics=[
            {
                "artifact_id": aid,
                "origin_step_number": 0,
                "content": content,
                "original_name": "protein_001",
                "extension": ".json",
                "metadata": "{}",
                "external_path": None,
            }
        ],
        index=[
            {
                "artifact_id": aid,
                "artifact_type": "metric",
                "origin_step_number": 0,
                "metadata": "{}",
            }
        ],
    )
    return base, aid


class TestCreatorLifecycleNameDerivation:
    """End-to-end: materialization -> match map -> lineage -> name derivation."""

    def test_output_gets_human_name_with_suffix(
        self, delta_with_named_input, tmp_path: Path
    ):
        """Output artifact's original_name is derived from input name + suffix."""
        delta_path, input_id = delta_with_named_input
        working = tmp_path / "working"
        working.mkdir()
        staging = tmp_path / "staging"
        staging.mkdir()

        config = RuntimeEnvironment(
            delta_root_path=delta_path,
            working_root_path=working,
            staging_root_path=staging,
        )
        unit = ExecutionUnit(
            operation=_SuffixOp(suffix="_scored"),
            inputs={"source": [input_id]},
            execution_spec_id="spec01" + "0" * 26,
            step_number=1,
        )

        result = run_creator_lifecycle(unit, config)

        assert isinstance(result, LifecycleResult)
        assert "output" in result.artifacts
        outputs = result.artifacts["output"]
        assert len(outputs) == 1

        # The output should have the human-readable derived name
        output_art = outputs[0]
        assert output_art.original_name == "protein_001_scored"
        assert output_art.artifact_id is not None

        # Lineage edge should exist
        assert len(result.edges) >= 1
        edge = result.edges[0]
        assert edge.source_artifact_id == input_id
        assert edge.target_artifact_id == output_art.artifact_id

    def test_no_collision_with_duplicate_names(self, tmp_path: Path):
        """Two inputs with the same original_name produce distinct outputs."""
        base = tmp_path / "delta"
        content_a = json.dumps({"v": 1}, sort_keys=True).encode("utf-8")
        content_b = json.dumps({"v": 2}, sort_keys=True).encode("utf-8")
        id_a = _compute_id(content_a)
        id_b = _compute_id(content_b)

        _setup_delta(
            base,
            metrics=[
                {
                    "artifact_id": id_a,
                    "origin_step_number": 0,
                    "content": content_a,
                    "original_name": "output",
                    "extension": ".json",
                    "metadata": "{}",
                    "external_path": None,
                },
                {
                    "artifact_id": id_b,
                    "origin_step_number": 0,
                    "content": content_b,
                    "original_name": "output",
                    "extension": ".json",
                    "metadata": "{}",
                    "external_path": None,
                },
            ],
            index=[
                {
                    "artifact_id": id_a,
                    "artifact_type": "metric",
                    "origin_step_number": 0,
                    "metadata": "{}",
                },
                {
                    "artifact_id": id_b,
                    "artifact_type": "metric",
                    "origin_step_number": 0,
                    "metadata": "{}",
                },
            ],
        )

        working = tmp_path / "working"
        working.mkdir()
        staging = tmp_path / "staging"
        staging.mkdir()

        config = RuntimeEnvironment(
            delta_root_path=base,
            working_root_path=working,
            staging_root_path=staging,
        )
        unit = ExecutionUnit(
            operation=_SuffixOp(suffix="_processed"),
            inputs={"source": [id_a, id_b]},
            execution_spec_id="spec02" + "0" * 26,
            step_number=1,
        )

        result = run_creator_lifecycle(unit, config)

        outputs = result.artifacts["output"]
        assert len(outputs) == 2
        names = {a.original_name for a in outputs}
        # Both get the same derived name because both inputs were "output"
        assert names == {"output_processed"}
        # But they have different artifact_ids
        ids = {a.artifact_id for a in outputs}
        assert len(ids) == 2
