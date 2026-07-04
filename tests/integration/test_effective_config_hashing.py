"""Integration test for effective-config hashing (class-default image bump).

The motivating bug: an op whose container image is a class-level default gets
bumped, the same pipeline is re-run, and the cache silently serves the old
artifacts because the typed-override cache key never saw the image. This test
reproduces the scenario end-to-end and asserts the fixed behavior — a class
default is part of the cache key, so bumping it forces re-execution.
"""

from __future__ import annotations

import os
from enum import StrEnum
from typing import ClassVar

import pytest

pytestmark = pytest.mark.integration

from artisan.operations.base.operation_definition import OperationDefinition
from artisan.orchestration import PipelineManager
from artisan.orchestration.runners import Runner
from artisan.schemas import ArtifactResult
from artisan.schemas.artifact.data import DataArtifact
from artisan.schemas.operation_config.environment_spec import DockerEnvironmentSpec
from artisan.schemas.operation_config.environments import Environments
from artisan.schemas.specs.input_models import ExecuteInput, PostprocessInput
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec

from .conftest import count_executions_by_step, get_execution_outputs


class _ImageStampV1(OperationDefinition):
    """Generative op that stamps its class-default image into its output.

    The image lives only in the class-level ``environments`` default (no
    per-step override), so it is exactly the config the old cache key missed.
    ``active="local"`` keeps execution pure-Python — no container is launched;
    the image is metadata that must nonetheless reach the cache key.
    """

    class OutputRole(StrEnum):
        RESULT = "result"

    name: ClassVar[str] = "image_stamp_cache_op"
    environments: Environments = Environments(
        active="local", docker=DockerEnvironmentSpec(image="lab/tool:v1")
    )
    inputs: ClassVar[dict[str, InputSpec]] = {}
    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.RESULT: OutputSpec(
            artifact_type="data",
            infer_lineage_from={"inputs": []},
        ),
    }

    def execute_function(self, inputs: ExecuteInput) -> dict:
        os.makedirs(inputs.execute_dir, exist_ok=True)
        out_path = os.path.join(inputs.execute_dir, "image.txt")
        with open(out_path, "w") as f:
            f.write(self.environments.docker.image)
        return {}

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        drafts: list[DataArtifact] = []
        for file_path in inputs.file_outputs:
            with open(file_path, "rb") as f:
                content = f.read()
            drafts.append(
                DataArtifact.draft(
                    content=content,
                    original_name=os.path.basename(file_path),
                    step_number=inputs.step_number,
                )
            )
        return ArtifactResult(success=True, artifacts={"result": drafts})


class _ImageStampV2(_ImageStampV1):
    """Same op (same ``name``) with the class-default image bumped to v2."""

    environments: Environments = Environments(
        active="local", docker=DockerEnvironmentSpec(image="lab/tool:v2")
    )


def _run(op: type[OperationDefinition], env: dict[str, str]) -> None:
    pipeline = PipelineManager.create(
        name="image_bump_cache",
        delta_root=env["delta_root"],
        staging_root=env["staging_root"],
        working_root=env["working_root"],
    )
    pipeline.run(op, step_runner=Runner.LOCAL)
    pipeline.finalize()


def test_class_default_image_bump_busts_cache(pipeline_env: dict[str, str]) -> None:
    """Bumping a class-default image forces re-execution and a new artifact.

    Control: re-running the identical v1 op is a cache hit (no new execution,
    same artifact). Bumping to v2 (same op name, only the class-default image
    changes) is a cache miss — the fix in effect. Under the old cache key both
    ids matched and the v2 run reused v1's artifact.
    """
    delta_root = pipeline_env["delta_root"]

    _run(_ImageStampV1, pipeline_env)
    exec_after_v1 = count_executions_by_step(delta_root, 0)
    ids_after_v1 = set(get_execution_outputs(delta_root, 0, "result"))
    assert exec_after_v1 == 1
    assert len(ids_after_v1) == 1

    # Control: identical op re-run must cache-hit (no new execution/artifact).
    _run(_ImageStampV1, pipeline_env)
    assert count_executions_by_step(delta_root, 0) == exec_after_v1
    assert set(get_execution_outputs(delta_root, 0, "result")) == ids_after_v1

    # Bump: same op name, only the class-default image changes → cache miss.
    _run(_ImageStampV2, pipeline_env)
    assert count_executions_by_step(delta_root, 0) > exec_after_v1
    ids_after_v2 = set(get_execution_outputs(delta_root, 0, "result"))
    new_ids = ids_after_v2 - ids_after_v1
    assert new_ids, "v2 run must produce a new (v2) artifact, not reuse v1's"
