"""Explicit external access uses normal artifact inputs without payload copies."""

from __future__ import annotations

import json
from enum import StrEnum
from pathlib import Path
from typing import Any, ClassVar

import pytest

from artisan.composites import CompositeContext
from artisan.composites.base.composite_definition import CompositeDefinition
from artisan.execution import run_operation
from artisan.execution.executor import Executor
from artisan.operations.base import OperationDefinition, PerArtifact
from artisan.schemas import ArtifactResult, InputSpec, OutputSpec
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.execution_config import ExecutionConfigArtifact
from artisan.schemas.artifact.external_descriptor import (
    ExternalFile,
    ExternalFilesDescriptor,
    PreparedExternalFiles,
)
from artisan.schemas.artifact.metric import MetricArtifact
from artisan.schemas.execution.batch_strategy import BatchStrategy
from artisan.schemas.external_artifacts import ExternalFilesArtifact
from artisan.schemas.specs.input_models import (
    ExecuteInput,
    PostprocessInput,
    PreprocessInput,
)
from artisan.storage.external_access import prepare_external_files
from artisan.utils.hashing import compute_content_digest

pytestmark = pytest.mark.usefixtures("external_artifact_types")


class ReadExternal(OperationDefinition):
    name = "read_external_runtime_fixture"
    batch_strategy: BatchStrategy = BatchStrategy(artifacts_per_unit=2)
    phases: ClassVar[list[str]] = []

    class InputRole(StrEnum):
        source = "source"

    class OutputRole(StrEnum):
        metrics = "metrics"

    inputs: ClassVar = {"source": InputSpec(artifact_type="external_files")}
    outputs: ClassVar = {
        "metrics": OutputSpec(
            artifact_type="metric", derives_from={"inputs": ["source"]}
        )
    }

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        self.phases.append("preprocess")
        artifacts = inputs.input_artifacts["source"]
        assert all(artifact.materialized_path is None for artifact in artifacts)
        return {
            "paths": PerArtifact(
                [artifact.external_root / "table.dat" for artifact in artifacts]
            )
        }

    def execute_function(self, inputs: ExecuteInput) -> dict[str, Any]:
        self.phases.append("execute")
        return {"values": [int(path.read_text()) for path in inputs.inputs["paths"]]}

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        self.phases.append("postprocess")
        result = ArtifactResult()
        for index, value in enumerate(inputs.memory_outputs["values"]):
            result.add_artifact(
                "metrics",
                MetricArtifact.draft({"value": value}, "value.json"),
                sources={"source": [inputs.ref("source", index=index)]},
            )
        return result


class ReadExternalConfig(ReadExternal):
    name = "read_external_config_runtime_fixture"
    inputs: ClassVar = {
        "source": InputSpec(
            artifact_type="config", materialize=False, resolve_references=True
        )
    }

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        self.phases.append("preprocess")
        paths = []
        for index in range(len(inputs.input_artifacts["source"])):
            references = inputs.referenced_artifacts(inputs.ref("source", index=index))
            artifact = next(iter(references.values()))
            assert artifact.materialized_path is None
            paths.append(artifact.external_root / "table.dat")
        return {"paths": PerArtifact(paths)}


class ReadMaterializedConfig(ReadExternal):
    name = "read_materialized_external_config_runtime_fixture"
    inputs: ClassVar = {"source": InputSpec(artifact_type="config")}

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        self.phases.append("preprocess")
        return {
            "paths": PerArtifact(
                [
                    Path(json.loads(Path(config.materialized_path).read_text())["root"])
                    / "table.dat"
                    for config in inputs.input_artifacts["source"]
                ]
            )
        }


class ExternalComposite(CompositeDefinition):
    name = "external_runtime_composite_fixture"
    InputRole: ClassVar = ReadExternal.InputRole
    OutputRole: ClassVar = ReadExternal.OutputRole
    inputs: ClassVar = ReadExternal.inputs
    outputs: ClassVar = {"metrics": OutputSpec(artifact_type="metric")}

    def compose(self, ctx: CompositeContext) -> None:
        result = ctx.call(ReadExternal, inputs={"source": ctx.input("source")})
        ctx.output("metrics", result.output("metrics"))


def installation(root: Path, value: int = 7) -> tuple[Artifact, PreparedExternalFiles]:
    root.mkdir()
    payload = str(value).encode()
    (root / "table.dat").write_bytes(payload)
    descriptor = ExternalFilesDescriptor(
        files=(
            ExternalFile(
                path="table.dat",
                content_hash=compute_content_digest(payload),
                size_bytes=len(payload),
            ),
        )
    )
    artifact = ExternalFilesArtifact.draft(descriptor).finalize()
    return artifact, prepare_external_files(root, descriptor)


@pytest.mark.parametrize("operation", [ReadExternal, ExternalComposite])
@pytest.mark.parametrize("with_directory", [False, True])
def test_direct_external_access_keeps_bytes_in_place_and_caller_unbound(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    with_directory: bool,
    operation: type[ReadExternal] | type[ExternalComposite],
) -> None:
    source, prepared = installation(tmp_path / "installation")

    def forbidden(*args: object, **kwargs: object) -> None:
        pytest.fail("Runtime attempted external payload materialization or hashing")

    monkeypatch.setattr(ExternalFilesArtifact, "materialize_to", forbidden)
    monkeypatch.setattr(
        "artisan.storage.external_access.compute_stream_digest", forbidden
    )
    before = source.to_row()
    work = tmp_path / "calls" if with_directory else None
    result = run_operation(
        operation,
        inputs={"source": [source, source, source]},
        external_files={source.artifact_id: prepared},
        work_dir=work,
    )
    assert [artifact.values for artifact in result.artifacts["metrics"]] == [
        {"value": 7},
        {"value": 7},
        {"value": 7},
    ]
    assert [item.source_input_index for item in result.lineage["metrics"]] == [0, 1, 2]
    assert list(tmp_path.rglob("table.dat")) == [prepared.root / "table.dat"]
    assert source.to_row() == before
    assert source.materialized_path is None
    with pytest.raises(ValueError, match="no bound external"):
        _ = source.external_root


@pytest.mark.parametrize("with_directory", [False, True])
def test_typed_config_references_share_external_access_without_materialization(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, with_directory: bool
) -> None:
    source, prepared = installation(tmp_path / "installation")
    config = ExecutionConfigArtifact.draft(
        {"root": {"$artifact": source.artifact_id}}, "config.json"
    ).finalize()

    def forbidden(*args: object, **kwargs: object) -> None:
        pytest.fail("Typed config reference materialized an external payload")

    monkeypatch.setattr(ExternalFilesArtifact, "materialize_to", forbidden)
    result = run_operation(
        ReadExternalConfig,
        inputs={"source": [config, config]},
        config_references={
            ("source", index): {source.artifact_id: source} for index in range(2)
        },
        external_files={source.artifact_id: prepared},
        work_dir=tmp_path / "calls" if with_directory else None,
    )
    assert [artifact.values for artifact in result.artifacts["metrics"]] == [
        {"value": 7},
        {"value": 7},
    ]
    assert source.materialized_path is None
    assert not list(tmp_path.rglob("references"))


def test_materialized_config_uses_external_root_without_copying_referent(
    tmp_path: Path,
) -> None:
    source, prepared = installation(tmp_path / "installation")
    config = ExecutionConfigArtifact.draft(
        {"root": {"$artifact": source.artifact_id}}, "config.json"
    ).finalize()
    result = run_operation(
        ReadMaterializedConfig,
        inputs={"source": [config]},
        config_references={("source", 0): {source.artifact_id: source}},
        external_files={source.artifact_id: prepared},
        work_dir=tmp_path / "calls",
    )
    assert result.artifacts["metrics"][0].values == {"value": 7}
    assert list(tmp_path.rglob("table.dat")) == [prepared.root / "table.dat"]
    assert source.materialized_path is None


def test_external_binding_is_required_even_for_prebound_caller_object(
    tmp_path: Path,
) -> None:
    source, prepared = installation(tmp_path / "installation")
    source.bind_external_root(prepared.root)
    ReadExternal.phases.clear()
    with pytest.raises(ValueError, match="Missing explicit external access"):
        run_operation(ReadExternal, inputs={"source": [source]})
    assert ReadExternal.phases == []


def test_external_binding_descriptor_mismatch_fails_before_science(
    tmp_path: Path,
) -> None:
    source, _ = installation(tmp_path / "first", 7)
    _, other = installation(tmp_path / "second", 9)
    ReadExternal.phases.clear()
    with pytest.raises(ValueError, match="descriptor differs"):
        run_operation(
            ReadExternal,
            inputs={"source": [source]},
            external_files={source.artifact_id: other},
        )
    assert ReadExternal.phases == []


def test_explicit_external_conversion_is_rejected_before_science(
    tmp_path: Path,
) -> None:
    class ConvertExternal(ReadExternal):
        name = "convert_external_runtime_fixture"
        inputs: ClassVar = {
            "source": InputSpec(artifact_type="external_files", materialize_as=".txt")
        }

    source, prepared = installation(tmp_path / "installation")
    ReadExternal.phases.clear()
    with pytest.raises(ValueError, match="format conversion"):
        run_operation(
            ConvertExternal,
            inputs={"source": [source]},
            external_files={source.artifact_id: prepared},
            work_dir=tmp_path / "calls",
        )
    assert ReadExternal.phases == []


def test_external_binding_mapping_is_frozen_before_authored_phases(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source, prepared = installation(tmp_path / "installation")
    bindings = {source.artifact_id: prepared}
    original = ReadExternal.preprocess

    def mutate(self: ReadExternal, inputs: PreprocessInput) -> dict[str, Any]:
        bindings.clear()
        return original(self, inputs)

    monkeypatch.setattr(ReadExternal, "preprocess", mutate)
    result = run_operation(
        ReadExternal, inputs={"source": [source, source]}, external_files=bindings
    )
    assert [artifact.values for artifact in result.artifacts["metrics"]] == [
        {"value": 7},
        {"value": 7},
    ]
    assert bindings == {}


def test_executor_and_composite_use_explicit_external_bindings(tmp_path: Path) -> None:
    source, prepared = installation(tmp_path / "installation")
    with Executor(working_root=tmp_path / "calls") as executor:
        for operation in (ReadExternal, ExternalComposite):
            result = executor.run(
                operation,
                inputs={"source": [source]},
                external_files={source.artifact_id: prepared},
            )
            assert result.artifacts["metrics"][0].values == {"value": 7}
        with pytest.raises(ValueError, match="Missing explicit external access"):
            executor.run(ReadExternal, inputs={"source": [source]})
