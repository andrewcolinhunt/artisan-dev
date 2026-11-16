"""Scientific invocation parity without store access or publication."""

from __future__ import annotations

import json
from collections.abc import Callable
from enum import StrEnum
from pathlib import Path
from typing import Any, ClassVar

import pytest

from artisan.execution.compute.local import LocalExecuteRouter
from artisan.execution.invocation import (
    ExecuteFailure,
    PostprocessFailure,
    reassemble_results,
    split_prepared_inputs,
)
from artisan.execution.session import ExecutionSession
from artisan.operations.base import OperationDefinition, PerArtifact
from artisan.schemas.artifact.appendable import AppendableArtifact
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.csv import CSVArtifact
from artisan.schemas.artifact.execution_config import ExecutionConfigArtifact
from artisan.schemas.artifact.file_ref import FileRefArtifact
from artisan.schemas.artifact.metric import MetricArtifact
from artisan.schemas.execution.curator_result import ArtifactResult
from artisan.schemas.specs.input_models import (
    ExecuteInput,
    FrozenInputContext,
    PostprocessInput,
    PreprocessInput,
)
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec
from artisan.storage.core.artifact_store import ArtifactStore
from artisan.utils.hashing import compute_content_digest


class _CopyMetrics(OperationDefinition):
    class InputRole(StrEnum):
        source = "source"

    class OutputRole(StrEnum):
        metrics = "metrics"

    name = "invocation_copy_metrics"
    inputs: ClassVar[dict[str, InputSpec]] = {
        "source": InputSpec(artifact_type="metric", materialize=False)
    }
    outputs: ClassVar[dict[str, OutputSpec]] = {
        "metrics": OutputSpec(
            artifact_type="metric", derives_from={"inputs": ["source"]}
        )
    }
    phases: ClassVar[list[str]] = []

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        self.phases.append("preprocess")
        return {
            "values": PerArtifact(
                [artifact.values for artifact in inputs.input_artifacts["source"]]
            ),
            "shared": [1, 2],
        }

    def execute_function(self, inputs: ExecuteInput) -> dict[str, Any]:
        self.phases.append("execute")
        assert inputs.inputs["shared"] == [1, 2]
        return {"values": inputs.inputs["values"]}

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        self.phases.append("postprocess")
        result = ArtifactResult()
        for index, values in enumerate(inputs.memory_outputs["values"]):
            result.add_artifact(
                "metrics",
                MetricArtifact.draft(values, "source.json", inputs.step_number),
                sources={"source": [inputs.ref("source", index=index)]},
            )
        return result


class _WholeMetrics(_CopyMetrics):
    name = "invocation_whole_metrics"
    per_artifact_dispatch: ClassVar[bool] = False


class _GenerateFile(OperationDefinition):
    class OutputRole(StrEnum):
        files = "files"

    name = "invocation_generate_file"
    outputs: ClassVar[dict[str, OutputSpec]] = {
        "files": OutputSpec(artifact_type="file_ref", derives_from={"inputs": []})
    }

    def execute_function(self, inputs: ExecuteInput) -> dict[str, list[str]]:
        assert inputs.files_dir is not None
        path = Path(inputs.files_dir) / "result.json"
        path.write_text('{"value": 7}')
        return {"paths": [str(path)]}

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        path = Path(inputs.memory_outputs["paths"][0])
        content = path.read_bytes()
        artifact = FileRefArtifact.draft(
            str(path),
            compute_content_digest(content),
            len(content),
            inputs.step_number,
        )
        return ArtifactResult(artifacts={"files": [artifact]}, lineage={"files": []})


class _MeasureFile(OperationDefinition):
    class InputRole(StrEnum):
        source = "source"

    class OutputRole(StrEnum):
        metrics = "metrics"

    name = "invocation_measure_file"
    inputs: ClassVar[dict[str, InputSpec]] = {
        "source": InputSpec(artifact_type="file_ref")
    }
    outputs: ClassVar[dict[str, OutputSpec]] = {
        "metrics": OutputSpec(
            artifact_type="metric", derives_from={"inputs": ["source"]}
        )
    }

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        return {
            "paths": PerArtifact(
                [
                    artifact.materialized_path
                    for artifact in inputs.input_artifacts["source"]
                ]
            )
        }

    def execute_function(self, inputs: ExecuteInput) -> dict[str, Any]:
        return {
            "values": [
                json.loads(Path(path).read_text()) for path in inputs.inputs["paths"]
            ]
        }

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        result = ArtifactResult()
        for index, content in enumerate(inputs.memory_outputs["values"]):
            result.add_artifact(
                "metrics",
                MetricArtifact.draft(content, "measured.json", inputs.step_number),
                sources={"source": [inputs.ref("source", index=index)]},
            )
        return result


@pytest.mark.parametrize(
    ("operation_class", "execute_count"), [(_CopyMetrics, 2), (_WholeMetrics, 1)]
)
def test_invocation_preserves_phases_ordered_equal_content_and_exact_lineage(
    tmp_path: Path,
    operation_class: type[_CopyMetrics],
    execute_count: int,
) -> None:
    source = MetricArtifact.draft({"value": 4}, "source.json").finalize()
    context = FrozenInputContext("copy", {"source": [source, source]})
    operation_class.phases.clear()
    with ExecutionSession(working_root=tmp_path / "session") as session:
        result = session.invoke(
            operation_class(),
            context,
            batch_size=2,
            execute_router=LocalExecuteRouter(),
            materialization_references={},
        )
    assert operation_class.phases == [
        "preprocess",
        *(["execute"] * execute_count),
        "postprocess",
    ]
    assert [artifact.artifact_id for artifact in result.artifacts["metrics"]] == [
        source.artifact_id
    ] * 2
    assert all(
        artifact.origin_step_number is None for artifact in result.artifacts["metrics"]
    )
    assert [mapping.source_input_index for mapping in result.lineage["metrics"]] == [
        0,
        1,
    ]
    assert {mapping.source_invocation for mapping in result.lineage["metrics"]} == {
        "copy"
    }


def test_session_keeps_file_intermediate_without_store_or_recording(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def forbidden(*args: object, **kwargs: object) -> None:
        msg = "Invocation accessed durable storage or recording"
        raise AssertionError(msg)

    # Artifact hydration is complete before any invocation enters this trap.
    for name in ("get_artifact", "get_artifacts_by_type"):
        monkeypatch.setattr(ArtifactStore, name, forbidden)
    monkeypatch.setattr(
        "artisan.execution.recording.recorder.record_execution_success", forbidden
    )
    monkeypatch.setattr(
        "artisan.execution.recording.recorder.record_execution_failure", forbidden
    )
    monkeypatch.setattr(
        "artisan.execution.context.builder.build_execution_context", forbidden
    )
    with ExecutionSession(working_root=tmp_path / "session") as session:
        first = session.invoke(
            _GenerateFile(),
            FrozenInputContext("generate", {}),
            batch_size=1,
            execute_router=LocalExecuteRouter(),
            materialization_references={},
        )
        file = first.artifacts["files"][0]
        path = Path(file.path)
        assert path.exists()
        assert file.origin_step_number is None
        second = session.invoke(
            _MeasureFile(),
            FrozenInputContext("measure", {"source": [file]}),
            batch_size=1,
            execute_router=LocalExecuteRouter(),
            materialization_references={},
        )
        assert second.artifacts["metrics"][0].values == {"value": 7}
        assert second.lineage["metrics"][0].source_artifact_id == file.artifact_id
        assert path.exists()
    assert not path.exists()


@pytest.mark.parametrize(
    "factory",
    [
        lambda: MetricArtifact.draft({}, "metric.json"),
        lambda: CSVArtifact.draft(b"value\n1\n", "input.csv"),
        lambda: ExecutionConfigArtifact.draft({}, "config.json"),
        lambda: FileRefArtifact.draft("file.bin", "a" * 32, 1),
        lambda: AppendableArtifact.draft("record", "a" * 32, 1, None, "records.jsonl"),
    ],
)
def test_core_draft_factories_allow_absent_origin(
    factory: Callable[[], Artifact],
) -> None:
    artifact: Artifact = factory().finalize()
    assert artifact.origin_step_number is None
    assert artifact.is_finalized


def test_execute_failure_keeps_cause_and_never_postprocesses(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail(*args: object, **kwargs: object) -> None:
        msg = "scientific execute failed"
        raise ValueError(msg)

    source = MetricArtifact.draft({"value": 1}, "source.json").finalize()
    _CopyMetrics.phases.clear()
    monkeypatch.setattr(_CopyMetrics, "execute_function", fail)
    with ExecutionSession(working_root=tmp_path / "session") as session:
        with pytest.raises(
            ExecuteFailure, match="scientific execute failed"
        ) as captured:
            session.invoke(
                _CopyMetrics(),
                FrozenInputContext("failure", {"source": [source]}),
                batch_size=1,
                execute_router=LocalExecuteRouter(),
                materialization_references={},
            )
        assert isinstance(captured.value.__cause__, ValueError)
    assert _CopyMetrics.phases == ["preprocess"]


def test_unsuccessful_postprocess_does_not_finalize(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        _GenerateFile,
        "postprocess",
        lambda *_: ArtifactResult(success=False, error="not valid"),
    )
    with (
        ExecutionSession(working_root=tmp_path / "session") as session,
        pytest.raises(PostprocessFailure, match="not valid"),
    ):
        session.invoke(
            _GenerateFile(),
            FrozenInputContext("failure", {}),
            batch_size=1,
            execute_router=LocalExecuteRouter(),
            materialization_references={},
        )


class _AssociatedMetrics(_CopyMetrics):
    name = "invocation_associated_metrics"
    inputs: ClassVar[dict[str, InputSpec]] = {
        "source": InputSpec(
            artifact_type="metric", materialize=False, with_associated=("metric",)
        )
    }

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        assert (
            inputs.associated_artifacts(inputs.ref("source", index=0), "metric") == []
        )
        return super().preprocess(inputs)


@pytest.mark.parametrize("captured", [False, True])
def test_invocation_requires_captured_context_before_operation_code(
    tmp_path: Path,
    captured: bool,
) -> None:
    source = MetricArtifact.draft({"value": 1}, "source.json").finalize()
    context = FrozenInputContext(
        "associated",
        {"source": [source]},
        associations={("source", 0, "metric"): []} if captured else {},
    )
    _AssociatedMetrics.phases.clear()
    with ExecutionSession(working_root=tmp_path / "session") as session:
        if not captured:
            with pytest.raises(ValueError, match="not captured"):
                session.invoke(
                    _AssociatedMetrics(),
                    context,
                    batch_size=1,
                    execute_router=LocalExecuteRouter(),
                    materialization_references={},
                )
            assert _AssociatedMetrics.phases == []
        else:
            result = session.invoke(
                _AssociatedMetrics(),
                context,
                batch_size=1,
                execute_router=LocalExecuteRouter(),
                materialization_references={},
            )
            assert result.artifacts["metrics"][0].values == {"value": 1}


def test_uncertain_command_cleanup_prevents_finalization(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from artisan.execution.invocation import (
        InvocationPaths,
        execute_invocation,
        postprocess_invocation,
        prepare_invocation,
    )
    from artisan.utils.command_supervisor import CommandOwnershipError

    def uncertain() -> None:
        msg = "Descendant cleanup could not be confirmed"
        raise CommandOwnershipError(msg)

    def forbidden(*args: object) -> None:
        pytest.fail("Uncertain invocation reached artifact finalization")

    prepared = prepare_invocation(
        _GenerateFile(),
        FrozenInputContext("uncertain", {}),
        InvocationPaths.create(tmp_path / "invocation"),
        batch_size=1,
        materialization_references={},
    )
    results = execute_invocation(prepared, LocalExecuteRouter())
    monkeypatch.setattr("artisan.execution.invocation.close_command_scope", uncertain)
    monkeypatch.setattr("artisan.execution.invocation.finalize_artifacts", forbidden)
    with pytest.raises(CommandOwnershipError, match="could not be confirmed"):
        postprocess_invocation(prepared, results)


class _EmptyMetrics(OperationDefinition):
    name = "invocation_empty_metrics"

    class OutputRole(StrEnum):
        metrics = "metrics"

    outputs: ClassVar[dict[str, OutputSpec]] = {
        "metrics": OutputSpec(
            artifact_type="metric", required=False, derives_from={"inputs": []}
        )
    }

    def execute_function(self, inputs: ExecuteInput) -> None:
        return None

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        return ArtifactResult(artifacts={"metrics": []}, lineage={"metrics": []})


def test_invocation_preserves_explicit_empty_optional_output(tmp_path: Path) -> None:
    with ExecutionSession(working_root=tmp_path / "session") as session:
        result = session.invoke(
            _EmptyMetrics(),
            FrozenInputContext("empty", {}),
            batch_size=1,
            execute_router=LocalExecuteRouter(),
            materialization_references={},
        )
    assert result.artifacts == {"metrics": []}
    assert result.lineage == {"metrics": []}
    assert result.success


class _OriginMetrics(_EmptyMetrics):
    name = "invocation_origin_metrics"

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        return ArtifactResult(
            artifacts={
                "metrics": [
                    MetricArtifact.draft({"value": 1}, "new.json"),
                    MetricArtifact.draft({"value": 2}, "draft.json", 99),
                    MetricArtifact.draft({"value": 3}, "existing.json", 2).finalize(),
                ]
            },
            lineage={"metrics": []},
        )


@pytest.mark.parametrize(
    ("step_number", "origins"), [(7, [7, 7, 2]), (None, [None, 99, 2])]
)
def test_pipeline_owns_drafts_before_finalization_and_preserves_existing_origins(
    tmp_path: Path, step_number: int | None, origins: list[int | None]
) -> None:
    with ExecutionSession(working_root=tmp_path / "session") as session:
        result = session.invoke(
            _OriginMetrics(),
            FrozenInputContext("origin", {}),
            batch_size=1,
            execute_router=LocalExecuteRouter(),
            materialization_references={},
            step_number=step_number,
        )
    assert [item.origin_step_number for item in result.artifacts["metrics"]] == origins
    for item in result.artifacts["metrics"]:
        item.to_row()
        with pytest.raises(TypeError, match="Cannot modify finalized"):
            item.origin_step_number = 100


class _ReadConfigReferences(OperationDefinition):
    name = "invocation_read_config_references"
    per_artifact_dispatch: ClassVar[bool] = False

    class InputRole(StrEnum):
        config = "config"

    class OutputRole(StrEnum):
        metrics = "metrics"

    inputs: ClassVar[dict[str, InputSpec]] = {
        "config": InputSpec(
            artifact_type="config", materialize=False, resolve_references=True
        )
    }
    outputs: ClassVar[dict[str, OutputSpec]] = {
        "metrics": OutputSpec(
            artifact_type="metric", derives_from={"inputs": ["config"]}
        )
    }

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        paths = []
        for index in range(len(inputs.input_artifacts["config"])):
            references = inputs.referenced_artifacts(inputs.ref("config", index=index))
            paths.extend(artifact.materialized_path for artifact in references.values())
        return {"paths": paths}

    def execute_function(self, inputs: ExecuteInput) -> dict[str, Any]:
        return {
            "sizes": [len(Path(path).read_bytes()) for path in inputs.inputs["paths"]]
        }

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        result = ArtifactResult()
        for index, size in enumerate(inputs.memory_outputs["sizes"]):
            result.add_artifact(
                "metrics",
                MetricArtifact.draft({"size": size}, "size.json", inputs.step_number),
                sources={"config": [inputs.ref("config", index=index)]},
            )
        return result


def test_equal_config_occurrences_materialize_each_captured_dependency_view(
    tmp_path: Path,
) -> None:
    first = CSVArtifact.draft(b"value\n1\n", "input.csv").finalize()
    second = CSVArtifact.draft(b"value\n1\n", "input.csv").finalize()
    configs = [
        ExecutionConfigArtifact.draft(
            {"input": {"$artifact": artifact.artifact_id}}, "config.json"
        ).finalize()
        for artifact in [first, second]
    ]
    context = FrozenInputContext(
        "configs",
        {"config": configs},
        references={
            ("config", 0): {first.artifact_id: first},
            ("config", 1): {second.artifact_id: second},
        },
    )
    with ExecutionSession(working_root=tmp_path / "session") as session:
        result = session.invoke(
            _ReadConfigReferences(),
            context,
            batch_size=2,
            execute_router=LocalExecuteRouter(),
            materialization_references={},
        )
        assert first.materialized_path == second.materialized_path
        assert [artifact.values for artifact in result.artifacts["metrics"]] == [
            {"size": 8}
        ] * 2
    assert [mapping.source_input_index for mapping in result.lineage["metrics"]] == [
        0,
        1,
    ]


class TestSplitPreparedInputs:
    def test_per_artifact_slices_and_wraps(self) -> None:
        prepared = {
            "data": PerArtifact(["/a.csv", "/b.csv"]),
            "config": "shared",
        }
        assert split_prepared_inputs(prepared, 0, 2) == {
            "data": ["/a.csv"],
            "config": "shared",
        }
        assert split_prepared_inputs(prepared, 1, 2) == {
            "data": ["/b.csv"],
            "config": "shared",
        }

    def test_per_artifact_wrong_length_raises(self) -> None:
        prepared = {"data": PerArtifact(["/a.csv"])}
        with pytest.raises(ValueError, match="batch_size is 3"):
            split_prepared_inputs(prepared, 0, 3)

    def test_raw_list_not_sliced(self) -> None:
        """Raw lists pass through unchanged regardless of length.

        Matching the batch length does not imply per-artifact slicing.
        """
        prepared = {"data": ["/a.csv", "/b.csv", "/c.csv"], "config": "shared"}
        for index in range(3):
            result = split_prepared_inputs(prepared, index, 3)
            assert result["data"] == ["/a.csv", "/b.csv", "/c.csv"]
            assert result["config"] == "shared"

    def test_passthrough_non_list_and_dict(self) -> None:
        prepared = {"count": 5, "options": {"key": "val"}}
        result = split_prepared_inputs(prepared, 0, 3)
        assert result == {"count": 5, "options": {"key": "val"}}

    def test_empty_dict(self) -> None:
        assert split_prepared_inputs({}, 0, 1) == {}


class TestReassembleResults:
    def test_all_none(self, tmp_path: Path) -> None:
        d0 = tmp_path / "artifact_0"
        d0.mkdir()
        memory, _files = reassemble_results([None, None], [str(d0), str(d0)])
        assert memory is None

    def test_dict_concatenates_lists(self, tmp_path: Path) -> None:
        d0 = tmp_path / "artifact_0"
        d0.mkdir()
        d1 = tmp_path / "artifact_1"
        d1.mkdir()

        results = [
            {"created_files": ["a.txt"]},
            {"created_files": ["b.txt"]},
        ]
        memory, _ = reassemble_results(results, [str(d0), str(d1)])
        assert memory == {"created_files": ["a.txt", "b.txt"]}

    def test_collects_files_from_all_dirs(self, tmp_path: Path) -> None:
        d0 = tmp_path / "artifact_0"
        d0.mkdir()
        (d0 / "out_0.csv").write_text("data0")

        d1 = tmp_path / "artifact_1"
        d1.mkdir()
        (d1 / "out_1.csv").write_text("data1")

        _, files = reassemble_results([None, None], [str(d0), str(d1)])
        basenames = sorted(Path(filename).name for filename in files)
        assert basenames == ["out_0.csv", "out_1.csv"]

    def test_non_dict_results_returned_as_list(self, tmp_path: Path) -> None:
        d0 = tmp_path / "artifact_0"
        d0.mkdir()
        results = ["result_a", "result_b"]
        memory, _ = reassemble_results(results, [str(d0), str(d0)])
        assert memory == ["result_a", "result_b"]

    def test_reassembly_preserves_duplicate_basenames_and_order(
        self, tmp_path: Path
    ) -> None:
        """File discovery preserves both paths without building provenance metadata."""
        directories = []
        expected = []
        for slot in range(2):
            directory = tmp_path / f"artifact_{slot}"
            directory.mkdir()
            path = directory / "result.bin"
            path.write_text(str(slot))
            directories.append(str(directory))
            expected.append(str(path))

        memory, files = reassemble_results([None, None], directories)

        assert memory is None
        assert files == expected
