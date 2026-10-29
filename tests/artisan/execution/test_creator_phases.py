"""Tests for the split creator lifecycle phases (prep_unit / post_unit)."""

from __future__ import annotations

import json
import os
import shutil
from dataclasses import replace
from enum import StrEnum, auto
from pathlib import Path
from types import SimpleNamespace
from typing import Any, ClassVar

import polars as pl
import pytest
from fixtures import managed_file_ops
from fixtures.managed_file_ops import ManagedFileCommand, WholeBatchManagedFileCommand
from fixtures.reference_ops import ReferenceConsumer
from fixtures.run_outputs import commit_outputs
from fixtures.store_format import commit_test_tables
from fsspec.implementations.local import LocalFileSystem

from artisan.execution.compute.local import LocalExecuteRouter
from artisan.execution.executors.creator import (
    LifecycleResult,
    run_creator_flow,
    run_creator_lifecycle,
)
from artisan.execution.executors.creator_phases import (
    PreppedUnit,
    _is_under_local_dir,
    _reassemble_results,
    _split_prepared_inputs,
    _upload_files_to_root,
    post_unit,
    prep_unit,
)
from artisan.execution.models.execution_unit import ExecutionUnit
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.operations.base.per_artifact import PerArtifact
from artisan.operations.lineage import match_outputs_to_inputs_by_stem
from artisan.schemas.artifact.execution_config import ExecutionConfigArtifact
from artisan.schemas.artifact.file_ref import FileRefArtifact
from artisan.schemas.artifact.metric import MetricArtifact
from artisan.schemas.execution.curator_result import ArtifactResult
from artisan.schemas.execution.runtime_environment import RuntimeEnvironment
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.schemas.specs.input_models import (
    ExecuteInput,
    PostprocessInput,
    PreprocessInput,
)
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec
from artisan.storage.core.table_schemas import ARTIFACT_INDEX_SCHEMA


def _setup_delta(base_path: Path, metrics: list[dict], index: list[dict]) -> None:
    fs = LocalFileSystem()
    staging_root = base_path.parent / "seed-staging"
    commit_test_tables(
        delta_root=str(base_path),
        staging_root=str(staging_root),
        fs=fs,
        tables={
            "artifacts/metrics": pl.DataFrame(
                metrics, schema=MetricArtifact.POLARS_SCHEMA
            ),
            "artifacts/index": pl.DataFrame(index, schema=ARTIFACT_INDEX_SCHEMA),
        },
        step_run_id="0" * 32,
    )


class _SimpleOp(OperationDefinition):
    """Minimal operation for lifecycle phase testing."""

    class InputRole(StrEnum):
        source = auto()

    class OutputRole(StrEnum):
        output = auto()

    name: ClassVar[str] = "phases_test"
    inputs: ClassVar[dict[str, InputSpec]] = {
        InputRole.source: InputSpec(artifact_type="metric", required=True),
    }
    outputs: ClassVar[dict[str, OutputSpec]] = {
        OutputRole.output: OutputSpec(
            artifact_type="metric",
            derives_from={"inputs": ["source"]},
        ),
    }

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        return {
            role: PerArtifact([a.materialized_path for a in artifacts])
            for role, artifacts in inputs.input_artifacts.items()
        }

    def execute_function(self, inputs: ExecuteInput) -> dict:
        for path in inputs.inputs["source"]:
            with open(path) as fh:
                content = json.loads(fh.read())
            content["processed"] = True
            stem = os.path.splitext(os.path.basename(path))[0]
            out = os.path.join(inputs.execute_dir, f"{stem}_out.json")
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
        source_ids = match_outputs_to_inputs_by_stem(
            [draft.original_name for draft in drafts],
            [
                (a.materialized_path, a.artifact_id)
                for a in inputs.input_artifacts["source"]
            ],
        )
        result = ArtifactResult()
        for draft, source_id in zip(drafts, source_ids, strict=True):
            result.add_artifact("output", draft, sources={"source": [source_id]})
        return result


class _NoFanOutOp(_SimpleOp):
    """Operation that opts out of per-artifact dispatch."""

    name: ClassVar[str] = "no_fanout_test"
    per_artifact_dispatch: ClassVar[bool] = False


@pytest.fixture
def delta_env(tmp_path: Path):
    """Create Delta root with two input artifacts for batch testing."""
    base = tmp_path / "delta"
    working = tmp_path / "working"
    working.mkdir()
    staging = tmp_path / "staging"
    staging.mkdir()

    metrics = []
    index = []
    ids = []
    for i in range(2):
        artifact = MetricArtifact.draft({"value": i}, f"metric_{i}.json", step_number=0)
        artifact.finalize()
        ids.append(artifact.artifact_id)
        metrics.append(artifact.to_row())
        index.append(
            {
                "artifact_id": artifact.artifact_id,
                "artifact_type": "metric",
                "origin_step_number": 0,
                "metadata": "{}",
            }
        )

    _setup_delta(base, metrics, index)

    runtime_env = RuntimeEnvironment(
        delta_root=str(base),
        working_root=str(working),
        staging_root=str(staging),
    )
    return runtime_env, ids


def test_typed_references_survive_portable_execute_and_postprocess(tmp_path):
    dependencies = [
        MetricArtifact.draft({"value": i}, "same.json", step_number=0) for i in range(2)
    ]
    for artifact in dependencies:
        artifact.finalize()
    configs = [
        ExecutionConfigArtifact.draft(
            {"source": {"$artifact": artifact.artifact_id}},
            f"config_{i}.json",
            step_number=0,
        )
        for i, artifact in enumerate(dependencies)
    ]
    for config in configs:
        config.finalize()
    runtime = RuntimeEnvironment(
        delta_root=str(tmp_path / "delta"),
        staging_root=str(tmp_path / "staging"),
        working_root=str(tmp_path / "work"),
    )
    commit_outputs(runtime.delta_root, artifacts=[*dependencies, *configs])
    unit = ExecutionUnit(
        operation=ReferenceConsumer(),
        inputs={"config": [config.artifact_id for config in configs]},
        execution_spec_id="a" * 32,
        step_number=1,
    )
    prepped = prep_unit(unit, runtime)
    remote = tmp_path / "relocated"
    remote.mkdir()
    raw_results = []
    for i, execute_input in enumerate(prepped.artifact_execute_inputs):
        prepared = Path(execute_input.inputs["config"][0])
        moved = remote / prepared.name
        shutil.move(prepared, moved)
        assert str(tmp_path) not in moved.read_text()
        execute_dir = remote / str(i)
        execute_dir.mkdir()
        remote_input = replace(
            execute_input,
            inputs={"config": [str(moved)]},
            execute_dir=str(execute_dir),
        )
        raw_results.extend(
            LocalExecuteRouter().route_execute(
                prepped.operation, [remote_input], str(remote)
            )
        )
        for output in execute_dir.iterdir():
            shutil.move(output, prepped.artifact_execute_dirs[i])
    assert all(
        Path(artifact.materialized_path).is_file()
        for references in prepped.references.values()
        for artifact in references.values()
    )
    result = post_unit(prepped, raw_results, runtime)
    assert len(result.artifacts["metrics"]) == 2
    assert {edge.source_artifact_id for edge in result.edges} == {
        config.artifact_id for config in configs
    }


class TestSplitPreparedInputs:
    def test_per_artifact_slices_and_wraps(self):
        prepared = {
            "data": PerArtifact(["/a.csv", "/b.csv"]),
            "config": "shared",
        }
        assert _split_prepared_inputs(prepared, 0, 2) == {
            "data": ["/a.csv"],
            "config": "shared",
        }
        assert _split_prepared_inputs(prepared, 1, 2) == {
            "data": ["/b.csv"],
            "config": "shared",
        }

    def test_per_artifact_wrong_length_raises(self):
        prepared = {"data": PerArtifact(["/a.csv"])}
        with pytest.raises(ValueError, match="batch_size is 3"):
            _split_prepared_inputs(prepared, 0, 3)

    def test_raw_list_not_sliced(self):
        """Raw lists pass through unchanged regardless of length.

        Matching the batch length does not imply per-artifact slicing.
        """
        prepared = {"data": ["/a.csv", "/b.csv", "/c.csv"], "config": "shared"}
        for index in range(3):
            result = _split_prepared_inputs(prepared, index, 3)
            assert result["data"] == ["/a.csv", "/b.csv", "/c.csv"]
            assert result["config"] == "shared"

    def test_passthrough_non_list_and_dict(self):
        prepared = {"count": 5, "options": {"key": "val"}}
        result = _split_prepared_inputs(prepared, 0, 3)
        assert result == {"count": 5, "options": {"key": "val"}}

    def test_empty_dict(self):
        assert _split_prepared_inputs({}, 0, 1) == {}


class TestReassembleResults:
    def test_all_none(self, tmp_path):
        d0 = tmp_path / "artifact_0"
        d0.mkdir()
        memory, _files = _reassemble_results([None, None], [str(d0), str(d0)])
        assert memory is None

    def test_dict_concatenates_lists(self, tmp_path):
        d0 = tmp_path / "artifact_0"
        d0.mkdir()
        d1 = tmp_path / "artifact_1"
        d1.mkdir()

        results = [
            {"created_files": ["a.txt"]},
            {"created_files": ["b.txt"]},
        ]
        memory, _ = _reassemble_results(results, [str(d0), str(d1)])
        assert memory == {"created_files": ["a.txt", "b.txt"]}

    def test_collects_files_from_all_dirs(self, tmp_path):
        d0 = tmp_path / "artifact_0"
        d0.mkdir()
        (d0 / "out_0.csv").write_text("data0")

        d1 = tmp_path / "artifact_1"
        d1.mkdir()
        (d1 / "out_1.csv").write_text("data1")

        _, files = _reassemble_results([None, None], [str(d0), str(d1)])
        basenames = sorted(os.path.basename(f) for f in files)
        assert basenames == ["out_0.csv", "out_1.csv"]

    def test_non_dict_results_returned_as_list(self, tmp_path):
        d0 = tmp_path / "artifact_0"
        d0.mkdir()
        results = ["result_a", "result_b"]
        memory, _ = _reassemble_results(results, [str(d0), str(d0)])
        assert memory == ["result_a", "result_b"]

    def test_reassembly_preserves_duplicate_basenames_and_order(self, tmp_path):
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

        memory, files = _reassemble_results([None, None], directories)

        assert memory is None
        assert files == expected


class TestPrepUnit:
    def test_returns_per_artifact_inputs(self, delta_env):
        runtime_env, ids = delta_env
        runtime_env = runtime_env.model_copy(update={"worker_id": 42})
        unit = ExecutionUnit(
            operation=_SimpleOp(),
            inputs={"source": ids},
            execution_spec_id="spec_pp" + "0" * 26,
            step_number=1,
        )

        prepped = prep_unit(unit, runtime_env)

        assert isinstance(prepped, PreppedUnit)
        assert len(prepped.artifact_execute_inputs) == 2
        assert len(prepped.artifact_execute_dirs) == 2
        for d in prepped.artifact_execute_dirs:
            assert os.path.isdir(d)
        # Each ExecuteInput has a single-element list (sliced from list of 2)
        for ei in prepped.artifact_execute_inputs:
            assert isinstance(ei.inputs["source"], list)
            assert len(ei.inputs["source"]) == 1

    def test_per_artifact_dispatch_false_single_input(self, delta_env):
        runtime_env, ids = delta_env
        unit = ExecutionUnit(
            operation=_NoFanOutOp(),
            inputs={"source": ids},
            execution_spec_id="spec_nf" + "0" * 26,
            step_number=1,
        )

        prepped = prep_unit(unit, runtime_env)

        assert len(prepped.artifact_execute_inputs) == 1
        assert len(prepped.artifact_execute_dirs) == 1
        # The single ExecuteInput has the full list
        assert isinstance(prepped.artifact_execute_inputs[0].inputs["source"], list)
        assert len(prepped.artifact_execute_inputs[0].inputs["source"]) == 2

    def test_split_batch_size_one_produces_single_input(self, delta_env):
        """A single-artifact unit with splitting produces 1 ExecuteInput."""
        runtime_env, ids = delta_env
        unit = ExecutionUnit(
            operation=_SimpleOp(),
            inputs={"source": [ids[0]]},
            execution_spec_id="spec_s1" + "0" * 26,
            step_number=1,
        )

        prepped = prep_unit(unit, runtime_env)

        assert len(prepped.artifact_execute_inputs) == 1
        assert len(prepped.artifact_execute_dirs) == 1


class TestRoundTripEquivalence:
    def test_manual_phases_match_lifecycle(self, delta_env):
        """prep_unit → route_execute → post_unit matches run_creator_lifecycle."""
        runtime_env, ids = delta_env

        # --- full lifecycle ---
        unit_mono = ExecutionUnit(
            operation=_SimpleOp(),
            inputs={"source": ids},
            execution_spec_id="spec_mo" + "0" * 26,
            step_number=1,
        )
        mono_result = run_creator_lifecycle(unit_mono, runtime_env)

        # --- split path ---
        unit_split = ExecutionUnit(
            operation=_SimpleOp(),
            inputs={"source": ids},
            execution_spec_id="spec_sp" + "0" * 26,
            step_number=1,
        )
        prepped = prep_unit(unit_split, runtime_env)

        # Route the unit's full per-artifact list; the router iterates
        router = LocalExecuteRouter()
        raw_results = router.route_execute(
            prepped.operation, prepped.artifact_execute_inputs, prepped.sandbox_path
        )

        split_result = post_unit(prepped, raw_results, runtime_env)

        assert isinstance(split_result, LifecycleResult)
        assert set(split_result.artifacts.keys()) == set(mono_result.artifacts.keys())
        for role in mono_result.artifacts:
            assert len(split_result.artifacts[role]) == len(mono_result.artifacts[role])
        assert len(split_result.edges) == len(mono_result.edges)


@pytest.fixture
def managed_file_unit(
    delta_env: tuple[RuntimeEnvironment, list[str]],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[ExecutionUnit, RuntimeEnvironment]:
    """Prepare declared inputs and make the command fixture importable by workers."""
    runtime, ids = delta_env
    tests_root = str(Path(__file__).resolve().parents[2])
    monkeypatch.setenv(
        "PYTHONPATH",
        os.pathsep.join(filter(None, [tests_root, os.getenv("PYTHONPATH")])),
    )
    return (
        ExecutionUnit(
            operation=ManagedFileCommand(),
            inputs={"source": ids},
            execution_spec_id="a" * 32,
            step_number=1,
        ),
        runtime.model_copy(update={"files_root": str(tmp_path / "files")}),
    )


@pytest.mark.parametrize(
    "operation_class", [ManagedFileCommand, WholeBatchManagedFileCommand]
)
def test_post_unit_retains_command_files_after_cleanup(
    managed_file_unit: tuple[ExecutionUnit, RuntimeEnvironment],
    operation_class: type[ManagedFileCommand],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Relocation preserves descriptors and parents across both dispatch shapes."""
    unit, runtime = managed_file_unit
    unit = unit.model_copy(update={"operation": operation_class()})
    prepped = prep_unit(unit, runtime)
    raw_results = LocalExecuteRouter().route_execute(
        prepped.operation, prepped.artifact_execute_inputs, prepped.sandbox_path
    )
    original_postprocess = ManagedFileCommand.postprocess
    descriptors = []

    def capture_drafts(
        operation: ManagedFileCommand, inputs: PostprocessInput
    ) -> ArtifactResult:
        assert inputs.files_dir == prepped.files_dir
        result = original_postprocess(operation, inputs)
        descriptors.extend(
            draft.model_copy(deep=True)
            .finalize()
            .model_dump(exclude={"path", "materialized_path"})
            for draft in result.artifacts["files"]
        )
        return result

    monkeypatch.setattr(ManagedFileCommand, "postprocess", capture_drafts)
    result = post_unit(prepped, raw_results, runtime)
    files = result.artifacts["files"]
    assert len(files) == 2
    assert len({artifact.path for artifact in files}) == 2
    assert len({artifact.artifact_id for artifact in files}) == 2
    assert [
        artifact.model_dump(exclude={"path", "materialized_path"}) for artifact in files
    ] == descriptors
    assert not Path(prepped.sandbox_path).exists()
    retained_paths = {
        str(path) for path in Path(runtime.files_root).rglob("*") if path.is_file()
    }
    assert retained_paths == {artifact.path for artifact in files}
    assert {Path(path).name for path in retained_paths} == {"output.bin"}
    assert {
        (edge.source_artifact_id, edge.target_artifact_id) for edge in result.edges
    } == {
        (artifact.read_content().decode(), artifact.artifact_id) for artifact in files
    }


def test_post_unit_managed_files_require_storage(
    managed_file_unit: tuple[ExecutionUnit, RuntimeEnvironment],
) -> None:
    """Operations requiring retention receive None when storage is absent."""
    unit, runtime = managed_file_unit
    runtime = runtime.model_copy(update={"files_root": None})
    prepped = prep_unit(unit, runtime)
    assert prepped.files_dir is None
    with pytest.raises(ValueError, match="Managed file storage must be configured"):
        post_unit(prepped, [None, None], runtime)
    assert Path(prepped.sandbox_path).exists()


@pytest.mark.parametrize(
    "failure", ["copy", "digest", "bad_digest", "bad_size", "upload"]
)
def test_managed_file_failure_rejects_outputs_and_preserves_command_evidence(
    managed_file_unit: tuple[ExecutionUnit, RuntimeEnvironment],
    failure: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Postprocess and storage failures stage no successful output artifacts."""
    unit, runtime = managed_file_unit

    def fail_io(*args: Any, **kwargs: Any) -> None:
        msg = f"injected {failure} failure"
        raise OSError(msg)

    original_postprocess = ManagedFileCommand.postprocess

    def corrupt_descriptor(
        operation: ManagedFileCommand, inputs: PostprocessInput
    ) -> ArtifactResult:
        result = original_postprocess(operation, inputs)
        artifact = result.artifacts["files"][0]
        if failure == "bad_digest":
            artifact.content_hash = "0" * 32
        else:
            artifact.size_bytes += 1
        return result

    if failure == "copy":
        monkeypatch.setattr(
            managed_file_ops, "shutil", SimpleNamespace(copyfile=fail_io)
        )
    elif failure == "digest":
        monkeypatch.setattr(managed_file_ops, "compute_stream_digest", fail_io)
    elif failure == "upload":
        monkeypatch.setattr(
            "artisan.execution.executors.creator_phases.shutil.move", fail_io
        )
    else:
        monkeypatch.setattr(ManagedFileCommand, "postprocess", corrupt_descriptor)

    result = run_creator_flow(unit, runtime)

    assert not result.success
    assert result.artifact_ids == []
    assert result.staging_path is not None
    expected_error = (
        "failed integrity"
        if failure.startswith("bad_")
        else f"injected {failure} failure"
    )
    assert expected_error in result.error
    command_outputs = list(Path(runtime.working_root).rglob("selected-*/output.bin"))
    assert {path.read_text() for path in command_outputs} == set(unit.inputs["source"])
    assert len(list(Path(runtime.working_root).rglob("scratch.txt"))) == 2


@pytest.fixture
def clear_memory_fs() -> Any:
    """Clear fsspec's MemoryFileSystem class-level store around each test."""
    import fsspec

    fs = fsspec.filesystem("memory")
    fs.store.clear()
    fs.pseudo_dirs.clear()
    yield fs
    fs.store.clear()
    fs.pseudo_dirs.clear()


def _make_runtime_env(
    files_root: str, storage: StorageConfig, working_root: Path
) -> RuntimeEnvironment:
    return RuntimeEnvironment(
        delta_root=str(working_root / "delta"),
        staging_root=str(working_root / "staging"),
        working_root=str(working_root),
        files_root=files_root,
        storage=storage,
    )


def _make_artifact(path: str) -> FileRefArtifact:
    return FileRefArtifact(
        artifact_id="a" * 32,
        origin_step_number=1,
        content_hash="b" * 32,
        size_bytes=10,
        path=path,
        original_name="output",
        extension=".bin",
    )


class TestUploadFilesToRoot:
    """Unit tests for the `_upload_files_to_root` upload helper.

    Uses ``MemoryFileSystem`` as a stand-in for S3 so the tests do
    not need MinIO.
    """

    def test_local_case_moves_file_and_rewrites_paths(self, tmp_path: Path) -> None:
        sandbox = tmp_path / "sandbox"
        files_dir = sandbox / "files_outputs"
        files_dir.mkdir(parents=True)
        source = files_dir / "output_00000.bin"
        source.write_bytes(b"x" * 10)
        files_root = tmp_path / "files_root"
        files_root.mkdir()

        artifact = _make_artifact(str(source))
        runtime_env = _make_runtime_env(
            str(files_root), StorageConfig(protocol="file"), tmp_path
        )

        _upload_files_to_root(
            {"files": [artifact]},
            files_dir=str(files_dir),
            runtime_env=runtime_env,
            execution_run_id="a" * 32,
            step_number=1,
            operation_name="test_op",
            sandbox_path=str(sandbox),
        )

        assert artifact.path is not None
        assert str(files_root) in artifact.path
        assert artifact.path.endswith("output_00000.bin")
        assert os.path.exists(artifact.path)
        # shutil.move relocated the bytes — source no longer exists.
        assert not source.exists()
        # Local: materialized_path points at the new (final) location.
        assert artifact.materialized_path == artifact.path

    def test_cloud_case_uploads_and_rewrites_to_uri(
        self, tmp_path: Path, clear_memory_fs: Any
    ) -> None:
        sandbox = tmp_path / "sandbox"
        files_dir = sandbox / "files_outputs"
        files_dir.mkdir(parents=True)
        source = files_dir / "output_00000.bin"
        source.write_bytes(b"x" * 10)

        artifact = _make_artifact(str(source))
        files_root = "memory://bucket/files"
        runtime_env = _make_runtime_env(
            files_root, StorageConfig(protocol="memory"), tmp_path
        )

        _upload_files_to_root(
            {"files": [artifact]},
            files_dir=str(files_dir),
            runtime_env=runtime_env,
            execution_run_id="c" * 32,
            step_number=2,
            operation_name="test_op",
            sandbox_path=str(sandbox),
        )

        assert artifact.path is not None
        assert artifact.path.startswith("memory://bucket/files/")
        assert artifact.path.endswith("output_00000.bin")
        fs = runtime_env.storage.filesystem()
        assert fs.exists(artifact.path)
        # fs.put copied (didn't move): the sandbox source survives.
        assert source.exists()
        # Cloud: materialized_path keeps the local sandbox ref so
        # in-process consumers avoid an fs.get round-trip.
        assert artifact.materialized_path == str(source)

    def test_already_cloud_path_left_alone(self, tmp_path: Path) -> None:
        sandbox = tmp_path / "sandbox"
        files_dir = sandbox / "files_outputs"
        files_dir.mkdir(parents=True)
        files_root = tmp_path / "files_root"
        files_root.mkdir()

        # Passthrough reference to a pre-existing cloud URI.
        artifact = _make_artifact("s3://other-bucket/already/there.bin")
        original_path = artifact.path
        runtime_env = _make_runtime_env(
            str(files_root), StorageConfig(protocol="file"), tmp_path
        )

        _upload_files_to_root(
            {"files": [artifact]},
            files_dir=str(files_dir),
            runtime_env=runtime_env,
            execution_run_id="d" * 32,
            step_number=1,
            operation_name="test_op",
            sandbox_path=str(sandbox),
        )

        assert artifact.path == original_path

    def test_outside_sandbox_path_left_alone(self, tmp_path: Path) -> None:
        sandbox = tmp_path / "sandbox"
        files_dir = sandbox / "files_outputs"
        files_dir.mkdir(parents=True)
        files_root = tmp_path / "files_root"
        files_root.mkdir()

        # Absolute path outside files_dir — operation ingested an
        # existing file by reference.
        elsewhere = tmp_path / "elsewhere.bin"
        elsewhere.write_bytes(b"x" * 5)
        artifact = _make_artifact(str(elsewhere))
        runtime_env = _make_runtime_env(
            str(files_root), StorageConfig(protocol="file"), tmp_path
        )

        _upload_files_to_root(
            {"files": [artifact]},
            files_dir=str(files_dir),
            runtime_env=runtime_env,
            execution_run_id="e" * 32,
            step_number=1,
            operation_name="test_op",
            sandbox_path=str(sandbox),
        )

        assert artifact.path == str(elsewhere)
        assert elsewhere.exists()

    def test_shared_source_path_dedups_upload(
        self, tmp_path: Path, clear_memory_fs: Any
    ) -> None:
        """AppendableGenerator emits N artifacts sharing one JSONL."""
        sandbox = tmp_path / "sandbox"
        files_dir = sandbox / "files_outputs"
        files_dir.mkdir(parents=True)
        source = files_dir / "records_0.jsonl"
        source.write_bytes(b'{"record_id":"r0"}\n{"record_id":"r1"}\n')

        art1 = _make_artifact(str(source))
        art2 = _make_artifact(str(source))

        files_root = "memory://bucket/files"
        runtime_env = _make_runtime_env(
            files_root, StorageConfig(protocol="memory"), tmp_path
        )

        fs = runtime_env.storage.filesystem()
        put_calls: list[tuple[str, str]] = []
        original_put = fs.put

        def counting_put(local: str, remote: str, *a: Any, **kw: Any) -> Any:
            put_calls.append((local, remote))
            return original_put(local, remote, *a, **kw)

        fs.put = counting_put  # type: ignore[method-assign]
        try:
            _upload_files_to_root(
                {"records": [art1, art2]},
                files_dir=str(files_dir),
                runtime_env=runtime_env,
                execution_run_id="f" * 32,
                step_number=1,
                operation_name="test_op",
                sandbox_path=str(sandbox),
            )
        finally:
            fs.put = original_put  # type: ignore[method-assign]

        assert len(put_calls) == 1, f"expected one fs.put, got {put_calls}"
        assert art1.path == art2.path
        assert art1.path is not None
        assert art1.path.startswith("memory://bucket/files/")

    def test_upload_failure_raises_upload_failure_caught_as_postprocess(
        self, tmp_path: Path, clear_memory_fs: Any
    ) -> None:
        """_UploadFailure propagates as _PostprocessFailure via subclassing."""
        from artisan.execution.executors.creator import (
            _ExecuteFailure,
            _PostprocessFailure,
            _UploadFailure,
        )

        sandbox = tmp_path / "sandbox"
        files_dir = sandbox / "files_outputs"
        files_dir.mkdir(parents=True)
        source = files_dir / "output.bin"
        source.write_bytes(b"x")

        artifact = _make_artifact(str(source))
        files_root = "memory://bucket/files"
        runtime_env = _make_runtime_env(
            files_root, StorageConfig(protocol="memory"), tmp_path
        )

        fs = runtime_env.storage.filesystem()
        original_put = fs.put

        def failing_put(*a: Any, **kw: Any) -> Any:
            msg = "disk full"
            raise OSError(msg)

        fs.put = failing_put  # type: ignore[method-assign]
        try:
            with pytest.raises(_UploadFailure) as exc_info:
                _upload_files_to_root(
                    {"files": [artifact]},
                    files_dir=str(files_dir),
                    runtime_env=runtime_env,
                    execution_run_id="9" * 32,
                    step_number=1,
                    operation_name="test_op",
                    sandbox_path=str(sandbox),
                )
        finally:
            fs.put = original_put  # type: ignore[method-assign]

        # Subclass invariant: the existing except tuple catches us.
        assert isinstance(exc_info.value, _PostprocessFailure)
        try:
            raise exc_info.value
        except (_PostprocessFailure, _ExecuteFailure):
            pass  # caught by existing clause
        except Exception as e:  # pragma: no cover — regression guard
            pytest.fail(
                f"_UploadFailure escaped the existing except tuple: {type(e).__name__}"
            )
        assert "disk full" in str(exc_info.value)


class TestIsUnderLocalDir:
    """Unit tests for `_is_under_local_dir`."""

    def test_inside_returns_true(self, tmp_path: Path) -> None:
        root = tmp_path / "sandbox"
        root.mkdir()
        candidate = root / "a" / "b.bin"
        assert _is_under_local_dir(str(candidate), str(root))

    def test_outside_returns_false(self, tmp_path: Path) -> None:
        root = tmp_path / "sandbox"
        root.mkdir()
        elsewhere = tmp_path / "elsewhere.bin"
        assert not _is_under_local_dir(str(elsewhere), str(root))

    def test_cloud_uri_returns_false(self, tmp_path: Path) -> None:
        root = tmp_path / "sandbox"
        root.mkdir()
        assert not _is_under_local_dir("s3://bucket/path.bin", str(root))
        assert not _is_under_local_dir("memory://bucket/path.bin", str(root))
