"""Tests for the split creator lifecycle phases (prep_unit / post_unit)."""

from __future__ import annotations

import json
import os
from enum import StrEnum, auto
from pathlib import Path
from typing import Any, ClassVar

import polars as pl
import pytest
import xxhash

from artisan.execution.compute.local import LocalComputeRouter
from artisan.execution.executors.creator import LifecycleResult, run_creator_lifecycle
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
from artisan.schemas.artifact.large_file import LargeFileArtifact
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

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _compute_id(content: bytes) -> str:
    return xxhash.xxh3_128(content).hexdigest()


def _setup_delta(base_path: Path, metrics: list[dict], index: list[dict]) -> None:
    metrics_path = base_path / "artifacts/metrics"
    pl.DataFrame(metrics, schema=MetricArtifact.POLARS_SCHEMA).write_delta(
        str(metrics_path)
    )
    index_path = base_path / "artifacts/index"
    pl.DataFrame(index, schema=ARTIFACT_INDEX_SCHEMA).write_delta(str(index_path))


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
            infer_lineage_from={"inputs": ["source"]},
        ),
    }

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        return {
            role: [a.materialized_path for a in artifacts]
            for role, artifacts in inputs.input_artifacts.items()
        }

    def execute(self, inputs: ExecuteInput) -> dict:
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
        return ArtifactResult(success=True, artifacts={"output": drafts})


class _NoFanOutOp(_SimpleOp):
    """Operation that opts out of per-artifact dispatch."""

    name: ClassVar[str] = "no_fanout_test"
    per_artifact_dispatch: ClassVar[bool] = False


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


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
        content = json.dumps({"value": i}, sort_keys=True).encode("utf-8")
        aid = _compute_id(content)
        ids.append(aid)
        metrics.append(
            {
                "artifact_id": aid,
                "origin_step_number": 0,
                "content": content,
                "original_name": f"metric_{i}",
                "extension": ".json",
                "metadata": "{}",
                "external_path": None,
            }
        )
        index.append(
            {
                "artifact_id": aid,
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


# ---------------------------------------------------------------------------
# _split_prepared_inputs tests
# ---------------------------------------------------------------------------


class TestSplitPreparedInputs:
    def test_slices_lists_matching_batch_size(self):
        prepared = {"data": ["/a.csv", "/b.csv"], "config": "shared"}
        assert _split_prepared_inputs(prepared, 0, 2) == {
            "data": ["/a.csv"],
            "config": "shared",
        }
        assert _split_prepared_inputs(prepared, 1, 2) == {
            "data": ["/b.csv"],
            "config": "shared",
        }

    def test_passthrough_non_list(self):
        prepared = {"count": 5, "options": {"key": "val"}}
        result = _split_prepared_inputs(prepared, 0, 3)
        assert result == {"count": 5, "options": {"key": "val"}}

    def test_passthrough_different_length_list(self):
        prepared = {"stages": ["a", "b", "c"], "data": ["/x.csv"]}
        result = _split_prepared_inputs(prepared, 0, 2)
        assert result["stages"] == ["a", "b", "c"]
        assert result["data"] == ["/x.csv"]

    def test_nested_dict_list(self):
        """Pattern B: list of dicts, each representing per-artifact config."""
        prepared = {
            "items": [
                {"config_path": "/a.json", "name": "a"},
                {"config_path": "/b.json", "name": "b"},
            ]
        }
        result = _split_prepared_inputs(prepared, 0, 2)
        assert result["items"] == [{"config_path": "/a.json", "name": "a"}]

    def test_empty_dict(self):
        assert _split_prepared_inputs({}, 0, 1) == {}


# ---------------------------------------------------------------------------
# _reassemble_results tests
# ---------------------------------------------------------------------------


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

    def test_filters_exceptions(self, tmp_path):
        d0 = tmp_path / "artifact_0"
        d0.mkdir()
        d1 = tmp_path / "artifact_1"
        d1.mkdir()

        results = [
            RuntimeError("boom"),
            {"results": ["ok"]},
        ]
        memory, _ = _reassemble_results(results, [str(d0), str(d1)])
        assert memory == {"results": ["ok"]}

    def test_all_exceptions_returns_none(self, tmp_path):
        d0 = tmp_path / "artifact_0"
        d0.mkdir()
        results = [RuntimeError("a"), ValueError("b")]
        memory, _ = _reassemble_results(results, [str(d0), str(d0)])
        assert memory is None

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


# ---------------------------------------------------------------------------
# prep_unit tests
# ---------------------------------------------------------------------------


class TestPrepUnit:
    def test_returns_per_artifact_inputs(self, delta_env):
        runtime_env, ids = delta_env
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


# ---------------------------------------------------------------------------
# Round-trip equivalence: prep → execute → post matches run_creator_lifecycle
# ---------------------------------------------------------------------------


class TestRoundTripEquivalence:
    def test_split_path_matches_monolithic(self, delta_env):
        """prep_unit → execute per-artifact → post_unit produces same results."""
        runtime_env, ids = delta_env

        # --- monolithic path ---
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

        # Execute each artifact individually
        router = LocalComputeRouter()
        raw_results = []
        for ei in prepped.artifact_execute_inputs:
            result = router.route_execute(prepped.operation, ei, prepped.sandbox_path)
            raw_results.append(result)

        split_result = post_unit(prepped, raw_results, runtime_env)

        # Compare results
        assert isinstance(split_result, LifecycleResult)
        assert set(split_result.artifacts.keys()) == set(mono_result.artifacts.keys())
        for role in mono_result.artifacts:
            assert len(split_result.artifacts[role]) == len(mono_result.artifacts[role])
        assert len(split_result.edges) == len(mono_result.edges)


# ---------------------------------------------------------------------------
# TestUploadFilesToRoot — unit tests for _upload_files_to_root
# ---------------------------------------------------------------------------


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


def _make_artifact(external_path: str) -> LargeFileArtifact:
    return LargeFileArtifact(
        artifact_id="a" * 32,
        origin_step_number=1,
        content_hash="b" * 32,
        size_bytes=10,
        external_path=external_path,
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

        assert artifact.external_path is not None
        assert str(files_root) in artifact.external_path
        assert artifact.external_path.endswith("output_00000.bin")
        assert os.path.exists(artifact.external_path)
        # shutil.move relocated the bytes — source no longer exists.
        assert not source.exists()
        # Local: materialized_path points at the new (final) location.
        assert artifact.materialized_path == artifact.external_path

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

        assert artifact.external_path is not None
        assert artifact.external_path.startswith("memory://bucket/files/")
        assert artifact.external_path.endswith("output_00000.bin")
        fs = runtime_env.storage.filesystem()
        assert fs.exists(artifact.external_path)
        # fs.put copied (didn't move): the sandbox source survives.
        assert source.exists()
        # Cloud: materialized_path keeps the local sandbox ref so
        # in-process consumers avoid an fs.get round-trip.
        assert artifact.materialized_path == str(source)

    def test_already_cloud_external_path_left_alone(self, tmp_path: Path) -> None:
        sandbox = tmp_path / "sandbox"
        files_dir = sandbox / "files_outputs"
        files_dir.mkdir(parents=True)
        files_root = tmp_path / "files_root"
        files_root.mkdir()

        # Passthrough reference to a pre-existing cloud URI.
        artifact = _make_artifact("s3://other-bucket/already/there.bin")
        original_ext_path = artifact.external_path
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

        assert artifact.external_path == original_ext_path

    def test_outside_sandbox_external_path_left_alone(self, tmp_path: Path) -> None:
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

        assert artifact.external_path == str(elsewhere)
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
        assert art1.external_path == art2.external_path
        assert art1.external_path is not None
        assert art1.external_path.startswith("memory://bucket/files/")

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
