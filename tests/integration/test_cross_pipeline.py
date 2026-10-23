"""Integration tests for cross-pipeline operations.

Tests IngestPipelineStep (importing artifacts from another pipeline's Delta
store) and ExecutionConfigArtifact with $artifact references.
"""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest
import xxhash
from fixtures.run_outputs import commit_outputs
from fsspec.implementations.local import LocalFileSystem

pytestmark = pytest.mark.integration

from artisan.operations.curator import IngestPipelineStep, Merge
from artisan.operations.examples import (
    DataGenerator,
    DataTransformer,
    DataTransformerConfig,
    MetricCalculator,
)
from artisan.orchestration import PipelineManager
from artisan.orchestration.engine.step_executor import check_cache_for_batch
from artisan.orchestration.engine.step_tracker import StepTracker
from artisan.orchestration.runners import Runner
from artisan.schemas.artifact.file_ref import FileRefArtifact
from artisan.schemas.enums import CachePolicy, FailurePolicy, TablePath
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.schemas.orchestration.pipeline_config import PipelineConfig
from artisan.schemas.orchestration.step_lifecycle import StepDisposition
from artisan.storage.core.artifact_store import ArtifactStore
from artisan.storage.core.run_scope import load_run_step_outputs

from .conftest import (
    FailingTransformer,
    count_artifacts_by_step,
    count_artifacts_by_type,
    get_execution_outputs,
    read_table,
)


def test_ingest_pipeline_step_basic(
    dual_pipeline_env: dict[str, dict[str, str]],
):
    """IngestPipelineStep imports artifacts from another pipeline."""
    env_a = dual_pipeline_env["a"]
    env_b = dual_pipeline_env["b"]

    # Pipeline A: Gen(3) → Transform(3)
    pa = PipelineManager.create(
        name="pipeline_a",
        delta_root=env_a["delta_root"],
        staging_root=env_a["staging_root"],
        working_root=env_a["working_root"],
    )
    step0 = pa.run(
        DataGenerator,
        params={"count": 3, "seed": 42},
        step_runner=Runner.LOCAL,
    )
    pa.run(
        DataTransformer,
        inputs={"dataset": step0.output("datasets")},
        params={
            "scale_factor": 1.5,
            "noise_amplitude": 0.0,
            "variants": 1,
            "seed": 100,
        },
        step_runner=Runner.LOCAL,
    )
    pa.finalize()

    # Pipeline B: Ingest from A step 1
    pb = PipelineManager.create(
        name="pipeline_b",
        delta_root=env_b["delta_root"],
        staging_root=env_b["staging_root"],
        working_root=env_b["working_root"],
    )
    pb.run(
        IngestPipelineStep,
        params={
            "source_delta_root": env_a["delta_root"],
            "source_run_id": pa.config.pipeline_run_id,
            "source_step": 1,
        },
        step_runner=Runner.LOCAL,
    )
    result = pb.finalize()

    assert result["overall_success"]

    # 3 imported artifacts
    assert count_artifacts_by_step(env_b["delta_root"], 0) == 3

    # Imported roots preserve bytes but receive distinct semantic identities
    # because their metadata records the source step.
    a_ids = set(get_execution_outputs(env_a["delta_root"], 1, "dataset"))
    b_ids = set(get_execution_outputs(env_b["delta_root"], 0, "data"))
    assert a_ids.isdisjoint(b_ids)
    source_rows = read_table(env_a["delta_root"], "artifacts/data")
    imported_rows = read_table(env_b["delta_root"], "artifacts/data")
    source_content = set(
        source_rows.filter(pl.col("artifact_id").is_in(a_ids))["content"].to_list()
    )
    imported = imported_rows.filter(pl.col("artifact_id").is_in(b_ids))
    assert set(imported["content"].to_list()) == source_content
    assert all(
        json.loads(metadata)["imported_from"]["origin_step_number"] == 1
        for metadata in imported["metadata"]
    )


def test_ingest_pipeline_step_type_filter(
    dual_pipeline_env: dict[str, dict[str, str]],
):
    """IngestPipelineStep with artifact_type filter imports only matching type."""
    env_a = dual_pipeline_env["a"]
    env_b = dual_pipeline_env["b"]

    # Pipeline A: Gen(2) → MetricCalc(2) — produces data + metric artifacts
    pa = PipelineManager.create(
        name="pipeline_a_filter",
        delta_root=env_a["delta_root"],
        staging_root=env_a["staging_root"],
        working_root=env_a["working_root"],
    )
    step0 = pa.run(
        DataGenerator,
        params={"count": 2, "seed": 42},
        step_runner=Runner.LOCAL,
    )
    pa.run(
        MetricCalculator,
        inputs={"dataset": step0.output("datasets")},
        step_runner=Runner.LOCAL,
    )
    pa.finalize()

    # Pipeline B: Ingest only metrics from A step 1
    pb = PipelineManager.create(
        name="pipeline_b_filter",
        delta_root=env_b["delta_root"],
        staging_root=env_b["staging_root"],
        working_root=env_b["working_root"],
    )
    pb.run(
        IngestPipelineStep,
        params={
            "source_delta_root": env_a["delta_root"],
            "source_run_id": pa.config.pipeline_run_id,
            "source_step": 1,
            "artifact_type": "metric",
        },
        step_runner=Runner.LOCAL,
    )
    result = pb.finalize()

    assert result["overall_success"]

    # Only metric artifacts imported
    assert count_artifacts_by_type(env_b["delta_root"], "metric") == 2
    assert count_artifacts_by_type(env_b["delta_root"], "data") == 0


def test_execution_config_artifact_references(pipeline_env: dict[str, str]):
    """DataTransformerConfig embeds $artifact references to input artifact IDs."""
    delta_root = pipeline_env["delta_root"]

    pipeline = PipelineManager.create(
        name="test_config_refs",
        delta_root=delta_root,
        staging_root=pipeline_env["staging_root"],
        working_root=pipeline_env["working_root"],
    )

    # Step 0: Generate 2 datasets
    step0 = pipeline.run(
        DataGenerator,
        params={"count": 2, "seed": 42},
        step_runner=Runner.LOCAL,
    )

    # Step 1: Generate configs with $artifact references
    pipeline.run(
        DataTransformerConfig,
        inputs={"dataset": step0.output("datasets")},
        params={
            "scale_factors": [1.0, 2.0],
            "noise_amplitudes": [0.0],
            "seed": 42,
        },
        step_runner=Runner.LOCAL,
    )

    result = pipeline.finalize()
    assert result["overall_success"]

    # Read config artifacts from delta
    df_configs = read_table(delta_root, "artifacts/configs")
    assert not df_configs.is_empty()

    # Get step 0 artifact IDs for validation
    step0_ids = set(get_execution_outputs(delta_root, 0, "datasets"))
    assert len(step0_ids) == 2

    # Verify $artifact references in config content
    for row in df_configs.iter_rows(named=True):
        raw = row["content"]
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        content = json.loads(raw)
        if "input" in content and isinstance(content["input"], dict):
            ref = content["input"].get("$artifact")
            if ref is not None:
                assert ref in step0_ids, (
                    f"$artifact reference {ref} should be a valid step 0 artifact ID"
                )


def _imported_source_ids(pipeline: PipelineManager, step_number: int) -> set[str]:
    """Resolve an imported step's original source identities."""
    rows = load_run_step_outputs(
        pipeline.config.delta_root,
        fs=pipeline.config.storage.filesystem(),
        storage_options=pipeline.config.storage.delta_storage_options(),
        pipeline_run_id=pipeline.config.pipeline_run_id,
        step_number=step_number,
        include_prior_steps=False,
    )
    store = ArtifactStore(
        pipeline.config.delta_root,
        fs=pipeline.config.storage.filesystem(),
        storage_options=pipeline.config.storage.delta_storage_options(),
    )
    return {
        artifact.metadata["imported_from"]["artifact_id"]
        for artifact in store.get_artifacts_by_type(
            rows["artifact_id"].to_list(), "data"
        ).values()
    }


def test_ingest_through_union_includes_cached_and_repeated_passthrough(
    dual_pipeline_env: dict[str, dict[str, str]],
) -> None:
    source_env, destination_env = dual_pipeline_env["a"], dual_pipeline_env["b"]
    first = PipelineManager.create(name="first-source", **source_env)
    first.run(DataGenerator, params={"count": 2, "seed": 10})
    first.finalize()
    source = PipelineManager.create(name="selected-source", **source_env)
    cached = source.run(DataGenerator, params={"count": 2, "seed": 10})
    assert cached.disposition == StepDisposition.CACHE_HIT
    source_ids = get_execution_outputs(source_env["delta_root"], 0, "datasets")
    subset = source.run(Merge, inputs={"selected": source_ids[:1]})
    source.run(Merge, inputs={"again": subset.output("merged")})
    source.finalize()
    unrelated = PipelineManager.create(name="unrelated-source", **source_env)
    unrelated.run(DataGenerator, params={"count": 1, "seed": 90})
    unrelated.finalize()

    destination = PipelineManager.create(name="destination", **destination_env)
    params = {
        "source_delta_root": source_env["delta_root"],
        "source_run_id": source.config.pipeline_run_id,
        "source_step": 2,
    }
    destination.run(IngestPipelineStep, params=params)
    destination.run(IngestPipelineStep, params={**params, "include_prior_steps": True})
    assert destination.finalize()["overall_success"]
    assert _imported_source_ids(destination, 0) == set(source_ids[:1])
    assert _imported_source_ids(destination, 1) == set(source_ids)
    imported = read_table(destination.config.delta_root, "artifacts/data")
    assert {
        json.loads(value)["imported_from"]["pipeline_run_id"]
        for value in imported["metadata"]
    } == {source.config.pipeline_run_id}
    assert read_table(
        destination_env["delta_root"], TablePath.ARTIFACT_EDGES.value
    ).is_empty()
    edges = read_table(destination_env["delta_root"], TablePath.EXECUTION_EDGES.value)
    assert edges.filter(pl.col("direction") == "input").is_empty()


@pytest.mark.parametrize("force_whole_step_miss", [False, True])
def test_ingest_repeated_position_observes_source_retry_and_always_executes(
    dual_pipeline_env: dict[str, dict[str, str]],
    monkeypatch: pytest.MonkeyPatch,
    force_whole_step_miss: bool,
) -> None:
    source_env, destination_env = dual_pipeline_env["a"], dual_pipeline_env["b"]
    source = PipelineManager.create(name="source-freshness", **source_env)
    source.run(DataGenerator, params={"count": 1, "seed": 1})
    source.finalize()
    destination = PipelineManager.create(
        name="destination-freshness", **destination_env
    )
    params = {
        "source_delta_root": source_env["delta_root"],
        "source_run_id": source.config.pipeline_run_id,
        "source_step": 0,
    }
    destination.run(
        IngestPipelineStep, params=params, cache_policy=CachePolicy.STEP_COMPLETED
    )
    destination.finalize()
    initial_ids = _imported_source_ids(destination, 0)
    tracker = StepTracker(destination.config.delta_root)
    state = tracker.load_current_states(destination.config.pipeline_run_id)[0]
    assert (
        tracker.check_cache(state.step_spec_id, CachePolicy.STEP_COMPLETED) is not None
    )

    initial_execution = read_table(
        destination.config.delta_root, TablePath.EXECUTIONS.value
    ).row(0, named=True)
    assert (
        check_cache_for_batch(
            initial_execution["execution_spec_id"],
            destination.config.delta_root,
            config=destination.config,
        )
        is not None
    )

    source_retry = PipelineManager(source.config)
    source_retry.run(DataGenerator, params={"count": 1, "seed": 2})
    source_retry.finalize()
    assert source_retry.config.pipeline_run_id == source.config.pipeline_run_id
    if force_whole_step_miss:
        monkeypatch.setattr(StepTracker, "check_cache", lambda *args, **kwargs: None)
    imported_ids = []
    for _ in range(2):
        repeated = PipelineManager(destination.config)
        result = repeated.run(
            IngestPipelineStep,
            params=params,
            skip_cache=False,
            cache_policy=CachePolicy.STEP_COMPLETED,
        )
        assert result.disposition == StepDisposition.EXECUTED
        assert repeated.finalize()["overall_success"]
        imported_ids.append(_imported_source_ids(repeated, 0))
    assert len(imported_ids[0]) == 1
    assert imported_ids[0] != initial_ids
    assert imported_ids[1] == imported_ids[0]
    executions = read_table(destination.config.delta_root, TablePath.EXECUTIONS.value)
    assert executions.height == 3
    assert executions["execution_spec_id"].n_unique() == 1
    assert read_table(
        destination.config.delta_root, TablePath.CACHE_REUSE.value
    ).is_empty()
    assert count_artifacts_by_type(destination.config.delta_root, "data") == 2


@pytest.mark.parametrize("backend", ["local", pytest.param("s3", marks=pytest.mark.s3)])
@pytest.mark.parametrize("damage", [None, "missing", "changed"])
def test_ingest_retains_verified_external_content(
    tmp_path: Path,
    request: pytest.FixtureRequest,
    backend: str,
    damage: str | None,
) -> None:
    if backend == "s3":
        fs, storage, prefix = request.getfixturevalue("s3_fs")
    else:
        fs, storage, prefix = LocalFileSystem(), StorageConfig(), str(tmp_path)
    source_root = f"{prefix}/source-delta"
    artifacts = []
    for name, content in [("first", b"first bytes"), ("second", b"second bytes")]:
        path = f"{prefix}/{name}.bin"
        with fs.open(path, "wb") as stream:
            stream.write(content)
        artifacts.append(
            FileRefArtifact.draft(
                path=path,
                content_hash=xxhash.xxh3_128_hexdigest(content),
                size_bytes=len(content),
                step_number=0,
                original_name=name,
                extension=".bin",
            ).finalize()
        )
    commit_outputs(
        source_root,
        artifacts=artifacts,
        fs=fs,
        storage_options=storage.delta_storage_options(),
    )
    if damage == "missing":
        fs.rm(artifacts[1].path)
    elif damage == "changed":
        with fs.open(artifacts[1].path, "wb") as stream:
            stream.write(b"altered file")
    destination = PipelineManager(
        PipelineConfig(
            name="external-ingest",
            delta_root=f"{prefix}/destination-delta",
            staging_root=f"{prefix}/destination-staging",
            working_root=str(tmp_path / "working"),
            storage=storage,
        )
    )
    result = destination.run(
        IngestPipelineStep,
        params={
            "source_delta_root": source_root,
            "source_run_id": "source-run",
            "source_step": 0,
            "source_storage": storage.model_dump(),
        },
    )
    summary = destination.finalize()
    index = read_table(
        destination.config.delta_root,
        TablePath.ARTIFACT_INDEX.value,
        fs=fs,
        storage_options=storage.delta_storage_options(),
    )
    if damage is not None:
        assert not summary["overall_success"]
        assert result.error
        assert "ArtifactIntegrityError" in result.error
        assert index.is_empty()
        return
    assert summary["overall_success"]
    assert index.height == 2
    store = ArtifactStore(
        destination.config.delta_root,
        fs=fs,
        storage_options=storage.delta_storage_options(),
    )
    imported = store.get_artifacts_by_type(index["artifact_id"].to_list(), "file_ref")
    locations = read_table(
        destination.config.delta_root,
        TablePath.ARTIFACT_LOCATIONS.value,
        fs=fs,
        storage_options=storage.delta_storage_options(),
    )
    assert set(locations["uri"].to_list()) == {artifact.path for artifact in artifacts}
    for artifact in imported.values():
        source = next(source for source in artifacts if source.path == artifact.path)
        assert artifact.content_hash == source.content_hash
        assert artifact.size_bytes == source.size_bytes
        (tmp_path / "materialized").mkdir(exist_ok=True)
        output_path = artifact.materialize_to(str(tmp_path / "materialized"), fs=fs)
        assert Path(output_path).read_bytes() == source.read_content(fs=fs)
        assert fs.exists(source.path)


def test_ingest_partial_source_imports_only_successful_execution_outputs(
    dual_pipeline_env: dict[str, dict[str, str]],
) -> None:
    source = PipelineManager.create(
        name="partial-source",
        failure_policy=FailurePolicy.CONTINUE,
        **dual_pipeline_env["a"],
    )
    generated = source.run(DataGenerator, params={"count": 3, "seed": 42})
    partial = source.run(
        FailingTransformer,
        inputs={"dataset": generated.output("datasets")},
        params={"fail_on_index": 1},
    )
    source.finalize()
    assert partial.succeeded_count == 2
    assert partial.failed_count == 1
    destination = PipelineManager.create(
        name="import-partial", **dual_pipeline_env["b"]
    )
    destination.run(
        IngestPipelineStep,
        params={
            "source_delta_root": source.config.delta_root,
            "source_run_id": source.config.pipeline_run_id,
            "source_step": 1,
        },
    )
    assert destination.finalize()["overall_success"]
    accepted = get_execution_outputs(source.config.delta_root, 1, "dataset")
    assert len(accepted) == 2
    assert _imported_source_ids(destination, 0) == set(accepted)


def test_config_sweep_preserves_declared_ancestry_and_resolved_paths(
    pipeline_env: dict[str, str],
) -> None:
    """Every dataset/config variant keeps its parent through an actual script run."""
    from artisan.operations.examples import DataTransformerScript

    from .conftest import load_artifact_edges

    pipeline = PipelineManager.create(
        name="explicit_config_workflow", preserve_working=True, **pipeline_env
    )
    data = pipeline.run(
        DataGenerator, params={"count": 2, "seed": 31, "rows_per_file": 3}
    )
    configs = pipeline.run(
        DataTransformerConfig,
        inputs={"dataset": data.output("datasets")},
        params={
            "scale_factors": [1.0, 2.0],
            "noise_amplitudes": [0.0, 0.1],
            "seed": 31,
        },
        batch_strategy={"artifacts_per_unit": 2},
    )
    pipeline.run(
        DataTransformerScript,
        inputs={"dataset": data.output("datasets"), "config": configs.output("config")},
        batch_strategy={"artifacts_per_unit": 4},
    )
    assert pipeline.finalize()["overall_success"]
    delta_root = pipeline_env["delta_root"]
    data_ids = set(get_execution_outputs(delta_root, 0, "datasets"))
    config_ids = set(get_execution_outputs(delta_root, 1, "config"))
    output_ids = set(get_execution_outputs(delta_root, 2, "dataset"))
    assert len(data_ids) == 2
    assert len(config_ids) == len(output_ids) == 8
    config_rows = read_table(delta_root, "artifacts/configs").filter(
        pl.col("artifact_id").is_in(config_ids)
    )
    config_edges = load_artifact_edges(delta_root, config_ids)
    output_edges = load_artifact_edges(delta_root, output_ids)
    assert config_edges.height == output_edges.height == 8
    assert set(config_edges["source_role"]) == {"dataset"}
    assert set(output_edges["source_role"]) == {"config"}
    assert set(output_edges["source_artifact_id"]) == config_ids
    expected_parents = {
        row["artifact_id"]: json.loads(row["content"])["input"]["$artifact"]
        for row in config_rows.iter_rows(named=True)
    }
    assert set(expected_parents.values()) == data_ids
    assert set(
        zip(
            config_edges["target_artifact_id"],
            config_edges["source_artifact_id"],
            strict=True,
        )
    ) == set(expected_parents.items())
    resolved_configs = []
    for path in Path(pipeline_env["working_root"]).rglob("*.json"):
        if path.stem not in config_ids:
            continue
        content = json.loads(path.read_text())
        if isinstance(content.get("input"), str):
            resolved_configs.append((path, content))
    assert {path.stem for path, _ in resolved_configs} == config_ids
    for path, content in resolved_configs:
        input_path = Path(content["input"])
        assert input_path.exists()
        assert input_path.stem == expected_parents[path.stem]
        assert input_path.read_bytes()


def test_imported_configs_are_explicit_new_roots(
    dual_pipeline_env: dict[str, dict[str, str]],
) -> None:
    """Importing configs deliberately preserves content without foreign-ID edges."""
    source = PipelineManager.create(name="config_source", **dual_pipeline_env["a"])
    data = source.run(DataGenerator, params={"count": 1, "seed": 24})
    source.run(DataTransformerConfig, inputs={"dataset": data.output("datasets")})
    assert source.finalize()["overall_success"]
    destination = PipelineManager.create(name="config_import", **dual_pipeline_env["b"])
    destination.run(
        IngestPipelineStep,
        params={
            "source_delta_root": source.config.delta_root,
            "source_run_id": source.config.pipeline_run_id,
            "source_step": 1,
            "artifact_type": "config",
        },
    )
    assert destination.finalize()["overall_success"]
    imported = read_table(destination.config.delta_root, "artifacts/configs")
    assert imported.height == 1
    original = read_table(source.config.delta_root, "artifacts/configs")
    assert imported["content"].to_list() == original["content"].to_list()
    assert imported["artifact_id"].to_list() != original["artifact_id"].to_list()
    assert read_table(
        destination.config.delta_root, TablePath.ARTIFACT_EDGES.value
    ).is_empty()


def test_import_rejects_source_store_with_inferred_lineage_format(
    dual_pipeline_env: dict[str, dict[str, str]],
) -> None:
    """The source-store boundary cannot admit historical inferred provenance."""
    from artisan.storage.core.store_format import STORE_MANIFEST_PATH

    source = PipelineManager.create(name="old_format_source", **dual_pipeline_env["a"])
    source.run(DataGenerator, params={"count": 1, "seed": 42})
    assert source.finalize()["overall_success"]
    manifest_path = Path(source.config.delta_root) / STORE_MANIFEST_PATH
    manifest = json.loads(manifest_path.read_text())
    manifest["store_format"] = 5
    manifest_path.write_text(json.dumps(manifest))
    destination = PipelineManager.create(
        name="reject_old_source", **dual_pipeline_env["b"]
    )
    result = destination.run(
        IngestPipelineStep,
        params={
            "source_delta_root": source.config.delta_root,
            "source_run_id": source.config.pipeline_run_id,
            "source_step": 0,
        },
    )
    assert not destination.finalize()["overall_success"]
    assert result.error
    assert "IncompatibleStoreError" in result.error
    assert read_table(
        destination.config.delta_root, TablePath.ARTIFACT_INDEX.value
    ).is_empty()
    assert json.loads(manifest_path.read_text())["store_format"] == 5
