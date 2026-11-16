"""Real executor failure paths cannot alter already published worker evidence."""

from __future__ import annotations

from pathlib import Path

import pytest
from fsspec.implementations.local import LocalFileSystem

from artisan.errors import StoreIntegrityError
from artisan.execution.executors.curator import run_curator_flow
from artisan.execution.models.execution_unit import ExecutionUnit
from artisan.execution.recording import parquet_writer
from artisan.operations.curator import Merge
from artisan.schemas.execution.runtime_environment import RuntimeEnvironment
from artisan.storage.io.worker_seal import verify_worker_seal


def _execute(tmp_path: Path, *, shared: bool = False):
    from artisan.schemas.execution.storage_config import StorageConfig
    from artisan.storage.io.input_registration import register_raw_inputs
    from artisan.storage.io.retained_store import create_retained_store

    storage = StorageConfig()
    committer = create_retained_store(
        str(tmp_path / "retained-delta"),
        str(tmp_path / "files"),
        fs=storage.filesystem(),
    )
    source = tmp_path / "input.csv"
    source.write_text("value\n1\n")
    registration = register_raw_inputs(
        committer,
        [str(source)],
        step_run_id="d" * 32,
        step_number=0,
        operation_name="merge",
        storage=storage,
    )
    runtime = RuntimeEnvironment(
        delta_root=committer.delta_root,
        files_root=committer.files_root,
        staging_root=str(tmp_path / "staging"),
        working_root=str(tmp_path / "working"),
        shared_filesystem=shared,
    )
    unit = ExecutionUnit(
        operation=Merge(),
        execution_spec_id="a" * 32,
        step_run_id="b" * 32,
        inputs={"source": [registration.inputs[0].artifact_id]},
    )
    return run_curator_flow(unit, runtime)


def test_error_after_success_seal_preserves_all_original_bytes(tmp_path, monkeypatch):
    original = parquet_writer._write_execution_record
    snapshots = {}

    def publish_then_fail(**kwargs):
        original(**kwargs)
        directory = Path(kwargs["staging_path"])
        snapshots.update({p: p.read_bytes() for p in directory.iterdir()})
        msg = "after seal publication"
        raise RuntimeError(msg)

    monkeypatch.setattr(parquet_writer, "_write_execution_record", publish_then_fail)
    with pytest.raises(RuntimeError, match="after seal publication"):
        _execute(tmp_path)
    assert snapshots
    assert {
        p: p.read_bytes() for p in next(iter(snapshots)).parent.iterdir()
    } == snapshots
    frame = verify_worker_seal(str(next(iter(snapshots)).parent), LocalFileSystem())
    assert frame.item(0, "success") is True


@pytest.mark.parametrize("shared", [False, True])
def test_payload_durability_precedes_seal_for_all_local_workers(
    tmp_path, monkeypatch, shared
):
    original = parquet_writer._sync_local_staging
    flushed = []

    def flush_before_seal(path):
        directory = Path(path)
        assert not (directory / "executions.parquet").exists()
        assert (directory / "execution_edges.parquet").exists()
        original(path)
        flushed.append(path)

    monkeypatch.setattr(parquet_writer, "_sync_local_staging", flush_before_seal)
    result = _execute(tmp_path, shared=shared)
    assert result.success
    assert flushed == [result.staging_path]
    verify_worker_seal(result.staging_path, LocalFileSystem())


def test_fresh_recording_cannot_reopen_sealed_shard(tmp_path):
    result = _execute(tmp_path)
    directory = Path(result.staging_path)
    before = {p.name: p.read_bytes() for p in directory.iterdir()}
    with pytest.raises(StoreIntegrityError, match="already sealed"):
        parquet_writer._create_staging_path(
            str(tmp_path / "staging"),
            result.execution_run_id,
            0,
            "merge",
            LocalFileSystem(),
        )
    assert {p.name: p.read_bytes() for p in directory.iterdir()} == before


def test_scientific_seal_publishes_complete_kernel_result_last(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from artisan.composites.base.compiler import compile_definition
    from artisan.execution.composition import execute_compiled_plan
    from artisan.execution.compute.local import LocalExecuteRouter
    from artisan.execution.session import ExecutionSession
    from artisan.operations.examples import DataGenerator
    from artisan.schemas.execution.assignment import (
        AssignmentAttempt,
        AssignmentIdentity,
        DeliveryIdentity,
        SuccessfulDelivery,
    )
    from artisan.schemas.execution.retained_result import RetainedResultSeal
    from artisan.storage.io import retained_seal
    from artisan.utils.command_supervisor import close_command_scope
    from artisan.utils.hashing import compute_content_digest

    published = {}
    original = retained_seal.publish_immutable_bytes

    def publish_then_fail(fs, uri, encoded):
        seal = RetainedResultSeal.model_validate_json(encoded)
        directory = Path(uri).parent
        for item in (*seal.tables, *seal.carriers):
            assert (directory / item.filename).is_file()
        original(fs, uri, encoded)
        published.update({path: path.read_bytes() for path in directory.iterdir()})
        message = "after retained seal publication"
        raise RuntimeError(message)

    monkeypatch.setattr(retained_seal, "publish_immutable_bytes", publish_then_fail)
    plan = compile_definition(DataGenerator, params={"count": 1})
    identity = AssignmentIdentity(step_run_id="b" * 32, unit_index=0)
    with (
        pytest.raises(RuntimeError, match="after retained seal publication"),
        ExecutionSession(working_root=tmp_path / "working") as session,
    ):
        result = execute_compiled_plan(
            plan,
            {},
            session,
            invocation_id=identity.assignment_id,
            execute_adapter=lambda operation, path: LocalExecuteRouter(),
        )
        assert close_command_scope()
        attempt = AssignmentAttempt(
            assignment=identity,
            attempt=0,
            generation=0,
            recipe_digest=plan.recipe.digest,
            software_digest="c" * 64,
            input_context_digest=result.projection.input_context_digest,
        )
        completion = SuccessfulDelivery(
            delivery=DeliveryIdentity.new(attempt),
            computation_complete=True,
            cleanup_complete=True,
        )
        retained_seal.stage_retained_result(
            str(tmp_path / "staging"),
            completion,
            result.projection,
            result.retained_artifacts,
            fs=LocalFileSystem(),
        )
    assert published
    assert all(path.read_bytes() == data for path, data in published.items())
    seal_path = next(path for path in published if path.name == "retained-result.json")
    receipt = retained_seal.SealedDelivery(
        delivery=completion.delivery,
        seal_uri=str(seal_path),
        seal_digest=compute_content_digest(published[seal_path]),
    )
    with retained_seal.capture_retained_delivery(
        receipt, {}, expected_recipe=plan.recipe, fs=LocalFileSystem()
    ) as captured:
        assert captured.projection == result.projection
    with pytest.raises(StoreIntegrityError, match="already sealed"):
        retained_seal.stage_retained_result(
            str(tmp_path / "staging"),
            completion,
            result.projection,
            result.retained_artifacts,
            fs=LocalFileSystem(),
        )
    assert all(path.read_bytes() == data for path, data in published.items())
