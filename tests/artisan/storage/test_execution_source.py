"""Execution selectors resolve original accepted owners without optional diagnostics."""

from __future__ import annotations

import hashlib
from pathlib import Path

import polars as pl
import pytest
from fixtures.curator_cache import STAMPS, DynamicCurator, stage_observed
from fixtures.retained_acceptance import admit_case
from fixtures.retained_curators import admit_curator
from fixtures.retained_readers import Backend

from artisan.errors import StoreIntegrityError
from artisan.execution.recording.retained_replay import (
    capture_replay_configuration,
    publish_replay_configuration,
    read_replay_configuration,
)
from artisan.orchestration.engine.retained_curator import CuratorResults
from artisan.schemas.enums import TablePath
from artisan.schemas.execution.assignment import SealedDelivery
from artisan.schemas.execution.curator_admission import CuratorAdmissionReference
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.storage.cache.curator_lookup import lookup_curator
from artisan.storage.core.curator_reader import CuratorReader
from artisan.storage.core.execution_source import (
    CuratorExecutionSource,
    ScientificExecutionSource,
    read_execution_source,
)
from artisan.storage.core.retained_reader import RetainedReader
from artisan.storage.io.retained_commit import RetainedCommitter
from artisan.storage.io.retained_store import create_retained_store


@pytest.fixture
def committer(backend_fs: Backend) -> RetainedCommitter:
    fs, storage, root = backend_fs
    return create_retained_store(
        f"{root}/store",
        f"{root}/store/files",
        fs=fs,
        storage_options=storage.delta_storage_options(),
    )


def _inventory(committer: RetainedCommitter) -> dict[str, str]:
    return {
        path: hashlib.sha256(committer.fs.cat_file(path)).hexdigest()
        for path in committer.fs.find(committer.delta_root)
    }


def _complete_curator(
    committer: RetainedCommitter,
    storage: StorageConfig,
    operation: DynamicCurator,
) -> tuple[CuratorResults, CuratorAdmissionReference, str]:
    results, admission, reference = admit_curator(
        committer, operation, {}, step_run_id="b" * 32, **STAMPS
    )
    staged, terminal, _ = stage_observed(results, admission, operation, storage=storage)
    assert staged.execution_run_id is not None
    results.finish(reference, staged.execution_run_id, terminal)
    return results, reference, staged.execution_run_id


def test_scientific_source_is_original_delivery_with_required_input_metadata(
    committer: RetainedCommitter, tmp_path: Path
) -> None:
    case, receipt = admit_case(committer, tmp_path, kind="alias")
    plan = committer.accept(receipt)
    assert not committer.fs.exists(receipt.seal_uri)
    before = _inventory(committer)
    source = read_execution_source(committer, receipt.delivery.delivery_id)
    assert isinstance(source, ScientificExecutionSource)
    assert source.plan == plan
    assert source.admission.recipe == case.recipe
    assert source.admission.attempt == receipt.delivery.attempt
    assert (
        read_execution_source(committer, plan.admission.attempt.assignment.step_run_id)
        is None
    )
    assert read_execution_source(committer, "f" * 32) is None
    assert _inventory(committer) == before


@pytest.mark.parametrize("claim", [False, True])
def test_uncompleted_delivery_is_not_a_source_or_recovered_by_reading(
    committer: RetainedCommitter,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    claim: bool,
) -> None:
    _, receipt = admit_case(committer, tmp_path, kind="alias")
    if claim:

        def checkpoint(
            event: str, _receipt: SealedDelivery, _table: str | None
        ) -> None:
            if event == "claimed":
                raise InterruptedError(event)

        monkeypatch.setattr(committer, "_checkpoint", checkpoint)
        with pytest.raises(InterruptedError, match="claimed"):
            committer.accept(receipt)
    before = _inventory(committer)
    assert read_execution_source(committer, receipt.delivery.delivery_id) is None
    assert _inventory(committer) == before


def test_source_metadata_does_not_claim_managed_payload_availability(
    committer: RetainedCommitter, tmp_path: Path
) -> None:
    _, receipt = admit_case(committer, tmp_path)
    plan = committer.accept(receipt)
    carrier = f"{committer.files_root}/{plan.carriers[0].relative_path}"
    committer.fs.rm(carrier)
    source = read_execution_source(committer, receipt.delivery.delivery_id)
    assert isinstance(source, ScientificExecutionSource)
    with pytest.raises(FileNotFoundError):
        RetainedReader(committer).read(receipt)


def test_optional_replay_configuration_is_separate_from_source_authority(
    committer: RetainedCommitter, tmp_path: Path
) -> None:
    _, receipt = admit_case(committer, tmp_path, kind="alias")
    plan = committer.accept(receipt)
    source = read_execution_source(committer, receipt.delivery.delivery_id)
    configuration = read_replay_configuration(
        receipt,
        admission=plan.admission,
        delta_root=committer.delta_root,
        fs=committer.fs,
    )
    assert configuration.status == "unavailable"
    publish_replay_configuration(
        capture_replay_configuration(receipt, {}, admission=plan.admission),
        delta_root=committer.delta_root,
        fs=committer.fs,
    )
    path = (
        f"{committer.delta_root}/_artisan/replay/"
        f"{receipt.delivery.delivery_id}/configuration.json"
    )
    committer.fs.pipe_file(path, b"invalid optional diagnostic")
    assert read_execution_source(committer, receipt.delivery.delivery_id) == source
    with pytest.raises(ValueError):
        read_replay_configuration(
            receipt,
            admission=plan.admission,
            delta_root=committer.delta_root,
            fs=committer.fs,
        )


@pytest.mark.parametrize("failed", [False, True])
def test_curator_source_keeps_original_frozen_admission_without_science_receipt(
    committer: RetainedCommitter, backend_fs: Backend, failed: bool
) -> None:
    operation = DynamicCurator(params=DynamicCurator.Params(fail=failed))
    _, reference, execution_id = _complete_curator(committer, backend_fs[1], operation)
    before = _inventory(committer)
    source = read_execution_source(committer, execution_id)
    assert isinstance(source, CuratorExecutionSource)
    assert source.plan.admission == reference
    assert source.admission.digest == reference.admission_digest
    assert source.admission.record.step_run_id == reference.step_run_id
    assert not committer.journal.all_states()
    assert read_execution_source(committer, reference.step_run_id) is None
    assert _inventory(committer) == before


def test_curator_reuse_does_not_create_another_source_execution(
    committer: RetainedCommitter, backend_fs: Backend
) -> None:
    operation = DynamicCurator()
    results, _, execution_id = _complete_curator(committer, backend_fs[1], operation)
    original = read_execution_source(committer, execution_id)
    _, _, current = admit_curator(
        committer, operation, {}, step_run_id="c" * 32, step_number=9, **STAMPS
    )
    hit = lookup_curator(current, reader=CuratorReader(committer))
    assert hit is not None
    results.reuse(hit)
    assert read_execution_source(committer, execution_id) == original
    assert read_execution_source(committer, current.step_run_id) is None


def test_selected_curator_execution_requires_exact_physical_metadata(
    committer: RetainedCommitter, backend_fs: Backend
) -> None:
    _, _, execution_id = _complete_curator(committer, backend_fs[1], DynamicCurator())
    path = f"{committer.delta_root}/{TablePath.EXECUTIONS.value}"
    rows = pl.read_delta(path, storage_options=committer.storage_options)
    rows.with_columns(pl.lit("changed operation").alias("operation_name")).write_delta(
        path, mode="overwrite", storage_options=committer.storage_options
    )
    with pytest.raises(StoreIntegrityError):
        read_execution_source(committer, execution_id)


def test_cross_kind_execution_id_collision_is_rejected(
    committer: RetainedCommitter,
    backend_fs: Backend,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, receipt = admit_case(committer, tmp_path, kind="alias")
    committer.accept(receipt)
    monkeypatch.setattr(
        "artisan.execution.executors.curator.generate_execution_run_id",
        lambda *args, **kwargs: receipt.delivery.delivery_id,
    )
    _complete_curator(committer, backend_fs[1], DynamicCurator())
    with pytest.raises(StoreIntegrityError, match="multiple completed source owners"):
        read_execution_source(committer, receipt.delivery.delivery_id)
