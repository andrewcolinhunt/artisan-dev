"""Output source bindings identify a configured store without repeated reads."""

from __future__ import annotations

import shutil
from pathlib import Path

import polars as pl
import pytest
from deltalake import DeltaTable
from fixtures.output_references import output_reference
from fsspec import AbstractFileSystem
from fsspec.implementations.local import LocalFileSystem

from artisan.orchestration.engine.pipeline_inputs import normalize_pipeline_inputs
from artisan.orchestration.engine.retained_step_tracker import RetainedStepTracker
from artisan.schemas.enums import TablePath
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.schemas.orchestration.step_start_record import StepStartRecord
from artisan.storage.core.table_schemas import get_physical_schema_for_path
from artisan.storage.io.retained_commit import RetainedCommitter, _identity_root
from artisan.storage.io.retained_store import create_retained_store, open_retained_store


def _create(root: Path) -> RetainedCommitter:
    return create_retained_store(
        str(root / "delta"), str(root / "files"), fs=LocalFileSystem()
    )


def test_store_identity_is_captured_at_open_and_stable_across_append(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    committer = _create(tmp_path)
    identity = committer.store_identity
    tracker = RetainedStepTracker(committer, "source-run")
    tracker.create_attempt(
        StepStartRecord(
            step_run_id="a" * 32,
            step_number=0,
            step_name="source",
            operation_class="example.Source",
            params_json="{}",
            input_refs_json="{}",
            compute_backend="local",
            compute_options_json="{}",
            output_roles_json='["data"]',
            output_types_json='{"data":"csv"}',
        )
    )
    reopened = open_retained_store(
        committer.delta_root, committer.files_root, fs=committer.fs
    )
    assert reopened.store_identity == identity
    assert DeltaTable(str(tmp_path / "delta" / TablePath.STEPS.value)).version() > 0

    def forbidden_snapshot(table_path: str) -> None:
        pytest.fail(f"Cached identity reopened {table_path}")

    monkeypatch.setattr(reopened, "_snapshot", forbidden_snapshot)
    assert reopened.store_identity is reopened.store_identity


def test_local_store_aliases_share_identity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    committer = _create(tmp_path / "source")
    (tmp_path / "alias").symlink_to(tmp_path / "source", target_is_directory=True)
    monkeypatch.chdir(tmp_path)
    bindings = (
        ("source/delta", "source/files"),
        ((tmp_path / "source/delta").as_uri(), (tmp_path / "source/files").as_uri()),
        (str(tmp_path / "alias/delta"), str(tmp_path / "alias/files")),
    )
    for delta_root, files_root in bindings:
        reopened = open_retained_store(delta_root, files_root, fs=committer.fs)
        assert reopened.store_identity == committer.store_identity


def test_copied_store_and_different_files_binding_have_distinct_identity(
    tmp_path: Path,
) -> None:
    committer = _create(tmp_path / "source")
    copy_root = tmp_path / "copy"
    shutil.copytree(tmp_path / "source", copy_root)
    copied = open_retained_store(
        str(copy_root / "delta"), str(copy_root / "files"), fs=committer.fs
    )
    rebound = open_retained_store(
        committer.delta_root, str(tmp_path / "other-files"), fs=committer.fs
    )
    assert (
        copied.store_identity.steps_table_id == committer.store_identity.steps_table_id
    )
    assert copied.store_identity != committer.store_identity
    assert rebound.store_identity != committer.store_identity


def test_replaced_steps_table_changes_store_identity(tmp_path: Path) -> None:
    committer = _create(tmp_path)
    identity = committer.store_identity
    steps = tmp_path / "delta" / TablePath.STEPS.value
    shutil.rmtree(steps)
    pl.DataFrame(
        schema=get_physical_schema_for_path(TablePath.STEPS.value)
    ).write_delta(str(steps))
    reopened = open_retained_store(
        committer.delta_root, committer.files_root, fs=committer.fs
    )
    assert reopened.store_identity.delta_root == identity.delta_root
    assert reopened.store_identity.steps_table_id != identity.steps_table_id


def test_independent_stores_have_distinct_table_ids(tmp_path: Path) -> None:
    first, second = _create(tmp_path / "first"), _create(tmp_path / "second")
    assert first.store_identity.steps_table_id != second.store_identity.steps_table_id


def test_remote_identity_preserves_object_key_components() -> None:
    fs = AbstractFileSystem()
    assert _identity_root("s3://bucket/a/../b//prefix/", fs) == (
        "s3://bucket/a/../b//prefix"
    )
    assert _identity_root("s3://bucket/prefix//", fs) != _identity_root(
        "s3://bucket/prefix/", fs
    )


@pytest.mark.s3
def test_same_s3_roots_on_distinct_endpoints_reject_foreign_reference(
    s3_fs: tuple[AbstractFileSystem, StorageConfig, str], minio_image: str
) -> None:
    from s3fs import S3FileSystem
    from testcontainers.minio import MinioContainer

    first_fs, storage, root = s3_fs
    delta_root, files_root = f"{root}/delta", f"{root}/files"
    first = create_retained_store(
        delta_root,
        files_root,
        fs=first_fs,
        storage_options=storage.delta_storage_options(),
    )
    with MinioContainer(image=minio_image) as second_endpoint:
        endpoint_url = (
            f"http://{second_endpoint.get_container_host_ip()}:"
            f"{second_endpoint.get_exposed_port(9000)}"
        )
        second_fs = S3FileSystem(
            key=second_endpoint.access_key,
            secret=second_endpoint.secret_key,
            client_kwargs={"endpoint_url": endpoint_url},
            use_ssl=False,
        )
        bucket = root.removeprefix("s3://")
        second_fs.mkdir(bucket)
        try:
            second = create_retained_store(
                delta_root,
                files_root,
                fs=second_fs,
                storage_options={
                    **storage.delta_storage_options(),
                    "AWS_ENDPOINT_URL": endpoint_url,
                    "AWS_ACCESS_KEY_ID": second_endpoint.access_key,
                    "AWS_SECRET_ACCESS_KEY": second_endpoint.secret_key,
                },
            )
            assert first.store_identity.delta_root == second.store_identity.delta_root
            assert first.store_identity.files_root == second.store_identity.files_root
            assert (
                first.store_identity.steps_table_id
                != second.store_identity.steps_table_id
            )
            reference = output_reference("a" * 32, store=first.store_identity)
            with pytest.raises(ValueError, match="different store"):
                normalize_pipeline_inputs(
                    {"data": reference}, store_identity=second.store_identity
                )
        finally:
            second_fs.rm(bucket, recursive=True)
