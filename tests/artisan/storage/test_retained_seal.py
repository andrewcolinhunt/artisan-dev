"""Complete retained delivery seals outlive workers without granting acceptance."""

from __future__ import annotations

import json
import os
import posixpath
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
from fixtures.retained_readers import Backend
from fixtures.retained_results import boundary_artifact, retained_case

from artisan.errors import ArtifactIntegrityError, StoreIntegrityError
from artisan.execution.exceptions import ArtifactValidationError
from artisan.schemas.composites.plan import OutputContract
from artisan.schemas.execution.assignment import (
    DeliveryIdentity,
    SealedDelivery,
    SuccessfulDelivery,
)
from artisan.schemas.execution.retained_result import RetainedResultSeal
from artisan.schemas.provenance.projection import RoleOccurrences
from artisan.storage.io.retained_seal import (
    capture_retained_delivery,
    stage_retained_result,
)


@pytest.mark.parametrize("kind", ["file", "appendable", "alias", "empty"])
def test_retained_seal_roundtrips_after_scratch_disappears(
    backend_fs, tmp_path: Path, kind: str
) -> None:
    fs, _, root = backend_fs
    scratch = tmp_path / "worker"
    case = retained_case(scratch, kind=kind)
    before = {
        address: artifact.to_row() for address, artifact in case.artifacts.items()
    }
    receipt = stage_retained_result(
        f"{root}/staging", case.completion(), case.projection, case.artifacts, fs=fs
    )
    if scratch.exists():
        shutil.rmtree(scratch)
    with capture_retained_delivery(
        receipt, case.boundary, expected_recipe=case.recipe, fs=fs
    ) as captured:
        assert captured.projection == case.projection
        assert all(
            artifact.to_row() == before[address]
            for address, artifact in captured.artifacts.items()
            if address in before
        )
        external = kind in {"file", "appendable"}
        assert len(captured.seal.carriers) == (1 if external else 0)
        assert len(captured.seal.external_bindings) == (2 if external else 0)
        if kind == "appendable":
            carrier = captured.seal.carriers[0]
            assert all(
                artifact.content_hash != carrier.digest
                and artifact.size_bytes < carrier.size_bytes
                for artifact in captured.artifacts.values()
                if artifact.EXTERNALLY_BACKED
            )
        paths = tuple(captured.carriers.values())
        for artifact in captured.artifacts.values():
            artifact.verify_external_content()
    assert all(not Path(path).exists() for path in paths)
    with pytest.raises(StoreIntegrityError, match="already sealed"):
        stage_retained_result(
            f"{root}/staging",
            captured.seal.completion,
            case.projection,
            case.artifacts,
            fs=fs,
        )


@pytest.mark.parametrize("mutation", ["missing", "changed", "extra", "seal"])
def test_retained_seal_rejects_incomplete_or_tampered_inventory(
    backend_fs, tmp_path: Path, mutation: str
) -> None:
    fs, _, root = backend_fs
    case = retained_case(tmp_path / "worker")
    receipt = stage_retained_result(
        f"{root}/staging", case.completion(), case.projection, case.artifacts, fs=fs
    )
    directory = posixpath.dirname(receipt.seal_uri)
    with fs.open(receipt.seal_uri, "rb") as stream:
        seal = RetainedResultSeal.model_validate_json(stream.read())
    path = f"{directory}/{seal.carriers[0].filename}"
    if mutation == "missing":
        fs.rm(path)
    else:
        path = {
            "changed": path,
            "extra": f"{directory}/surprise.json",
            "seal": receipt.seal_uri,
        }[mutation]
        with fs.open(path, "wb") as stream:
            stream.write(b"changed")
    with (
        pytest.raises(StoreIntegrityError),
        capture_retained_delivery(
            receipt, case.boundary, expected_recipe=case.recipe, fs=fs
        ),
    ):
        pytest.fail("Invalid delivery capture succeeded")


@pytest.mark.parametrize("interrupt", [False, True])
def test_separate_worker_death_preserves_only_a_complete_seal(
    backend_fs, tmp_path: Path, interrupt: bool
) -> None:
    fs, storage, root = backend_fs
    scratch = tmp_path / "process-worker"
    case = retained_case(tmp_path / "expected")
    completion = case.completion()
    receipt_file = tmp_path / "receipt.json"
    request = {
        "staging_root": f"{root}/staging",
        "scratch": str(scratch),
        "receipt": str(receipt_file),
        "completion": completion.model_dump(mode="json"),
        "storage": storage.model_dump(),
        "interrupt": interrupt,
    }
    tests = Path(__file__).resolve().parents[2]
    process = subprocess.run(
        [sys.executable, "-m", "fixtures.retained_results"],
        check=False,
        input=json.dumps(request),
        text=True,
        capture_output=True,
        timeout=30,
        env={**os.environ, "PYTHONPATH": str(tests)},
    )
    assert process.returncode == (29 if interrupt else 23), process.stderr
    shutil.rmtree(scratch)
    directory = f"{root}/staging/{completion.delivery.delivery_id}"
    if interrupt:
        assert not fs.exists(f"{directory}/retained-result.json")
        assert not receipt_file.exists()
    else:
        receipt = SealedDelivery.model_validate_json(receipt_file.read_text())
        with capture_retained_delivery(
            receipt, case.boundary, expected_recipe=case.recipe, fs=fs
        ) as captured:
            assert captured.projection == case.projection
            assert len(captured.seal.external_bindings) == 2


def test_capture_revalidates_trusted_boundary_content(
    backend_fs, tmp_path: Path
) -> None:
    fs, _, root = backend_fs
    case = retained_case(tmp_path / "worker", kind="alias")
    receipt = stage_retained_result(
        f"{root}/staging", case.completion(), case.projection, case.artifacts, fs=fs
    )
    changed = boundary_artifact().model_copy(update={"content": b"x\nchanged\n"})
    with (
        pytest.raises(ArtifactIntegrityError),
        capture_retained_delivery(
            receipt,
            {next(iter(case.boundary)): changed},
            expected_recipe=case.recipe,
            fs=fs,
        ),
    ):
        pytest.fail("Changed boundary content was accepted")


@pytest.mark.parametrize("change", ["extra-role", "missing-required", "required-empty"])
def test_capture_enforces_admitted_root_output_contract(
    backend_fs: Backend, tmp_path: Path, change: str
) -> None:
    fs, _, root = backend_fs
    case = retained_case(tmp_path / "worker")
    if change == "extra-role":
        projection = case.projection.model_copy(
            update={
                "exports": (
                    *case.projection.exports,
                    RoleOccurrences(role="unexpected", occurrences=()),
                )
            }
        )
        artifacts = case.artifacts
    else:
        projection = case.projection.model_copy(
            update={
                "exports": ()
                if change == "missing-required"
                else tuple(
                    RoleOccurrences(role=group.role, occurrences=())
                    for group in case.projection.exports
                ),
                "occurrences": tuple(
                    item
                    for item in case.projection.occurrences
                    if item.address in case.projection.boundary_addresses()
                ),
                "support": (),
                "parents": (),
                "associations": (),
                "references": (),
            }
        )
        artifacts = {}
    receipt = stage_retained_result(
        f"{root}/staging", case.completion(), projection, artifacts, fs=fs
    )
    with (
        pytest.raises((StoreIntegrityError, ArtifactValidationError)),
        capture_retained_delivery(
            receipt, case.boundary, expected_recipe=case.recipe, fs=fs
        ),
    ):
        pytest.fail("Export contract violation was accepted")


def test_capture_preserves_omitted_optional_role_without_inventing_empty_membership(
    backend_fs: Backend, tmp_path: Path
) -> None:
    fs, _, root = backend_fs
    case = retained_case(tmp_path / "worker")
    projection = case.projection.model_copy(
        update={"exports": case.projection.exports[:1]}
    )
    receipt = stage_retained_result(
        f"{root}/staging", case.completion(), projection, case.artifacts, fs=fs
    )
    with capture_retained_delivery(
        receipt, case.boundary, expected_recipe=case.recipe, fs=fs
    ) as captured:
        assert captured.projection == projection
        assert captured.projection.exports != case.projection.exports


def test_capture_checks_required_references_against_original_relations(
    backend_fs, tmp_path: Path
) -> None:
    fs, _, root = backend_fs
    case = retained_case(tmp_path / "worker")
    projection = case.projection.model_copy(
        update={
            "exports": (
                *case.projection.exports,
                RoleOccurrences(role="files", occurrences=case.projection.support),
            ),
            "support": (),
            "references": (),
        }
    )
    recipe = case.recipe.model_copy(
        update={
            "outputs": (
                *case.recipe.outputs,
                OutputContract(
                    role="files",
                    artifact_type="file_ref",
                    required=True,
                    parent_kind=None,
                    parent_roles=(),
                ),
            )
        }
    )
    attempt = case.completion().delivery.attempt.model_copy(
        update={"recipe_digest": recipe.digest}
    )
    completion = SuccessfulDelivery(
        delivery=DeliveryIdentity.new(attempt),
        computation_complete=True,
        cleanup_complete=True,
    )
    receipt = stage_retained_result(
        f"{root}/staging", completion, projection, case.artifacts, fs=fs
    )
    with (
        pytest.raises(StoreIntegrityError, match="required-reference"),
        capture_retained_delivery(
            receipt, case.boundary, expected_recipe=recipe, fs=fs
        ),
    ):
        pytest.fail("Missing semantic reference membership was accepted")


def test_capture_requires_the_admitted_recipe_digest(
    backend_fs, tmp_path: Path
) -> None:
    fs, _, root = backend_fs
    case = retained_case(tmp_path / "worker", kind="alias")
    receipt = stage_retained_result(
        f"{root}/staging", case.completion(), case.projection, case.artifacts, fs=fs
    )
    different = case.recipe.model_copy(update={"params_json": '{"changed":true}'})
    with (
        pytest.raises(StoreIntegrityError, match="admitted recipe"),
        capture_retained_delivery(
            receipt, case.boundary, expected_recipe=different, fs=fs
        ),
    ):
        pytest.fail("Wrong recipe was admitted")
