"""Malformed singleton membership fails both admission publication and capture."""

from __future__ import annotations

from pathlib import Path

import pytest
from fixtures.input_contract_operations import (
    AnchoredOffset,
    SharedOffset,
    metric,
    named_metric,
)
from fsspec.implementations.local import LocalFileSystem

from artisan.composites.base.compiler import compile_definition
from artisan.errors import StoreIntegrityError
from artisan.execution.lineage.graph import OccurrenceGraph
from artisan.schemas.execution.admission import AdmissionRecord, AdmissionReference
from artisan.schemas.execution.assignment import AssignmentAttempt, AssignmentIdentity
from artisan.storage.core.store_format import initialize_retained_store
from artisan.storage.io.admission import (
    admission_directory,
    capture_admission,
    publish_admission,
)
from artisan.storage.io.publication import publish_immutable_bytes
from artisan.utils.hashing import canonical_json_bytes, compute_content_digest


@pytest.mark.parametrize(
    ("source_count", "shared_count", "context_count", "message"),
    [(2, 2, 2, "exactly one occurrence"), (2, 1, 1, "equal input lengths")],
)
def test_invalid_shared_partition_fails_publication_and_capture(
    tmp_path: Path,
    source_count: int,
    shared_count: int,
    context_count: int,
    message: str,
) -> None:
    fs = LocalFileSystem()
    delta_root, files_root = str(tmp_path / "delta"), str(tmp_path / "files")
    initialize_retained_store(delta_root, fs)
    recipe = compile_definition(SharedOffset).recipe
    graph = OccurrenceGraph()
    for role, count in (
        ("source", source_count),
        ("offset", shared_count),
        ("context", context_count),
    ):
        for index in range(count):
            graph.add_input(role, metric(index))
    boundary = graph.project({}, {})
    artifacts = {address: node.artifact for address, node in graph.nodes.items()}
    attempt = AssignmentAttempt(
        assignment=AssignmentIdentity(step_run_id="a" * 32, unit_index=0),
        attempt=0,
        generation=0,
        recipe_digest=recipe.digest,
        software_digest="c" * 64,
        input_context_digest=boundary.input_context_digest,
    )
    with pytest.raises(StoreIntegrityError, match=message):
        publish_admission(
            attempt,
            recipe,
            boundary,
            artifacts,
            step_number=0,
            delta_root=delta_root,
            files_root=files_root,
            fs=fs,
        )
    # A self-consistent immutable record still cannot authorize invalid membership.
    record = AdmissionRecord(
        attempt=attempt, step_number=0, recipe=recipe, boundary=boundary, tables=()
    )
    encoded = canonical_json_bytes(record.model_dump(mode="json"))
    publish_immutable_bytes(
        fs, f"{admission_directory(delta_root, attempt)}/admission.json", encoded
    )
    reference = AdmissionReference(
        attempt=attempt, record_digest=compute_content_digest(encoded)
    )
    with (
        pytest.raises(StoreIntegrityError, match=message),
        capture_admission(
            reference, delta_root=delta_root, files_root=files_root, fs=fs
        ),
    ):
        pytest.fail("Malformed input membership reached hydration")


@pytest.mark.parametrize("context_names", [("A", "A"), ("A", "orphan"), ("A", "B")])
def test_anchored_publication_and_capture_require_exact_names_and_bounds(
    tmp_path: Path,
    context_names: tuple[str, str],
) -> None:
    fs = LocalFileSystem()
    delta_root, files_root = str(tmp_path / "delta"), str(tmp_path / "files")
    initialize_retained_store(delta_root, fs)
    recipe = compile_definition(
        AnchoredOffset, batch_strategy={"artifacts_per_unit": 1}
    ).recipe
    graph = OccurrenceGraph()
    for role, names in {
        "source": ("A", "B"),
        "context": context_names,
        "offset": ("shared",),
    }.items():
        for index, name in enumerate(names):
            graph.add_input(role, named_metric(name, 10 * len(role) + index))
    boundary = graph.project({}, {})
    artifacts = {address: node.artifact for address, node in graph.nodes.items()}
    attempt = AssignmentAttempt(
        assignment=AssignmentIdentity(step_run_id="a" * 32, unit_index=0),
        attempt=0,
        generation=0,
        recipe_digest=recipe.digest,
        software_digest="c" * 64,
        input_context_digest=boundary.input_context_digest,
    )
    message = (
        "group-count bound" if context_names == ("A", "B") else "duplicate or orphan"
    )
    with pytest.raises(StoreIntegrityError, match=message):
        publish_admission(
            attempt,
            recipe,
            boundary,
            artifacts,
            step_number=0,
            delta_root=delta_root,
            files_root=files_root,
            fs=fs,
        )
    # Existing paired roles can carry these names; reinterpretation as an anchored
    # contract must still fail when its metadata is captured from immutable tables.
    paired_recipe = compile_definition(SharedOffset).recipe
    paired_attempt = attempt.model_copy(update={"recipe_digest": paired_recipe.digest})
    original = publish_admission(
        paired_attempt,
        paired_recipe,
        boundary,
        artifacts,
        step_number=0,
        delta_root=delta_root,
        files_root=files_root,
        fs=fs,
    )
    path = f"{admission_directory(delta_root, attempt)}/admission.json"
    with fs.open(path, "rb") as stream:
        record = AdmissionRecord.model_validate_json(stream.read())
    changed = AdmissionRecord.model_validate(
        {**record.model_dump(), "attempt": attempt, "recipe": recipe}
    )
    encoded = canonical_json_bytes(changed.model_dump(mode="json"))
    with fs.open(path, "wb") as stream:
        stream.write(encoded)
    reference = AdmissionReference(
        attempt=attempt, record_digest=compute_content_digest(encoded)
    )
    assert reference != original
    with (
        pytest.raises(StoreIntegrityError, match=message),
        capture_admission(
            reference, delta_root=delta_root, files_root=files_root, fs=fs
        ),
    ):
        pytest.fail("Invalid anchored membership reached invocation")
