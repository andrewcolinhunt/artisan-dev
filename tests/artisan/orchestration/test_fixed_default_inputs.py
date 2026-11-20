"""Constants preserve occurrence identity through admission, storage and reuse."""

from __future__ import annotations

import io
from pathlib import Path

import polars as pl
import pytest
from fixtures.fixed_default_operations import NestedDefaultPair
from fixtures.input_contract_operations import metric
from fixtures.retained_acceptance import open_committer

from artisan.composites.base.compiler import compile_definition
from artisan.errors import StoreIntegrityError
from artisan.orchestration.engine.retained_inputs import prepare_step_inputs
from artisan.schemas.execution.assignment import AssignmentAttempt, DeliveryIdentity
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.schemas.provenance.projection import FrozenBoundaryContext
from artisan.storage.cache.retained_identity import retained_cache_key
from artisan.storage.core.retained_reader import RetainedReader
from artisan.storage.io.admission import _validate_input_contract
from artisan.storage.io.projection_encoding import decode_projection, encode_projection


@pytest.fixture
def reader(tmp_path: Path) -> RetainedReader:
    storage = StorageConfig()
    return RetainedReader(
        open_committer(
            str(tmp_path / "store"), storage.filesystem(), storage, initialize=True
        )
    )


def test_constant_capture_preserves_batches_occurrences_and_cache_identity(
    reader: RetainedReader,
) -> None:
    plan = compile_definition(NestedDefaultPair)
    inputs = {"source": [metric(index) for index in range(4)]}
    first, second = (
        prepare_step_inputs(
            plan.recipe,
            inputs,
            step_run_id=owner * 32,
            reader=reader,
            constants=plan.constants,
        )
        for owner in ("a", "b")
    )
    assert [len(unit.boundary.input_roles()["source"]) for unit in first] == [3, 1]
    for left, right in zip(first, second, strict=True):
        assert list(left.boundary.input_roles()) == ["source"]
        captures = left.boundary.frozen_context.constants
        addresses = [capture.occurrences[0] for capture in captures]
        assert len(set(addresses)) == 3
        assert (
            left.artifacts[addresses[0]].artifact_id
            == left.artifacts[addresses[2]].artifact_id
        )
        _validate_input_contract(plan.recipe, left.boundary)
        attempts = [
            AssignmentAttempt(
                assignment=unit.assignment,
                attempt=0,
                generation=0,
                recipe_digest=plan.recipe.digest,
                software_digest="c" * 64,
                input_context_digest=unit.boundary.input_context_digest,
            )
            for unit in (left, right)
        ]
        assert left.boundary.input_context_digest != right.boundary.input_context_digest
        assert retained_cache_key(
            plan.recipe, attempts[0], left.boundary
        ) == retained_cache_key(plan.recipe, attempts[1], right.boundary)
        delivery = DeliveryIdentity.new(attempts[0])
        encoded = encode_projection(left.boundary, delivery=delivery)
        tables = {}
        for name, frame in encoded.items():
            content = io.BytesIO()
            frame.reverse().write_parquet(content)
            content.seek(0)
            tables[name] = pl.read_parquet(content)
        assert decode_projection(tables, delivery=delivery) == left.boundary


@pytest.mark.parametrize("tamper", ["missing", "path", "order", "identity"])
def test_constant_admission_rejects_missing_or_tampered_capture(
    reader: RetainedReader, tamper: str
) -> None:
    plan = compile_definition(NestedDefaultPair)
    unit = prepare_step_inputs(
        plan.recipe,
        {"source": [metric(1)]},
        step_run_id="a" * 32,
        reader=reader,
        constants=plan.constants,
    )[0]
    boundary = unit.boundary
    constants = boundary.frozen_context.constants
    if tamper == "missing":
        boundary = boundary.model_copy(
            update={"frozen_context": FrozenBoundaryContext()}
        )
    elif tamper == "path":
        changed = constants[0].model_copy(update={"call_path": ("unknown",)})
        boundary = boundary.model_copy(
            update={
                "frozen_context": FrozenBoundaryContext(
                    schema_version=2, constants=(changed, *constants[1:])
                )
            }
        )
    elif tamper == "order":
        boundary = boundary.model_copy(
            update={
                "frozen_context": FrozenBoundaryContext(
                    schema_version=2, constants=tuple(reversed(constants))
                )
            }
        )
    else:
        target = constants[0].occurrences[0]
        boundary = boundary.model_copy(
            update={
                "occurrences": tuple(
                    item.model_copy(update={"artifact_id": "f" * 32})
                    if item.address == target
                    else item
                    for item in boundary.occurrences
                )
            }
        )
    with pytest.raises(StoreIntegrityError, match="[Cc]onstant"):
        _validate_input_contract(plan.recipe, boundary)


def test_used_defaults_require_the_complete_live_capture(
    reader: RetainedReader,
) -> None:
    plan = compile_definition(NestedDefaultPair)
    with pytest.raises(ValueError, match="Compiled constants"):
        prepare_step_inputs(
            plan.recipe, {"source": [metric(1)]}, step_run_id="a" * 32, reader=reader
        )
