"""Prepare and recapture exact controller-owned retained-result effects."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory

from fsspec import AbstractFileSystem

from artisan.errors import StoreIntegrityError
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.schemas.enums import TablePath
from artisan.schemas.execution.admission import ManagedCarrier
from artisan.schemas.execution.assignment import AssignmentIdentity
from artisan.storage.core.projection_schemas import PROJECTION_TABLES
from artisan.storage.core.table_schemas import (
    ARTIFACT_INDEX_SCHEMA,
    ARTIFACT_LOCATIONS_SCHEMA,
)
from artisan.storage.io.admission import CapturedAdmission
from artisan.storage.io.assignment_journal import WinningDelivery
from artisan.storage.io.bounded_evidence import DiskTable
from artisan.storage.io.bounded_io import write_parquet_rows
from artisan.storage.io.publication import (
    publish_immutable_bytes,
)
from artisan.storage.io.published_effects import (
    capture_table_effects,
    publish_table_effect,
)
from artisan.storage.io.retained_plan import (
    RetainedCommitPlan,
    retained_plan_digest,
    retained_table_paths,
)
from artisan.storage.io.retained_seal import CapturedRetainedDelivery
from artisan.utils.hashing import canonical_json_bytes, compute_content_digest
from artisan.utils.path import uri_join


@dataclass(frozen=True)
class RetainedPlanEvidence:
    """Private validated table spools with lifetime owned by the capture scope."""

    plan: RetainedCommitPlan
    tables: Mapping[str, DiskTable]


def retained_plan_directory(delta_root: str, assignment: AssignmentIdentity) -> str:
    return uri_join(
        delta_root, "_artisan", "retained-commits", assignment.assignment_id
    )


def read_retained_plan(
    delta_root: str, assignment: AssignmentIdentity, fs: AbstractFileSystem
) -> RetainedCommitPlan | None:
    """Read an exact canonical plan without treating its presence as acceptance."""
    path = uri_join(retained_plan_directory(delta_root, assignment), "plan.json")
    if not fs.exists(path):
        return None
    with fs.open(path, "rb", cache_type="none") as stream:
        encoded = stream.read()
    plan = RetainedCommitPlan.model_validate_json(encoded)
    if (
        plan.receipt.delivery.attempt.assignment != assignment
        or canonical_json_bytes(plan.model_dump(mode="json")) != encoded
    ):
        msg = "Retained plan has noncanonical bytes or another assignment owner"
        raise StoreIntegrityError(msg)
    return plan


def prepare_retained_plan(
    captured: CapturedRetainedDelivery,
    admission: CapturedAdmission,
    winner: WinningDelivery,
    *,
    delta_root: str,
    files_root: str,
    fs: AbstractFileSystem,
) -> RetainedCommitPlan:
    """Publish exact table evidence and its plan after caller verifies durable claim.

    Managed carrier effects remain unapplied. They are declared in this plan and
    copied only by the controller before artifact locations become visible.
    """
    if winner.receipt != captured.receipt or admission.reference.attempt != (
        captured.receipt.delivery.attempt
    ):
        msg = "Retained planning requires one matching admission and captured winner"
        raise StoreIntegrityError(msg)
    assignment = captured.receipt.delivery.attempt.assignment
    directory = retained_plan_directory(delta_root, assignment)
    logical_id = f"retained_result:{assignment.assignment_id}"
    content = _content(captured)
    bindings = _carrier_bindings(captured, admission)
    with TemporaryDirectory(prefix="artisan-retained-plan-") as scratch:
        sources = _table_sources(captured, content, bindings, files_root, Path(scratch))
        tables = tuple(
            publish_table_effect(
                logical_id, path, sources[path], directory, scratch, fs
            )
            for path in retained_table_paths()
            if path in sources
        )
    carriers = {item.digest: item for item in bindings.values()}
    payload = {
        "domain": "artisan.retained-commit",
        "schema_version": 1,
        "receipt": captured.receipt.model_dump(mode="json"),
        "decision_sequence": winner.decision_sequence,
        "admission": admission.reference.model_dump(mode="json"),
        "step_number": admission.record.step_number,
        "tables": [table.model_dump(mode="json") for table in tables],
        "carriers": [carriers[key].model_dump(mode="json") for key in sorted(carriers)],
    }
    plan = RetainedCommitPlan.model_validate(
        {**payload, "plan_digest": retained_plan_digest(payload)}
    )
    publish_immutable_bytes(
        fs,
        uri_join(directory, "plan.json"),
        canonical_json_bytes(plan.model_dump(mode="json")),
    )
    return plan


def _content(captured: CapturedRetainedDelivery) -> dict[str, Artifact]:
    boundary = captured.projection.boundary_addresses()
    ordered = sorted(
        captured.artifacts,
        key=lambda address: (address not in boundary, address.key),
    )
    content: dict[str, Artifact] = {}
    for address in ordered:
        artifact = captured.artifacts[address]
        assert artifact.artifact_id is not None
        previous = content.setdefault(artifact.artifact_id, artifact)
        if {
            key: value
            for key, value in previous.to_row().items()
            if key != "origin_step_number"
        } != {
            key: value
            for key, value in artifact.to_row().items()
            if key != "origin_step_number"
        }:
            msg = "Retained occurrences disagree on content for one artifact identity"
            raise StoreIntegrityError(msg)
    return dict(sorted(content.items()))


def _carrier_bindings(
    captured: CapturedRetainedDelivery, admission: CapturedAdmission
) -> dict[str, ManagedCarrier]:
    bindings = dict(admission.carriers)
    carriers = {item.filename: item for item in captured.seal.carriers}
    for binding in captured.seal.external_bindings:
        carrier = carriers[binding.filename]
        managed = ManagedCarrier(digest=carrier.digest, size_bytes=carrier.size_bytes)
        if binding.artifact_id in bindings and bindings[binding.artifact_id] != managed:
            msg = "One retained content identity names conflicting external carriers"
            raise StoreIntegrityError(msg)
        bindings[binding.artifact_id] = managed
    return bindings


def _table_sources(
    captured: CapturedRetainedDelivery,
    content: Mapping[str, Artifact],
    bindings: Mapping[str, ManagedCarrier],
    files_root: str,
    scratch: Path,
) -> dict[str, str]:
    sources = {
        table.table_path: captured.tables[table.table_path].path
        for table in PROJECTION_TABLES
    }
    for kind in sorted({artifact.artifact_type for artifact in content.values()}):
        definition = ArtifactTypeDef.get(kind)
        path = scratch / f"content-{compute_content_digest(kind.encode())}.parquet"
        write_parquet_rows(
            str(path),
            (
                artifact.to_row()
                for artifact in content.values()
                if artifact.artifact_type == kind
            ),
            definition.polars_schema(),
        )
        sources[definition.table_path] = str(path)
    if content:
        path = scratch / "index.parquet"
        write_parquet_rows(
            str(path),
            (
                {
                    "artifact_id": artifact_id,
                    "artifact_type": artifact.artifact_type,
                    "origin_step_number": artifact.origin_step_number,
                    "metadata": artifact.to_row()["metadata"],
                }
                for artifact_id, artifact in content.items()
            ),
            ARTIFACT_INDEX_SCHEMA,
        )
        sources[TablePath.ARTIFACT_INDEX.value] = str(path)
    if bindings:
        path = scratch / "locations.parquet"
        write_parquet_rows(
            str(path),
            (
                {
                    "artifact_id": artifact_id,
                    "uri": uri_join(files_root, carrier.relative_path),
                }
                for artifact_id, carrier in sorted(bindings.items())
            ),
            ARTIFACT_LOCATIONS_SCHEMA,
        )
        sources[TablePath.ARTIFACT_LOCATIONS.value] = str(path)
    return sources


@contextmanager
def capture_retained_plan(
    plan: RetainedCommitPlan, *, delta_root: str, fs: AbstractFileSystem
) -> Iterator[RetainedPlanEvidence]:
    """Recapture exact published files; no caller-built plan grants ownership."""
    assignment = plan.receipt.delivery.attempt.assignment
    if read_retained_plan(delta_root, assignment, fs) != plan:
        msg = "Persisted retained plan differs from requested effects"
        raise StoreIntegrityError(msg)
    directory = retained_plan_directory(delta_root, assignment)
    with capture_table_effects(plan, plan.tables, directory=directory, fs=fs) as tables:
        yield RetainedPlanEvidence(plan, tables)
