"""Inspection copies preserve accepted assertions without acquiring their authority."""

from __future__ import annotations

import importlib
import json
import shutil
from collections.abc import Callable, Mapping
from enum import StrEnum
from pathlib import Path
from typing import ClassVar

import polars as pl
import pytest
from fixtures.curator_cache import STAMPS, DynamicCurator, stage_observed
from fixtures.retained_acceptance import admit_case, open_committer
from fixtures.retained_curators import admit_curator, stage_curator
from pydantic import BaseModel

from artisan.errors import ArtifactIntegrityError, StoreIntegrityError
from artisan.execution.recording.retained_replay import (
    capture_replay_configuration,
    publish_replay_configuration,
)
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.operations.curator.ingest_csv import IngestCSV
from artisan.operations.curator.merge import Merge
from artisan.orchestration.engine.curator_inputs import prepare_curator_inputs
from artisan.orchestration.engine.retained_curator import CuratorResults
from artisan.orchestration.engine.retained_step_tracker import RetainedStepTracker
from artisan.schemas.artifact.execution_config import ExecutionConfigArtifact
from artisan.schemas.artifact.file_ref import FileRefArtifact
from artisan.schemas.enums import TablePath
from artisan.schemas.execution.admission import ManagedCarrier
from artisan.schemas.execution.curator_result import ArtifactResult
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.schemas.orchestration.step_lifecycle import StepStatus
from artisan.schemas.orchestration.step_result import StepResult
from artisan.schemas.orchestration.step_start_record import StepStartRecord
from artisan.schemas.specs.output_spec import OutputSpec
from artisan.storage._publication_policy import PublicationPolicy
from artisan.storage._retained_publication import (
    PublicationExport,
    export_retained_publication,
)
from artisan.storage.cache.curator_lookup import lookup_curator
from artisan.storage.core.artifact_store import ArtifactStore
from artisan.storage.core.curator_reader import CuratorReader
from artisan.storage.core.retained_reader import RetainedReader
from artisan.storage.core.table_schemas import get_physical_schema_for_path
from artisan.storage.io.curator_plan import read_curator_plan
from artisan.storage.io.input_registration import register_raw_inputs
from artisan.storage.io.input_registration_plan import (
    REGISTRATION_TABLES,
    InputRegistrationPlan,
    registration_directory,
)
from artisan.storage.io.retained_store import initialize_framework_tables
from artisan.utils.hashing import compute_content_digest
from artisan.utils.path import uri_join


class ExternalCurator(OperationDefinition):
    """Publish a new external file and a config referring to that content."""

    name = "publication_external_curator"

    class OutputRole(StrEnum):
        config = "config"
        file = "file"

    outputs: ClassVar[dict[str, OutputSpec]] = {
        "config": OutputSpec(artifact_type="config", derives_from={"inputs": []}),
        "file": OutputSpec(artifact_type="file_ref", derives_from={"inputs": []}),
    }

    class Params(BaseModel):
        """Fixture input.

        Attributes:
            source: External file copied into managed curator storage.
        """

        source: Path

    params: Params

    def execute_curator(
        self,
        inputs: Mapping[str, pl.DataFrame],
        step_number: int,
        artifact_store: ArtifactStore,
    ) -> ArtifactResult:
        payload = self.params.source.read_bytes()
        artifact = FileRefArtifact.draft(
            str(self.params.source),
            compute_content_digest(payload),
            len(payload),
            step_number,
        ).finalize()
        config = ExecutionConfigArtifact.draft(
            {"input": {"$artifact": artifact.artifact_id}}, "config.json", step_number
        )
        return ArtifactResult(
            artifacts={"config": [config], "file": [artifact]},
            lineage={"config": [], "file": []},
        )


@pytest.fixture
def source(tmp_path: Path) -> RetainedReader:
    storage = StorageConfig()
    committer = open_committer(
        str(tmp_path / "source"), storage.filesystem(), storage, initialize=True
    )
    return RetainedReader(committer)


def _register_files(
    source: RetainedReader,
    paths: list[Path],
    *,
    checkpoint: Callable[[str], None] | None = None,
) -> InputRegistrationPlan:
    return register_raw_inputs(
        source.committer,
        [str(path) for path in paths],
        step_run_id="d" * 32,
        step_number=3,
        operation_name="IngestCSV",
        storage=StorageConfig(),
        checkpoint=checkpoint,
    )


def _input_file(tmp_path: Path, name: str = "input.csv") -> Path:
    path = tmp_path / name
    path.write_bytes(b"x\n1\n")
    return path


def _registration_record(operation: OperationDefinition) -> StepStartRecord:
    return StepStartRecord(
        step_run_id="d" * 32,
        step_number=3,
        step_name=operation.name,
        operation_class=f"{type(operation).__module__}.{type(operation).__qualname__}",
        params_json="{}",
        input_refs_json="{}",
        compute_backend="local",
        compute_options_json=json.dumps(
            {"dynamic_output_roles": not operation.outputs}
        ),
        output_roles_json=json.dumps(list(operation.outputs)),
        output_types_json=json.dumps(
            {role: spec.artifact_type for role, spec in operation.outputs.items()}
        ),
    )


@pytest.mark.parametrize("failed_preparation", [False, True])
def test_registration_only_inspection_retains_order_after_source_removal(
    source: RetainedReader, tmp_path: Path, failed_preparation: bool
) -> None:
    first, second = _input_file(tmp_path), _input_file(tmp_path, "other.csv")
    plan = _register_files(source, [second, first, second])
    if failed_preparation:
        initialize_framework_tables(source.committer)
        tracker = RetainedStepTracker(source.committer, "registration-run")
        operation = IngestCSV()
        record = _registration_record(operation)
        tracker.create_attempt(record)
        tracker.transition(plan.step_run_id, StepStatus.PENDING, StepStatus.RUNNING)
        with pytest.raises(ValueError, match="role"):
            prepare_curator_inputs(
                operation,
                record,
                {"unknown": [plan.inputs[0].artifact_id]},
                software_digest="a" * 64,
                software_cacheable=False,
                committer=source.committer,
            )
        tracker.transition(
            plan.step_run_id,
            StepStatus.RUNNING,
            StepStatus.FAILED,
            result=StepResult(
                step_run_id=plan.step_run_id,
                step_number=plan.step_number,
                step_name=record.step_name,
                status=StepStatus.FAILED,
                error="Input preparation failed",
            ),
        )
    first.write_bytes(b"changed original")
    second.unlink()
    publication = export_retained_publication(
        source, tmp_path / "publication", redact=[str(tmp_path / "source"), str(first)]
    )
    manifest = publication.metadata()
    assert not manifest.sources
    assert not manifest.curator_sources
    assert len(manifest.registration_sources) == len(manifest.carriers) == 1
    selected = manifest.registration_sources[0]
    assert selected.step_run_id == plan.step_run_id
    assert selected.step_number == plan.step_number
    assert selected.operation_name == plan.operation_name
    assert selected.plan_digest == plan.plan_digest
    assert selected.input_artifact_ids == tuple(
        item.artifact_id for item in plan.inputs
    )
    assert selected.input_artifact_ids[0] == selected.input_artifact_ids[2]
    assert selected.input_artifact_ids[0] != selected.input_artifact_ids[1]
    assert selected.omitted_locators == ("input_uris",)
    encoded = (publication.directory / "inspection.json").read_text()
    assert str(first) not in encoded
    assert str(second) not in encoded
    assert not list(publication.directory.rglob("_delta_log"))
    shutil.rmtree(source.committer.delta_root)
    moved = tmp_path / "relocated"
    publication.directory.rename(moved)
    with PublicationExport(moved).artifacts() as artifacts:
        assert set(artifacts) == set(selected.input_artifact_ids)
        assert {item.origin_step_number for item in artifacts.values()} == {3}
        for artifact in artifacts.values():
            assert isinstance(artifact, FileRefArtifact)
            assert artifact.read_content() == b"x\n1\n"


def test_registration_and_curator_keep_distinct_sources_with_one_carrier(
    source: RetainedReader, tmp_path: Path
) -> None:
    registered = _register_files(source, [_input_file(tmp_path)] * 2)
    operation = Merge()
    results = CuratorResults(
        RetainedStepTracker(source.committer, "registration-run"),
        f"{source.committer.delta_root}/curator-staging",
    )
    results.initialize()
    admission = prepare_curator_inputs(
        operation,
        _registration_record(operation),
        {"files": [item.artifact_id for item in registered.inputs]},
        software_digest="a" * 64,
        software_cacheable=False,
        committer=source.committer,
    )
    reference = results.admit(admission)
    staged, terminal = stage_curator(results, admission, operation)
    assert staged.execution_run_id is not None
    results.finish(reference, staged.execution_run_id, terminal)
    publication = export_retained_publication(
        source, tmp_path / "publication", redact=[]
    )
    manifest = publication.metadata()
    assert (
        manifest.registration_sources[0].step_run_id
        == manifest.curator_sources[0].step_run_id
    )
    assert len(manifest.carriers) == len(manifest.external_bindings) == 1
    assert len(manifest.registration_sources[0].input_artifact_ids) == 2
    with publication.artifacts() as artifacts:
        assert len(artifacts) == 1
        assert next(iter(artifacts.values())).origin_step_number == 3


@pytest.mark.parametrize("event", ["captured", "plan", "planned"])
def test_inspection_never_adopts_incomplete_registrations(
    source: RetainedReader, tmp_path: Path, event: str
) -> None:
    def interrupt(observed: str) -> None:
        if observed == event:
            msg = "interrupted acquisition"
            raise RuntimeError(msg)

    with pytest.raises(RuntimeError, match="interrupted acquisition"):
        _register_files(source, [_input_file(tmp_path)], checkpoint=interrupt)
    root = Path(source.committer.delta_root)
    before = {
        str(path.relative_to(root)): path.read_bytes()
        for path in root.rglob("*")
        if path.is_file()
    }
    publication = export_retained_publication(
        source, tmp_path / "publication", redact=[]
    )
    assert not publication.metadata().registration_sources
    with publication.artifacts() as artifacts:
        assert not artifacts
    assert {
        str(path.relative_to(root)): path.read_bytes()
        for path in root.rglob("*")
        if path.is_file()
    } == before


@pytest.mark.parametrize(
    "corruption",
    ["carrier", "spool", "content", "missing_table", "index", "location", "edge"],
)
def test_registration_inspection_rejects_changed_evidence(
    source: RetainedReader, tmp_path: Path, corruption: str
) -> None:
    committer = source.committer
    plan = _register_files(source, [_input_file(tmp_path)])
    if corruption in {"carrier", "spool"}:
        path = (
            uri_join(committer.files_root, plan.carriers[0].relative_path)
            if corruption == "carrier"
            else uri_join(
                registration_directory(committer.delta_root, plan.step_run_id),
                plan.tables[0].files[0].relative_path,
            )
        )
        committer.fs.pipe_file(path, b"changed")
    elif corruption == "missing_table":
        committer.fs.rm(
            uri_join(committer.delta_root, REGISTRATION_TABLES[0]), recursive=True
        )
    elif corruption == "edge":
        initialize_framework_tables(committer)
        path = uri_join(committer.delta_root, TablePath.ARTIFACT_EDGES.value)
        row = {
            "execution_run_id": "f" * 32,
            "source_artifact_id": plan.inputs[0].artifact_id,
            "target_artifact_id": "e" * 32,
            "source_artifact_type": "file_ref",
            "target_artifact_type": "file_ref",
            "source_role": "input",
            "target_role": "output",
            "group_id": None,
            "step_boundary": True,
            "logical_commit_id": plan.logical_commit_id,
        }
        pl.DataFrame(
            [row], schema=get_physical_schema_for_path(TablePath.ARTIFACT_EDGES)
        ).write_delta(path, mode="append")
    else:
        table = REGISTRATION_TABLES[
            {"content": 0, "index": 1, "location": 2}[corruption]
        ]
        path = uri_join(committer.delta_root, table)
        changed = (
            pl.lit("/unowned/file").alias("uri")
            if corruption == "location"
            else pl.lit(99, dtype=pl.Int32).alias("origin_step_number")
        )
        pl.read_delta(path).with_columns(changed).write_delta(path, mode="overwrite")
    destination = tmp_path / "publication"
    with pytest.raises(
        (StoreIntegrityError, ArtifactIntegrityError, OSError, ValueError)
    ):
        export_retained_publication(source, destination, redact=[])
    assert not destination.exists()


def test_registration_inspection_rechecks_source_after_carrier_copy(
    source: RetainedReader, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from artisan.storage import _retained_publication as module

    plan = _register_files(source, [_input_file(tmp_path)])
    original = module._copy_carrier

    def copy_then_change(
        reader: RetainedReader,
        carrier: ManagedCarrier,
        target: Path,
        policy: PublicationPolicy,
    ) -> None:
        original(reader, carrier, target, policy)
        source.committer.fs.pipe_file(
            uri_join(source.committer.files_root, plan.carriers[0].relative_path),
            b"changed after copy",
        )

    monkeypatch.setattr(module, "_copy_carrier", copy_then_change)
    with pytest.raises(StoreIntegrityError, match="carrier bytes changed"):
        export_retained_publication(source, tmp_path / "publication", redact=[])
    assert not (tmp_path / "publication").exists()


@pytest.mark.parametrize("protected", ["identity", "bytes"])
def test_registration_inspection_rejects_protected_redaction(
    source: RetainedReader, tmp_path: Path, protected: str
) -> None:
    plan = _register_files(source, [_input_file(tmp_path)])
    redact = plan.inputs[0].artifact_id if protected == "identity" else "x\n1\n"
    with pytest.raises(ValueError, match="protected"):
        export_retained_publication(source, tmp_path / "publication", redact=[redact])
    assert not (tmp_path / "publication").exists()


@pytest.mark.parametrize("kind", ["file", "appendable", "alias", "empty"])
def test_retained_inspection_is_portable_and_preserves_source_assertions(
    source: RetainedReader, tmp_path: Path, kind: str
) -> None:
    case, receipt = admit_case(source.committer, tmp_path / "worker", kind=kind)
    plan = source.committer.accept(receipt)
    configuration = capture_replay_configuration(
        receipt, {"PRIVATE_TOKEN": "synthetic-secret"}, admission=plan.admission
    )
    publish_replay_configuration(
        configuration, delta_root=source.committer.delta_root, fs=source.committer.fs
    )
    publication = export_retained_publication(
        source,
        tmp_path / "publication",
        redact=["synthetic-secret", str(tmp_path / "source")],
    )
    metadata = publication.metadata()
    original = metadata.sources[0]
    assert original.delivery == receipt.delivery
    assert original.seal_digest == receipt.seal_digest
    assert original.projection == case.projection
    assert original.recipe.digest == receipt.delivery.attempt.recipe_digest
    assert original.replay_configuration.required_runtime_keys == ("PRIVATE_TOKEN",)
    assert original.omitted_locators == ("source_seal_uri",)
    encoded = (publication.directory / "inspection.json").read_text()
    assert receipt.seal_uri not in encoded
    assert "synthetic-secret" not in encoded
    assert "sanitized_inspection" in encoded
    assert metadata.inspection_digest != receipt.seal_digest
    assert not list(publication.directory.rglob("retained-result.json"))
    assert not list(publication.directory.rglob("_delta_log"))
    shutil.rmtree(source.committer.delta_root)
    moved = tmp_path / "moved"
    shutil.move(publication.directory, moved)
    publication = PublicationExport(moved)
    with publication.artifacts() as artifacts:
        assert set(artifacts) == {
            item.artifact_id for item in case.projection.occurrences
        }
        for artifact in artifacts.values():
            artifact.verify_external_content()
    assert not any(
        hasattr(publication, name) for name in ("accept", "recover", "cache", "resume")
    )


@pytest.mark.parametrize(
    "protected", ["delivery", "seal", "recipe", "software", "occurrence"]
)
def test_inspection_redaction_cannot_change_protected_evidence(
    source: RetainedReader, tmp_path: Path, protected: str
) -> None:
    case, receipt = admit_case(source.committer, tmp_path / "worker", kind="alias")
    source.committer.accept(receipt)
    needles = {
        "delivery": receipt.delivery.delivery_id,
        "seal": receipt.seal_digest,
        "recipe": receipt.delivery.attempt.recipe_digest,
        "software": receipt.delivery.attempt.software_digest,
        "occurrence": case.projection.occurrences[0].address.invocation_id,
    }
    destination = tmp_path / "publication"
    with pytest.raises(ValueError, match="protected"):
        export_retained_publication(source, destination, redact=[needles[protected]])
    assert not destination.exists()
    assert source.read(receipt).projection == case.projection


def test_curator_owned_file_and_reference_support_survive_relocation(
    source: RetainedReader, tmp_path: Path
) -> None:
    path = tmp_path / "original.bin"
    payload = b"curator-owned external content\n"
    path.write_bytes(payload)
    operation = ExternalCurator(params=ExternalCurator.Params(source=path))
    results, admission, reference = admit_curator(
        source.committer, operation, {}, step_run_id="b" * 32, **STAMPS
    )
    staged, terminal = stage_curator(results, admission, operation)
    assert staged.success, staged.error
    assert staged.execution_run_id is not None
    results.finish(reference, staged.execution_run_id, terminal)
    plan = read_curator_plan(
        source.committer.delta_root, reference.step_run_id, source.committer.fs
    )
    assert plan is not None
    original = CuratorReader(source.committer).read_result(
        plan.step_run_id, plan.plan_digest
    )
    merge = Merge()
    results, admission, reference = admit_curator(
        source.committer,
        merge,
        {"selected": original.output("config")},
        step_run_id="c" * 32,
        step_number=9,
        **STAMPS,
    )
    staged, terminal = stage_curator(results, admission, merge)
    assert staged.success, staged.error
    assert staged.execution_run_id is not None
    results.finish(reference, staged.execution_run_id, terminal)
    path.unlink()
    publication = export_retained_publication(
        source, tmp_path / "publication", redact=[str(path)]
    )
    manifest = publication.metadata()
    assert not manifest.sources
    assert len(manifest.curator_sources) == 2
    selected = manifest.curator_sources[1].projection
    assert len(selected.references) == 1
    support = selected.references[0].members[0]
    assert support not in {
        address for group in selected.outputs for address in group.occurrences
    }
    shutil.rmtree(source.committer.delta_root)
    moved = tmp_path / "moved"
    shutil.move(publication.directory, moved)
    with PublicationExport(moved).artifacts() as artifacts:
        assert len(artifacts) == 2
        file = next(
            value for value in artifacts.values() if isinstance(value, FileRefArtifact)
        )
        assert Path(file.path).read_bytes() == payload


def test_inspection_metadata_requires_no_recorded_name_imports(
    source: RetainedReader, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _, receipt = admit_case(source.committer, tmp_path / "worker", kind="alias")
    source.committer.accept(receipt)
    publication = export_retained_publication(
        source, tmp_path / "publication", redact=[]
    )
    monkeypatch.setattr(
        importlib,
        "import_module",
        lambda *args, **kwargs: pytest.fail("Inspection imported recorded code"),
    )
    assert publication.metadata().sources[0].delivery == receipt.delivery


def test_inspection_rejects_unlisted_files_and_modified_carrier(
    source: RetainedReader, tmp_path: Path
) -> None:
    _, receipt = admit_case(source.committer, tmp_path / "worker", kind="file")
    source.committer.accept(receipt)
    publication = export_retained_publication(
        source, tmp_path / "publication", redact=[]
    )
    extra = publication.directory / "unlisted"
    extra.write_text("unexpected")
    with pytest.raises(StoreIntegrityError, match="inventory"):
        publication.metadata()
    extra.unlink()
    carrier = publication.metadata().carriers[0]
    (publication.directory / carrier.relative_path).write_bytes(b"changed")
    with pytest.raises(StoreIntegrityError, match="carrier"), publication.artifacts():
        pytest.fail("Changed carrier was hydrated")


def test_private_unavailable_diagnostic_is_redacted_with_explicit_evidence(
    source: RetainedReader, tmp_path: Path
) -> None:
    from artisan.schemas.execution.retained_replay import RetainedReplayConfiguration

    _, receipt = admit_case(source.committer, tmp_path / "worker", kind="alias")
    plan = source.committer.accept(receipt)
    configuration = RetainedReplayConfiguration(
        delivery=receipt.delivery,
        admission_digest=plan.admission.record_digest,
        seal_digest=receipt.seal_digest,
        status="unavailable",
        unavailable_reason="synthetic-private diagnostic failure",
    )
    publish_replay_configuration(
        configuration, delta_root=source.committer.delta_root, fs=source.committer.fs
    )
    publication = export_retained_publication(
        source, tmp_path / "publication", redact=["synthetic-private"]
    )
    original = publication.metadata().sources[0]
    assert original.replay_configuration.status == "unavailable"
    assert (
        original.replay_configuration.unavailable_reason
        == "[redacted] diagnostic failure"
    )
    assert original.redacted_configuration_fields == ("unavailable_reason",)
    assert (
        "synthetic-private"
        not in (publication.directory / "inspection.json").read_text()
    )


@pytest.mark.parametrize("kind", ["populated", "empty", "failed", "reused"])
def test_curator_inspection_preserves_original_source_after_source_removal(
    source: RetainedReader, tmp_path: Path, kind: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    operation = DynamicCurator(
        params=DynamicCurator.Params(empty=kind == "empty", fail=kind == "failed")
    )
    results, admission, reference = admit_curator(
        source.committer, operation, {}, step_run_id="b" * 32, **STAMPS
    )
    staged, terminal, _ = stage_observed(results, admission, operation)
    assert staged.execution_run_id is not None
    results.finish(reference, staged.execution_run_id, terminal)
    if kind == "reused":
        _, _, reused = admit_curator(
            source.committer,
            operation,
            {},
            step_run_id="c" * 32,
            step_number=9,
            **STAMPS,
        )
        hit = lookup_curator(reused, reader=CuratorReader(source.committer))
        assert hit is not None
        results.reuse(hit)
    publication = export_retained_publication(
        source, tmp_path / "publication", redact=[str(tmp_path / "source")]
    )
    original = publication.metadata().curator_sources
    assert len(original) == 1
    selected = original[0]
    assert selected.step_run_id == reference.step_run_id
    assert selected.execution_run_id == staged.execution_run_id
    assert selected.admission_digest == reference.admission_digest
    assert selected.definition == admission.definition
    assert selected.software_digest == admission.software_digest
    assert selected.view == admission.view
    assert selected.success == (kind != "failed")
    assert not publication.metadata().sources
    shutil.rmtree(source.committer.delta_root)
    moved = tmp_path / "moved"
    shutil.move(publication.directory, moved)
    publication = PublicationExport(moved)
    monkeypatch.setattr(
        importlib, "import_module", lambda *_: pytest.fail("Imported recorded code")
    )
    assert publication.metadata().curator_sources == original
    with publication.artifacts() as artifacts:
        assert set(artifacts) == {
            item.occurrence.artifact_id for item in selected.projection.occurrences
        }
        assert len(artifacts) == int(kind in {"populated", "reused"})
    encoded = (moved / "inspection.json").read_text()
    assert "step_record" in encoded
    assert "source_seal_uri" not in encoded
    assert not list(moved.rglob("_delta_log"))


def test_curator_inspection_copies_carriers_owned_by_another_source(
    source: RetainedReader, tmp_path: Path
) -> None:
    _, receipt = admit_case(source.committer, tmp_path / "worker", kind="file")
    source.committer.accept(receipt)
    accepted = source.read(receipt)
    operation = Merge()
    inputs = {
        "passthrough": tuple(
            item for group in accepted.outputs for item in group.occurrences
        )
    }
    results, admission, reference = admit_curator(
        source.committer, operation, inputs, step_run_id="b" * 32, **STAMPS
    )
    staged, terminal = stage_curator(results, admission, operation)
    assert staged.execution_run_id is not None
    results.finish(reference, staged.execution_run_id, terminal)
    publication = export_retained_publication(
        source, tmp_path / "publication", redact=[]
    )
    manifest = publication.metadata()
    selected = manifest.curator_sources[0]
    assert selected.inputs == admission.inputs
    assert any(item.sources for item in selected.projection.occurrences)
    assert len(manifest.carriers) == 1
    shutil.rmtree(source.committer.delta_root)
    with publication.artifacts() as artifacts:
        assert {
            item.occurrence.artifact_id for item in selected.projection.occurrences
        } <= set(artifacts)
    (publication.directory / manifest.carriers[0].relative_path).write_bytes(b"changed")
    with pytest.raises(StoreIntegrityError, match="carrier"), publication.artifacts():
        pytest.fail("Modified curator carrier was hydrated")


@pytest.mark.parametrize(
    "protected",
    ["execution_run_id", "plan_digest", "admission_digest", "software_digest"],
)
def test_curator_inspection_rejects_protected_source_redaction(
    source: RetainedReader, tmp_path: Path, protected: str
) -> None:
    operation = DynamicCurator()
    results, admission, reference = admit_curator(
        source.committer, operation, {}, step_run_id="b" * 32, **STAMPS
    )
    staged, terminal, _ = stage_observed(results, admission, operation)
    assert staged.execution_run_id is not None
    results.finish(reference, staged.execution_run_id, terminal)
    original = (
        export_retained_publication(source, tmp_path / "original", redact=[])
        .metadata()
        .curator_sources[0]
    )
    destination = tmp_path / "redacted"
    with pytest.raises(ValueError, match="protected"):
        export_retained_publication(
            source, destination, redact=[getattr(original, protected)]
        )
    assert not destination.exists()
