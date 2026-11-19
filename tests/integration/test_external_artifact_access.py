"""External file descriptors survive the ordinary retained artifact lifecycle."""

from __future__ import annotations

import shutil
from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Any, ClassVar

import polars as pl
import pytest
from fixtures.input_contract_operations import SharedOffset, named_metric
from pydantic import BaseModel, Field

from artisan.composites.base.compiled import CompiledPlan
from artisan.execution.admitted_worker import execute_admitted_assignment
from artisan.execution.assignment_transport import (
    AdmittedAssignment,
    AssignmentDispatch,
)
from artisan.execution.external_access import freeze_external_files
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.operations.curator import IngestPipelineStep, Merge
from artisan.orchestration import PipelineManager, replay_execution
from artisan.orchestration.engine.retained_execution_query import (
    read_retained_execution_membership,
)
from artisan.orchestration.runners import LocalRunner
from artisan.registry.discovery import bootstrap_applications
from artisan.schemas import ArtifactResult
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.execution_config import ExecutionConfigArtifact
from artisan.schemas.artifact.external_descriptor import (
    ExternalFile,
    ExternalFilesDescriptor,
    PreparedExternalFiles,
)
from artisan.schemas.artifact.metric import MetricArtifact
from artisan.schemas.enums import FailurePolicy, GroupByStrategy
from artisan.schemas.execution.admission import AdmissionRecordWithAccess
from artisan.schemas.execution.assignment import SealedDelivery
from artisan.schemas.execution.batch_strategy import BatchStrategy
from artisan.schemas.execution.external_access import ExternalAccessMount
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.schemas.operation_config.runner_resources import RunnerResources
from artisan.schemas.orchestration.step_lifecycle import StepDisposition, StepStatus
from artisan.schemas.specs.input_models import (
    ExecuteInput,
    PostprocessInput,
    PreprocessInput,
)
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec
from artisan.storage import ArtifactStore, export_publication
from artisan.storage.core.execution_source import (
    ScientificExecutionSource,
    read_execution_source,
)
from artisan.storage.core.retained_reader import RetainedReader
from artisan.storage.external_access import prepare_external_files
from artisan.storage.io.repair import repair_store
from artisan.storage.io.retained_commit import RetainedCommitter
from artisan.storage.io.retained_store import create_retained_store
from artisan.utils.hashing import compute_content_digest

from .conftest import (
    _DirectNativeSoftware,
    _native_software_digest,
    _SerialNativePipelineRunner,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.usefixtures("external_artifact_types"),
]
_APPLICATIONS = ("artisan.schemas.external_artifacts",)


@dataclass(frozen=True)
class _ExternalSoftware(_DirectNativeSoftware):
    external_files: Mapping[str, PreparedExternalFiles] = field(default_factory=dict)
    external_provider: ClassVar[str] = "local"
    external_mounts: ClassVar[tuple[ExternalAccessMount, ...]] = ()

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "external_files", freeze_external_files(self.external_files)
        )


class _ExternalRunner(_SerialNativePipelineRunner):
    def __init__(
        self,
        committer: RetainedCommitter,
        bindings: Mapping[str, PreparedExternalFiles],
    ) -> None:
        super().__init__(committer)
        self.external_files = freeze_external_files(bindings)

    def resolve(
        self,
        plan: CompiledPlan,
        runner_resources: RunnerResources,
        *,
        application_modules: tuple[str, ...] = (),
    ) -> _ExternalSoftware:
        super().resolve(plan, runner_resources, application_modules=application_modules)
        return self._software(application_modules)

    def _software(self, application_modules: tuple[str, ...]) -> _ExternalSoftware:
        return _ExternalSoftware(
            software_digest=_native_software_digest(application_modules),
            application_modules=application_modules,
            external_files=self.external_files,
        )

    def _invoke(
        self, assignment: AdmittedAssignment, dispatch: AssignmentDispatch
    ) -> SealedDelivery:
        with self._science_lock:
            return execute_admitted_assignment(
                assignment,
                self._software(dispatch.application_modules),
                dispatch,
                working_root=Path(dispatch.working_root),
            )


class _EmitExternal(OperationDefinition):
    name = "emit_external_database"

    class OutputRole(StrEnum):
        database = "database"

    inputs: ClassVar[dict[str, InputSpec]] = {}
    outputs: ClassVar[dict[str, OutputSpec]] = {
        "database": OutputSpec(artifact_type="any", derives_from={"inputs": []})
    }

    class Params(BaseModel):
        descriptor: ExternalFilesDescriptor = Field(
            description="Pinned external file inventory"
        )

    params: Params

    def execute_function(self, inputs: ExecuteInput) -> dict[str, Any]:
        return {}

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        from artisan.schemas.external_artifacts import ExternalFilesArtifact

        result = ArtifactResult()
        result.add_artifact(
            "database",
            ExternalFilesArtifact.draft(
                self.params.descriptor, step_number=inputs.step_number
            ),
            sources={},
        )
        return result


class _ReadExternal(OperationDefinition):
    name = "read_external_database"

    class InputRole(StrEnum):
        database = "database"

    class OutputRole(StrEnum):
        value = "value"

    per_artifact_dispatch = False
    inputs: ClassVar[dict[str, InputSpec]] = {
        "database": InputSpec(artifact_type="any")
    }
    outputs: ClassVar[dict[str, OutputSpec]] = {
        "value": OutputSpec(
            artifact_type="metric", derives_from={"inputs": ["database"]}
        )
    }

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        artifacts = inputs.input_artifacts["database"]
        assert all(artifact.materialized_path is None for artifact in artifacts)
        return {
            "paths": [artifact.external_root / "index.bin" for artifact in artifacts]
        }

    def execute_function(self, inputs: ExecuteInput) -> dict[str, Any]:
        return {"values": [int(path.read_text()) for path in inputs.inputs["paths"]]}

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        result = ArtifactResult()
        for index, value in enumerate(inputs.memory_outputs["values"]):
            result.add_artifact(
                "value",
                MetricArtifact.draft(
                    content={"value": value},
                    original_name="value.json",
                    step_number=inputs.step_number,
                ),
                sources={"database": [inputs.ref("database", index=index)]},
            )
        return result


class _MakeConfigs(OperationDefinition):
    name = "configure_external_database"

    class InputRole(StrEnum):
        database = "database"

    class OutputRole(StrEnum):
        config = "config"

    per_artifact_dispatch = False
    inputs: ClassVar[dict[str, InputSpec]] = {
        "database": InputSpec(artifact_type="any", materialize=False)
    }
    outputs: ClassVar[dict[str, OutputSpec]] = {
        "config": OutputSpec(
            artifact_type="config", derives_from={"inputs": ["database"]}
        )
    }

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        return {}

    def execute_function(self, inputs: ExecuteInput) -> dict[str, Any]:
        return {}

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        result = ArtifactResult()
        for index, artifact in enumerate(inputs.input_artifacts["database"]):
            result.add_artifact(
                "config",
                ExecutionConfigArtifact.draft(
                    content={
                        "database": {"$artifact": artifact.artifact_id},
                        "sample": index,
                    },
                    original_name=f"config-{index}.json",
                    step_number=inputs.step_number,
                ),
                sources={"database": [inputs.ref("database", index=index)]},
            )
        return result


class _ReadConfig(_ReadExternal):
    name = "read_configured_external_database"

    class InputRole(StrEnum):
        config = "config"

    inputs: ClassVar[dict[str, InputSpec]] = {
        "config": InputSpec(
            artifact_type="config", materialize=False, resolve_references=True
        )
    }
    outputs: ClassVar[dict[str, OutputSpec]] = {
        "value": OutputSpec(artifact_type="metric", derives_from={"inputs": ["config"]})
    }

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        paths = []
        for index in range(len(inputs.input_artifacts["config"])):
            references = inputs.referenced_artifacts(inputs.ref("config", index=index))
            assert len(references) == 1
            artifact = next(iter(references.values()))
            assert artifact.materialized_path is None
            paths.append(artifact.external_root / "index.bin")
        return {"paths": paths}

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        result = ArtifactResult()
        for index, value in enumerate(inputs.memory_outputs["values"]):
            result.add_artifact(
                "value",
                MetricArtifact.draft(
                    content={"value": value},
                    original_name="value.json",
                    step_number=inputs.step_number,
                ),
                sources={"config": [inputs.ref("config", index=index)]},
            )
        return result


def _installation(
    root: Path, payload: bytes = b"7"
) -> tuple[Artifact, PreparedExternalFiles]:
    from artisan.schemas.external_artifacts import ExternalFilesArtifact

    root.mkdir(parents=True)
    (root / "index.bin").write_bytes(payload)
    descriptor = ExternalFilesDescriptor(
        files=(
            ExternalFile(
                path="index.bin",
                content_hash=compute_content_digest(payload),
                size_bytes=len(payload),
            ),
        )
    )
    artifact = ExternalFilesArtifact.draft(descriptor).finalize()
    return artifact, prepare_external_files(root, descriptor)


def _manager(
    root: Path, bindings: Mapping[str, PreparedExternalFiles]
) -> PipelineManager:
    bootstrap_applications(_APPLICATIONS, purpose="external artifact integration test")
    storage = StorageConfig()
    committer = create_retained_store(
        str(root / "delta"), str(root / "files"), fs=storage.filesystem()
    )
    return PipelineManager.create(
        "external-artifacts",
        committer.delta_root,
        str(root / "staging"),
        files_root=committer.files_root,
        working_root=str(root / "work"),
        default_step_runner=_ExternalRunner(committer, bindings),
        application_modules=_APPLICATIONS,
        failure_policy=FailurePolicy.CONTINUE,
    )


def _sources(
    manager: PipelineManager, step_run_id: str
) -> list[ScientificExecutionSource]:
    selected = [
        item.source
        for item in read_retained_execution_membership(
            manager.committer, pipeline_run_id=manager.config.pipeline_run_id
        )
        if item.state.step_run_id == step_run_id
    ]
    assert all(isinstance(item, ScientificExecutionSource) for item in selected)
    return selected


def _forbid_payload_transport(monkeypatch: pytest.MonkeyPatch) -> None:
    def forbidden(*args: object, **kwargs: object) -> None:
        message = "Framework attempted to open, copy, or hash an external payload"
        raise AssertionError(message)

    from artisan.schemas.external_artifacts import ExternalFilesArtifact

    monkeypatch.setattr("artisan.storage.io.admission.open_external", forbidden)
    monkeypatch.setattr("artisan.storage.io.external_carriers.open_external", forbidden)
    monkeypatch.setattr(
        "artisan.storage.external_access.compute_stream_digest", forbidden
    )
    monkeypatch.setattr(ExternalFilesArtifact, "_materialize_content", forbidden)


def test_retained_batch_records_one_binding_for_repeated_external_artifact(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    artifact, prepared = _installation(tmp_path / "database")
    _forbid_payload_transport(monkeypatch)
    with _manager(tmp_path / "store", {artifact.artifact_id: prepared}) as manager:
        emitted = manager.run(_EmitExternal, params={"descriptor": prepared.descriptor})
        assert emitted.status is StepStatus.SUCCEEDED, emitted.error
        repeated = [artifact.artifact_id] * 3
        measured = manager.run(
            _ReadExternal,
            inputs={"database": repeated},
            batch_strategy={"artifacts_per_unit": 2},
        )
        assert measured.status is StepStatus.SUCCEEDED, measured.error
        sources = _sources(manager, measured.step_run_id)
        assert len(sources) == 2
        reader = RetainedReader(manager.committer)
        counts = []
        for source in sources:
            assert isinstance(source.admission, AdmissionRecordWithAccess)
            with reader.capture_inputs(source.plan.receipt) as captured:
                assert captured.external_access.bindings[0].prepared == prepared
                assert len(captured.external_access.bindings) == 1
                counts.append(
                    len(captured.record.boundary.boundary_inputs[0].occurrences)
                )
            values = reader.read(source.plan.receipt).output("value")
            assert all(item.artifact.values == {"value": 7} for item in values)
        assert sorted(counts) == [1, 2]
        assert not list(Path(manager.config.files_root).rglob("index.bin"))


def test_external_cache_reuses_replicas_without_probing_unavailable_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    artifact, first = _installation(tmp_path / "first")
    replica_artifact, second = _installation(tmp_path / "replica")
    changed_artifact, changed = _installation(tmp_path / "changed", b"8")
    assert artifact.artifact_id == replica_artifact.artifact_id
    _forbid_payload_transport(monkeypatch)
    with _manager(tmp_path / "store", {artifact.artifact_id: first}) as manager:
        emitted = manager.run(_EmitExternal, params={"descriptor": first.descriptor})
        original = manager.run(
            _ReadExternal, inputs={"database": emitted.output("database")}
        )
        assert original.status is StepStatus.SUCCEEDED, original.error
        replica_runner = _ExternalRunner(
            manager.committer, {artifact.artifact_id: second}
        )
        replica = manager.run(
            _ReadExternal,
            inputs={"database": emitted.output("database")},
            step_runner=replica_runner,
        )
        assert replica.status is StepStatus.SUCCEEDED, replica.error
        assert replica.disposition is StepDisposition.CACHE_HIT
        shutil.rmtree(second.root)
        unavailable = manager.run(
            _ReadExternal,
            inputs={"database": emitted.output("database")},
            step_runner=replica_runner,
        )
        assert unavailable.status is StepStatus.SUCCEEDED, unavailable.error
        assert unavailable.disposition is StepDisposition.CACHE_HIT
        missing = manager.run(
            _ReadExternal,
            inputs={"database": emitted.output("database")},
            step_runner=_ExternalRunner(manager.committer, {}),
        )
        assert missing.status is StepStatus.FAILED
        assert "external" in missing.error.lower()
        changed_output = manager.run(
            _EmitExternal, params={"descriptor": changed.descriptor}
        )
        miss = manager.run(
            _ReadExternal,
            inputs={"database": changed_output.output("database")},
            step_runner=_ExternalRunner(
                manager.committer, {changed_artifact.artifact_id: changed}
            ),
        )
        assert miss.status is StepStatus.SUCCEEDED, miss.error
        assert miss.disposition is StepDisposition.EXECUTED


def test_external_config_dependencies_are_shared_and_retained_in_batches(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    artifact, prepared = _installation(tmp_path / "database")
    _forbid_payload_transport(monkeypatch)
    with _manager(tmp_path / "store", {artifact.artifact_id: prepared}) as manager:
        manager.run(_EmitExternal, params={"descriptor": prepared.descriptor})
        configured = manager.run(
            _MakeConfigs,
            inputs={"database": [artifact.artifact_id] * 3},
            batch_strategy={"artifacts_per_unit": 3},
        )
        assert configured.status is StepStatus.SUCCEEDED, configured.error
        measured = manager.run(
            _ReadConfig,
            inputs={"config": configured.output("config")},
            batch_strategy={"artifacts_per_unit": 2},
        )
        assert measured.status is StepStatus.SUCCEEDED, measured.error
        sources = _sources(manager, measured.step_run_id)
        assert len(sources) == 2
        for source in sources:
            with RetainedReader(manager.committer).capture_inputs(
                source.plan.receipt
            ) as captured:
                assert len(captured.external_access.bindings) == 1
                assert (
                    captured.external_access.bindings[0].artifact_id
                    == artifact.artifact_id
                )
                references = captured.record.boundary.frozen_context.references
                assert references
                assert all(len(reference.members) == 1 for reference in references)
                assert {
                    captured.artifacts[reference.members[0]].artifact_id
                    for reference in references
                } == {artifact.artifact_id}


def test_external_descriptors_import_and_recover_without_installation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    artifact, prepared = _installation(tmp_path / "database")
    _forbid_payload_transport(monkeypatch)
    with _manager(tmp_path / "source", {}) as source:
        emitted = source.run(_EmitExternal, params={"descriptor": prepared.descriptor})
        assert emitted.status is StepStatus.SUCCEEDED, emitted.error
        merged = source.run(
            Merge, inputs=[emitted.output("database"), emitted.output("database")]
        )
        assert merged.status is StepStatus.SUCCEEDED, merged.error
        emitted_source = _sources(source, emitted.step_run_id)[0]
    shutil.rmtree(prepared.root)
    original = RetainedReader(source.committer).read(emitted_source.plan.receipt)
    assert (
        original.output("database")[0].artifact.external_descriptor()
        == prepared.descriptor
    )
    report = repair_store(
        delta_root=source.config.delta_root,
        staging_root=source.config.staging_root,
        fs=StorageConfig().filesystem(),
        files_root=source.config.files_root,
        apply=True,
    )
    assert not report.blocking
    assert not report.unresolved
    with _manager(tmp_path / "destination", {}) as destination:
        imported = destination.run(
            IngestPipelineStep,
            params={
                "source_delta_root": source.config.delta_root,
                "source_files_root": source.config.files_root,
                "source_run_id": source.config.pipeline_run_id,
                "source_step": merged.step_number,
                "include_prior_steps": True,
                "artifact_type": "external_files",
            },
        )
        assert imported.status is StepStatus.SUCCEEDED, imported.error
        store = ArtifactStore(
            destination.config.delta_root, files_root=destination.config.files_root
        )
        artifacts = store.get_artifacts_by_type(
            store.provenance.load_artifact_ids_by_type("external_files"),
            "external_files",
        )
        assert len(artifacts) == 1
        imported_artifact = next(iter(artifacts.values()))
        assert imported_artifact.external_descriptor() == prepared.descriptor
        assert (
            imported_artifact.metadata["imported_from"]["pipeline_run_id"]
            == source.config.pipeline_run_id
        )
        assert (
            store.provenance.get_direct_ancestors(imported_artifact.artifact_id) == []
        )
        assert imported_artifact.artifact_id != artifact.artifact_id


def test_external_replay_uses_explicit_new_binding_and_preserves_scientific_ids(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    artifact, first = _installation(tmp_path / "original")
    _, replica = _installation(tmp_path / "replica")
    _forbid_payload_transport(monkeypatch)
    with _manager(tmp_path / "store", {artifact.artifact_id: first}) as manager:
        emitted = manager.run(_EmitExternal, params={"descriptor": first.descriptor})
        measured = manager.run(
            _ReadExternal, inputs={"database": emitted.output("database")}
        )
        assert measured.status is StepStatus.SUCCEEDED, measured.error
        source = _sources(manager, measured.step_run_id)[0]
    shutil.rmtree(first.root)
    exported = export_publication(
        ArtifactStore(manager.config.delta_root, files_root=manager.config.files_root),
        tmp_path / "inspection",
        redact=(),
    )
    copied = exported.open_store().get_artifact(artifact.artifact_id, "external_files")
    assert copied.external_descriptor() == first.descriptor
    assert not list(exported.directory.rglob("index.bin"))
    report = repair_store(
        delta_root=manager.config.delta_root,
        staging_root=manager.config.staging_root,
        files_root=manager.config.files_root,
        fs=StorageConfig().filesystem(),
        apply=True,
    )
    assert not report.blocking
    assert not report.unresolved
    source_id = source.plan.receipt.delivery.delivery_id
    result = replay_execution(
        source_id,
        delta_root=manager.config.delta_root,
        files_root=manager.config.files_root,
        operation=_ReadExternal,
        application_modules=_APPLICATIONS,
        step_runner=_ExternalRunner(manager.committer, {artifact.artifact_id: replica}),
        working_root=tmp_path / "replay-work",
        staging_root=str(tmp_path / "replay-staging"),
    )
    assert result.step_result.status is StepStatus.SUCCEEDED, result.step_result.error
    assert result.diagnostic_status == "complete", result.diagnostic_errors
    replayed = read_execution_source(manager.committer, result.execution_run_id)
    assert isinstance(replayed, ScientificExecutionSource)
    assert replayed.admission.attempt.replay_of_execution_run_id == source_id
    assert replayed.admission.access_digest != source.admission.access_digest
    reader = RetainedReader(manager.committer)
    with reader.capture_inputs(replayed.plan.receipt) as captured:
        assert captured.external_access.bindings[0].prepared == replica
    assert [
        item.artifact.artifact_id
        for item in reader.read(source.plan.receipt).output("value")
    ] == [
        item.artifact.artifact_id
        for item in reader.read(replayed.plan.receipt).output("value")
    ]


def test_spawned_native_worker_records_preset_inputs_without_materializing(
    tmp_path: Path,
) -> None:
    from artisan.operations.examples.external_files import scale_with_weights

    value = MetricArtifact.draft(
        content={"value": 3}, original_name="value.json"
    ).finalize()
    selection = scale_with_weights([value])
    root = tmp_path / "weights"
    root.mkdir()
    (root / "scale.txt").write_bytes(b"2\n")
    prepared = prepare_external_files(root, selection.weights.external_descriptor())
    runner = LocalRunner(
        cache_root=tmp_path / "software",
        external_files={selection.weights.artifact_id: prepared},
    )
    runner.prepare(selection.operation, application_modules=_APPLICATIONS)
    with PipelineManager.create(
        "spawned-external-preset",
        str(tmp_path / "delta"),
        str(tmp_path / "staging"),
        files_root=str(tmp_path / "files"),
        working_root=str(tmp_path / "work"),
        default_step_runner=runner,
        application_modules=_APPLICATIONS,
    ) as manager:
        measured = manager.run(selection.operation, inputs=selection.inputs)
        assert measured.status is StepStatus.SUCCEEDED, measured.error
        source = _sources(manager, measured.step_run_id)[0]
        assert isinstance(source.admission, AdmissionRecordWithAccess)
        assert source.admission.access_artifact_ids == (selection.weights.artifact_id,)
        result = RetainedReader(manager.committer).read(source.plan.receipt)
        assert result.output("result")[0].artifact.values == {"value": 6.0}
    assert not list((tmp_path / "files").rglob("scale.txt"))
    assert not list((tmp_path / "work").rglob("scale.txt"))


class _NamedExternalOffset(SharedOffset):
    name = "named_external_offset"
    group_by: GroupByStrategy | None = GroupByStrategy.NAME
    batch_strategy: BatchStrategy = BatchStrategy(artifacts_per_unit=3)
    inputs: ClassVar = {
        **SharedOffset.inputs,
        "offset": InputSpec(artifact_type="any", materialize=False),
    }

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        artifact = inputs.input_artifacts["offset"][0]
        assert artifact.EXTERNAL_REFERENCE
        assert artifact.materialized_path is None
        offset = int((artifact.external_root / "index.bin").read_text())
        return {
            "values": [
                source.values["value"] + offset
                for source in inputs.input_artifacts["source"]
            ]
        }


def _named_external_inputs(artifact: Artifact) -> dict[str, list[Artifact]]:
    return {
        "source": [named_metric(name, index) for index, name in enumerate("ABCDEF")],
        "offset": [artifact],
        "context": [
            named_metric(name, 20 + index) for index, name in enumerate("FEDCBA")
        ],
    }


def test_external_singleton_name_pairing_survives_cache_and_replay_chain(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    artifact, first = _installation(tmp_path / "original")
    _, replica = _installation(tmp_path / "replica")
    _forbid_payload_transport(monkeypatch)
    inputs = _named_external_inputs(artifact)
    with _manager(tmp_path / "store", {artifact.artifact_id: first}) as manager:
        result = manager.run(_NamedExternalOffset, inputs=inputs)
        assert result.status is StepStatus.SUCCEEDED, result.error
        sources = _sources(manager, result.step_run_id)
        assert len(sources) == 2
        reader = RetainedReader(manager.committer)
        values = []
        for source in sources:
            assert isinstance(source.admission, AdmissionRecordWithAccess)
            assert source.admission.recipe.schema_version == 2
            roles = source.admission.boundary.input_roles()
            assert len(roles["offset"]) == 1
            assert roles["offset"][0].index == 0
            with reader.capture_inputs(source.plan.receipt) as captured:
                assert captured.record.carriers == ()
                assert captured.record.external_bindings == ()
                assert len(captured.external_access.bindings) == 1
                assert captured.external_access.bindings[0].prepared == first
            accepted = reader.read(source.plan.receipt)
            for item in accepted.output("metrics"):
                values.append(item.artifact.values["value"])
                assert [
                    edge.source.index
                    for edge in accepted.projection.parents
                    if edge.target == item.reference.address
                    and edge.source.role == "offset"
                ] == [0]
        assert sorted(values) == list(range(7, 13))
        provenance = ArtifactStore(
            manager.config.delta_root, files_root=manager.config.files_root
        ).provenance
        assert provenance.get_artifact_step_number(artifact.artifact_id) is None
        targets = {
            item.artifact.artifact_id
            for source in sources
            for item in reader.read(source.plan.receipt).output("metrics")
        }
        edges = provenance.load_direct_edges_df(
            targets,
            include_roles=True,
            execution_ids={
                source.plan.receipt.delivery.delivery_id for source in sources
            },
        )
        external_parents = edges.filter(pl.col("source_role") == "offset")
        assert external_parents.height == 6
        assert set(external_parents["target_artifact_id"]) == targets
        assert set(external_parents["source_artifact_id"]) == {artifact.artifact_id}
        original = next(
            source
            for source in sources
            if source.plan.admission.attempt.assignment.unit_index == 0
        )
        assert len(original.admission.boundary.input_roles()["context"]) == 3
        cached = manager.run(_NamedExternalOffset, inputs=inputs)
        assert cached.status is StepStatus.SUCCEEDED, cached.error
        assert cached.disposition is StepDisposition.CACHE_HIT
    shutil.rmtree(first.root)
    previous = original.plan.receipt.delivery.delivery_id
    for index in range(2):
        diagnostic = replay_execution(
            previous,
            delta_root=manager.config.delta_root,
            files_root=manager.config.files_root,
            operation=_NamedExternalOffset,
            application_modules=_APPLICATIONS,
            step_runner=_ExternalRunner(
                manager.committer, {artifact.artifact_id: replica}
            ),
            working_root=tmp_path / f"replay-work-{index}",
            staging_root=str(tmp_path / f"replay-staging-{index}"),
        )
        assert diagnostic.step_result.status is StepStatus.SUCCEEDED, (
            diagnostic.step_result.error
        )
        selected = read_execution_source(manager.committer, diagnostic.execution_run_id)
        assert isinstance(selected, ScientificExecutionSource)
        assert isinstance(selected.admission, AdmissionRecordWithAccess)
        assert len(selected.admission.boundary.input_roles()["context"]) == 3
        assert selected.admission.attempt.replay_of_execution_run_id == previous
        with reader.capture_inputs(selected.plan.receipt) as captured:
            assert captured.external_access.bindings[0].prepared == replica
        assert [
            item.artifact.artifact_id
            for item in reader.read(selected.plan.receipt).output("metrics")
        ] == [
            item.artifact.artifact_id
            for item in reader.read(original.plan.receipt).output("metrics")
        ]
        previous = diagnostic.execution_run_id
    assert not list(Path(manager.config.files_root).rglob("index.bin"))


@pytest.mark.parametrize("damage", ["missing", "changed"])
def test_replay_rechecks_selected_external_access_before_publication(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, damage: str
) -> None:
    from artisan.errors import StoreIntegrityError
    from artisan.orchestration.retained_replay import prepare_retained_replay
    from artisan.storage.io.admission import admission_directory

    artifact, prepared = _installation(tmp_path / "database")
    _forbid_payload_transport(monkeypatch)
    with _manager(tmp_path / "store", {artifact.artifact_id: prepared}) as manager:
        result = manager.run(
            _NamedExternalOffset, inputs=_named_external_inputs(artifact)
        )
        assert result.status is StepStatus.SUCCEEDED, result.error
        original = next(
            source
            for source in _sources(manager, result.step_run_id)
            if source.plan.admission.attempt.assignment.unit_index == 0
        )
        replay = prepare_retained_replay(
            manager.committer,
            original.plan.receipt.delivery.delivery_id,
            registered_definitions={
                (
                    _NamedExternalOffset.__module__,
                    _NamedExternalOffset.__qualname__,
                ): _NamedExternalOffset,
            },
            runtime_replacements={},
            allow_code_change=False,
        )
        path = (
            Path(
                admission_directory(manager.config.delta_root, replay.admission.attempt)
            )
            / "external-access.json"
        )
        if damage == "missing":
            path.unlink()
        else:
            path.write_bytes(path.read_bytes() + b" ")
        expected = FileNotFoundError if damage == "missing" else StoreIntegrityError
        with pytest.raises(expected):
            replay.validate_selection(
                manager.committer, replay.delivery.attempt.software_digest
            )
        assert manager.committer.journal.state(replay.identity) is None
