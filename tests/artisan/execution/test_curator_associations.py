"""Prehydrated metrics are invocation inputs even without authored store queries."""

from __future__ import annotations

import json
from collections.abc import Mapping
from enum import StrEnum
from pathlib import Path
from typing import ClassVar

import polars as pl
import pytest
from fixtures.retained_acceptance import open_committer
from fixtures.retained_curators import admit_curator
from fixtures.retained_readers import admit_read_case
from fixtures.retained_results import RetainedCase, _derive, boundary_artifact
from fsspec.implementations.local import LocalFileSystem

from artisan.errors import StoreIntegrityError
from artisan.execution.executors.curator import run_curator_flow
from artisan.execution.lineage.graph import OccurrenceGraph
from artisan.execution.models.execution_unit import ExecutionUnit
from artisan.operations.base import OperationDefinition
from artisan.schemas import InputSpec, MetricArtifact, OutputSpec, PassthroughResult
from artisan.schemas.execution.curator_admission import CuratorInputAssociation
from artisan.schemas.execution.replay import ReplayAssociation, ReplaySnapshot
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.schemas.execution.worker_config import WorkerConfig
from artisan.schemas.orchestration.step_lifecycle import StepDisposition, StepStatus
from artisan.schemas.orchestration.step_result import StepResult
from artisan.schemas.specs.input_models import CuratorInput
from artisan.storage.cache.curator_dependencies import CuratorReadRecorder
from artisan.storage.cache.curator_lookup import lookup_curator
from artisan.storage.core.artifact_store import ArtifactStore
from artisan.storage.core.curator_reader import CuratorReader
from artisan.storage.core.curator_view import CuratorView
from artisan.storage.core.retained_reader import AcceptedArtifact, RetainedReader
from artisan.storage.io.retained_commit import RetainedCommitter
from artisan.storage.io.worker_seal import capture_worker_files


class ReadMetricAssociations(OperationDefinition):
    name = "read_metric_associations"

    class InputRole(StrEnum):
        source = "source"

    class OutputRole(StrEnum):
        data = "data"

    inputs: ClassVar = {
        "source": InputSpec(artifact_type="csv", with_associated=("metric",))
    }
    outputs: ClassVar = {"data": OutputSpec(artifact_type="csv")}

    def execute_curator(
        self,
        inputs: Mapping[str, pl.DataFrame],
        step_number: int,
        artifact_store: ArtifactStore,
    ) -> PassthroughResult:
        assert isinstance(inputs, CuratorInput)
        primary = inputs["source"]["artifact_id"][0]
        metrics = inputs.associated_artifacts(primary, "metric")
        values = []
        for metric in metrics:
            assert isinstance(metric, MetricArtifact)
            assert metric.content is not None
            values.append(json.loads(metric.content)["score"])
        return PassthroughResult(
            passthrough={"data": [primary]}, metadata={"metric_values": values}
        )


@pytest.fixture
def measured(
    tmp_path: Path,
) -> tuple[RetainedCommitter, AcceptedArtifact, tuple[str, ...]]:
    committer = open_committer(
        str(tmp_path / "candidate"), LocalFileSystem(), StorageConfig(), initialize=True
    )
    graph = OccurrenceGraph()
    primary = boundary_artifact()
    slot = graph.add_input("data", primary)
    metrics = [
        MetricArtifact.draft({"score": score}, f"score-{score}.json", 7).finalize()
        for score in (1, 2)
    ]
    addresses = [
        _derive(graph, f"measure-{index}", metric, [slot])
        for index, metric in enumerate(metrics)
    ]
    case = RetainedCase(
        graph.project(
            {"data": [slot], "metrics": addresses},
            {"data": OutputSpec(), "metrics": OutputSpec()},
        ),
        {slot: primary},
        dict(zip(addresses, metrics, strict=True)),
    )
    _, receipt = admit_read_case(committer, case, step_run_id="a" * 32)
    committer.accept(receipt)
    result = RetainedReader(committer).read(receipt)
    return (
        committer,
        result.output("data")[0],
        tuple(metric.artifact_id for metric in metrics if metric.artifact_id),
    )


@pytest.mark.parametrize("populated", [False, True])
def test_preloaded_metrics_change_cache_identity_without_authored_queries(
    measured: tuple[RetainedCommitter, AcceptedArtifact, tuple[str, ...]],
    populated: bool,
) -> None:
    committer, supplied, metric_ids = measured
    primary = supplied.artifact.artifact_id
    assert primary is not None
    operation = ReadMetricAssociations()
    association = CuratorInputAssociation(
        primary_id=primary,
        artifact_type="metric",
        artifact_ids=metric_ids if populated else (),
    )
    coordinator, admission, reference = admit_curator(
        committer,
        operation,
        {"source": (supplied,)},
        step_run_id="b" * 32,
        software_digest="e" * 64,
        associations=(association,),
    )
    view = CuratorView(committer, admission.view)
    recorder = CuratorReadRecorder(view, admission)
    unit = ExecutionUnit(
        operation=operation,
        inputs={"source": [primary]},
        associated_inputs=[ReplayAssociation(**association.model_dump())],
        step_number=admission.record.step_number,
        step_run_id=admission.record.step_run_id,
        execution_spec_id=admission.digest,
        replay_snapshot=ReplaySnapshot.unavailable("association-fixture"),
    )
    runtime = WorkerConfig(
        delta_root=committer.delta_root,
        staging_root=coordinator.staging.staging_dir,
        files_root=committer.files_root,
    )
    for change in (
        {"associated_inputs": []},
        {"inputs": {"source": []}},
        {"step_run_id": "f" * 32},
    ):
        with pytest.raises(ValueError, match="exact admitted invocation"):
            run_curator_flow(
                unit.model_copy(update=change),
                runtime,
                curator_view=view,
                read_recorder=recorder,
            )
    staged = run_curator_flow(unit, runtime, curator_view=view, read_recorder=recorder)
    assert staged.success, staged.error
    assert staged.staging_path is not None
    with capture_worker_files(staged.staging_path, committer.fs) as captured:
        row = captured.read_frame("executions.parquet").row(0, named=True)
    values = [1, 2] if populated else []
    assert json.loads(row["metadata"])["metric_values"] == values
    assert recorder.proof.views == ()
    assert recorder.proof.ineligible_reasons == ()
    assert staged.execution_run_id is not None
    coordinator.finish(
        reference,
        staged.execution_run_id,
        StepResult(
            step_run_id=reference.step_run_id,
            step_number=admission.record.step_number,
            step_name=operation.name,
            status=StepStatus.SUCCEEDED,
            disposition=StepDisposition.EXECUTED,
            total_count=1,
            succeeded_count=1,
            failed_count=0,
            output_roles=frozenset({"data"}),
            output_types={"data": "csv"},
            metadata={"metric_values": values},
        ),
    )
    for owner, ids in zip(
        "cdef", ((), metric_ids, metric_ids[::-1], metric_ids[:1]), strict=True
    ):
        _, _, current_ref = admit_curator(
            committer,
            operation,
            {"source": (supplied,)},
            step_run_id=owner * 32,
            software_digest="e" * 64,
            associations=(association.model_copy(update={"artifact_ids": ids}),),
        )
        hit = lookup_curator(current_ref, reader=CuratorReader(committer))
        assert (hit is not None) == (ids == association.artifact_ids)


def test_associated_content_must_exist_in_admitted_view(
    measured: tuple[RetainedCommitter, AcceptedArtifact, tuple[str, ...]],
) -> None:
    committer, supplied, _ = measured
    primary = supplied.artifact.artifact_id
    assert primary is not None
    with pytest.raises(StoreIntegrityError, match="absent from its frozen"):
        admit_curator(
            committer,
            ReadMetricAssociations(),
            {"source": (supplied,)},
            step_run_id="b" * 32,
            software_digest="e" * 64,
            associations=(
                CuratorInputAssociation(
                    primary_id=primary, artifact_type="metric", artifact_ids=("0" * 32,)
                ),
            ),
        )
