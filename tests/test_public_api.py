"""Exact publication contract for the supported Artisan facades."""

from __future__ import annotations

import importlib
import subprocess
import sys
import tomllib
from dataclasses import fields
from importlib.metadata import version
from pathlib import Path

import pytest

from artisan.composites import CompositeRef


def test_package_version_matches_manifest_and_installed_metadata() -> None:
    import artisan

    manifest = tomllib.loads((Path(__file__).parents[1] / "pyproject.toml").read_text())
    assert (
        artisan.__version__
        == version("dexterity-artisan")
        == manifest["project"]["version"]
    )


_PUBLIC_EXPORTS: dict[str, tuple[str, ...]] = {
    "artisan": ("__version__",),
    "artisan.execution.environments": (),
    "artisan.inspection": (
        "EvidenceExportReceipt",
        "InspectionCatalog",
        "InspectionDescription",
        "OperatingGuideCatalog",
        "OperatingGuideTopic",
        "describe_inspection",
        "export_evidence",
        "inspect_evidence",
        "list_inspections",
        "list_operating_guides",
        "load_evidence_export",
        "read_operating_guide",
    ),
    "artisan.operations.base": (
        "OperationDefinition",
        "PerArtifact",
    ),
    "artisan.operations.lineage": ("match_outputs_to_inputs_by_stem",),
    "artisan.operations.curator": (
        "ConsolidateAppendables",
        "DeclareLineage",
        "Filter",
        "IngestCSV",
        "IngestFiles",
        "IngestPipelineStep",
        "InteractiveFilter",
        "Merge",
    ),
    "artisan.operations.examples": (
        "AppendableGenerator",
        "CsvHead",
        "CsvPair",
        "DataGenerator",
        "DataGeneratorWithMetrics",
        "DataTransformer",
        "DataTransformerConfig",
        "DataTransformerScript",
        "FileGenerator",
        "MetricCalculator",
        "SequentialDataTransformer",
        "SequentialSlowTransformer",
        "SlowTransformer",
        "StreamingEcho",
        "Wait",
        "WaitTool",
    ),
    "artisan.composites": (
        "CompositeCall",
        "CompositeContext",
        "CompositeDefinition",
        "CompositeRef",
    ),
    "artisan.orchestration": (
        "CancellationStatus",
        "OutputReference",
        "PipelineConfig",
        "PipelineManager",
        "ReplayResult",
        "StepDisposition",
        "StepFuture",
        "StepResult",
        "StepStatus",
        "list_runs",
        "replay_execution",
    ),
    "artisan.orchestration.runner_api": (
        "AdmittedAssignment",
        "AdmittedCurator",
        "ApptainerRuntime",
        "Assignment",
        "AssignmentAttempt",
        "AssignmentDispatch",
        "AssignmentHandle",
        "AssignmentLaunchError",
        "AssignmentReply",
        "AssignmentReplyContext",
        "AssignmentSoftware",
        "CancellationAcknowledgement",
        "CancellationStatus",
        "CommandOwnershipError",
        "CommandRecorder",
        "CompiledPlan",
        "ContainerBuildInputs",
        "ContainerLauncherIdentity",
        "CuratorAdmissionReference",
        "CuratorHandle",
        "CuratorLaunchError",
        "CuratorReply",
        "CuratorReplyContext",
        "Definition",
        "DerivedWheel",
        "Environment",
        "EnvironmentSource",
        "ExecuteAdapter",
        "InertCallControl",
        "InertCallResult",
        "InertWorkerCall",
        "LocalLauncherIdentity",
        "LocalPackStartupError",
        "LocalRunner",
        "LocalWorkerProfile",
        "NativeGPUAllocation",
        "NativeGPUSoftware",
        "OperationDefinition",
        "OwnershipState",
        "Parameters",
        "PreparedAssignmentSoftware",
        "PreparedCommandWitness",
        "PreparedCuratorSoftware",
        "PreparedLocalBindings",
        "PreparedNativeEnvironment",
        "PreparedSIFEnvironment",
        "PreparedToolBinding",
        "RunnerResources",
        "RuntimeCompatibility",
        "SealedDelivery",
        "StagingResult",
        "StorageConfig",
        "WorkerOwner",
        "WorkerTransportHeader",
        "capture_container_inputs",
        "capture_curator_definition",
        "capture_native_gpu",
        "capture_runtime_compatibility",
        "compile_definition",
        "controller_definition_sources",
        "controller_worker_sources",
        "execute_admitted_assignment",
        "execute_admitted_curator",
        "execute_inert_call",
        "launch_inert_calls",
        "observed_controller_sources",
        "observed_definition_sources",
        "prepare_command_witness",
        "select_runtime_compatibility",
        "select_tool_environments",
        "serialize_worker_call",
        "verify_command_witness",
        "verify_container_source",
        "verify_native_environment",
        "verify_native_gpu_loader",
        "verify_runtime_compatibility",
        "verify_sif_environment",
        "verify_wheel_installation",
        "wheel_builder_digest",
        "write_container_payload",
    ),
    "artisan.schemas": (
        "AppendableArtifact",
        "Artifact",
        "ArtifactEmission",
        "ArtifactOccurrence",
        "ArtifactResult",
        "ArtifactStreamResult",
        "ArtifactTypeDef",
        "ArtifactTypes",
        "BatchStrategy",
        "CSVArtifact",
        "CachePolicy",
        "CuratorInput",
        "CuratorResult",
        "Environment",
        "ExecuteInput",
        "ExecutionConfigArtifact",
        "FailurePolicy",
        "FileRefArtifact",
        "FrozenInputContext",
        "GroupByStrategy",
        "InputReference",
        "InputSpec",
        "JsonContentMixin",
        "LineageMapping",
        "MetricArtifact",
        "OccurrenceAddress",
        "OutputSpec",
        "PassthroughResult",
        "PostprocessInput",
        "PreprocessInput",
        "ReferenceContext",
        "RequiredReference",
        "RunnerResources",
        "StorageConfig",
        "TablePath",
        "ToolSpec",
        "get_compound_extension",
    ),
    "artisan.storage": (
        "ArtifactStore",
        "PublicationExport",
        "export_publication",
    ),
    "artisan.provenance": (
        "ProvenanceEdges",
        "provenance_edges",
        "walk_backward",
        "walk_forward",
    ),
    "artisan.visualization": (
        "AssessmentCheck",
        "AssessmentCheckResult",
        "AssessmentCheckSummary",
        "AssessmentObservation",
        "AssessmentSpec",
        "ComparisonCoverage",
        "ComparisonDiagnostic",
        "ComparisonMetric",
        "ComparisonMetricResult",
        "ComparisonMetricSummary",
        "ComparisonPair",
        "ComparisonSide",
        "ComparisonSpec",
        "CountAssessment",
        "ExplicitPair",
        "ExplicitPairsMatch",
        "MetadataKeyMatch",
        "MetricEvidenceRef",
        "MetricMeasurement",
        "MetricObservation",
        "MetricObservationSet",
        "OutcomeAssessment",
        "OutcomeSelection",
        "PipelineTimings",
        "ResolvedOutcomeSelection",
        "RunComparison",
        "SelectedMetadata",
        "assess_outcomes",
        "build_macro_graph",
        "build_micro_graph",
        "compare_runs",
        "display_provenance_stepper",
        "get_max_step_number",
        "inspect_commands",
        "inspect_csv",
        "inspect_failures",
        "inspect_metrics",
        "inspect_pipeline",
        "inspect_step",
        "inspect_worker_log",
        "read_metric_observations",
        "render_macro_graph",
        "render_micro_graph",
        "render_micro_graph_steps",
    ),
    "artisan.registry": (
        "CapabilitiesPayload",
        "DiscoveryError",
        "DiscoveryReport",
        "DiscoverySource",
        "InputSpecMetadata",
        "NameCollision",
        "OperationExample",
        "OperationMetadata",
        "OperationSummary",
        "OutputSpecMetadata",
        "bootstrap_applications",
        "capabilities",
        "describe",
        "discover",
        "examples",
        "list_operations",
        "params_schema_for",
    ),
    "artisan.utils": (
        "TutorialEnv",
        "configure_logging",
        "env_or_dotenv",
        "find_project_root",
        "format_args",
        "run_command",
        "strip_extensions",
        "to_cli_value",
        "tutorial_setup",
    ),
    "artisan_mcp": (
        "__version__",
        "build_mcp_app",
    ),
    "artisan.execution": ("ExecutionSession",),
    "artisan.orchestration.runners": (
        "ApptainerRuntime",
        "DockerRuntime",
        "LocalRunner",
    ),
}

_SCHEMA_SUBPACKAGE_EXPORTS: dict[str, tuple[str, ...]] = {
    "artisan.schemas.artifact": (
        "AppendableArtifact",
        "Artifact",
        "ArtifactTypeDef",
        "ArtifactTypes",
        "CSVArtifact",
        "ExecutionConfigArtifact",
        "FileRefArtifact",
        "JsonContentMixin",
        "MetricArtifact",
        "get_compound_extension",
    ),
    "artisan.schemas.composites": (),
    "artisan.schemas.execution": (
        "ArtifactEmission",
        "ArtifactResult",
        "ArtifactStreamResult",
        "BatchStrategy",
        "CuratorResult",
        "PassthroughResult",
        "StorageConfig",
    ),
    "artisan.schemas.operation_config": (
        "RunnerResources",
        "ToolSpec",
    ),
    "artisan.schemas.orchestration": (),
    "artisan.schemas.provenance": ("InputReference", "LineageMapping"),
    "artisan.schemas.specs": (
        "CuratorInput",
        "ExecuteInput",
        "InputSpec",
        "OutputSpec",
        "PostprocessInput",
        "PreprocessInput",
    ),
}


@pytest.mark.parametrize(
    ("module_name", "expected"),
    [*_PUBLIC_EXPORTS.items(), *_SCHEMA_SUBPACKAGE_EXPORTS.items()],
)
def test_facade_exports_are_exact_and_importable(
    module_name: str, expected: tuple[str, ...]
) -> None:
    module = importlib.import_module(module_name)

    assert tuple(module.__all__) == expected
    for name in expected:
        assert getattr(module, name) is not None


@pytest.mark.parametrize(
    ("module_name", "removed"),
    [
        ("artisan", ("PerArtifact",)),
        (
            "artisan.schemas",
            (
                "ArtifactProvenanceEdge",
                "BatchConfig",
                "CacheHit",
                "CacheMiss",
                "CancellationAcknowledgement",
                "CancellationStatus",
                "DataArtifact",
                "ExecutionContext",
                "ExecutionRecord",
                "OutputReference",
                "PipelineConfig",
                "StepDisposition",
                "StepResult",
                "StepStatus",
            ),
        ),
        ("artisan.schemas.artifact", ("ArtifactProvenanceEdge", "DataArtifact")),
        ("artisan.schemas.artifact.csv", ("DataArtifact", "DataTypeDef")),
        ("artisan.operations.curator", ("IngestData",)),
        ("artisan.visualization", ("inspect_data",)),
        ("artisan.visualization.inspect", ("inspect_data",)),
        (
            "artisan.schemas.execution",
            (
                "CacheHit",
                "CacheMiss",
                "ExecutionContext",
                "ExecutionRecord",
                "RuntimeEnvironment",
                "UnitResult",
            ),
        ),
        (
            "artisan.schemas.operation_config",
            ("ComputeConfig", "LocalComputeConfig"),
        ),
    ],
)
def test_removed_facade_names_are_absent(
    module_name: str, removed: tuple[str, ...]
) -> None:
    module = importlib.import_module(module_name)

    for name in removed:
        assert name not in vars(module)
        with pytest.raises(AttributeError):
            getattr(module, name)


def test_execution_record_module_was_deleted() -> None:
    with pytest.raises(ModuleNotFoundError):
        importlib.import_module("artisan.schemas.execution.execution_record")


@pytest.mark.parametrize(
    "module_name",
    ["artisan.schemas.artifact.data", "artisan.operations.curator.ingest_data"],
)
def test_retired_csv_modules_are_absent(module_name: str) -> None:
    with pytest.raises(ModuleNotFoundError) as error:
        importlib.import_module(module_name)
    assert error.value.name == module_name


def test_composite_ref_has_only_the_supported_fields() -> None:
    assert tuple(field.name for field in fields(CompositeRef)) == (
        "owner",
        "call",
        "role",
    )


@pytest.mark.parametrize(
    "first",
    [
        "artisan.storage.core.store_format",
        "artisan.execution.curator_worker",
        "artisan.execution.inert_worker",
        "artisan.execution",
        "artisan.composites",
        "artisan.orchestration.runner_api",
        "artisan.visualization.inspect",
        "artisan.storage.publication",
    ],
)
def test_public_facades_import_in_fresh_worker_and_storage_orders(first: str) -> None:
    program = (
        f"import {first}; "
        "from artisan.execution import ExecutionSession; "
        "from artisan.execution.session import ExecutionSession as ActualSession; "
        "from artisan.orchestration import PipelineManager, replay_execution; "
        "from artisan.orchestration.replay import replay_execution as ActualReplay; "
        "assert replay_execution is ActualReplay; "
        "from artisan.orchestration.pipeline_manager import PipelineManager as ActualManager; "
        "from artisan.orchestration.runners import LocalRunner, ApptainerRuntime, DockerRuntime; "
        "from artisan.execution.environments.container_runtime import "
        "ApptainerRuntime as ActualApptainer, DockerRuntime as ActualDocker; "
        "from artisan.orchestration.runners.local import LocalRunner as ActualRunner; "
        "from artisan.storage import ArtifactStore, PublicationExport, export_publication; "
        "from artisan.storage.core.artifact_store import ArtifactStore as ActualStore; "
        "from artisan.storage.publication import PublicationExport as ActualExport, "
        "export_publication as actual_export; "
        "assert ExecutionSession is ActualSession; "
        "assert PipelineManager is ActualManager; "
        "assert LocalRunner is ActualRunner; "
        "assert ApptainerRuntime is ActualApptainer; "
        "assert DockerRuntime is ActualDocker; "
        "assert ArtifactStore is ActualStore; "
        "assert PublicationExport is ActualExport; "
        "assert export_publication is actual_export"
    )
    subprocess.run([sys.executable, "-c", program], check=True, timeout=20)


@pytest.mark.parametrize(
    "module_name",
    [
        "artisan.orchestration.engine.retained_pipeline",
        "artisan.orchestration.runners.prepared_local",
        "artisan.composites.base.results",
        "artisan.orchestration.runners.base",
        "artisan.orchestration.engine.step_executor",
        "artisan.execution.tool_endpoint",
        "artisan.schemas.operation_config.compute",
        "artisan.schemas.operation_config.environment_spec",
        "artisan.schemas.operation_config.execute_environment",
    ],
)
def test_replaced_execution_modules_are_absent(module_name: str) -> None:
    with pytest.raises(ModuleNotFoundError) as error:
        importlib.import_module(module_name)
    assert error.value.name == module_name


@pytest.mark.parametrize(
    ("module_name", "names"),
    [
        ("artisan.composites", ("CompositeResult", "CompositeStepHandle")),
        (
            "artisan.composites.base.compiler",
            ("PlanContext", "PlanCall", "PlanReference"),
        ),
        ("artisan.orchestration", ("Runner", "RunnerBase")),
        ("artisan.orchestration.runners", ("Runner", "RunnerBase", "resolve_runner")),
        (
            "artisan.orchestration.runner_api",
            ("RunnerBase", "LifecycleRouter", "ExecutionUnit", "UnitResult"),
        ),
    ],
)
def test_removed_execution_contracts_have_no_compatibility_exports(
    module_name: str, names: tuple[str, ...]
) -> None:
    module = importlib.import_module(module_name)
    for name in names:
        assert not hasattr(module, name)
