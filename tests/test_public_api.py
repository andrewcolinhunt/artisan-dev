"""Exact publication contract for the supported Artisan facades."""

from __future__ import annotations

import importlib
from dataclasses import fields

import pytest

from artisan.composites import CompositeRef

_PUBLIC_EXPORTS: dict[str, tuple[str, ...]] = {
    "artisan": ("__version__", "__version_tuple__"),
    "artisan.operations.base": ("OperationDefinition", "PerArtifact"),
    "artisan.operations.curator": (
        "ConsolidateAppendables",
        "DeclareLineage",
        "Filter",
        "IngestData",
        "IngestFiles",
        "IngestPipelineStep",
        "InteractiveFilter",
        "Merge",
    ),
    "artisan.operations.examples": (
        "AppendableGenerator",
        "CsvHead",
        "DataGenerator",
        "DataGeneratorWithMetrics",
        "DataTransformer",
        "DataTransformerConfig",
        "DataTransformerScript",
        "LargeFileGenerator",
        "MetricCalculator",
        "SequentialDataTransformer",
        "SequentialSlowTransformer",
        "SlowTransformer",
        "StreamingEcho",
        "Wait",
        "WaitTool",
    ),
    "artisan.composites": (
        "CompositeContext",
        "CompositeDefinition",
        "CompositeRef",
        "CompositeResult",
        "CompositeStepHandle",
    ),
    "artisan.orchestration": (
        "CancellationStatus",
        "OutputReference",
        "PipelineConfig",
        "PipelineManager",
        "Runner",
        "RunnerBase",
        "StepDisposition",
        "StepFuture",
        "StepResult",
        "StepStatus",
        "list_runs",
    ),
    "artisan.orchestration.runner_api": (
        "BatchStrategy",
        "CancellationAcknowledgement",
        "CancellationStatus",
        "ExecutionUnit",
        "LifecycleRouter",
        "OrchestratorTraits",
        "RunnerBase",
        "RunnerResources",
        "RuntimeEnvironment",
        "UnitResult",
        "WorkerTraits",
        "execute_unit",
        "execute_unit_batch",
        "failure_results_for_units",
        "pack_units",
        "validate_batch_results",
    ),
    "artisan.schemas": (
        "AppendableArtifact",
        "ApptainerEnvironmentSpec",
        "Artifact",
        "ArtifactResult",
        "ArtifactTypeDef",
        "ArtifactTypes",
        "BatchStrategy",
        "CachePolicy",
        "ComputeProvider",
        "ComputeResources",
        "CuratorResult",
        "DataArtifact",
        "DockerEnvironmentSpec",
        "EnvironmentSpec",
        "Environments",
        "ExecuteInput",
        "ExecutionConfigArtifact",
        "FailurePolicy",
        "FileRefArtifact",
        "GroupByStrategy",
        "InputSpec",
        "JsonContentMixin",
        "LargeFileArtifact",
        "LineageMapping",
        "LocalEnvironmentSpec",
        "MetricArtifact",
        "ModalComputeConfig",
        "OutputSpec",
        "PassthroughResult",
        "PixiEnvironmentSpec",
        "PostprocessInput",
        "PreprocessInput",
        "RunnerResources",
        "StorageConfig",
        "TablePath",
        "ToolEndpointDataPolicy",
        "ToolSpec",
        "get_compound_extension",
    ),
    "artisan.storage": ("ArtifactStore",),
    "artisan.provenance": (
        "ProvenanceEdges",
        "provenance_edges",
        "walk_backward",
        "walk_forward",
    ),
    "artisan.visualization": (
        "PipelineTimings",
        "build_macro_graph",
        "build_micro_graph",
        "display_provenance_stepper",
        "get_max_step_number",
        "inspect_commands",
        "inspect_data",
        "inspect_failures",
        "inspect_metrics",
        "inspect_pipeline",
        "inspect_step",
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
    "artisan_mcp": ("__version__", "build_mcp_app"),
}

_SCHEMA_SUBPACKAGE_EXPORTS: dict[str, tuple[str, ...]] = {
    "artisan.schemas.artifact": (
        "AppendableArtifact",
        "Artifact",
        "ArtifactTypeDef",
        "ArtifactTypes",
        "DataArtifact",
        "ExecutionConfigArtifact",
        "FileRefArtifact",
        "JsonContentMixin",
        "LargeFileArtifact",
        "MetricArtifact",
        "get_compound_extension",
    ),
    "artisan.schemas.composites": (),
    "artisan.schemas.execution": (
        "ArtifactResult",
        "BatchStrategy",
        "CuratorResult",
        "PassthroughResult",
        "StorageConfig",
    ),
    "artisan.schemas.operation_config": (
        "ApptainerEnvironmentSpec",
        "ComputeProvider",
        "ComputeResources",
        "DockerEnvironmentSpec",
        "EnvironmentSpec",
        "Environments",
        "LocalEnvironmentSpec",
        "ModalComputeConfig",
        "PixiEnvironmentSpec",
        "RunnerResources",
        "ToolEndpointDataPolicy",
        "ToolSpec",
    ),
    "artisan.schemas.orchestration": (),
    "artisan.schemas.provenance": ("LineageMapping",),
    "artisan.schemas.specs": (
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
                "ExecutionContext",
                "ExecutionRecord",
                "OutputReference",
                "PipelineConfig",
                "StepDisposition",
                "StepResult",
                "StepStatus",
            ),
        ),
        ("artisan.schemas.artifact", ("ArtifactProvenanceEdge",)),
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
            ("ARTISAN_WORKER_IMAGE", "ComputeConfig", "LocalComputeConfig"),
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


def test_composite_ref_has_only_the_supported_fields() -> None:
    assert tuple(field.name for field in fields(CompositeRef)) == (
        "output_reference",
        "role",
    )
