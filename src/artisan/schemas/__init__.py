"""Artisan schema exports.

This module provides the stable import surface for artisan framework schemas.
"""

from __future__ import annotations

from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.data import DataArtifact
from artisan.schemas.artifact.execution_config import ExecutionConfigArtifact
from artisan.schemas.artifact.file_ref import FileRefArtifact
from artisan.schemas.artifact.metric import MetricArtifact
from artisan.schemas.artifact.provenance import ArtifactProvenanceEdge
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.schemas.enums import (
    CacheValidationReason,
    GroupByStrategy,
)
from artisan.schemas.execution.cache_result import CacheHit, CacheMiss
from artisan.schemas.execution.curator_result import (
    ArtifactResult,
    CuratorResult,
    PassthroughResult,
)
from artisan.schemas.execution.execution_config import ExecutionConfig
from artisan.schemas.execution.execution_context import ExecutionContext
from artisan.schemas.execution.execution_record import ExecutionRecord
from artisan.schemas.execution.runtime_environment import RuntimeEnvironment
from artisan.schemas.operation_config.environment_spec import (
    ApptainerEnvironmentSpec,
    DockerEnvironmentSpec,
    EnvironmentSpec,
    LocalEnvironmentSpec,
    PixiEnvironmentSpec,
)
from artisan.schemas.operation_config.environments import Environments
from artisan.schemas.operation_config.resource_config import ResourceConfig
from artisan.schemas.operation_config.tool_spec import ToolSpec
from artisan.schemas.orchestration.batch_config import BatchConfig
from artisan.schemas.orchestration.output_reference import OutputReference
from artisan.schemas.orchestration.pipeline_config import PipelineConfig
from artisan.schemas.orchestration.step_result import StepResult, StepResultBuilder
from artisan.schemas.provenance.execution_edge import ExecutionEdge
from artisan.schemas.provenance.lineage_mapping import LineageMapping
from artisan.schemas.provenance.source_target_pair import SourceTargetPair
from artisan.schemas.specs.input_models import (
    ExecuteInput,
    PostprocessInput,
    PreprocessInput,
)
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec

__all__ = [
    # Enums / Types
    "ArtifactTypes",
    "CacheValidationReason",
    "GroupByStrategy",
    # Artifacts
    "Artifact",
    "DataArtifact",
    "MetricArtifact",
    "ExecutionConfigArtifact",
    "FileRefArtifact",
    # Execution
    "ExecutionContext",
    "ExecutionRecord",
    "CacheHit",
    "CacheMiss",
    # Provenance Edges
    "SourceTargetPair",
    "ArtifactProvenanceEdge",
    "ExecutionEdge",
    # Specs
    "InputSpec",
    "OutputSpec",
    # Input models
    "PreprocessInput",
    "ExecuteInput",
    "PostprocessInput",
    # Operations
    "RuntimeEnvironment",
    # Lineage
    "LineageMapping",
    # Curator result types
    "ArtifactResult",
    "PassthroughResult",
    "CuratorResult",
    # Config types
    "ResourceConfig",
    "ExecutionConfig",
    "ToolSpec",
    "EnvironmentSpec",
    "LocalEnvironmentSpec",
    "DockerEnvironmentSpec",
    "ApptainerEnvironmentSpec",
    "PixiEnvironmentSpec",
    "Environments",
    "OutputReference",
    "PipelineConfig",
    "BatchConfig",
    "StepResult",
    "StepResultBuilder",
]
