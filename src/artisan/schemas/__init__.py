"""Operation-author schema exports."""

from __future__ import annotations

from artisan.schemas.artifact.appendable import AppendableArtifact
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.common import JsonContentMixin, get_compound_extension
from artisan.schemas.artifact.data import DataArtifact
from artisan.schemas.artifact.execution_config import ExecutionConfigArtifact
from artisan.schemas.artifact.file_ref import FileRefArtifact
from artisan.schemas.artifact.large_file import LargeFileArtifact
from artisan.schemas.artifact.metric import MetricArtifact
from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.schemas.enums import CachePolicy, FailurePolicy, GroupByStrategy, TablePath
from artisan.schemas.execution.batch_strategy import BatchStrategy
from artisan.schemas.execution.curator_result import (
    ArtifactResult,
    CuratorResult,
    PassthroughResult,
)
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.schemas.operation_config.compute import (
    ComputeProvider,
    ModalComputeConfig,
)
from artisan.schemas.operation_config.compute_resources import ComputeResources
from artisan.schemas.operation_config.endpoint_policy import ToolEndpointDataPolicy
from artisan.schemas.operation_config.environment_spec import (
    ApptainerEnvironmentSpec,
    DockerEnvironmentSpec,
    EnvironmentSpec,
    LocalEnvironmentSpec,
    PixiEnvironmentSpec,
)
from artisan.schemas.operation_config.environments import Environments
from artisan.schemas.operation_config.runner_resources import RunnerResources
from artisan.schemas.operation_config.tool_spec import ToolSpec
from artisan.schemas.provenance.lineage_mapping import LineageMapping
from artisan.schemas.specs.input_models import (
    ExecuteInput,
    PostprocessInput,
    PreprocessInput,
)
from artisan.schemas.specs.input_spec import InputSpec
from artisan.schemas.specs.output_spec import OutputSpec

__all__ = [
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
]
