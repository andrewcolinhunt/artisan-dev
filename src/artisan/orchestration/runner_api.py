"""Supported software, admission, transport, and ownership contracts for providers."""

from __future__ import annotations

from artisan.composites.base.compiled import CompiledPlan
from artisan.composites.base.compiler import Definition, Parameters, compile_definition
from artisan.execution.admitted_worker import execute_admitted_assignment
from artisan.execution.assignment import Assignment, PreparedAssignmentSoftware
from artisan.execution.assignment_reply import AssignmentReply, AssignmentReplyContext
from artisan.execution.assignment_transport import (
    AdmittedAssignment,
    AssignmentDispatch,
)
from artisan.execution.composition import ExecuteAdapter
from artisan.execution.curator_definition import capture_curator_definition
from artisan.execution.curator_reply import CuratorReply, CuratorReplyContext
from artisan.execution.curator_worker import (
    AdmittedCurator,
    PreparedCuratorSoftware,
    execute_admitted_curator,
)
from artisan.execution.environments.command_witness import (
    prepare_command_witness,
    verify_command_witness,
)
from artisan.execution.environments.container_runtime import ApptainerRuntime
from artisan.execution.environments.container_source import (
    ContainerBuildInputs,
    capture_container_inputs,
    verify_container_source,
    write_container_payload,
)
from artisan.execution.environments.native import wheel_builder_digest
from artisan.execution.environments.native_gpu import (
    capture_native_gpu,
    verify_native_gpu_loader,
)
from artisan.execution.environments.native_target import verify_native_environment
from artisan.execution.environments.prepared_sif import verify_sif_environment
from artisan.execution.environments.requirements import select_tool_environments
from artisan.execution.environments.wheels import verify_wheel_installation
from artisan.execution.environments.worker_qualification import (
    controller_definition_sources,
    controller_worker_sources,
)
from artisan.execution.inert_worker import (
    InertCallControl,
    InertCallResult,
    execute_inert_call,
)
from artisan.execution.local_assignment import (
    PreparedLocalBindings,
    PreparedToolBinding,
)
from artisan.execution.local_call import InertWorkerCall
from artisan.execution.local_packing import LocalPackStartupError, launch_inert_calls
from artisan.execution.local_profile import LocalLauncherIdentity, LocalWorkerProfile
from artisan.execution.recording.commands import CommandRecorder
from artisan.execution.recording.parquet_writer import StagingResult
from artisan.execution.runtime_compatibility import (
    capture_runtime_compatibility,
    select_runtime_compatibility,
    verify_runtime_compatibility,
)
from artisan.execution.worker_bootstrap import serialize_worker_call
from artisan.execution.worker_requirements import (
    observed_controller_sources,
    observed_definition_sources,
)
from artisan.operations.base.operation_definition import OperationDefinition
from artisan.orchestration.runners.assignment import (
    AssignmentHandle,
    AssignmentLaunchError,
    AssignmentSoftware,
)
from artisan.orchestration.runners.curator import CuratorHandle, CuratorLaunchError
from artisan.orchestration.runners.local import LocalRunner
from artisan.schemas.execution.assignment import AssignmentAttempt, SealedDelivery
from artisan.schemas.execution.command_witness import PreparedCommandWitness
from artisan.schemas.execution.container_environment import (
    ContainerLauncherIdentity,
    PreparedSIFEnvironment,
)
from artisan.schemas.execution.curator_admission import CuratorAdmissionReference
from artisan.schemas.execution.gpu import NativeGPUAllocation, NativeGPUSoftware
from artisan.schemas.execution.prepared_environment import (
    DerivedWheel,
    EnvironmentSource,
    PreparedNativeEnvironment,
)
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.schemas.execution.worker_ownership import OwnershipState, WorkerOwner
from artisan.schemas.execution.worker_transport import (
    RuntimeCompatibility,
    WorkerTransportHeader,
)
from artisan.schemas.operation_config.environment import Environment
from artisan.schemas.operation_config.runner_resources import (
    RunnerResources,
    merge_runner_resources,
)
from artisan.schemas.orchestration.step_lifecycle import (
    CancellationAcknowledgement,
    CancellationStatus,
)
from artisan.utils.command_supervisor import CommandOwnershipError

__all__ = [
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
    "merge_runner_resources",
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
]
