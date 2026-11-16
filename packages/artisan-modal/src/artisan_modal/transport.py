"""Closed bytes-only exchange into one qualified SDK-owned worker invocation."""

from __future__ import annotations

from pathlib import Path
from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from artisan.orchestration.runner_api import (
    InertCallControl,
    InertCallResult,
    InertWorkerCall,
    NativeGPUSoftware,
    WorkerTransportHeader,
    execute_inert_call,
    verify_runtime_compatibility,
)
from artisan_modal.bindings import ModalFunctionRuntime, ModalRuntimePayload
from artisan_modal.gpu import capture_worker_gpu
from artisan_modal.runtime import verify_modal_function, verify_modal_runtime
from artisan_modal.worker import verify_worker_activation

WORKING_ROOT = Path("/tmp/artisan-modal-calls")


class ModalCall(BaseModel):
    """Inert provider preflight plus the exact shared live-call envelope."""

    model_config = ConfigDict(
        extra="forbid", frozen=True, ser_json_bytes="base64", val_json_bytes="base64"
    )

    domain: Literal["artisan.modal-call"] = "artisan.modal-call"
    schema_version: Literal[1] = 1
    function: ModalFunctionRuntime
    runtime: ModalRuntimePayload
    gpu: NativeGPUSoftware | None = None
    header: WorkerTransportHeader
    payload: bytes = Field(min_length=1)
    reply_context: bytes = Field(min_length=1)
    preserve_working: bool = Field(default=False, strict=True)

    @field_validator("schema_version", mode="before")
    @classmethod
    def _version(cls, value: object) -> object:
        if type(value) is not int:
            message = "Modal call schema version must be an integer"
            raise ValueError(message)
        return value

    @model_validator(mode="after")
    def _exact_call(self) -> Self:
        self.inert()
        if bool(self.function.gpu_count) != (self.gpu is not None) or (
            self.gpu is not None and self.gpu.device_count != self.function.gpu_count
        ):
            message = "Modal call GPU profile differs from its assigned resources"
            raise ValueError(message)
        return self

    def inert(self) -> InertWorkerCall:
        """Validate exact reply domain and payload hash before any live decoding."""
        return InertWorkerCall(self.header, self.payload, self.reply_context)


def execute_call(request: ModalCall, control: InertCallControl) -> InertCallResult:
    """Verify actual activation/platform before each core-owned live entry."""
    request.inert()
    verify_worker_activation()
    verify_modal_runtime(request.runtime)
    verify_modal_function(request.function)
    if request.gpu is not None:
        verify_runtime_compatibility(request.header.compatibility)
        capture_worker_gpu(request.function.gpu_count, request.gpu)
    return execute_inert_call(
        request.inert(),
        working_root=WORKING_ROOT,
        preserve_working=request.preserve_working,
        control=control,
    )
