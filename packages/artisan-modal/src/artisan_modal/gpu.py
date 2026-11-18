"""Fresh allocation-free GPU evidence from the already deployed worker."""

from __future__ import annotations

import hashlib
from typing import Literal, Self
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, model_validator

from artisan.orchestration.runner_api import (
    CommandOwnershipError,
    NativeGPUAllocation,
    NativeGPUSoftware,
    RuntimeCompatibility,
    capture_native_gpu,
    verify_native_gpu_loader,
    verify_runtime_compatibility,
)
from artisan_modal.bindings import ModalFunctionProfile, ModalRuntimePayload
from artisan_modal.diagnostics import redacted_message
from artisan_modal.runtime import verify_modal_function, verify_modal_runtime
from artisan_modal.worker import verify_worker_activation


class GPUProbe(BaseModel):
    """Fixed measurement request; no scientific callable, pickle, or storage access."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    domain: Literal["artisan.modal-gpu-probe"] = "artisan.modal-gpu-probe"
    nonce: UUID
    function: ModalFunctionProfile
    runtime: ModalRuntimePayload
    compatibility: RuntimeCompatibility

    @model_validator(mode="after")
    def _requested_gpu(self) -> Self:
        if not self.function.gpu_count:
            msg = "GPU measurement requires a positive allocated count"
            raise ValueError(msg)
        return self

    @property
    def digest(self) -> str:
        return hashlib.sha256(self.model_dump_json().encode()).hexdigest()


class GPUProbeReply(BaseModel):
    """Exact completed measurement, never a seal or acceptance authority."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    nonce: UUID
    request_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    entry_id: UUID
    software: NativeGPUSoftware | None = None
    error: str | None = Field(default=None, min_length=1, max_length=512)

    @model_validator(mode="after")
    def _outcome(self) -> Self:
        if (self.software is None) == (self.error is None):
            msg = "GPU measurement requires exactly one profile or failure"
            raise ValueError(msg)
        return self

    def verify(self, request: GPUProbe) -> None:
        if self.nonce != request.nonce or self.request_sha256 != request.digest:
            msg = "GPU measurement does not match its fresh request"
            raise ValueError(msg)
        if (
            self.software is not None
            and self.software.device_count != request.function.gpu_count
        ):
            msg = "GPU measurement count differs from the assigned resources"
            raise ValueError(msg)


def capture_worker_gpu(
    count: int, expected: NativeGPUSoftware | None = None
) -> NativeGPUAllocation | None:
    """Measure the actual visible allocation and loader before authored entry."""
    allocation = capture_native_gpu(count)
    if allocation is None:
        if expected is not None:
            msg = "GPU software was supplied for a CPU-only worker"
            raise ValueError(msg)
        return None
    if expected is not None and allocation.software_profile() != expected:
        msg = "Modal GPU software changed after current qualification"
        raise ValueError(msg)
    verify_native_gpu_loader(allocation)
    return allocation


def probe_gpu(request: GPUProbe) -> GPUProbeReply:
    """Finish bounded core probes before returning a closed primitive observation.

    Base exceptions propagate to the SDK, leaving controller ownership unknown.
    Ordinary probe failures are reported only after their synchronous subprocess
    contexts have returned; none of this path executes an authored callback.
    """
    software = None
    error = None
    try:
        verify_worker_activation()
        verify_modal_runtime(request.runtime)
        verify_modal_function(request.function)
        verify_runtime_compatibility(request.compatibility)
        allocation = capture_worker_gpu(request.function.gpu_count)
        assert allocation is not None
        software = allocation.software_profile()
    except CommandOwnershipError:
        raise
    except Exception as failure:
        try:
            error = redacted_message(f"{type(failure).__name__}: {failure}", ())[:512]
        except Exception:
            error = "GPU measurement failed; diagnostic unavailable"
    return GPUProbeReply(
        nonce=request.nonce,
        request_sha256=request.digest,
        entry_id=uuid4(),
        software=software,
        error=error,
    )
