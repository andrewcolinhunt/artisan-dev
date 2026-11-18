"""Full admitted Modal software, separate from the SDK's input ownership handle."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Literal

from artisan.orchestration.runner_api import (
    Assignment,
    CompiledPlan,
    ExecuteAdapter,
    ExternalAccessMount,
    NativeGPUSoftware,
    PreparedExternalFiles,
    PreparedLocalBindings,
    PreparedNativeEnvironment,
    RuntimeCompatibility,
    select_runtime_compatibility,
)
from artisan_modal.bindings import PIXI, ModalFunctionProfile, PreparedModalImage
from artisan_modal.gpu import capture_worker_gpu
from artisan_modal.runtime import verify_modal_function, verify_modal_runtime
from artisan_modal.volumes import validate_external_roots


@dataclass(frozen=True)
class ModalSoftware:
    """Bind canonical prefixes, actual SDK/image and exact function resources."""

    image: PreparedModalImage
    local: PreparedLocalBindings
    function: ModalFunctionProfile
    gpu: NativeGPUSoftware | None = None

    def __post_init__(self) -> None:
        validate_external_roots(
            self.local.external_files,
            tuple(mount.mount_path for mount in self.function.mount_inventory()),
        )
        if self.function.image_id != self.image.image_id:
            message = "Modal function and prepared image identities differ"
            raise ValueError(message)
        if (
            self.local.worker
            != self.image.startup.worker.model_copy(
                update={"compatibility": self.local.worker.compatibility}
            )
            or self.local.launcher != self.image.startup.launcher
            or self.local.pixi != PIXI
            or self.local.system_env != Path("/usr/bin/env")
            or self.local.container_runtime is not None
        ):
            message = "Modal software differs from its measured native image layout"
            raise ValueError(message)
        if (
            self.local.native_gpu is not None
            or bool(self.function.gpu_count) != (self.gpu is not None)
            or (
                self.gpu is not None
                and self.gpu.device_count != self.function.gpu_count
            )
        ):
            message = (
                "Modal GPU worker software requires a qualified allocation profile"
            )
            raise ValueError(message)
        available = self.image.startup.worker.compatibility
        assert available is not None
        if self.local.compatibility != select_runtime_compatibility(
            available,
            tuple(package.name for package in self.local.compatibility.packages),
        ):
            message = "Modal worker compatibility differs from its prepared image"
            raise ValueError(message)
        targets = {
            target.model_copy(update={"compatibility": None})
            for target in self.image.startup.targets
        }
        if any(
            not isinstance(tool.target, PreparedNativeEnvironment)
            or tool.target.model_copy(update={"compatibility": None}) not in targets
            for tool in self.local.tools
        ):
            message = "Modal tool selection is outside the prepared image"
            raise ValueError(message)
        witness = self.image.startup.layout.witness if self.local.tools else None
        if self.local.command_witness != witness:
            message = "Modal command witness differs from its measured image input"
            raise ValueError(message)

    @property
    def external_files(self) -> Mapping[str, PreparedExternalFiles]:
        """Expose configured metadata without reading remote file payloads."""
        return self.local.external_files

    @property
    def external_provider(self) -> Literal["modal"]:
        """Identify the provider owning this mount evidence."""
        return "modal"

    @property
    def external_mounts(self) -> tuple[ExternalAccessMount, ...]:
        """Return the complete attested endpoint mount scope."""
        return tuple(item.access_mount() for item in self.function.mount_inventory())

    @property
    def software_cacheable(self) -> bool:
        return self.local.software_cacheable

    @property
    def compatibility(self) -> RuntimeCompatibility:
        return self.local.compatibility

    @property
    def application_modules(self) -> tuple[str, ...]:
        return self.local.application_modules

    @property
    def software_digest(self) -> str:
        descriptor = {
            "image": self.image.digest,
            "selection": self.local.software_digest,
            "function": self.function.scientific_profile(),
        }
        if self.gpu is not None:
            descriptor["gpu"] = self.gpu.model_dump(mode="json")
        encoded = json.dumps(descriptor, sort_keys=True, separators=(",", ":")).encode()
        return hashlib.sha256(b"artisan.modal-software-v2\0" + encoded).hexdigest()

    def _verify_provider(self) -> None:
        verify_modal_runtime(self.image.startup.runtime)
        verify_modal_function(self.function)
        capture_worker_gpu(self.function.gpu_count, self.gpu)

    def verify(self, plan: CompiledPlan) -> None:
        """Check prepared targets and the actual admitted worker's compatibility."""
        self._verify_provider()
        self.local.verify(plan)

    def verify_worker(self) -> None:
        """Verify a curator worker without inventing a scientific plan."""
        self._verify_provider()
        self.local.verify_worker()

    def create_adapter(
        self,
        assignment: Assignment,
        *,
        working_root: Path,
        runtime_env: Mapping[str, str],
    ) -> ExecuteAdapter:
        local = replace(
            self.local, native_gpu=capture_worker_gpu(self.function.gpu_count, self.gpu)
        )
        return local.create_adapter(
            assignment, working_root=working_root, runtime_env=runtime_env
        )
