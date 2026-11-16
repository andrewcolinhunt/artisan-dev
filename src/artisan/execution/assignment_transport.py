"""Trusted compiled calls and persisted admission locators crossing worker transport."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path

from artisan.composites.base.compiled import CompiledPlan
from artisan.execution.compute.prepared import freeze_runtime_injections
from artisan.schemas.execution.admission import AdmissionReference
from artisan.schemas.execution.assignment import AssignmentAttempt
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.utils.application_modules import validate_application_modules


@dataclass(frozen=True)
class AdmittedAssignment:
    """Trusted imported code paired with exact persisted input admission.

    A provider verifies and localizes that admission before worker science. Local
    artifact paths from temporary controller captures are never the transport API.
    """

    plan: CompiledPlan
    admission: AdmissionReference

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "admission", AdmissionReference.model_validate(self.admission)
        )
        if self.plan.recipe.digest != self.admission.attempt.recipe_digest:
            msg = "Assignment code differs from the admitted recipe"
            raise ValueError(msg)

    @property
    def attempt(self) -> AssignmentAttempt:
        """Return the exact persisted attempt without constructing scientific inputs."""
        return self.admission.attempt


@dataclass(frozen=True)
class AssignmentDispatch:
    """Explicit transport storage and worker-local scratch, separate from software."""

    delta_root: str
    files_root: str
    staging_root: str
    storage: StorageConfig
    working_root: Path
    runtime_env: Mapping[str, str] = field(default_factory=dict, repr=False)
    application_modules: tuple[str, ...] = ()
    preserve_working: bool = False
    preserve_staging: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "application_modules",
            validate_application_modules(self.application_modules),
        )
        if not self.working_root.is_absolute():
            msg = "Worker scratch must be an explicit absolute path"
            raise ValueError(msg)
        object.__setattr__(
            self, "runtime_env", freeze_runtime_injections(self.runtime_env)
        )
