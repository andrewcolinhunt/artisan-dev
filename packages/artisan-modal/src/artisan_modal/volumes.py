"""Explicit existing-volume declarations and immutable endpoint mount evidence."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from artisan.orchestration.runner_api import ExternalAccessMount, PreparedExternalFiles

MOUNT_ROOT = Path("/mnt/artisan-external")


class ModalVolumeBinding(BaseModel):
    """Select an existing named volume in an explicit Modal environment.

    Mounts live under ``/mnt/artisan-external`` so they cannot replace software,
    runtime files or worker scratch. Construction performs no service access.
    """

    model_config = ConfigDict(
        extra="forbid", frozen=True, revalidate_instances="always"
    )

    name: str = Field(min_length=1)
    environment_name: str = Field(min_length=1)
    mount_path: Path

    @field_validator("mount_path")
    @classmethod
    def _mount_path(cls, value: Path) -> Path:
        return validate_mount_path(value)


class ModalVolumeMount(BaseModel):
    """Observed read-only mount fields emitted by the pinned Modal SDK."""

    model_config = ConfigDict(
        extra="forbid", frozen=True, revalidate_instances="always"
    )

    volume_id: str = Field(pattern=r"^vo-[A-Za-z0-9]+$")
    mount_path: Path
    read_only: Literal[True] = True
    allow_background_commits: Literal[True] = True

    @field_validator("mount_path")
    @classmethod
    def _mount_path(cls, value: Path) -> Path:
        return validate_mount_path(value)

    @field_validator("read_only", "allow_background_commits", mode="before")
    @classmethod
    def _strict_flags(cls, value: object) -> object:
        if value is not True:
            message = "Modal volume flags must match the read-only SDK mount"
            raise ValueError(message)
        return value

    def access_mount(self) -> ExternalAccessMount:
        """Expose generic mount identity without provider-specific dependencies."""
        return ExternalAccessMount(source_id=self.volume_id, target=self.mount_path)


def validate_mount_path(path: Path) -> Path:
    """Require an absolute consumer mount outside software and scratch roots."""
    if (
        not path.is_absolute()
        or not path.is_relative_to(MOUNT_ROOT)
        or path == MOUNT_ROOT
        or ".." in path.parts
        or any(character in str(path) for character in "\x00\n,:")
    ):
        message = f"Modal volume mounts must be beneath {MOUNT_ROOT}"
        raise ValueError(message)
    return path


def validate_volume_bindings(
    bindings: Sequence[ModalVolumeBinding],
) -> tuple[ModalVolumeBinding, ...]:
    """Freeze declarations and reject duplicate names or overlapping targets."""
    frozen = tuple(ModalVolumeBinding.model_validate(item) for item in bindings)
    _validate_mount_targets(tuple(item.mount_path for item in frozen))
    names = {(item.environment_name, item.name) for item in frozen}
    if len(names) != len(frozen):
        message = "A Modal volume must have exactly one declared mount"
        raise ValueError(message)
    return tuple(sorted(frozen, key=lambda item: str(item.mount_path)))


def validate_volume_mounts(
    mounts: Sequence[ModalVolumeMount],
) -> tuple[ModalVolumeMount, ...]:
    """Validate exact resolved source and target uniqueness."""
    frozen = tuple(ModalVolumeMount.model_validate(item) for item in mounts)
    _validate_mount_targets(tuple(item.mount_path for item in frozen))
    if len({item.volume_id for item in frozen}) != len(frozen):
        message = "A Modal volume must have exactly one resolved mount"
        raise ValueError(message)
    return tuple(sorted(frozen, key=lambda item: str(item.mount_path)))


def _validate_mount_targets(paths: tuple[Path, ...]) -> None:
    for index, path in enumerate(paths):
        if any(
            path.is_relative_to(other) or other.is_relative_to(path)
            for other in paths[:index]
        ):
            message = "Modal volume mount targets must not overlap"
            raise ValueError(message)


def resolve_volume_mounts(
    bindings: Sequence[ModalVolumeBinding],
) -> tuple[ModalVolumeMount, ...]:
    """Resolve explicitly named existing volumes without creating or populating."""
    if not bindings:
        return ()
    import modal

    mounts = []
    for binding in validate_volume_bindings(bindings):
        volume = modal.Volume.from_name(
            binding.name,
            environment_name=binding.environment_name,
            create_if_missing=False,
        ).hydrate()
        mounts.append(
            ModalVolumeMount(volume_id=volume.object_id, mount_path=binding.mount_path)
        )
    return validate_volume_mounts(mounts)


def validate_external_roots(
    external_files: Mapping[str, PreparedExternalFiles],
    mount_paths: Sequence[Path],
) -> None:
    """Require every configured installation to use one declared volume subdirectory."""
    for prepared in external_files.values():
        owners = [path for path in mount_paths if prepared.root.is_relative_to(path)]
        if len(owners) != 1 or prepared.root == owners[0]:
            message = (
                "External files require a version directory within one Modal volume"
            )
            raise ValueError(message)
