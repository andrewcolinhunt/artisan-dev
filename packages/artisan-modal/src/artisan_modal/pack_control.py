"""Fixed shared-store records for one ordered SDK input, never acceptance authority."""

from __future__ import annotations

import contextlib
import hashlib
from typing import Literal, Self, cast
from uuid import UUID

from botocore.exceptions import ClientError  # type: ignore[import-untyped]
from fsspec import AbstractFileSystem
from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, model_validator

from artisan.orchestration.runner_api import (
    AssignmentReplyContext,
    CuratorReplyContext,
    InertCallResult,
    OwnershipState,
    StorageConfig,
)
from artisan_modal.diagnostics import sensitive_strings
from artisan_modal.transport import ModalCall

_CONTEXT: TypeAdapter[AssignmentReplyContext | CuratorReplyContext] = TypeAdapter(
    AssignmentReplyContext | CuratorReplyContext
)
_MAX_RECORD_BYTES = 32 * 1024 * 1024


class ModalPack(BaseModel):
    """One immutable ordered live request with explicit shared control storage."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    pack_id: UUID
    staging_root: str
    storage: StorageConfig = Field(repr=False)
    calls: tuple[ModalCall, ...] = Field(min_length=1, repr=False)

    @model_validator(mode="after")
    def _membership(self) -> Self:
        if self.storage.protocol != "s3" or not self.staging_root.startswith("s3://"):
            msg = "Modal packs require explicit shared S3 staging"
            raise ValueError(msg)
        parts = self.staging_root.removeprefix("s3://").split("/")
        if not parts[0] or any(part in {".", ".."} for part in parts):
            msg = "Modal pack staging root is not canonical"
            raise ValueError(msg)
        first = self.calls[0]
        identities = set()
        for call in self.calls:
            if (call.function, call.runtime, call.header.compatibility, call.gpu) != (
                first.function,
                first.runtime,
                first.header.compatibility,
                first.gpu,
            ):
                msg = "Modal pack calls require identical worker software"
                raise ValueError(msg)
            context = _CONTEXT.validate_json(call.reply_context)
            identity = (
                context.domain,
                context.attempt.model_dump_json()
                if isinstance(context, AssignmentReplyContext)
                else context.admission.model_dump_json(),
            )
            if identity in identities:
                msg = "Modal pack contains a duplicate admitted invocation"
                raise ValueError(msg)
            identities.add(identity)
        return self

    @property
    def sensitive_values(self) -> tuple[str, ...]:
        values = tuple(
            value
            for call in self.calls
            for value in _CONTEXT.validate_json(call.reply_context).sensitive_values
        )
        return (
            *values,
            *sensitive_strings(self.storage.options),
            *self.storage.delta_options.values(),
        )

    @property
    def digest(self) -> str:
        return hashlib.sha256(self.model_dump_json().encode()).hexdigest()


class PackEntry(BaseModel):
    """Claim rejects concurrent entry while its pack control records remain.

    Closed cleanup removes the claim. Later redelivery has a fresh entry and
    delivery identity; the controller winner decides acceptance.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)
    pack_id: UUID
    request_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    entry_id: UUID


class UnitProgress(PackEntry):
    """A core outcome, or explicit evidence that a queued unit never entered."""

    index: int = Field(ge=0, strict=True)
    payload_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    skipped: bool = Field(default=False, strict=True)
    outcome: InertCallResult | None = None

    @model_validator(mode="after")
    def _selection(self) -> Self:
        if self.skipped == (self.outcome is not None):
            msg = "Unit progress requires exactly one skipped/core outcome"
            raise ValueError(msg)
        return self


class CancelRequest(BaseModel):
    """A duration is interpreted on observation using only the worker's clock."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    pack_id: UUID
    request_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    index: int | None = Field(default=None, ge=0, strict=True)
    cleanup_seconds: float = Field(ge=0, le=5, allow_inf_nan=False)


class PackResult(PackEntry):
    ownership: Literal[OwnershipState.CLOSED, OwnershipState.UNKNOWN]
    results: tuple[UnitProgress, ...]
    failure: str | None = Field(default=None, max_length=1024)

    def verify(self, request: ModalPack) -> None:
        if self.pack_id != request.pack_id or self.request_sha256 != request.digest:
            msg = "Modal result differs from its exact pack"
            raise ValueError(msg)
        indices = tuple(result.index for result in self.results)
        if indices != tuple(sorted(set(indices))):
            msg = "Modal results must have unique ordered indices"
            raise ValueError(msg)
        entries = set()
        for result in self.results:
            verify_progress(request, self, result)
            if result.outcome is not None:
                if result.outcome.entry_id in entries:
                    msg = "Modal units reused an actual core entry"
                    raise ValueError(msg)
                entries.add(result.outcome.entry_id)
        if self.ownership == OwnershipState.CLOSED and (
            indices != tuple(range(len(request.calls)))
            or any(
                result.outcome is not None
                and result.outcome.ownership != OwnershipState.CLOSED
                for result in self.results
            )
        ):
            msg = "Closed pack requires every unit's confirmed closure"
            raise ValueError(msg)


def verify_progress(request: ModalPack, entry: PackEntry, result: UnitProgress) -> None:
    if (
        result.pack_id != request.pack_id
        or result.request_sha256 != request.digest
        or result.entry_id != entry.entry_id
        or result.index >= len(request.calls)
        or result.payload_sha256 != request.calls[result.index].header.payload_sha256
    ):
        msg = "Modal unit progress differs from its exact pack entry"
        raise ValueError(msg)


def write_once(fs: AbstractFileSystem, path: str, value: bytes) -> None:
    """Create one object, retaining exact S3 collision evidence across backends."""
    try:
        fs.pipe_file(path, value, mode="create")
    except OSError as error:
        cause = error.__cause__
        if isinstance(cause, ClientError):
            detail = cause.response.get("Error", {})
            metadata = cause.response.get("ResponseMetadata", {})
            if (
                cause.operation_name == "PutObject"
                and detail.get("Code") in {"PreconditionFailed", "412"}
                and detail.get("Condition") in {None, "", "If-None-Match"}
                and metadata.get("HTTPStatusCode") == 412
            ):
                raise FileExistsError(path) from error
        raise


class PackChannel:
    """Read only fixed keys; writes are conditional and never contain live payloads."""

    def __init__(
        self, request: ModalPack, *, fs: AbstractFileSystem | None = None
    ) -> None:
        self.request = request
        self.fs = request.storage.filesystem() if fs is None else fs
        self.root = (
            f"{request.staging_root.rstrip('/')}/_artisan_modal/{request.pack_id}"
        )
        self.digest = request.digest

    def _write(self, key: str, record: BaseModel) -> None:
        encoded = record.model_dump_json().encode()
        if len(encoded) > _MAX_RECORD_BYTES:
            msg = "Modal control record exceeds its bounded size"
            raise ValueError(msg)
        # Pinned s3fs maps create to S3 If-None-Match: *. Re-entry cannot overwrite.
        write_once(self.fs, f"{self.root}/{key}.json", encoded)

    def _read(self, key: str) -> bytes | None:
        try:
            with self.fs.open(f"{self.root}/{key}.json", "rb") as stream:
                encoded = cast(bytes, stream.read(_MAX_RECORD_BYTES + 1))
        except FileNotFoundError:
            return None
        if len(encoded) > _MAX_RECORD_BYTES:
            msg = "Modal control record exceeds its bounded size"
            raise ValueError(msg)
        return encoded

    def claim(self, entry_id: UUID) -> PackEntry:
        entry = PackEntry(
            pack_id=self.request.pack_id, request_sha256=self.digest, entry_id=entry_id
        )
        self._write("entry", entry)
        return entry

    def entry(self) -> PackEntry | None:
        encoded = self._read("entry")
        if encoded is None:
            return None
        entry = PackEntry.model_validate_json(encoded)
        if entry.pack_id != self.request.pack_id or entry.request_sha256 != self.digest:
            msg = "Modal entry claim differs from its request"
            raise ValueError(msg)
        return entry

    def publish(self, result: UnitProgress) -> None:
        self._write(f"unit-{result.index}", result)

    def progress(self, entry: PackEntry, index: int) -> UnitProgress | None:
        encoded = self._read(f"unit-{index}")
        if encoded is None:
            return None
        result = UnitProgress.model_validate_json(encoded)
        verify_progress(self.request, entry, result)
        if result.index != index:
            msg = "Modal progress is stored under another unit index"
            raise ValueError(msg)
        return result

    def cancel(self, index: int | None, seconds: float) -> None:
        key = "cancel-pack" if index is None else f"cancel-{index}"
        record = CancelRequest(
            pack_id=self.request.pack_id,
            request_sha256=self.digest,
            index=index,
            cleanup_seconds=max(0.0, min(5.0, seconds)),
        )
        try:
            self._write(key, record)
        except FileExistsError:
            self.cancellation(index)

    def cancellation(self, index: int | None) -> CancelRequest | None:
        key = "cancel-pack" if index is None else f"cancel-{index}"
        encoded = self._read(key)
        if encoded is None:
            return None
        record = CancelRequest.model_validate_json(encoded)
        if (
            record.pack_id != self.request.pack_id
            or record.request_sha256 != self.digest
            or record.index != index
        ):
            msg = "Modal cancellation differs from its exact pack unit"
            raise ValueError(msg)
        return record

    def remove(self) -> None:
        keys = ["entry", "cancel-pack"]
        keys.extend(
            f"{kind}-{index}"
            for index in range(len(self.request.calls))
            for kind in ("unit", "cancel")
        )
        for key in keys:
            with contextlib.suppress(FileNotFoundError):
                self.fs.rm(f"{self.root}/{key}.json")
