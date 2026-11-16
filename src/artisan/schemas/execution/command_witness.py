"""Measured internal launcher capability and invocation-specific exit evidence."""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from artisan.schemas.execution.worker_transport import PayloadFile
from artisan.utils.hashing import canonical_json_bytes


class PreparedCommandWitness(BaseModel):
    """One static Linux helper, independent of selected scientific tool software."""

    model_config = ConfigDict(frozen=True, extra="forbid")
    executable: Path
    architecture: Literal["x86_64"]
    source_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    binary_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    recipe_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    builder_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    compiler_version: str
    compiler_files: tuple[PayloadFile, ...]

    @property
    def digest(self) -> str:
        """Bind built bytes and their measured derivation, excluding the locator."""
        return hashlib.sha256(
            canonical_json_bytes(self.model_dump(mode="json", exclude={"executable"}))
        ).hexdigest()


class CommandExitReceipt(BaseModel):
    """Exact child result; exec failure never claims authored scientific execution."""

    model_config = ConfigDict(frozen=True, extra="forbid", strict=True)
    nonce: str = Field(pattern=r"^[0-9a-f]{32}$")
    kind: Literal["exit", "signal", "exec_error"]
    code: int = Field(ge=0, le=4095)

    @model_validator(mode="after")
    def valid_status(self) -> CommandExitReceipt:
        if (
            (self.kind == "exit" and self.code > 255)
            or (self.kind == "signal" and not 1 <= self.code <= 64)
            or (self.kind == "exec_error" and self.code == 0)
        ):
            msg = "Command witness returned an invalid Linux child status"
            raise ValueError(msg)
        return self
