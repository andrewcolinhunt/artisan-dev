"""Storage backend configuration for fsspec and delta-rs."""

from __future__ import annotations

from typing import Any

import fsspec
from fsspec import AbstractFileSystem
from pydantic import BaseModel, Field


class StorageConfig(BaseModel):
    """Storage backend configuration.

    Credentials are NOT stored here — they come from the execution
    environment (IAM roles, env vars, service accounts). This config
    carries only the protocol and non-sensitive fsspec options.

    ``options`` feeds fsspec only. Delta-rs reads credentials and
    config from the same environment variables (``AWS_REGION``,
    ``AWS_ENDPOINT_URL``, ``GOOGLE_APPLICATION_CREDENTIALS``, etc.)
    via the Rust ``object_store`` crate. No key translation needed.

    Args:
        protocol: fsspec protocol identifier. ``"file"`` for local
            filesystem, ``"s3"`` for S3, ``"gcs"`` for Google Cloud
            Storage.
        options: Non-sensitive fsspec constructor arguments. Values
            may be any type fsspec accepts (str, bool, int).
            Credentials come from the environment.
    """

    model_config = {"frozen": True}

    protocol: str = "file"
    options: dict[str, Any] = Field(default_factory=dict)

    @property
    def is_local(self) -> bool:
        """Whether this config targets a local filesystem."""
        return self.protocol == "file"

    def filesystem(self) -> AbstractFileSystem:
        """Create an fsspec filesystem instance.

        Returns:
            Configured filesystem for the protocol.
        """
        return fsspec.filesystem(self.protocol, **self.options)

    def delta_storage_options(self) -> dict[str, str] | None:
        """Storage options dict for Polars/delta-rs.

        Returns None for all protocols. Delta-rs reads credentials
        and config from environment variables (IAM roles,
        ``AWS_ENDPOINT_URL``, ``GOOGLE_APPLICATION_CREDENTIALS``,
        etc.) via the Rust ``object_store`` crate — no explicit
        options dict needed.

        This method exists as the stable API surface for docs 02–03
        call sites. If a future scenario requires explicit options
        (e.g. MinIO endpoint not in env vars), it can be extended
        here without changing callers.

        Returns:
            None. Delta-rs handles auth and config via environment.
        """
        return None
