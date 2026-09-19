"""Storage configuration for fsspec and delta-rs."""

from __future__ import annotations

from typing import Any

import fsspec
from fsspec import AbstractFileSystem
from pydantic import BaseModel, Field


class StorageConfig(BaseModel):
    """Storage configuration.

    Backends use ambient credentials by default (IAM roles, environment
    variables, or service accounts). Explicit configuration and credentials
    may be supplied separately for fsspec and delta-rs; option keys are not
    translated between the two backends.

    Attributes:
        protocol: fsspec protocol identifier. ``"file"`` for local
            filesystem, ``"s3"`` for S3, ``"gcs"`` for Google Cloud
            Storage.
        options: Arguments passed to the fsspec filesystem constructor,
            including explicit credentials when needed.
        delta_options: Delta-rs ``storage_options`` dict passed to
            :func:`polars.read_delta`/:func:`polars.DataFrame.write_delta`.
            Keys follow the delta-rs / object_store schema
            (``AWS_ENDPOINT_URL``, ``AWS_ACCESS_KEY_ID``,
            ``AWS_SECRET_ACCESS_KEY``, ``AWS_REGION``, ``AWS_ALLOW_HTTP``,
            etc.). An empty dict uses ambient configuration. Explicit options
            configure endpoints and credentials without changing process-wide
            environment variables.
    """

    model_config = {"extra": "forbid", "frozen": True}

    protocol: str = "file"
    options: dict[str, Any] = Field(default_factory=dict)
    delta_options: dict[str, str] = Field(default_factory=dict)

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

        Returns a copy of :attr:`delta_options` when non-empty,
        otherwise ``None``. Returning ``None`` lets delta-rs read
        credentials and config from environment variables (IAM
        roles, ``AWS_ENDPOINT_URL``, ``GOOGLE_APPLICATION_CREDENTIALS``,
        etc.) via the Rust ``object_store`` crate.

        Returns a dict copy (not the underlying field) so callers
        can mutate the result without violating the frozen-model
        contract.

        Returns:
            Copy of ``delta_options`` if populated; otherwise ``None``.
        """
        return dict(self.delta_options) if self.delta_options else None
