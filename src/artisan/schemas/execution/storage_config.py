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
        delta_options: Delta-rs ``storage_options`` dict passed to
            :func:`polars.read_delta`/:func:`polars.DataFrame.write_delta`.
            Keys follow the delta-rs / object_store schema
            (``AWS_ENDPOINT_URL``, ``AWS_ACCESS_KEY_ID``,
            ``AWS_SECRET_ACCESS_KEY``, ``AWS_REGION``, ``AWS_ALLOW_HTTP``,
            etc.). Empty dict (default) means "let delta-rs read from the
            environment" — production IAM-role default. Populated when
            an explicit non-default endpoint is needed (MinIO, LocalStack,
            on-prem S3) without leaking credentials into the process env.
    """

    model_config = {"frozen": True}

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
