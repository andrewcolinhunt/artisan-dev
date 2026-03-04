"""Artifact type definition registry.

Each artifact type is described by a concrete ``ArtifactTypeDef`` subclass
that carries three pieces of metadata: ``key``, ``table_path``, and ``model``.
Registration is automatic via ``__init_subclass__``.

Example::

    class DataTypeDef(ArtifactTypeDef):
        key = "data"
        table_path = "artifacts/data"
        model = DataArtifact
"""

from __future__ import annotations

from pathlib import PurePosixPath
from typing import Any, ClassVar

from artisan.schemas.artifact.types import ArtifactTypes


class ArtifactTypeDef:
    """Base class for artifact type definitions.

    Concrete subclasses must set ``key``, ``table_path``, and ``model``
    as class variables. Registration happens automatically at class
    definition time.

    Attributes:
        key: Unique string identifier (e.g. "data").
        table_path: Delta Lake table path (e.g. "artifacts/data").
        model: The artifact model class (must have POLARS_SCHEMA, to_row, from_row).
    """

    _registry: ClassVar[dict[str, type[ArtifactTypeDef]]] = {}

    key: ClassVar[str]
    table_path: ClassVar[str]
    model: ClassVar[type]

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)

        # Only register concrete types (those that set 'key' directly)
        if "key" not in cls.__dict__:
            return

        key = cls.key

        # Validate required attributes
        if "table_path" not in cls.__dict__:
            raise TypeError(f"{cls.__name__} must set 'table_path' class variable")
        if "model" not in cls.__dict__:
            raise TypeError(f"{cls.__name__} must set 'model' class variable")

        # Validate model has required serialization interface
        model = cls.model
        for attr in ("POLARS_SCHEMA", "to_row", "from_row"):
            if not hasattr(model, attr):
                raise TypeError(
                    f"{cls.__name__}.model ({model.__name__}) " f"must have '{attr}'"
                )

        # Reject duplicate keys
        if key in ArtifactTypeDef._registry:
            existing = ArtifactTypeDef._registry[key]
            if existing is not cls:
                raise ValueError(
                    f"Duplicate artifact type key {key!r}: "
                    f"{cls.__name__} conflicts with {existing.__name__}"
                )
            return

        # Register in both registries
        ArtifactTypes.register(key)
        ArtifactTypeDef._registry[key] = cls

    @classmethod
    def parquet_filename(cls) -> str:
        """Return the staging Parquet filename derived from table_path."""
        return PurePosixPath(cls.table_path).name + ".parquet"

    @classmethod
    def polars_schema(cls) -> dict[str, Any]:
        """Return the Polars column schema from the model."""
        return cls.model.POLARS_SCHEMA

    # --- Public lookup API ---

    @staticmethod
    def get(key: str) -> type[ArtifactTypeDef]:
        """Get the type def class for a given key.

        Args:
            key: Artifact type key (e.g. "data").

        Returns:
            The ArtifactTypeDef subclass.

        Raises:
            KeyError: If key is not registered.
        """
        if key not in ArtifactTypeDef._registry:
            raise KeyError(
                f"Unknown artifact type: {key!r}. "
                f"Registered: {list(ArtifactTypeDef._registry.keys())}"
            )
        return ArtifactTypeDef._registry[key]

    @staticmethod
    def get_all() -> dict[str, type[ArtifactTypeDef]]:
        """Return all registered type definitions keyed by type string."""
        return dict(ArtifactTypeDef._registry)

    @staticmethod
    def get_model(key: str) -> type:
        """Get the artifact model class for a given key.

        Args:
            key: Artifact type key (e.g. "data").

        Returns:
            The artifact model class (e.g. DataArtifact).
        """
        return ArtifactTypeDef.get(key).model

    @staticmethod
    def get_table_path(key: str) -> str:
        """Get the Delta Lake table path for a given key.

        Args:
            key: Artifact type key (e.g. "data").

        Returns:
            Table path string (e.g. "artifacts/data").
        """
        return ArtifactTypeDef.get(key).table_path

    @staticmethod
    def get_schema(key: str) -> dict[str, Any]:
        """Get the Polars schema for a given key.

        Args:
            key: Artifact type key (e.g. "data").

        Returns:
            Dict mapping column names to Polars data types.
        """
        return ArtifactTypeDef.get(key).model.POLARS_SCHEMA


# =============================================================================
# Concrete type definitions (auto-registered via __init_subclass__)
# =============================================================================

# Imports are deferred to module level to avoid circular imports at
# class-definition time. The models are fully defined before this
# module's bottom-of-file code executes.

from artisan.schemas.artifact.execution_config import (
    ExecutionConfigArtifact,
)
from artisan.schemas.artifact.file_ref import FileRefArtifact  # noqa: E402
from artisan.schemas.artifact.metric import MetricArtifact  # noqa: E402


class MetricTypeDef(ArtifactTypeDef):
    """Type definition for MetricArtifact."""

    key = "metric"
    table_path = "artifacts/metrics"
    model = MetricArtifact


class ConfigTypeDef(ArtifactTypeDef):
    """Type definition for ExecutionConfigArtifact."""

    key = "config"
    table_path = "artifacts/configs"
    model = ExecutionConfigArtifact


class FileRefTypeDef(ArtifactTypeDef):
    """Type definition for FileRefArtifact."""

    key = "file_ref"
    table_path = "artifacts/file_refs"
    model = FileRefArtifact
