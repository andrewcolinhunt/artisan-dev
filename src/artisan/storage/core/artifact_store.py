"""Read operations and write preparation for artifact Delta tables.

Artifacts are content-addressed; duplicate writes are no-ops.  All
actual writes go through the staging/commit path (see ``staging.py``
and ``commit.py``). This module provides query methods and DataFrame
preparation for the artifact_index.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.schemas.enums import TablePath
from artisan.storage.core.provenance_store import ProvenanceStore
from artisan.storage.core.table_schemas import get_schema


class ArtifactStore:
    """Query and prepare artifacts stored in Delta Lake tables.

    Attributes:
        base_path (Path): Root directory for Delta Lake tables.
    """

    def __init__(self, base_path: Path | str):
        """Initialize with the Delta Lake root directory.

        Args:
            base_path: Root directory containing artifact and framework
                Delta tables (e.g. ``file_refs/``, ``data/``,
                ``artifact_index/``).
        """
        self.base_path = Path(base_path)
        self._provenance: ProvenanceStore | None = None

    @property
    def provenance(self) -> ProvenanceStore:
        """Lazy-initialized provenance store for graph queries."""
        if self._provenance is None:
            self._provenance = ProvenanceStore(self.base_path)
        return self._provenance

    def _table_path(self, table: TablePath) -> Path:
        """Resolve the filesystem path for a Delta table."""
        return self.base_path / table

    # -------------------------------------------------------------------------
    # Read operations
    # -------------------------------------------------------------------------

    def get_artifact(
        self,
        artifact_id: str,
        artifact_type: str | None = None,
        *,
        hydrate: bool = True,
    ) -> Artifact | None:
        """Retrieve a single artifact by its content-addressed ID.

        Args:
            artifact_id: Content-addressed artifact identifier.
            artifact_type: Type key hint. When provided, skips the
                artifact_index lookup to determine the storage table.
            hydrate: If True, load all fields from the content table.
                If False, return a minimal model with only the ID and
                type populated.

        Returns:
            Typed artifact model, or None if the ID is not found in
            the index or content table.
        """
        # Determine which table to query
        if artifact_type is None:
            artifact_type = self.get_artifact_type(artifact_id)
            if artifact_type is None:
                return None

        # ID-only mode - return minimal artifact
        if not hydrate:
            model_cls = ArtifactTypeDef.get_model(artifact_type)
            return model_cls(artifact_id=artifact_id, artifact_type=artifact_type)

        # Full hydration - load from storage
        table_path_str = ArtifactTypeDef.get_table_path(artifact_type)
        table_path = self.base_path / table_path_str

        if not table_path.exists():
            return None

        result = (
            pl.scan_delta(str(table_path))
            .filter(pl.col("artifact_id") == artifact_id)
            .limit(1)
            .collect()
        )

        if result.is_empty():
            return None

        row = result.row(0, named=True)
        model_cls = ArtifactTypeDef.get_model(artifact_type)
        return model_cls.from_row(row)

    def get_artifacts_by_type(
        self,
        artifact_ids: list[str],
        artifact_type: str,
    ) -> dict[str, Artifact]:
        """Bulk-load artifacts of one type in a single Delta scan.

        Args:
            artifact_ids: Artifact IDs to load. An empty list returns
                an empty dict immediately without scanning.
            artifact_type: Determines which content table to scan.

        Returns:
            Mapping of artifact ID to typed model. IDs not found in
            storage are silently omitted.
        """
        if not artifact_ids:
            return {}

        table_path_str = ArtifactTypeDef.get_table_path(artifact_type)
        table_path = self.base_path / table_path_str

        if not table_path.exists():
            return {}

        result = (
            pl.scan_delta(str(table_path))
            .filter(pl.col("artifact_id").is_in(artifact_ids))
            .collect()
        )

        if result.is_empty():
            return {}

        model_cls = ArtifactTypeDef.get_model(artifact_type)
        artifacts: dict[str, Artifact] = {}
        for row in result.iter_rows(named=True):
            artifact = model_cls.from_row(row)
            artifacts[artifact.artifact_id] = artifact

        return artifacts

    def artifact_exists(self, artifact_id: str) -> bool:
        """Check whether an artifact exists via the artifact_index.

        Args:
            artifact_id: Content-addressed ID to look up.
        """
        return self.get_artifact_type(artifact_id) is not None

    def get_artifact_type(self, artifact_id: str) -> str | None:
        """Look up the type string for an artifact from the index.

        Args:
            artifact_id: Content-addressed ID to look up.

        Returns:
            Artifact type string, or None if not in the index.
        """
        index_path = self._table_path(TablePath.ARTIFACT_INDEX)
        if not index_path.exists():
            return None

        result = (
            pl.scan_delta(str(index_path))
            .filter(pl.col("artifact_id") == artifact_id)
            .select("artifact_type")
            .limit(1)
            .collect()
        )

        if result.is_empty():
            return None

        return result["artifact_type"][0]

    def get_ancestor_artifact_ids(self, artifact_id: str) -> list[str]:
        """Return direct ancestor (source) artifact IDs.

        Delegates to ``self.provenance.get_direct_ancestors``.
        """
        return self.provenance.get_direct_ancestors(artifact_id)

    def load_provenance_map(self) -> dict[str, list[str]]:
        """Load the full backward provenance map.

        Delegates to ``self.provenance.load_backward_map``.
        """
        return self.provenance.load_backward_map()

    def get_associated(
        self,
        artifact_ids: set[str],
        associated_type: str,
    ) -> dict[str, list[Artifact]]:
        """Load direct descendant artifacts of a specific type.

        Find provenance edges from each source to descendants matching
        ``associated_type``, then bulk-load and return the hydrated
        artifact models.

        Args:
            artifact_ids: Source artifact IDs to find associations for.
                An empty set returns immediately.
            associated_type: Only include descendants of this artifact
                type (e.g. ``"metric"``).

        Returns:
            Mapping of source artifact ID to its associated artifacts.
            Sources with no matching descendants are omitted.
        """
        if not artifact_ids:
            return {}

        descendant_map = self.provenance.get_direct_descendants(
            artifact_ids, target_artifact_type=associated_type
        )
        if not descendant_map:
            return {}

        all_target_ids = [tid for tids in descendant_map.values() for tid in tids]
        loaded = self.get_artifacts_by_type(all_target_ids, associated_type)

        result: dict[str, list[Artifact]] = {}
        for source_id, target_ids in descendant_map.items():
            artifacts = [loaded[tid] for tid in target_ids if tid in loaded]
            if artifacts:
                result[source_id] = artifacts
        return result

    def get_descendant_artifact_ids(
        self,
        source_artifact_ids: set[str],
        target_artifact_type: str | None = None,
    ) -> dict[str, list[str]]:
        """Delegates to ``self.provenance.get_direct_descendants``."""
        return self.provenance.get_direct_descendants(
            source_artifact_ids, target_artifact_type
        )

    def load_step_number_map(
        self, artifact_ids: set[str] | None = None
    ) -> dict[str, int]:
        """Delegates to ``self.provenance.load_step_map``."""
        return self.provenance.load_step_map(artifact_ids)

    def get_step_range(self, artifact_ids: pl.Series) -> tuple[int, int] | None:
        """Delegates to ``self.provenance.get_step_range``."""
        return self.provenance.get_step_range(artifact_ids)

    def get_descendant_ids_df(
        self,
        source_ids: pl.Series,
        target_artifact_type: str | None = None,
    ) -> pl.DataFrame:
        """Delegates to ``self.provenance.get_descendant_ids_df``."""
        return self.provenance.get_descendant_ids_df(source_ids, target_artifact_type)

    def get_artifact_step_number(self, artifact_id: str) -> int | None:
        """Delegates to ``self.provenance.get_artifact_step_number``."""
        return self.provenance.get_artifact_step_number(artifact_id)

    def load_artifact_type_map(
        self, artifact_ids: list[str] | None = None
    ) -> dict[str, str]:
        """Delegates to ``self.provenance.load_type_map``."""
        return self.provenance.load_type_map(artifact_ids)

    def load_artifact_ids_by_type(
        self,
        artifact_type: str,
        *,
        step_numbers: list[int] | None = None,
        artifact_ids: list[str] | None = None,
    ) -> set[str]:
        """Delegates to ``self.provenance.load_artifact_ids_by_type``."""
        return self.provenance.load_artifact_ids_by_type(
            artifact_type, step_numbers=step_numbers, artifact_ids=artifact_ids
        )

    def load_forward_provenance_map(self) -> dict[str, list[str]]:
        """Delegates to ``self.provenance.load_forward_map``."""
        return self.provenance.load_forward_map()

    def load_step_name_map(self, pipeline_run_id: str | None = None) -> dict[int, str]:
        """Delegates to ``self.provenance.load_step_name_map``."""
        return self.provenance.load_step_name_map(pipeline_run_id)

    def load_provenance_edges_df(
        self,
        step_min: int,
        step_max: int,
        *,
        include_target_type: bool = False,
    ) -> pl.DataFrame:
        """Delegates to ``self.provenance.load_edges_df``."""
        return self.provenance.load_edges_df(
            step_min, step_max, include_target_type=include_target_type
        )

    def load_metrics_df(self, artifact_ids: list[str]) -> pl.DataFrame:
        """Load metric artifacts as a two-column DataFrame.

        Args:
            artifact_ids: Metric artifact IDs to load. An empty list
                returns the empty schema immediately.

        Returns:
            DataFrame with columns ``[artifact_id, content]``. The
            ``content`` column is raw ``Binary``; the caller is
            responsible for JSON decoding. Empty with correct schema
            when no metrics are found.
        """
        empty = pl.DataFrame(schema={"artifact_id": pl.String, "content": pl.Binary})

        if not artifact_ids:
            return empty

        table_path_str = ArtifactTypeDef.get_table_path("metric")
        table_path = self.base_path / table_path_str

        if not table_path.exists():
            return empty

        result = (
            pl.scan_delta(str(table_path))
            .filter(pl.col("artifact_id").is_in(artifact_ids))
            .select(["artifact_id", "content"])
            .collect()
        )

        return result if not result.is_empty() else empty

    # -------------------------------------------------------------------------
    # Write preparation (returns DataFrames for staging)
    # -------------------------------------------------------------------------

    def prepare_artifact_index_entry(
        self, artifact_id: str, artifact_type: str, step_number: int
    ) -> pl.DataFrame:
        """Build a single-row DataFrame for the artifact_index.

        Args:
            artifact_id: Content-addressed artifact identifier.
            artifact_type: Artifact type string (e.g. ``"data"``).
            step_number: Pipeline step that produced this artifact.

        Returns:
            Single-row DataFrame matching the artifact_index schema,
            ready to be staged via ``StagingArea.stage_dataframe``.
        """

        data = {
            "artifact_id": [artifact_id],
            "artifact_type": [artifact_type],
            "origin_step_number": [step_number],
            "metadata": ["{}"],
        }
        return pl.DataFrame(data, schema=get_schema(TablePath.ARTIFACT_INDEX))
