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

        Trace one step backward in the provenance graph by querying
        artifact_edges for rows where ``artifact_id`` is the target.

        Args:
            artifact_id: Target artifact whose parents are requested.

        Returns:
            Source artifact IDs. Empty if no ancestors exist or the
            artifact_edges table is missing.
        """
        prov_path = self._table_path(TablePath.ARTIFACT_EDGES)
        if not prov_path.exists():
            return []

        result = (
            pl.scan_delta(str(prov_path))
            .filter(pl.col("target_artifact_id") == artifact_id)
            .select("source_artifact_id")
            .collect()
        )

        if result.is_empty():
            return []

        return result["source_artifact_id"].to_list()

    def load_provenance_map(self) -> dict[str, list[str]]:
        """Load the full backward provenance map in one Delta scan.

        Returns:
            Mapping of target artifact ID to its source (ancestor) IDs.
            Empty dict if the artifact_edges table does not exist.
        """
        prov_path = self._table_path(TablePath.ARTIFACT_EDGES)
        if not prov_path.exists():
            return {}

        result = (
            pl.scan_delta(str(prov_path))
            .select(["source_artifact_id", "target_artifact_id"])
            .collect()
        )

        if result.is_empty():
            return {}

        provenance_map: dict[str, list[str]] = {}
        for row in result.iter_rows():
            source_id, target_id = row
            provenance_map.setdefault(target_id, []).append(source_id)
        return provenance_map

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

        descendant_map = self.get_descendant_artifact_ids(
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
        """Return direct descendant artifact IDs for given sources.

        Trace one step forward in the provenance graph by querying
        artifact_edges for rows where the given IDs are the source.

        Args:
            source_artifact_ids: Source IDs to query. An empty set
                returns immediately.
            target_artifact_type: If given, only include descendants
                of this type (e.g. ``"metric"``).

        Returns:
            Mapping of source ID to its descendant target IDs. Sources
            with no descendants are omitted. Empty dict when the
            artifact_edges table does not exist.
        """
        if not source_artifact_ids:
            return {}

        prov_path = self._table_path(TablePath.ARTIFACT_EDGES)
        if not prov_path.exists():
            return {}

        query = pl.scan_delta(str(prov_path)).filter(
            pl.col("source_artifact_id").is_in(list(source_artifact_ids))
        )

        if target_artifact_type is not None:
            query = query.filter(pl.col("target_artifact_type") == target_artifact_type)

        result = query.select(["source_artifact_id", "target_artifact_id"]).collect()

        if result.is_empty():
            return {}

        descendant_map: dict[str, list[str]] = {}
        for row in result.iter_rows():
            source_id, target_id = row
            descendant_map.setdefault(source_id, []).append(target_id)
        return descendant_map

    def load_step_number_map(
        self, artifact_ids: set[str] | None = None
    ) -> dict[str, int]:
        """Load origin step numbers from the artifact_index.

        Args:
            artifact_ids: Restrict results to these IDs. If None, load
                every entry in the index.

        Returns:
            Mapping of artifact ID to origin step number. Empty dict
            if the artifact_index table does not exist.
        """
        index_path = self._table_path(TablePath.ARTIFACT_INDEX)
        if not index_path.exists():
            return {}

        query = pl.scan_delta(str(index_path)).select(
            ["artifact_id", "origin_step_number"]
        )

        if artifact_ids is not None:
            query = query.filter(pl.col("artifact_id").is_in(list(artifact_ids)))

        result = query.collect()

        if result.is_empty():
            return {}

        return dict(
            zip(
                result["artifact_id"].to_list(),
                result["origin_step_number"].to_list(),
                strict=True,
            )
        )

    def get_step_range(self, artifact_ids: pl.Series) -> tuple[int, int] | None:
        """Return the min and max origin step numbers for the given IDs.

        Use a single lazy scan with aggregation to avoid materializing
        the full step number map.

        Args:
            artifact_ids: Artifact IDs to query. An empty Series
                returns None immediately.

        Returns:
            ``(step_min, step_max)`` tuple, or None if no matches exist
            or the artifact_index table is missing.
        """
        if artifact_ids.is_empty():
            return None

        index_path = self._table_path(TablePath.ARTIFACT_INDEX)
        if not index_path.exists():
            return None

        result = (
            pl.scan_delta(str(index_path))
            .filter(pl.col("artifact_id").is_in(artifact_ids))
            .select(
                pl.col("origin_step_number").min().alias("step_min"),
                pl.col("origin_step_number").max().alias("step_max"),
            )
            .collect()
        )

        if result.is_empty() or result["step_min"][0] is None:
            return None

        return (result["step_min"][0], result["step_max"][0])

    def get_descendant_ids_df(
        self,
        source_ids: pl.Series,
        target_artifact_type: str | None = None,
    ) -> pl.DataFrame:
        """Return direct descendant IDs as a two-column DataFrame.

        DataFrame-native alternative to ``get_descendant_artifact_ids``
        that avoids materializing a Python dict-of-lists.

        Args:
            source_ids: Source artifact IDs to query. An empty Series
                returns the empty schema immediately.
            target_artifact_type: If given, restrict to descendants of
                this type.

        Returns:
            DataFrame with columns ``[source_artifact_id,
            target_artifact_id]``. Empty with correct schema when no
            matches exist.
        """
        empty = pl.DataFrame(
            schema={
                "source_artifact_id": pl.String,
                "target_artifact_id": pl.String,
            }
        )

        if source_ids.is_empty():
            return empty

        prov_path = self._table_path(TablePath.ARTIFACT_EDGES)
        if not prov_path.exists():
            return empty

        query = pl.scan_delta(str(prov_path)).filter(
            pl.col("source_artifact_id").is_in(source_ids)
        )

        if target_artifact_type is not None:
            query = query.filter(pl.col("target_artifact_type") == target_artifact_type)

        result = query.select(["source_artifact_id", "target_artifact_id"]).collect()

        return result if not result.is_empty() else empty

    def get_artifact_step_number(self, artifact_id: str) -> int | None:
        """Return the origin step number for a single artifact.

        Args:
            artifact_id: Content-addressed ID to look up in the index.

        Returns:
            Origin step number, or None if the artifact is not indexed.
        """
        index_path = self._table_path(TablePath.ARTIFACT_INDEX)
        if not index_path.exists():
            return None

        result = (
            pl.scan_delta(str(index_path))
            .filter(pl.col("artifact_id") == artifact_id)
            .select("origin_step_number")
            .limit(1)
            .collect()
        )

        if result.is_empty():
            return None

        return result["origin_step_number"][0]

    def load_artifact_type_map(
        self, artifact_ids: list[str] | None = None
    ) -> dict[str, str]:
        """Bulk-load artifact type strings from the index.

        Args:
            artifact_ids: Restrict results to these IDs. If None, load
                every entry in the index.

        Returns:
            Mapping of artifact ID to type string. Empty dict if the
            artifact_index table does not exist.
        """
        index_path = self._table_path(TablePath.ARTIFACT_INDEX)
        if not index_path.exists():
            return {}

        query = pl.scan_delta(str(index_path)).select(["artifact_id", "artifact_type"])
        if artifact_ids is not None:
            query = query.filter(pl.col("artifact_id").is_in(artifact_ids))
        result = query.collect()

        if result.is_empty():
            return {}

        return dict(
            zip(
                result["artifact_id"].to_list(),
                result["artifact_type"].to_list(),
                strict=True,
            )
        )

    def load_artifact_ids_by_type(
        self,
        artifact_type: str,
        *,
        step_numbers: list[int] | None = None,
        artifact_ids: list[str] | None = None,
    ) -> set[str]:
        """Return artifact IDs from the index matching a given type.

        Args:
            artifact_type: Required type filter.
            step_numbers: If given, restrict to these origin steps.
            artifact_ids: If given, restrict to these IDs (useful for
                validating that a set of IDs actually exist with the
                expected type).

        Returns:
            Matching artifact IDs. Empty set if the artifact_index
            table does not exist.
        """

        index_path = self._table_path(TablePath.ARTIFACT_INDEX)
        if not index_path.exists():
            return set()

        query = pl.scan_delta(str(index_path)).filter(
            pl.col("artifact_type") == artifact_type
        )
        if step_numbers is not None:
            query = query.filter(pl.col("origin_step_number").is_in(step_numbers))
        if artifact_ids is not None:
            query = query.filter(pl.col("artifact_id").is_in(artifact_ids))

        result = query.select("artifact_id").collect()
        return set(result["artifact_id"].to_list())

    def load_forward_provenance_map(self) -> dict[str, list[str]]:
        """Load the full forward provenance map in one Delta scan.

        Complement of ``load_provenance_map`` (which maps targets to
        sources).

        Returns:
            Mapping of source artifact ID to its descendant (target)
            IDs. Empty dict if the artifact_edges table does not exist.
        """
        prov_path = self._table_path(TablePath.ARTIFACT_EDGES)
        if not prov_path.exists():
            return {}

        result = (
            pl.scan_delta(str(prov_path))
            .select(["source_artifact_id", "target_artifact_id"])
            .collect()
        )

        if result.is_empty():
            return {}

        forward_map: dict[str, list[str]] = {}
        for row in result.iter_rows():
            source_id, target_id = row
            forward_map.setdefault(source_id, []).append(target_id)
        return forward_map

    def load_step_name_map(self, pipeline_run_id: str | None = None) -> dict[int, str]:
        """Load a mapping of step number to human-readable step name.

        Prefer the steps table (most recent completed entry per step).
        Fall back to ``executions.operation_name`` when the steps table
        is missing.

        Args:
            pipeline_run_id: If given, restrict the steps query to this
                pipeline run. None uses the latest available names.

        Returns:
            Mapping of step number to step or operation name. Empty
            dict if neither table exists.
        """
        steps_path = self._table_path(TablePath.STEPS)
        if steps_path.exists():
            lf = pl.scan_delta(str(steps_path)).filter(pl.col("status") == "completed")
            if pipeline_run_id:
                lf = lf.filter(pl.col("pipeline_run_id") == pipeline_run_id)

            df = (
                lf.sort("timestamp", descending=True)
                .unique(subset=["step_number"], keep="first")
                .select(["step_number", "step_name"])
                .collect()
            )
            if not df.is_empty():
                return dict(
                    zip(
                        df["step_number"].to_list(),
                        df["step_name"].to_list(),
                        strict=True,
                    )
                )

        records_path = self._table_path(TablePath.EXECUTIONS)
        if records_path.exists():
            df = (
                pl.scan_delta(str(records_path))
                .filter(pl.col("success") == True)  # noqa: E712
                .select(["origin_step_number", "operation_name"])
                .unique(subset=["origin_step_number"], keep="first")
                .collect()
            )
            if not df.is_empty():
                return dict(
                    zip(
                        df["origin_step_number"].to_list(),
                        df["operation_name"].to_list(),
                        strict=True,
                    )
                )

        return {}

    def load_provenance_edges_df(self, step_min: int, step_max: int) -> pl.DataFrame:
        """Load provenance edges where both endpoints fall within a step range.

        Join artifact_edges with artifact_index to resolve step numbers,
        then keep only edges whose source and target both lie in
        ``[step_min, step_max]``.

        Args:
            step_min: Minimum step number (inclusive).
            step_max: Maximum step number (inclusive).

        Returns:
            DataFrame with columns ``[source_artifact_id,
            target_artifact_id]``. Empty with correct schema when no
            edges match.
        """
        empty = pl.DataFrame(
            schema={"source_artifact_id": pl.String, "target_artifact_id": pl.String}
        )

        prov_path = self._table_path(TablePath.ARTIFACT_EDGES)
        index_path = self._table_path(TablePath.ARTIFACT_INDEX)

        if not prov_path.exists() or not index_path.exists():
            return empty

        edges = pl.scan_delta(str(prov_path)).select(
            ["source_artifact_id", "target_artifact_id"]
        )
        index = pl.scan_delta(str(index_path)).select(
            ["artifact_id", "origin_step_number"]
        )

        result = (
            edges.join(index, left_on="source_artifact_id", right_on="artifact_id")
            .rename({"origin_step_number": "source_step"})
            .join(index, left_on="target_artifact_id", right_on="artifact_id")
            .rename({"origin_step_number": "target_step"})
            .filter(
                (pl.col("source_step") >= step_min)
                & (pl.col("source_step") <= step_max)
                & (pl.col("target_step") >= step_min)
                & (pl.col("target_step") <= step_max)
            )
            .select(["source_artifact_id", "target_artifact_id"])
            .collect()
        )

        return result if not result.is_empty() else empty

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
