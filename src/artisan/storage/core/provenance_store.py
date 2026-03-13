"""Read operations for provenance and artifact-metadata Delta tables.

Extracts provenance-related query methods from ArtifactStore into a
focused class: edge loading, step/type maps, ancestor/descendant lookups.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from artisan.schemas.enums import TablePath


class ProvenanceStore:
    """Query provenance edges and artifact metadata from Delta Lake.

    Provides all provenance graph queries and artifact-metadata lookups
    (step numbers, types, IDs-by-type) without access to artifact content
    tables. Use ``ArtifactStore.provenance`` for the standard access path.

    Attributes:
        base_path (Path): Root directory for Delta Lake tables.
    """

    def __init__(self, base_path: Path | str):
        """Initialize with the Delta Lake root directory.

        Args:
            base_path: Root directory containing Delta tables.
        """
        self.base_path = Path(base_path)

    def _table_path(self, table: TablePath) -> Path:
        """Resolve the filesystem path for a Delta table."""
        return self.base_path / table

    # -------------------------------------------------------------------------
    # Backward / forward maps (dict-based, full scan)
    # -------------------------------------------------------------------------

    def load_backward_map(self) -> dict[str, list[str]]:
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

    def load_forward_map(self) -> dict[str, list[str]]:
        """Load the full forward provenance map in one Delta scan.

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

    # -------------------------------------------------------------------------
    # Type and step maps
    # -------------------------------------------------------------------------

    def load_type_map(self, artifact_ids: list[str] | None = None) -> dict[str, str]:
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

    def load_step_map(self, artifact_ids: set[str] | None = None) -> dict[str, int]:
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

    # -------------------------------------------------------------------------
    # Direct queries (1-hop)
    # -------------------------------------------------------------------------

    def get_direct_ancestors(self, artifact_id: str) -> list[str]:
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

    def get_direct_descendants(
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

    def get_descendant_ids_df(
        self,
        source_ids: pl.Series,
        target_artifact_type: str | None = None,
    ) -> pl.DataFrame:
        """Return direct descendant IDs as a two-column DataFrame.

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

    # -------------------------------------------------------------------------
    # Step queries
    # -------------------------------------------------------------------------

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

    def get_step_range(self, artifact_ids: pl.Series) -> tuple[int, int] | None:
        """Return the min and max origin step numbers for the given IDs.

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
            artifact_ids: If given, restrict to these IDs.

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

    def load_step_name_map(self, pipeline_run_id: str | None = None) -> dict[int, str]:
        """Load a mapping of step number to human-readable step name.

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

    # -------------------------------------------------------------------------
    # Edge loading (DataFrame-native)
    # -------------------------------------------------------------------------

    def load_edges_df(
        self,
        step_min: int,
        step_max: int,
        *,
        include_target_type: bool = False,
    ) -> pl.DataFrame:
        """Load provenance edges where both endpoints fall within a step range.

        Args:
            step_min: Minimum step number (inclusive).
            step_max: Maximum step number (inclusive).
            include_target_type: When True, include ``target_artifact_type``
                column in the output.

        Returns:
            DataFrame with columns ``[source_artifact_id,
            target_artifact_id]`` (plus ``target_artifact_type`` when
            requested). Empty with correct schema when no edges match.
        """
        base_cols = ["source_artifact_id", "target_artifact_id"]
        output_cols = (
            [*base_cols, "target_artifact_type"] if include_target_type else base_cols
        )
        empty = pl.DataFrame(schema={c: pl.String for c in output_cols})

        prov_path = self._table_path(TablePath.ARTIFACT_EDGES)
        index_path = self._table_path(TablePath.ARTIFACT_INDEX)

        if not prov_path.exists() or not index_path.exists():
            return empty

        edge_select = (
            [*base_cols, "target_artifact_type"] if include_target_type else base_cols
        )
        edges = pl.scan_delta(str(prov_path)).select(edge_select)
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
            .select(output_cols)
            .collect()
        )

        return result if not result.is_empty() else empty

    # -------------------------------------------------------------------------
    # Transitive walks (multi-hop)
    # -------------------------------------------------------------------------

    def get_ancestor_ids(
        self,
        artifact_id: str,
        *,
        ancestor_type: str | None = None,
    ) -> list[str]:
        """Return all transitive ancestor IDs reachable via backward walk.

        Iterative BFS backward through provenance edges from
        ``artifact_id``, collecting all reachable ancestors.

        Args:
            artifact_id: Starting artifact ID.
            ancestor_type: If given, only return ancestors of this type.

        Returns:
            Ancestor artifact IDs (excludes the starting artifact itself).
            Empty list if no ancestors exist or tables are missing.
        """
        prov_path = self._table_path(TablePath.ARTIFACT_EDGES)
        if not prov_path.exists():
            return []

        edges = (
            pl.scan_delta(str(prov_path))
            .select(["source_artifact_id", "target_artifact_id"])
            .collect()
        )
        if edges.is_empty():
            return []

        collected: set[str] = set()
        frontier = {artifact_id}

        while frontier:
            frontier_s = pl.Series(sorted(frontier))
            parents = (
                edges.filter(pl.col("target_artifact_id").is_in(frontier_s))
                .select("source_artifact_id")
                .to_series()
                .to_list()
            )
            new = set(parents) - collected - {artifact_id}
            collected.update(new)
            frontier = new

        if not collected:
            return []

        ancestor_ids = list(collected)

        if ancestor_type is not None:
            type_map = self.load_type_map(ancestor_ids)
            ancestor_ids = [
                aid for aid in ancestor_ids if type_map.get(aid) == ancestor_type
            ]

        return ancestor_ids

    def get_descendant_ids(
        self,
        artifact_id: str,
        *,
        descendant_type: str | None = None,
    ) -> list[str]:
        """Return all transitive descendant IDs reachable via forward walk.

        Iterative BFS forward through provenance edges from
        ``artifact_id``, collecting all reachable descendants.

        Args:
            artifact_id: Starting artifact ID.
            descendant_type: If given, only return descendants of this type.

        Returns:
            Descendant artifact IDs (excludes the starting artifact itself).
            Empty list if no descendants exist or tables are missing.
        """
        prov_path = self._table_path(TablePath.ARTIFACT_EDGES)
        if not prov_path.exists():
            return []

        select_cols = ["source_artifact_id", "target_artifact_id"]
        if descendant_type is not None:
            select_cols.append("target_artifact_type")

        edges = pl.scan_delta(str(prov_path)).select(select_cols).collect()
        if edges.is_empty():
            return []

        collected: set[str] = set()
        type_matched: set[str] = set()
        frontier = {artifact_id}

        while frontier:
            frontier_s = pl.Series(sorted(frontier))
            children_df = edges.filter(pl.col("source_artifact_id").is_in(frontier_s))
            child_ids = set(children_df["target_artifact_id"].to_list())
            new = child_ids - collected - {artifact_id}

            if descendant_type is not None and new:
                matched_df = children_df.filter(
                    (pl.col("target_artifact_type") == descendant_type)
                    & pl.col("target_artifact_id").is_in(sorted(new))
                )
                type_matched.update(matched_df["target_artifact_id"].to_list())

            collected.update(new)
            frontier = new

        if descendant_type is not None:
            return list(type_matched)

        return list(collected)
