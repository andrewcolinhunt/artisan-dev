"""Read operations for provenance and artifact-metadata Delta tables.

Groups the provenance-related query methods in a focused class: edge
loading, step/type maps, and ancestor/descendant lookups.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import polars as pl
from fsspec import AbstractFileSystem

from artisan.errors import StoreIntegrityError
from artisan.schemas.enums import TablePath
from artisan.storage.core.store_read_scope import (
    RetainedStoreReadScope,
    with_read_scope,
)
from artisan.storage.io.retained_store import (
    open_retained_store,
    resolve_store_files_root,
)

if TYPE_CHECKING:
    from artisan.storage.core.query_view import QueryScope, QueryView


class ProvenanceStore:
    """Query provenance edges and artifact metadata from Delta Lake.

    Provides all provenance graph queries and artifact-metadata lookups
    (step numbers, types, IDs-by-type) without access to artifact content
    tables. Use ``ArtifactStore.provenance`` for the standard access path.

    Attributes:
        base_path: Root URI/path for Delta Lake tables.
    """

    def __init__(
        self,
        base_path: str,
        *,
        fs: AbstractFileSystem,
        storage_options: dict[str, str] | None = None,
        files_root: str | None = None,
        _view: QueryView | None = None,
        _read_scope: QueryScope | None = None,
    ) -> None:
        """Initialize with the Delta Lake root directory.

        Args:
            base_path: Root URI/path containing Delta tables.
            fs: Filesystem implementation (LocalFileSystem, S3FileSystem, etc.).
            storage_options: Credentials/config passed to delta-rs calls.
            files_root: Managed carrier root. Local stores default to the sibling
                ``files`` directory; nonlocal stores require an explicit root.
        """
        self.base_path = base_path
        self._fs = fs
        self._storage_options = storage_options or {}
        if _view is not None and _read_scope is not None:
            msg = "A store cannot combine fixed and fresh retained visibility"
            raise ValueError(msg)
        source = _view if _view is not None else _read_scope
        if source is None:
            files_root = resolve_store_files_root(
                base_path, files_root, protocols=fs.protocol
            )
            _read_scope = RetainedStoreReadScope(
                open_retained_store(
                    base_path, files_root, fs=fs, storage_options=storage_options
                )
            )
            source = _read_scope
        if files_root is not None and files_root != source.files_root:
            msg = "Provenance store has another managed files root"
            raise StoreIntegrityError(msg)
        source.validate_binding(self.base_path, self._fs, self._storage_options)
        self.files_root = source.files_root
        self._fixed_view = _view
        self._read_scope = _read_scope

    @property
    def _view(self) -> QueryView | None:
        return (
            self._read_scope.view if self._read_scope is not None else self._fixed_view
        )

    def _scan(self, table: str | TablePath, *, direct: bool = False) -> pl.LazyFrame:
        view = self._view
        if view is None:
            msg = "Table reads require an active query scope"
            raise RuntimeError(msg)
        if direct and table == TablePath.ARTIFACT_EDGES:
            return view.artifact_edges(direct=True)
        return view.scan(table)

    # -------------------------------------------------------------------------
    # Backward / forward maps (dict-based, full scan)
    # -------------------------------------------------------------------------

    def _build_adjacency(
        self, key_index: int, value_index: int
    ) -> dict[str, list[str]]:
        """Build an adjacency map from provenance edges.

        Args:
            key_index: Column index to use as dict keys (0=source, 1=target).
            value_index: Column index to use as dict values (0=source, 1=target).

        Returns:
            Adjacency mapping. Empty dict when no visible edges exist.
        """
        result = (
            self._scan(TablePath.ARTIFACT_EDGES)
            .select(["source_artifact_id", "target_artifact_id"])
            .collect()
        )

        if result.is_empty():
            return {}

        adj: dict[str, list[str]] = {}
        for row in result.iter_rows():
            adj.setdefault(row[key_index], []).append(row[value_index])
        return adj

    @with_read_scope
    def load_backward_map(self) -> dict[str, list[str]]:
        """Load the full backward provenance map in one Delta scan.

        Returns:
            Mapping of target artifact ID to its source (ancestor) IDs.
            Empty dict when no visible edges exist.
        """
        return self._build_adjacency(key_index=1, value_index=0)

    @with_read_scope
    def load_forward_map(self) -> dict[str, list[str]]:
        """Load the full forward provenance map in one Delta scan.

        Returns:
            Mapping of source artifact ID to its descendant (target)
            IDs. Empty dict when no visible edges exist.
        """
        return self._build_adjacency(key_index=0, value_index=1)

    # -------------------------------------------------------------------------
    # Type and step maps
    # -------------------------------------------------------------------------

    def _load_index_column(
        self,
        value_column: str,
        artifact_ids: list[str] | set[str] | None = None,
    ) -> dict[str, Any]:
        """Load a single column from artifact_index as a dict keyed by artifact_id.

        Args:
            value_column: Column name to load alongside artifact_id.
            artifact_ids: Optional restriction to these IDs.

        Returns:
            Mapping of artifact ID to column value. Empty dict for no matches.
        """
        query = self._scan(TablePath.ARTIFACT_INDEX).select(
            ["artifact_id", value_column]
        )
        if artifact_ids is not None:
            query = query.filter(pl.col("artifact_id").is_in(list(artifact_ids)))
        result = query.collect()

        if result.is_empty():
            return {}

        return dict(
            zip(
                result["artifact_id"].to_list(),
                result[value_column].to_list(),
                strict=True,
            )
        )

    @with_read_scope
    def load_type_map(self, artifact_ids: list[str] | None = None) -> dict[str, str]:
        """Bulk-load artifact type strings from the index.

        Args:
            artifact_ids: Restrict results to these IDs. If None, load
                every entry in the index.

        Returns:
            Mapping of artifact ID to type string. Empty dict for no matches.
        """
        return self._load_index_column("artifact_type", artifact_ids)

    @with_read_scope
    def load_step_map(self, artifact_ids: set[str] | None = None) -> dict[str, int]:
        """Load origin step numbers from the artifact_index.

        Args:
            artifact_ids: Restrict results to these IDs. If None, load
                every entry in the index.

        Returns:
            Mapping of artifact ID to origin step number. Empty dict for no matches.
        """
        return self._load_index_column("origin_step_number", artifact_ids)

    # -------------------------------------------------------------------------
    # Direct queries (1-hop)
    # -------------------------------------------------------------------------

    @with_read_scope
    def get_direct_ancestors(self, artifact_id: str) -> list[str]:
        """Return direct ancestor (source) artifact IDs.

        Trace one step backward in the provenance graph by querying
        artifact_edges for rows where ``artifact_id`` is the target.

        Args:
            artifact_id: Target artifact whose parents are requested.

        Returns:
            Source artifact IDs. Empty if no direct ancestors exist.
        """
        result = (
            self._scan(TablePath.ARTIFACT_EDGES, direct=True)
            .filter(pl.col("target_artifact_id") == artifact_id)
            .select("source_artifact_id")
            .collect()
        )

        if result.is_empty():
            return []

        return result["source_artifact_id"].to_list()

    @with_read_scope
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
            with no descendants are omitted.
        """
        if not source_artifact_ids:
            return {}

        query = self._scan(TablePath.ARTIFACT_EDGES, direct=True).filter(
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

    # -------------------------------------------------------------------------
    # Step queries
    # -------------------------------------------------------------------------

    @with_read_scope
    def get_artifact_step_number(self, artifact_id: str) -> int | None:
        """Return the origin step number for a single artifact.

        Args:
            artifact_id: Content-addressed ID to look up in the index.

        Returns:
            Origin step number, or None if the artifact is not indexed.
        """
        result = (
            self._scan(TablePath.ARTIFACT_INDEX)
            .filter(pl.col("artifact_id") == artifact_id)
            .select("origin_step_number")
            .limit(1)
            .collect()
        )

        if result.is_empty():
            return None

        step_number: int = result["origin_step_number"][0]
        return step_number

    @with_read_scope
    def get_step_range(self, artifact_ids: list[str]) -> tuple[int, int] | None:
        """Return the min and max origin step numbers for the given IDs.

        Args:
            artifact_ids: Artifact IDs to query. An empty list
                returns None immediately.

        Returns:
            ``(step_min, step_max)`` tuple, or None if no matches exist.
        """
        if not artifact_ids:
            return None

        result = (
            self._scan(TablePath.ARTIFACT_INDEX)
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

    @with_read_scope
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
            Matching artifact IDs. Empty set if no matches exist.
        """
        query = self._scan(TablePath.ARTIFACT_INDEX).filter(
            pl.col("artifact_type") == artifact_type
        )
        if step_numbers is not None:
            query = query.filter(pl.col("origin_step_number").is_in(step_numbers))
        if artifact_ids is not None:
            query = query.filter(pl.col("artifact_id").is_in(artifact_ids))

        result = query.select("artifact_id").collect()
        return set(result["artifact_id"].to_list())

    @with_read_scope
    def load_accepted_artifact_ids_by_step(
        self, artifact_type: str
    ) -> dict[int, set[str]]:
        """Return accepted exported IDs grouped by current logical step.

        An admitted curator queries its owning run at its frozen view. Ordinary
        stores select the latest run. Reused content keeps its original artifact
        identity and may occur in several accepted steps. This query reads exact
        terminal selections, without hydrating payloads or finding associations.

        Args:
            artifact_type: Registered artifact type to select.

        Returns:
            Nonempty sets of accepted IDs keyed by logical step number.
        """
        view = self._view
        if view is None:
            msg = "Accepted membership requires an active query scope"
            raise RuntimeError(msg)
        return view.accepted_artifact_ids_by_step(artifact_type)

    @with_read_scope
    def load_step_name_map(self, pipeline_run_id: str | None = None) -> dict[int, str]:
        """Load a mapping of step number to human-readable step name.

        Args:
            pipeline_run_id: Exact run to query. None selects the admitted
                curator's owning run, or the most recent authoritative run
                for ordinary store reads.

        Returns:
            Mapping of logical step number to its latest attempt's step name.
            Empty when the selected run has no steps.
        """
        view = self._view
        if view is None:
            msg = "Step-name reads require an active query scope"
            raise RuntimeError(msg)
        return view.step_names(pipeline_run_id)

    # -------------------------------------------------------------------------
    # Edge loading (DataFrame-native)
    # -------------------------------------------------------------------------

    @with_read_scope
    def load_edges_df(
        self,
        step_min: int,
        step_max: int,
        *,
        include_target_type: bool = False,
        include_roles: bool = False,
        execution_ids: set[str] | list[str] | None = None,
    ) -> pl.DataFrame:
        """Load edges whose endpoints' origin steps fall within a range.

        Origin steps belong to the artifacts' first committed rows; cached
        reuse does not change them to current-run logical step numbers.

        Args:
            step_min: Minimum origin step number (inclusive).
            step_max: Maximum origin step number (inclusive).
            include_target_type: When True, include ``target_artifact_type``
                column in the output.
            include_roles: When True, include ``source_role``,
                ``target_role``, and ``group_id`` columns. Used by
                callers that need to dedup edges by their full identity
                tuple (e.g. ``DeclareLineage``).
            execution_ids: When provided, restrict edges to executions in an
                authoritative current-run membership projection.

        Returns:
            DataFrame with columns ``[source_artifact_id,
            target_artifact_id]`` (plus ``target_artifact_type`` when
            ``include_target_type`` is True; plus ``source_role``,
            ``target_role``, ``group_id`` when ``include_roles`` is True).
            Empty with correct schema when no edges match.
        """
        base_cols = ["source_artifact_id", "target_artifact_id"]
        extra_cols: list[str] = []
        if include_target_type:
            extra_cols.append("target_artifact_type")
        if include_roles:
            extra_cols.extend(["source_role", "target_role", "group_id"])
        output_cols = [*base_cols, *extra_cols]
        empty = pl.DataFrame(schema=dict.fromkeys(output_cols, pl.String))

        edges = self._scan(TablePath.ARTIFACT_EDGES)
        if execution_ids is not None:
            edges = edges.filter(pl.col("execution_run_id").is_in(execution_ids))
        edges = edges.select(output_cols)
        index = self._scan(TablePath.ARTIFACT_INDEX).select(
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

    def _walk_transitive(
        self,
        artifact_id: str,
        *,
        frontier_col: str,
        collect_col: str,
        type_col: str | None = None,
        type_value: str | None = None,
    ) -> list[str]:
        """Iterative BFS over provenance edges in one direction.

        Walk from ``artifact_id`` by repeatedly matching the current
        frontier against ``frontier_col`` and collecting the opposite
        endpoint (``collect_col``). Shared by the ancestor (backward) and
        descendant (forward) walks with the source/target roles swapped.

        Args:
            artifact_id: Starting artifact ID.
            frontier_col: Edge column matched against the current frontier
                (``target_artifact_id`` backward, ``source_artifact_id``
                forward).
            collect_col: Edge column whose values form the next frontier
                (the opposite endpoint of ``frontier_col``).
            type_col: Edge column carrying the collected node's type. When
                given, only nodes whose ``type_col`` equals ``type_value``
                are returned; the walk itself still traverses every edge.
            type_value: Required type when ``type_col`` is set.

        Returns:
            Reachable artifact IDs (excludes the starting artifact). When
            ``type_col`` is set, only type-matching IDs. Empty list if no
            matches exist.
        """
        select_cols = [frontier_col, collect_col]
        if type_col is not None:
            select_cols.append(type_col)

        edges = self._scan(TablePath.ARTIFACT_EDGES).select(select_cols).collect()
        if edges.is_empty():
            return []

        collected: set[str] = set()
        type_matched: set[str] = set()
        frontier = {artifact_id}

        while frontier:
            next_edges = edges.filter(pl.col(frontier_col).is_in(sorted(frontier)))
            next_ids = set(next_edges[collect_col].to_list())
            new = next_ids - collected - {artifact_id}

            if type_col is not None and new:
                matched = next_edges.filter(
                    (pl.col(type_col) == type_value)
                    & pl.col(collect_col).is_in(sorted(new))
                )
                type_matched.update(matched[collect_col].to_list())

            collected.update(new)
            frontier = new

        if type_col is not None:
            return list(type_matched)
        return list(collected)

    @with_read_scope
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
            Empty list if no ancestors exist.
        """
        # Resolve ancestor types from the authoritative index after traversal.
        ancestor_ids = self._walk_transitive(
            artifact_id,
            frontier_col="target_artifact_id",
            collect_col="source_artifact_id",
        )
        if not ancestor_ids:
            return []

        if ancestor_type is not None:
            type_map = self.load_type_map(ancestor_ids)
            ancestor_ids = [
                aid for aid in ancestor_ids if type_map.get(aid) == ancestor_type
            ]

        return ancestor_ids

    @with_read_scope
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
            Empty list if no descendants exist.
        """
        # Descendant type filtering uses the edge's target_artifact_type
        # inline during the walk, so it is pushed into the helper.
        return self._walk_transitive(
            artifact_id,
            frontier_col="source_artifact_id",
            collect_col="target_artifact_id",
            type_col="target_artifact_type" if descendant_type is not None else None,
            type_value=descendant_type,
        )
