"""Read operations and write preparation for artifact Delta tables.

Artifacts are content-addressed; duplicate writes are no-ops.  All
actual writes go through the staging/commit path (see ``staging.py``
and ``commit.py``). This module provides query methods and DataFrame
preparation for the artifact_index.
"""

from __future__ import annotations

from typing import cast

import polars as pl
from fsspec import AbstractFileSystem

from artisan.errors import ArtifactIntegrityError
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.external import sanitized_uri, validate_persistable_uri
from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.schemas.enums import TablePath
from artisan.storage.core.committed_scan import read_committed, scan_committed
from artisan.storage.core.provenance_store import ProvenanceStore
from artisan.storage.core.store_format import assert_store_format
from artisan.storage.core.table_schemas import get_schema
from artisan.utils.path import uri_join


class ArtifactStore:
    """Query and prepare artifacts stored in Delta Lake tables.

    Attributes:
        base_path: Root URI/path for Delta Lake tables.
    """

    def __init__(
        self,
        base_path: str,
        *,
        fs: AbstractFileSystem | None = None,
        storage_options: dict[str, str] | None = None,
        files_root: str | None = None,
    ):
        """Initialize with the Delta Lake root directory.

        Args:
            base_path: Root URI/path containing artifact and framework
                Delta tables (e.g. ``file_refs/``, ``data/``,
                ``artifact_index/``).
            fs: Filesystem implementation (LocalFileSystem, S3FileSystem,
                etc.). Defaults to ``LocalFileSystem()`` when None.
            storage_options: Credentials/config passed to delta-rs calls.
            files_root: Root URI/path for Artisan-managed external files.
                None when external file storage is not configured.
        """
        if fs is None:
            from fsspec.implementations.local import LocalFileSystem

            fs = LocalFileSystem()
        self.base_path = base_path
        self._fs = fs
        self._storage_options = storage_options or {}
        self.files_root = files_root
        self._provenance: ProvenanceStore | None = None
        assert_store_format(self.base_path, self._fs, self._storage_options)

    @property
    def provenance(self) -> ProvenanceStore:
        """Lazy-initialized provenance store for graph queries."""
        if self._provenance is None:
            self._provenance = ProvenanceStore(
                self.base_path, fs=self._fs, storage_options=self._storage_options
            )
        return self._provenance

    @property
    def filesystem(self) -> AbstractFileSystem:
        """Return the configured filesystem used for artifact I/O."""
        return self._fs

    def _table_path(self, table: TablePath) -> str:
        """Resolve the URI for a Delta table."""
        return uri_join(self.base_path, table)

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
            artifact_type: Expected type, checked against the artifact index.
            hydrate: If True, load content fields and verify external bytes.
                If False, return a minimal model with only the ID and
                type populated.

        Returns:
            Typed artifact model, or None if the ID is not found in
            the index or content table.

        Raises:
            ArtifactIntegrityError: If the indexed type conflicts with the
                expected type, persisted content is invalid, or an external
                artifact has no readable location with matching content.
        """
        stored_type = self.get_artifact_type(artifact_id)
        if stored_type is None:
            return None
        if artifact_type is not None and artifact_type != stored_type:
            msg = (
                f"Artifact {artifact_id} is indexed as {stored_type!r}, "
                f"not {artifact_type!r}"
            )
            raise ArtifactIntegrityError(msg)
        artifact_type = stored_type

        if not hydrate:
            model_cls = ArtifactTypeDef.get_model(artifact_type)
            return cast(
                "Artifact",
                model_cls(artifact_id=artifact_id, artifact_type=artifact_type),
            )

        table_path_str = ArtifactTypeDef.get_table_path(artifact_type)
        table_path = uri_join(self.base_path, table_path_str)

        if not self._fs.exists(table_path):
            return None

        result = (
            scan_committed(
                self.base_path,
                table_path_str,
                fs=self._fs,
                storage_options=self._storage_options,
            )
            .filter(pl.col("artifact_id") == artifact_id)
            .limit(1)
            .collect()
        )

        if result.is_empty():
            return None

        row = result.row(0, named=True)
        model_cls = ArtifactTypeDef.get_model(artifact_type)
        artifact = cast("Artifact", model_cls.from_row(row))  # type: ignore[attr-defined]
        self._attach_verified_location(artifact)
        return artifact

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

        Raises:
            ArtifactIntegrityError: If persisted content is invalid or an
                external artifact has no readable location with matching content.
        """
        if not artifact_ids:
            return {}

        table_path_str = ArtifactTypeDef.get_table_path(artifact_type)
        table_path = uri_join(self.base_path, table_path_str)

        if not self._fs.exists(table_path):
            return {}

        result = (
            scan_committed(
                self.base_path,
                table_path_str,
                fs=self._fs,
                storage_options=self._storage_options,
            )
            .filter(pl.col("artifact_id").is_in(artifact_ids))
            .collect()
        )

        if result.is_empty():
            return {}

        return self._hydrate_rows(artifact_type, result)

    def _hydrate_rows(
        self,
        artifact_type: str,
        rows: pl.DataFrame,
        locations: pl.DataFrame | None = None,
    ) -> dict[str, Artifact]:
        """Hydrate committed or staged rows through the same integrity boundary."""
        model_cls = ArtifactTypeDef.get_model(artifact_type)
        artifacts: dict[str, Artifact] = {}
        for row in rows.iter_rows(named=True):
            artifact = cast("Artifact", model_cls.from_row(row))  # type: ignore[attr-defined]
            # Artifacts loaded from storage are always finalized (artifact_id
            # is non-None).
            assert artifact.artifact_id is not None
            self._attach_verified_location(artifact, locations)
            artifacts[artifact.artifact_id] = artifact

        return artifacts

    def validate_staged_artifacts(
        self,
        frames: dict[str, pl.DataFrame],
        referenced_ids: set[str],
    ) -> dict[str, str]:
        """Validate candidate artifacts and references without publishing any rows."""
        index = self._candidate_rows(TablePath.ARTIFACT_INDEX.value, frames)
        locations = self._candidate_rows(TablePath.ARTIFACT_LOCATIONS.value, frames)
        for frame in frames.values():
            if "artifact_id" in frame.columns:
                referenced_ids.update(frame["artifact_id"].to_list())
        selected = index.filter(pl.col("artifact_id").is_in(referenced_ids))
        types: dict[str, str] = {}
        for artifact_id, artifact_type in selected.select(
            "artifact_id", "artifact_type"
        ).iter_rows():
            if types.setdefault(artifact_id, artifact_type) != artifact_type:
                msg = f"Conflicting staged artifact type for {artifact_id}"
                raise ArtifactIntegrityError(msg)
        missing = referenced_ids - types.keys()
        if missing:
            msg = f"Missing staged artifact references: {sorted(missing)!r}"
            raise ArtifactIntegrityError(msg)
        for artifact_type in sorted(set(types.values())):
            definition = ArtifactTypeDef.get(artifact_type)
            ids = {key for key, kind in types.items() if kind == artifact_type}
            content = self._candidate_rows(definition.table_path, frames).filter(
                pl.col("artifact_id").is_in(ids)
            )
            loaded = self._hydrate_rows(artifact_type, content, locations)
            if ids != loaded.keys():
                msg = f"Missing staged content for {sorted(ids - loaded.keys())!r}"
                raise ArtifactIntegrityError(msg)
        for definition in ArtifactTypeDef.get_all().values():
            candidate_frame = frames.get(definition.table_path)
            if candidate_frame is not None and any(
                types.get(value) != definition.key
                for value in candidate_frame["artifact_id"]
            ):
                msg = "Staged content disagrees with its artifact index"
                raise ArtifactIntegrityError(msg)
        return types

    def _candidate_rows(
        self, table: str, frames: dict[str, pl.DataFrame]
    ) -> pl.DataFrame:
        """Overlay candidate evidence for validation without changing stored data."""
        committed = read_committed(
            self.base_path, table, fs=self._fs, storage_options=self._storage_options
        )
        candidate = frames.get(table)
        if candidate is None:
            return committed
        return pl.concat(
            [committed, candidate.select(committed.columns)], how="vertical"
        ).unique(maintain_order=True)

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
        if not self._fs.exists(index_path):
            return None

        result = (
            scan_committed(
                self.base_path,
                TablePath.ARTIFACT_INDEX,
                fs=self._fs,
                storage_options=self._storage_options,
            )
            .filter(pl.col("artifact_id") == artifact_id)
            .select("artifact_type")
            .collect()
        )

        if result.is_empty():
            return None

        stored_types = set(result["artifact_type"].to_list())
        if len(stored_types) != 1:
            msg = (
                f"Artifact {artifact_id} has contradictory index types {stored_types!r}"
            )
            raise ArtifactIntegrityError(msg)
        return cast("str", next(iter(stored_types)))

    def load_type_map(self, artifact_ids: list[str]) -> dict[str, str]:
        """Bulk-load and validate unique type assignments for artifact IDs."""
        if not artifact_ids:
            return {}
        result = (
            scan_committed(
                self.base_path,
                TablePath.ARTIFACT_INDEX,
                fs=self._fs,
                storage_options=self._storage_options,
            )
            .filter(pl.col("artifact_id").is_in(artifact_ids))
            .select(["artifact_id", "artifact_type"])
            .collect()
        )
        type_map: dict[str, str] = {}
        for artifact_id, artifact_type in result.iter_rows():
            previous = type_map.setdefault(artifact_id, artifact_type)
            if previous != artifact_type:
                msg = (
                    f"Artifact {artifact_id} has contradictory index types "
                    f"{previous!r} and {artifact_type!r}"
                )
                raise ArtifactIntegrityError(msg)
        return type_map

    def _attach_verified_location(
        self, artifact: Artifact, candidate_locations: pl.DataFrame | None = None
    ) -> None:
        """Select and verify a deterministic location for an external artifact."""
        if not artifact.EXTERNALLY_BACKED:
            return
        assert artifact.artifact_id is not None
        locations_path = self._table_path(TablePath.ARTIFACT_LOCATIONS)
        if candidate_locations is not None:
            rows = candidate_locations.filter(
                pl.col("artifact_id") == artifact.artifact_id
            ).select("uri")
        elif self._fs.exists(locations_path):
            rows = (
                scan_committed(
                    self.base_path,
                    TablePath.ARTIFACT_LOCATIONS,
                    fs=self._fs,
                    storage_options=self._storage_options,
                )
                .filter(pl.col("artifact_id") == artifact.artifact_id)
                .select("uri")
                .collect()
            )
        else:
            rows = pl.DataFrame(schema={"uri": pl.String})
        locations = sorted(
            set(rows["uri"].to_list()),
            key=lambda uri: (not self._is_managed_location(uri), uri),
        )
        attempted: list[str] = []
        locator = next(iter(artifact.LOCATOR_FIELDS))
        for uri in locations:
            attempted.append(sanitized_uri(uri))
            try:
                validate_persistable_uri(uri)
            except ValueError as exc:
                msg = f"Persisted artifact location is invalid: {sanitized_uri(uri)!r}"
                raise ArtifactIntegrityError(msg) from exc
            setattr(artifact, locator, uri)
            try:
                artifact.verify_external_content(fs=self._fs)
            except ArtifactIntegrityError:
                raise
            except Exception:
                continue
            return
        msg = (
            f"External {artifact.artifact_type} artifact {artifact.artifact_id} "
            f"has no readable verified location; attempted {attempted!r}"
        )
        raise ArtifactIntegrityError(msg)

    def _is_managed_location(self, uri: str) -> bool:
        """Return whether a URI is under the configured managed files root."""
        if self.files_root is None:
            return False
        root = self.files_root.rstrip("/")
        return uri == root or uri.startswith(f"{root}/")

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
        table_path = uri_join(self.base_path, table_path_str)

        if not self._fs.exists(table_path):
            return empty

        result = (
            scan_committed(
                self.base_path,
                table_path_str,
                fs=self._fs,
                storage_options=self._storage_options,
            )
            .filter(pl.col("artifact_id").is_in(artifact_ids))
            .select(["artifact_id", "content"])
            .collect()
        )

        return result if not result.is_empty() else empty

    def load_original_names(self, artifact_ids: list[str]) -> dict[str, str]:
        """Bulk-load ``original_name`` values for the given artifact IDs.

        Used by name-based input pairing (``GroupByStrategy.NAME``) to
        obtain the source filenames needed for stem matching. Two-phase
        lookup: an index scan resolves each artifact's type, then per-type
        content tables are scanned with an ``is_in`` filter to fetch
        ``(artifact_id, original_name)`` pairs.

        Types whose schema does not declare ``original_name`` are skipped
        silently. Artifacts whose row exists but whose ``original_name``
        is ``None`` are also omitted.

        Args:
            artifact_ids: IDs to look up. An empty list returns ``{}``
                immediately without scanning.

        Returns:
            Mapping of artifact ID to ``original_name``. IDs missing
            from the index, missing from their content table, or with a
            null ``original_name`` are omitted.
        """
        if not artifact_ids:
            return {}

        type_map = self.provenance.load_type_map(artifact_ids)
        if not type_map:
            return {}

        ids_by_type: dict[str, list[str]] = {}
        for artifact_id, artifact_type in type_map.items():
            ids_by_type.setdefault(artifact_type, []).append(artifact_id)

        names: dict[str, str] = {}
        for artifact_type, ids in ids_by_type.items():
            schema = ArtifactTypeDef.get_schema(artifact_type)
            if "original_name" not in schema:
                continue

            table_path = uri_join(
                self.base_path, ArtifactTypeDef.get_table_path(artifact_type)
            )
            if not self._fs.exists(table_path):
                continue

            df = (
                scan_committed(
                    self.base_path,
                    ArtifactTypeDef.get_table_path(artifact_type),
                    fs=self._fs,
                    storage_options=self._storage_options,
                )
                .filter(pl.col("artifact_id").is_in(ids))
                .select(["artifact_id", "original_name"])
                .collect()
            )

            for row in df.iter_rows(named=True):
                name = row["original_name"]
                if name:
                    names[row["artifact_id"]] = name

        return names

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
            ready for ``StagingManager.stage_orchestrator_dataframe``.
        """

        data = {
            "artifact_id": [artifact_id],
            "artifact_type": [artifact_type],
            "origin_step_number": [step_number],
            "metadata": ["{}"],
        }
        return pl.DataFrame(data, schema=get_schema(TablePath.ARTIFACT_INDEX))

    def prepare_artifact_location_entry(
        self,
        artifact_id: str,
        uri: str,
    ) -> pl.DataFrame:
        """Build one ownerless global artifact-location row."""
        return pl.DataFrame(
            {"artifact_id": [artifact_id], "uri": [uri]},
            schema=get_schema(TablePath.ARTIFACT_LOCATIONS),
        )
