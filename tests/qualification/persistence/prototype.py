"""Single-controller acceptance proof using production publication and Delta I/O.

This isolated candidate does not register a production format or replace the
production committer. Exactly one controller owns its journal at a time; recovery
starts only after that controller exits. Delta commits serialize each durable
decision, including cancellation and the immutable winner claim.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from tempfile import TemporaryDirectory

import polars as pl
from fsspec import AbstractFileSystem
from qualification.lineage.projection import BoundaryProjection
from qualification.persistence.encoding import (
    DOMAIN,
    NATURAL_KEYS,
    SCHEMAS,
    Envelope,
    boundary_identity,
    decode_projection,
    read_delivery,
    stage_delivery,
)

from artisan.errors import StoreIntegrityError
from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.execution_config import ExecutionConfigArtifact
from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.storage.io.bounded_evidence import DiskTable
from artisan.storage.io.delta_files import DeltaSnapshot, append_table_files
from artisan.storage.io.publication import publish_immutable_bytes
from artisan.utils.hashing import canonical_json_bytes

__all__ = ["CandidateStore", "Envelope", "stage_delivery"]

JOURNAL_SCHEMA = {
    "sequence": pl.Int64,
    "kind": pl.String,
    "unit_id": pl.String,
    "attempt": pl.Int64,
    "generation": pl.Int64,
    "delivery_id": pl.String,
    "seal_digest": pl.String,
    "recipe_digest": pl.String,
    "software_digest": pl.String,
    "input_digest": pl.String,
    "directory": pl.String,
    "diagnostic": pl.Boolean,
}
INPUT_SCHEMA = {
    "artifact_id": pl.String,
    "artifact_type": pl.String,
    "content": pl.Binary,
}


class CandidateStore:
    """One serialized controller, a durable winner journal and exact table effects."""

    def __init__(
        self,
        root: str,
        *,
        fs: AbstractFileSystem,
        storage_options: dict[str, str] | None = None,
        checkpoint: Callable[[str], None] | None = None,
    ) -> None:
        self.root = root
        self.fs = fs
        self.storage_options = storage_options or {}
        self.checkpoint = checkpoint or (lambda _name: None)
        manifest = f"{root}/candidate-domain.json"
        expected = canonical_json_bytes({"domain": DOMAIN})
        if not fs.exists(manifest) and fs.exists(root) and fs.ls(root):
            msg = "Nonempty root is outside the candidate domain"
            raise StoreIntegrityError(msg)
        publish_immutable_bytes(fs, manifest, expected)
        schemas = {
            **{name: self._physical_schema(name) for name in SCHEMAS},
            "journal": JOURNAL_SCHEMA,
            "inputs": INPUT_SCHEMA,
        }
        for name, schema in schemas.items():
            path = f"{root}/{name}"
            if not fs.exists(f"{path}/_delta_log"):
                pl.DataFrame(schema=schema).write_delta(
                    path, storage_options=self.storage_options
                )
            self._snapshot(name, schema)

    def _physical_schema(self, name: str) -> dict[str, object]:
        return {**SCHEMAS[name], "logical_commit_id": pl.String}

    def _snapshot(self, name: str, schema: dict[str, object]) -> DeltaSnapshot:
        return DeltaSnapshot(
            f"{self.root}/{name}",
            schema,
            fs=self.fs,
            storage_options=self.storage_options,
        )

    def _read(self, name: str, schema: dict[str, object]) -> pl.DataFrame:
        frames = [frame for _, _, frame in self._snapshot(name, schema).iter_located()]
        return pl.concat(frames) if frames else pl.DataFrame(schema=schema)

    def _append(self, name: str, frame: pl.DataFrame, keys: list[str]) -> None:
        append_table_files(
            self._snapshot(name, dict(frame.schema)),
            [frame],
            step_number=0,
            statistics_columns=keys,
        )

    def journal(self) -> pl.DataFrame:
        """Read controller decisions in their durable serialization order."""
        journal = self._read("journal", JOURNAL_SCHEMA).sort("sequence")
        if journal["sequence"].to_list() != list(range(journal.height)):
            msg = "Controller journal has duplicate or missing decisions"
            raise StoreIntegrityError(msg)
        active, winners, completed = {}, {}, set()
        cancelled = set()
        for row in journal.to_dicts():
            unit, kind = row["unit_id"], row["kind"]
            if kind == "active":
                if unit in winners or (
                    unit in active and row["generation"] <= active[unit]["generation"]
                ):
                    msg = "Controller journal advances a claimed or obsolete unit"
                    raise StoreIntegrityError(msg)
                if unit in active and any(
                    row[field] != active[unit][field]
                    for field in (
                        "recipe_digest",
                        "software_digest",
                        "input_digest",
                        "diagnostic",
                    )
                ):
                    msg = "Retry changed its frozen assignment identity"
                    raise StoreIntegrityError(msg)
                active[unit] = row
                cancelled.discard(unit)
            elif kind == "cancel":
                self._same_attempt(row, active.get(unit))
                cancelled.add(unit)
            elif kind == "claim":
                self._same_attempt(row, active.get(unit))
                if (
                    unit in winners
                    or unit in cancelled
                    or not row["seal_digest"]
                    or row["input_digest"] != active[unit]["input_digest"]
                ):
                    msg = "Controller journal contains an ineligible winner"
                    raise StoreIntegrityError(msg)
                winners[unit] = row
            elif kind == "complete":
                winner = winners.get(unit)
                self._same_attempt(row, winner)
                if (
                    unit in completed
                    or winner is None
                    or any(
                        row[field] != winner[field]
                        for field in ("delivery_id", "seal_digest")
                    )
                ):
                    msg = "Completion has no unique matching winner"
                    raise StoreIntegrityError(msg)
                completed.add(unit)
            else:
                msg = "Unknown controller decision"
                raise StoreIntegrityError(msg)
        return journal

    @staticmethod
    def _same_attempt(row: dict[str, object], active: dict[str, object] | None) -> None:
        fields = (
            "attempt",
            "generation",
            "recipe_digest",
            "software_digest",
            "diagnostic",
        )
        if active is None or any(row[field] != active[field] for field in fields):
            msg = "Controller decision refers to a different active attempt"
            raise StoreIntegrityError(msg)

    def _events(self, unit_id: str) -> list[dict[str, object]]:
        return self.journal().filter(pl.col("unit_id") == unit_id).to_dicts()

    def _record(self, kind: str, envelope: Envelope, **values: object) -> None:
        journal = self.journal()
        row = {
            "sequence": journal.height,
            "kind": kind,
            "unit_id": envelope.unit_id,
            "attempt": envelope.attempt,
            "generation": envelope.generation,
            "delivery_id": envelope.delivery_id,
            "seal_digest": "",
            "recipe_digest": envelope.recipe_digest,
            "software_digest": envelope.software_digest,
            "input_digest": "",
            "directory": "",
            "diagnostic": envelope.diagnostic,
            **values,
        }
        self._append(
            "journal", pl.DataFrame([row], schema=JOURNAL_SCHEMA), ["sequence"]
        )

    def seed_inputs(self, artifacts: dict[str, tuple[str, bytes]]) -> None:
        """Register trusted preexisting input content for the storage experiment."""
        existing = {
            row["artifact_id"]: row
            for row in self._read("inputs", INPUT_SCHEMA).to_dicts()
        }
        rows = []
        for artifact_id, (artifact_type, content) in artifacts.items():
            self._validate_artifact(artifact_id, artifact_type, content)
            row = {
                "artifact_id": artifact_id,
                "artifact_type": artifact_type,
                "content": content,
            }
            if artifact_id in existing and existing[artifact_id] != row:
                msg = "Conflicting trusted boundary content"
                raise StoreIntegrityError(msg)
            if artifact_id not in existing:
                rows.append(row)
        if rows:
            self._append(
                "inputs", pl.DataFrame(rows, schema=INPUT_SCHEMA), ["artifact_id"]
            )

    def _validate_artifact(
        self,
        artifact_id: str,
        artifact_type: str,
        content: bytes,
        directory: str | None = None,
    ) -> Artifact:
        model = ArtifactTypeDef.get(artifact_type).model
        artifact = model.model_validate_json(content)
        if (
            artifact.artifact_id != artifact_id
            or artifact.artifact_type != artifact_type
        ):
            msg = "Transport content identity disagrees with occurrence membership"
            raise StoreIntegrityError(msg)
        artifact.to_row()
        if artifact.EXTERNALLY_BACKED:
            if directory is not None:
                filename = f"file-{artifact_id}.bin"
                if artifact.path != filename:
                    msg = "External locator is not the sealed content object"
                    raise StoreIntegrityError(msg)
                artifact.path = f"{directory}/{filename}"
            artifact.verify_external_content(fs=self.fs)
        return artifact

    def activate(self, envelope: Envelope, projection: BoundaryProjection) -> None:
        """Bind the current attempt to immutable recipe, software and input context."""
        events = self._events(envelope.unit_id)
        if any(row["kind"] == "claim" for row in events):
            msg = "A claimed unit cannot acquire another attempt"
            raise StoreIntegrityError(msg)
        active = [row for row in events if row["kind"] == "active"]
        if active and envelope.generation <= active[-1]["generation"]:
            msg = "Attempt generation must advance"
            raise StoreIntegrityError(msg)
        if active and (
            active[-1]["input_digest"] != boundary_identity(projection)
            or any(
                active[-1][field] != getattr(envelope, field)
                for field in ("recipe_digest", "software_digest", "diagnostic")
            )
        ):
            msg = "Retry changed its frozen assignment identity"
            raise StoreIntegrityError(msg)
        self._record("active", envelope, input_digest=boundary_identity(projection))

    def cancel(self, unit_id: str) -> None:
        """Order cancellation after all decisions already committed by this controller."""
        events = self._events(unit_id)
        active = next(row for row in reversed(events) if row["kind"] == "active")
        self._record("cancel", self._envelope(active))

    @staticmethod
    def _envelope(row: dict[str, object]) -> Envelope:
        fields = (
            "unit_id",
            "attempt",
            "generation",
            "delivery_id",
            "recipe_digest",
            "software_digest",
            "diagnostic",
        )
        return Envelope.model_validate({key: row[key] for key in fields})

    def _validate_membership(
        self,
        projection: BoundaryProjection,
        tables: dict[str, pl.DataFrame],
        directory: str,
    ) -> None:
        data = projection.model_dump(mode="json")
        boundary = projection.boundary_keys()
        occurrences = {row["key"]: row for row in data["occurrences"]}
        inputs = {
            row["artifact_id"]: row
            for row in self._read("inputs", INPUT_SCHEMA).to_dicts()
        }
        boundary_ids = {occurrences[key]["artifact_id"] for key in boundary}
        for key in boundary:
            occurrence = occurrences[key]
            existing = inputs.get(occurrence["artifact_id"])
            if (
                existing is None
                or existing["artifact_type"] != occurrence["artifact_type"]
            ):
                msg = "Boundary input is missing or has the wrong type"
                raise StoreIntegrityError(msg)
            self._validate_artifact(
                occurrence["artifact_id"],
                occurrence["artifact_type"],
                existing["content"],
            )
        expected = {
            row["artifact_id"]
            for key, row in occurrences.items()
            if key not in boundary
        } - boundary_ids
        actual = tables["artifacts"]["artifact_id"].to_list()
        if set(actual) != expected or len(actual) != len(expected):
            msg = "Retained payload membership is not exact"
            raise StoreIntegrityError(msg)
        types: dict[str, str] = {}
        for occurrence in occurrences.values():
            artifact_id, artifact_type = (
                occurrence["artifact_id"],
                occurrence["artifact_type"],
            )
            if artifact_id in types and types[artifact_id] != artifact_type:
                msg = "One content identity has conflicting artifact types"
                raise StoreIntegrityError(msg)
            types[artifact_id] = artifact_type
        for artifact_id, content in tables["artifacts"].iter_rows():
            self._validate_artifact(artifact_id, types[artifact_id], content, directory)
        payloads = {
            key: row["content"] for key, row in inputs.items() if key in boundary_ids
        }
        payloads.update(dict(tables["artifacts"].iter_rows()))
        self._validate_references(projection, payloads)

    @staticmethod
    def _validate_references(
        projection: BoundaryProjection, payloads: dict[str, bytes]
    ) -> None:
        occurrences = {item.key: item for item in projection.occurrences}
        for occurrence in projection.occurrences:
            model = ArtifactTypeDef.get(occurrence.artifact_type).model
            artifact = model.model_validate_json(payloads[occurrence.artifact_id])
            if isinstance(artifact, ExecutionConfigArtifact):
                required = set(artifact.get_artifact_references())
            elif occurrence.artifact_type in {"csv", "metric", "file_ref"}:
                required = set()
            else:
                msg = "Candidate artifact type has no required-reference policy"
                raise StoreIntegrityError(msg)
            actual = {
                occurrences[edge.target].artifact_id
                for edge in projection.references
                if edge.source == occurrence.key
            }
            if (
                isinstance(artifact, ExecutionConfigArtifact)
                and occurrence.key in projection.boundary_keys()
            ):
                actual.update(
                    occurrences[key].artifact_id
                    for key in projection.frozen_context.referenced(occurrence.key)
                )
            if actual != required:
                msg = "Required-reference membership disagrees with artifact content"
                raise StoreIntegrityError(msg)

    def _claim(self, directory: str) -> tuple[str, Envelope]:
        seal, digest, tables, _ = read_delivery(directory, fs=self.fs)
        envelope = seal.envelope
        projection = decode_projection(tables)
        self._validate_membership(projection, tables, directory)
        events = self._events(envelope.unit_id)
        winners = [row for row in events if row["kind"] == "claim"]
        if winners:
            if winners[0]["seal_digest"] != digest:
                msg = "Another delivery already owns this unit"
                raise StoreIntegrityError(msg)
            return digest, envelope
        if not events or events[-1]["kind"] != "active":
            msg = "Unit is cancelled or has no active attempt"
            raise StoreIntegrityError(msg)
        active = events[-1]
        for field in (
            "attempt",
            "generation",
            "recipe_digest",
            "software_digest",
            "diagnostic",
        ):
            if active[field] != getattr(envelope, field):
                msg = f"Obsolete or mismatched assignment {field}"
                raise StoreIntegrityError(msg)
        if active["input_digest"] != boundary_identity(projection):
            msg = "Frozen boundary context changed after admission"
            raise StoreIntegrityError(msg)
        self._record(
            "claim",
            envelope,
            seal_digest=digest,
            directory=directory,
            input_digest=active["input_digest"],
        )
        self.checkpoint("claim")
        return digest, envelope

    def accept(self, directory: str) -> str:
        """Claim one complete delivery, then publish only that immutable result."""
        if not self.fs.exists(f"{directory}/seal.json"):
            prior = self.journal().filter(
                (pl.col("kind") == "claim") & (pl.col("directory") == directory)
            )
            if prior.height == 1 and self._complete(prior["seal_digest"].item()):
                digest = prior["seal_digest"].item()
                self._finish(digest)
                return digest
        digest, _ = self._claim(directory)
        self._finish(digest)
        return digest

    def _winner(self, digest: str) -> dict[str, object]:
        rows = self.journal().filter(
            (pl.col("kind") == "claim") & (pl.col("seal_digest") == digest)
        )
        if rows.height != 1:
            msg = "Exactly one controller claim is required"
            raise StoreIntegrityError(msg)
        return rows.row(0, named=True)

    def _complete(self, digest: str) -> bool:
        return (
            not self.journal()
            .filter((pl.col("kind") == "complete") & (pl.col("seal_digest") == digest))
            .is_empty()
        )

    def _finish(self, digest: str) -> None:
        winner = self._winner(digest)
        if self._complete(digest):
            self.result(str(winner["unit_id"]))
            self._cleanup(str(winner["directory"]))
            return
        evidence = f"{self.root}/evidence/{digest}"
        source = (
            evidence
            if self.fs.exists(f"{evidence}/seal.json")
            else str(winner["directory"])
        )
        seal, observed, tables, payloads = read_delivery(source, fs=self.fs)
        if observed != digest or seal.envelope != self._envelope(winner):
            msg = "Claimed evidence changed"
            raise StoreIntegrityError(msg)
        self._validate_membership(decode_projection(tables), tables, source)
        for filename, content in payloads.items():
            if filename != "seal.json":
                publish_immutable_bytes(self.fs, f"{evidence}/{filename}", content)
        publish_immutable_bytes(self.fs, f"{evidence}/seal.json", payloads["seal.json"])
        with TemporaryDirectory(prefix="candidate-evidence-") as scratch:
            effects = self._effects(payloads, scratch)
            plan = {
                "seal_digest": digest,
                "tables": {
                    name: {
                        "keys": effect.keys,
                        "natural_key": effect.natural_key,
                        "rows": effect.row_count,
                        "table_plan_key": effect.canonical_key(digest),
                    }
                    for name, effect in effects.items()
                },
            }
            publish_immutable_bytes(
                self.fs, f"{self.root}/plans/{digest}.json", canonical_json_bytes(plan)
            )
            self.checkpoint("planned")
            for name, effect in effects.items():
                self._commit_effect(name, digest, effect, tables[name], scratch)
                self.checkpoint(f"table:{name}")
        if not self._complete(digest):
            self._record("complete", seal.envelope, seal_digest=digest)
        self.checkpoint("complete")
        self._cleanup(str(winner["directory"]))

    def _cleanup(self, directory: str) -> None:
        if self.fs.exists(directory):
            self.fs.rm(directory, recursive=True)
        self.checkpoint("cleanup")

    @staticmethod
    def _effects(payloads: dict[str, bytes], scratch: str) -> dict[str, DiskTable]:
        effects = {}
        for name, schema in SCHEMAS.items():
            path = Path(scratch) / f"{name}.parquet"
            path.write_bytes(payloads[path.name])
            effects[name] = DiskTable(
                name,
                [str(path)],
                scratch,
                schema=schema,
                natural_key=NATURAL_KEYS[name],
            )
        return effects

    def _commit_effect(
        self,
        name: str,
        digest: str,
        expected: DiskTable,
        frame: pl.DataFrame,
        scratch: str,
    ) -> None:
        physical = self._read(name, self._physical_schema(name))
        existing = physical.filter(pl.col("logical_commit_id") == digest).drop(
            "logical_commit_id"
        )
        if existing.is_empty() and frame.height:
            self._append(
                name,
                frame.with_columns(pl.lit(digest).alias("logical_commit_id")),
                ["logical_commit_id", *NATURAL_KEYS[name]],
            )
            physical = self._read(name, self._physical_schema(name))
            existing = physical.filter(pl.col("logical_commit_id") == digest).drop(
                "logical_commit_id"
            )
        if existing.height:
            path = str(Path(scratch) / f"observed-{name}.parquet")
            existing.write_parquet(path)
            observed = DiskTable(
                name,
                [path],
                scratch,
                schema=SCHEMAS[name],
                natural_key=NATURAL_KEYS[name],
            )
            if observed.keys != expected.keys or observed.canonical_key(
                digest
            ) != expected.canonical_key(digest):
                msg = f"Conflicting committed effect {name}"
                raise StoreIntegrityError(msg)
            return
        if expected.row_count:
            msg = f"Missing committed effect {name}"
            raise StoreIntegrityError(msg)

    def recover(self) -> None:
        """Recover claimed ordinary owners; never infer permission from a late seal."""
        claims = self.journal().filter(
            (pl.col("kind") == "claim") & ~pl.col("diagnostic")
        )
        for digest in claims["seal_digest"]:
            self._finish(digest)

    def recover_diagnostic(self, unit_id: str) -> None:
        """Finish only the explicitly selected diagnostic owner's publication."""
        claims = self.journal().filter(
            (pl.col("kind") == "claim")
            & pl.col("diagnostic")
            & (pl.col("unit_id") == unit_id)
        )
        for digest in claims["seal_digest"]:
            self._finish(digest)

    def result(self, unit_id: str) -> BoundaryProjection | None:
        """Read only a complete winner and verify its exact physical table effects."""
        winners = [row for row in self._events(unit_id) if row["kind"] == "claim"]
        if not winners or not self._complete(str(winners[0]["seal_digest"])):
            return None
        digest = str(winners[0]["seal_digest"])
        seal, observed, expected, _ = read_delivery(
            f"{self.root}/evidence/{digest}", fs=self.fs
        )
        if observed != digest or seal.envelope != self._envelope(winners[0]):
            msg = "Accepted evidence no longer matches its winner"
            raise StoreIntegrityError(msg)
        actual = {}
        for name in SCHEMAS:
            frame = (
                self._read(name, self._physical_schema(name))
                .filter(pl.col("logical_commit_id") == digest)
                .drop("logical_commit_id")
            )
            keys = list(NATURAL_KEYS[name])
            if not frame.sort(keys).equals(expected[name].sort(keys)):
                msg = f"Accepted table changed: {name}"
                raise StoreIntegrityError(msg)
            actual[name] = frame
        projection = decode_projection(actual)
        self._validate_membership(projection, actual, f"{self.root}/evidence/{digest}")
        return projection

    def hydrate(self, unit_id: str, artifact_id: str) -> Artifact:
        """Hydrate accepted content solely from durable evidence, including FileRefs."""
        projection = self.result(unit_id)
        if projection is None:
            msg = "Unit has no complete accepted result"
            raise StoreIntegrityError(msg)
        occurrence = next(
            item for item in projection.occurrences if item.artifact_id == artifact_id
        )
        winner = next(row for row in self._events(unit_id) if row["kind"] == "claim")
        digest = str(winner["seal_digest"])
        rows = self._read("artifacts", self._physical_schema("artifacts")).filter(
            (pl.col("logical_commit_id") == digest)
            & (pl.col("artifact_id") == artifact_id)
        )
        if rows.is_empty() and any(
            item.artifact_id == artifact_id and item.key in projection.boundary_keys()
            for item in projection.occurrences
        ):
            inputs = self._read("inputs", INPUT_SCHEMA).filter(
                pl.col("artifact_id") == artifact_id
            )
            return self._validate_artifact(
                artifact_id, occurrence.artifact_type, inputs["content"].item()
            )
        return self._validate_artifact(
            artifact_id,
            occurrence.artifact_type,
            rows["content"].item(),
            f"{self.root}/evidence/{digest}",
        )

    def cache(
        self, envelope: Envelope, projection: BoundaryProjection
    ) -> list[BoundaryProjection]:
        """Apply the same winner/completion eligibility used by ordinary readers."""
        claims = self.journal().filter(
            (pl.col("kind") == "claim")
            & ~pl.col("diagnostic")
            & (pl.col("recipe_digest") == envelope.recipe_digest)
            & (pl.col("software_digest") == envelope.software_digest)
            & (pl.col("input_digest") == boundary_identity(projection))
        )
        return [
            result
            for unit in claims["unit_id"]
            if (result := self.result(unit)) is not None
        ]
