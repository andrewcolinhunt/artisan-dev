"""Closed Parquet encoding for exact retained occurrences and their payloads."""

from __future__ import annotations

import io
import re
from collections.abc import Callable
from typing import Literal

import polars as pl
from fsspec import AbstractFileSystem
from fsspec.core import url_to_fs
from pydantic import BaseModel, ConfigDict, Field, JsonValue
from qualification.lineage.projection import BoundaryProjection

from artisan.errors import StoreIntegrityError
from artisan.schemas.artifact.file_ref import FileRefArtifact
from artisan.storage.io.publication import (
    is_publication_temporary,
    publish_immutable_bytes,
    publish_immutable_file,
)
from artisan.utils.hashing import canonical_json_bytes, compute_content_digest

DOMAIN = "retained-result-qualification-v1"
IDENTIFIER = r"^[a-zA-Z0-9_-]+$"


class Envelope(BaseModel):
    """Assignment identity and required successful-runtime evidence."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)
    domain: Literal["retained-result-qualification-v1"] = DOMAIN
    unit_id: str = Field(pattern=IDENTIFIER)
    attempt: int = Field(ge=0)
    generation: int = Field(ge=0)
    delivery_id: str = Field(pattern=IDENTIFIER)
    recipe_digest: str
    software_digest: str
    diagnostic: bool = False
    computation_complete: Literal[True] = True
    cleanup_complete: Literal[True] = True


class FileEvidence(BaseModel):
    """An exact table object in the closed seal inventory."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)
    filename: str
    digest: str = Field(pattern=r"^[0-9a-f]{32}$")
    size: int = Field(ge=0)
    rows: int = Field(ge=0)


class Seal(BaseModel):
    """The final immutable object published after every retained payload."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)
    envelope: Envelope
    files: list[FileEvidence]


SCHEMAS = {
    "artifacts": {"artifact_id": pl.String, "content": pl.Binary},
    "occurrences": {
        "key": pl.String,
        "artifact_id": pl.String,
        "artifact_type": pl.String,
    },
    "roles": {"kind": pl.String, "position": pl.Int64, "role": pl.String},
    "memberships": {
        "kind": pl.String,
        "role": pl.String,
        "position": pl.Int64,
        "occurrence": pl.String,
    },
    "relations": {
        "kind": pl.String,
        "position": pl.Int64,
        "source": pl.String,
        "target": pl.String,
    },
    "context": {"key": pl.String, "json": pl.String},
}
NATURAL_KEYS = {
    "artifacts": ("artifact_id",),
    "occurrences": ("key",),
    "roles": ("kind", "position"),
    "memberships": ("kind", "role", "position"),
    "relations": ("kind", "position"),
    "context": ("key",),
}


def boundary_identity(projection: BoundaryProjection) -> str:
    """Bind ordered boundary attribution and the explicitly captured context."""
    data = projection.model_dump(mode="json")
    boundary = projection.boundary_keys()
    identity = {
        "boundary_inputs": data["boundary_inputs"],
        "occurrences": sorted(
            (row for row in data["occurrences"] if row["key"] in boundary),
            key=lambda row: row["key"],
        ),
        "frozen_context": data["frozen_context"],
    }
    return compute_content_digest(canonical_json_bytes(identity))


def encode_projection(
    projection: BoundaryProjection, artifacts: dict[str, bytes]
) -> dict[str, pl.DataFrame]:
    """Encode order as explicit positions rather than Parquet physical order."""
    data = projection.model_dump(mode="json")
    rows: dict[str, list[dict[str, object]]] = {name: [] for name in SCHEMAS}
    rows["artifacts"] = [
        {"artifact_id": key, "content": content}
        for key, content in sorted(artifacts.items())
    ]
    rows["occurrences"] = data["occurrences"]
    for kind, field in (("input", "boundary_inputs"), ("export", "exports")):
        for position, (role, members) in enumerate(data[field].items()):
            rows["roles"].append({"kind": kind, "position": position, "role": role})
            rows["memberships"].extend(
                {"kind": kind, "role": role, "position": index, "occurrence": key}
                for index, key in enumerate(members)
            )
    rows["memberships"].extend(
        {"kind": "support", "role": "", "position": index, "occurrence": key}
        for index, key in enumerate(data["support"])
    )
    for kind in ("parents", "associations", "references"):
        rows["relations"].extend(
            {"kind": kind, "position": index, **edge}
            for index, edge in enumerate(data[kind])
        )
    rows["context"] = [
        {
            "key": "frozen_context",
            "json": canonical_json_bytes(data["frozen_context"]).decode(),
        }
    ]
    return {
        name: pl.DataFrame(rows[name], schema=schema)
        for name, schema in SCHEMAS.items()
    }


def decode_projection(tables: dict[str, pl.DataFrame]) -> BoundaryProjection:
    """Rebuild the exact boundary using explicit role-local indices."""
    import json

    data: dict[str, object] = {"boundary_inputs": {}, "exports": {}}
    members = tables["memberships"]
    for kind, field in (("input", "boundary_inputs"), ("export", "exports")):
        roles = tables["roles"].filter(pl.col("kind") == kind).sort("position")
        data[field] = {
            role: members.filter((pl.col("kind") == kind) & (pl.col("role") == role))
            .sort("position")["occurrence"]
            .to_list()
            for role in roles["role"]
        }
    data["support"] = (
        members.filter(pl.col("kind") == "support")
        .sort("position")["occurrence"]
        .to_list()
    )
    data["occurrences"] = tables["occurrences"].sort("key").to_dicts()
    for kind in ("parents", "associations", "references"):
        data[kind] = (
            tables["relations"]
            .filter(pl.col("kind") == kind)
            .sort("position")
            .select("source", "target")
            .to_dicts()
        )
    context = tables["context"]
    if context.height != 1 or context["key"].item() != "frozen_context":
        msg = "Missing or repeated frozen context"
        raise StoreIntegrityError(msg)
    data["frozen_context"] = json.loads(context["json"].item())
    projection = BoundaryProjection.model_validate(data)
    canonical = encode_projection(projection, dict(tables["artifacts"].iter_rows()))
    for name, expected in canonical.items():
        keys = list(NATURAL_KEYS[name])
        if not expected.sort(keys).equals(tables[name].sort(keys)):
            msg = f"Noncanonical projection table {name}"
            raise StoreIntegrityError(msg)
    return projection


def stage_delivery(
    directory: str,
    envelope: Envelope,
    projection: dict[str, JsonValue] | BoundaryProjection,
    artifacts: dict[str, bytes],
    *,
    fs: AbstractFileSystem,
    checkpoint: Callable[[str], None] | None = None,
) -> str:
    """Transfer complete tables first, then atomically publish the closed seal."""
    projection = BoundaryProjection.model_validate(projection)
    artifacts, files = _stage_external(directory, projection, artifacts, fs)
    tables = encode_projection(projection, artifacts)
    for name, frame in tables.items():
        stream = io.BytesIO()
        frame.write_parquet(stream)
        content = stream.getvalue()
        filename = f"{name}.parquet"
        publish_immutable_bytes(fs, f"{directory}/{filename}", content)
        files.append(
            FileEvidence(
                filename=filename,
                digest=compute_content_digest(content),
                size=len(content),
                rows=frame.height,
            )
        )
        if checkpoint is not None:
            checkpoint(f"staged:{name}")
    seal = Seal(envelope=envelope, files=files)
    content = canonical_json_bytes(seal.model_dump(mode="json"))
    publish_immutable_bytes(fs, f"{directory}/seal.json", content)
    return compute_content_digest(content)


def _stage_external(
    directory: str,
    projection: BoundaryProjection,
    artifacts: dict[str, bytes],
    fs: AbstractFileSystem,
) -> tuple[dict[str, bytes], list[FileEvidence]]:
    """Transfer FileRef bytes before replacing the worker locator with a sealed name."""
    portable, files = dict(artifacts), []
    external_ids = {
        item.artifact_id
        for item in projection.occurrences
        if item.artifact_type == "file_ref"
    }
    for artifact_id in sorted(external_ids & artifacts.keys()):
        artifact = FileRefArtifact.model_validate_json(artifacts[artifact_id])
        artifact.to_row()
        source_fs, _ = url_to_fs(artifact.path)
        artifact.verify_external_content(fs=source_fs)
        filename = f"file-{artifact_id}.bin"
        publish_immutable_file(
            fs, f"{directory}/{filename}", artifact.path, source_fs=source_fs
        )
        files.append(
            FileEvidence(
                filename=filename,
                digest=artifact.content_hash,
                size=artifact.size_bytes,
                rows=0,
            )
        )
        artifact.path = filename
        portable[artifact_id] = artifact.model_dump_json().encode()
    return portable, files


def read_delivery(
    directory: str, *, fs: AbstractFileSystem
) -> tuple[Seal, str, dict[str, pl.DataFrame], dict[str, bytes]]:
    """Capture and verify the entire inventory without trusting object listings."""
    with fs.open(f"{directory}/seal.json", "rb") as stream:
        content = stream.read()
    seal = Seal.model_validate_json(content)
    expected = {f"{name}.parquet" for name in SCHEMAS}
    names = [item.filename for item in seal.files]
    if (
        len(names) != len(set(names))
        or not expected <= set(names)
        or any(
            re.fullmatch(r"file-[0-9a-f]{32}\.bin", name) is None
            for name in set(names) - expected
        )
    ):
        msg = "Seal must name exactly the registered tables and safe external objects"
        raise StoreIntegrityError(msg)
    actual = {str(path).rsplit("/", 1)[-1] for path in fs.ls(directory, detail=False)}
    extras = actual - set(names) - {"seal.json"}
    if any(not is_publication_temporary(name) for name in extras):
        msg = "Unexpected object outside the closed seal"
        raise StoreIntegrityError(msg)
    tables, payloads = {}, {"seal.json": content}
    for item in seal.files:
        with fs.open(f"{directory}/{item.filename}", "rb") as stream:
            payload = stream.read()
        if len(payload) != item.size or compute_content_digest(payload) != item.digest:
            msg = f"Changed sealed payload {item.filename}"
            raise StoreIntegrityError(msg)
        payloads[item.filename] = payload
        if item.filename not in expected:
            if item.rows != 0:
                msg = "External file cannot declare Parquet rows"
                raise StoreIntegrityError(msg)
            continue
        name = item.filename.removesuffix(".parquet")
        frame = pl.read_parquet(io.BytesIO(payload))
        if dict(frame.schema) != SCHEMAS[name] or frame.height != item.rows:
            msg = f"Changed sealed schema/count {name}"
            raise StoreIntegrityError(msg)
        tables[name] = frame
    projection = decode_projection(tables)
    external = {
        f"file-{item.artifact_id}.bin"
        for item in projection.occurrences
        if item.artifact_type == "file_ref"
        and item.artifact_id in set(tables["artifacts"]["artifact_id"])
    }
    if set(names) != expected | external:
        msg = "External file inventory disagrees with retained content membership"
        raise StoreIntegrityError(msg)
    return seal, compute_content_digest(content), tables, payloads
