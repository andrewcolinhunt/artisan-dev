---
name: artifact-write
description: Write, scaffold, or review a custom Artisan artifact type, including its Artifact model, ArtifactTypeDef registration, persistence schema, and lifecycle tests. Use when adding or changing a domain artifact type; ordinary operation outputs using existing artifact types do not require this skill.
---

# Write an Artisan artifact type

Preserve the requested domain package and representation. First check whether
an existing artifact type fits; a new file extension alone does not require a
new type.

## Find the current contract

Resolve these paths relative to this skill's canonical package, not the user's
working directory:

- [Custom-artifact guide](../../docs/how-to-guides/creating-artifact-types.md):
  the detailed model, registration, serialization, and testing example.
- [Artifact base](../../src/artisan/schemas/artifact/base.py) and
  [registry](../../src/artisan/schemas/artifact/registry.py): read before authoring.
- [DataArtifact](../../src/artisan/schemas/artifact/data.py): embedded bytes,
  derived descriptors, and structured column encoding. Inspect the nearest
  existing model for other representations.
- [Identity and hydration](../../docs/concepts/artifacts-and-content-addressing.md):
  read when separating semantic content, descriptors, and locations.

If bundled source is unavailable, locate the installed implementation with
`inspect.getfile(Artifact)` and `inspect.getfile(ArtifactTypeDef)` after importing
them from `artisan.schemas`. Also inspect the downstream package's existing
models and exports. Source inspection does not make internal helpers public:
authoring imports come from `artisan.schemas`, `artisan.storage`, and
`artisan.utils`, as applicable. The guide's conceptual `canonical_json_bytes`
calls are not a public helper to import.

## Authoring decisions

- **Registration and workers:** Keep the model and its `ArtifactTypeDef` in the
  domain package. Give them a unique, matching type key and frozen
  `artifact_type` default, plus an `artifacts/<unique_name>` table path.
  Registration happens on import; no enum edit or manual registry mutation is
  needed. Export the model and import it from the producing operation's module
  or package initialization. Every execution environment must be able to import
  that package. An import only in the orchestration script is insufficient for
  fresh workers. Import registrations before creating or opening the store.
- **Reference state:** Construction with only `artifact_id` and `artifact_type`
  must work. Content, names, descriptors, and locators normally default to
  `None`; the draft factory supplies hydrated values. Validators must accept
  legitimate ID-only references.
- **Persisted fields:** `POLARS_SCHEMA` includes `artifact_id`,
  `origin_step_number`, `metadata`, and every durable domain field. The registry's
  table association supplies the type discriminator. Exclude `materialized_path`,
  embedded source paths, and external locators from content rows.
- **Physical schema:** Follow built-ins: `pl.String` for IDs, names, and
  JSON-encoded metadata; `pl.Int32` for origin steps; `pl.Binary` for content;
  `pl.Int64` for byte sizes. Use supported scalar columns for other fields.
  Encode structured fields as JSON strings instead of nested List/Struct
  columns. Stores enforce exact schemas: use a fresh Delta root when adding a
  type or changing its schema; registration does not migrate an existing store.
- **Drafts and identity:** A factory accepts payload, name where applicable,
  and step number, derives descriptors, and returns an unfinalized `cls`.
  Operations return drafts for normal name derivation and finalization.
  Inherit `finalize()`; never assign an ID or hash directly into `artifact_id`.
  Embedded `content` bytes use the default `_identity_payload()`. Additional
  independently meaningful fields must enter a deterministic type-owned
  payload. Validate content-derived fields in `_validate_identity_descriptors()`.
  The framework adds the concrete type, declared name/extension, and metadata
  to identity; `origin_step_number` is protected occurrence information and
  does not affect identity.
- **JSON and rows:** Domain-owned JSON encoding must define accepted values and
  deterministic key ordering. `JsonContentMixin` from `artisan.schemas` adds
  cached decoded `values`; it does not canonicalize encoding. Inherit `to_row()`
  and `from_row()`. Extend `_row_encoders()` / `_row_decoders()` only for needed
  conversions, merging `super()` to preserve metadata handling.
- **Materialization and mutation:** Implement
  `_materialize_content(directory, *, fs=None)` for the representation. Embedded
  types require content and a finalized ID, write an ID-based filename, set
  only `materialized_path`, and return the path. Finish metadata, names, and
  descriptors before finalizing. Durable fields then become immutable; create
  another draft to change them. Hydrate references before serialization or
  materialization.

## Compact embedded example

Adapt the names and payload validation to the domain. This complete model uses
the framework's identity and row conversion without additional hooks:

```python
from __future__ import annotations

from pathlib import Path
from typing import Any, ClassVar, Self

import polars as pl
from pydantic import Field

from artisan.schemas import Artifact, ArtifactTypeDef, get_compound_extension
from artisan.utils import strip_extensions


class BinaryArtifact(Artifact):
    """Embedded binary content with an original filename."""

    POLARS_SCHEMA: ClassVar[dict[str, type[pl.DataType]]] = {
        "artifact_id": pl.String,
        "origin_step_number": pl.Int32,
        "content": pl.Binary,
        "original_name": pl.String,
        "extension": pl.String,
        "metadata": pl.String,
    }

    artifact_type: str = Field(default="domain_binary", frozen=True)
    content: bytes | None = None
    original_name: str | None = None
    extension: str | None = None

    @classmethod
    def draft(
        cls,
        content: bytes,
        original_name: str,
        step_number: int,
        metadata: dict[str, Any] | None = None,
    ) -> Self:
        """Create a mutable draft from bytes and a filename."""
        return cls(
            content=content,
            original_name=strip_extensions(original_name),
            extension=get_compound_extension(original_name),
            origin_step_number=step_number,
            metadata=metadata or {},
        )

    def _materialize_content(self, directory: str, *, fs: Any = None) -> str:
        """Write finalized bytes to an ID-based filename."""
        if self.content is None:
            raise ValueError("Cannot materialize: artifact not hydrated")
        if self.artifact_id is None:
            raise ValueError("Cannot materialize: artifact not finalized")
        path = Path(directory) / f"{self.artifact_id}{self.extension or ''}"
        path.write_bytes(self.content)
        self.materialized_path = str(path)
        return str(path)


class BinaryTypeDef(ArtifactTypeDef):
    """Register the domain binary model when this module is imported."""

    key = "domain_binary"
    table_path = "artifacts/domain_binaries"
    model = BinaryArtifact
```

Keep optional descriptors out until the domain needs them. If adding a byte
count, for example, derive it in `draft()`, persist it, and validate it against
`len(content)` in `_validate_identity_descriptors()` when content is present.

## Externally backed types

For external bytes, first consider `FileRefArtifact` or `LargeFileArtifact`
from `artisan.schemas`, or a compatible subclass inheriting verified I/O. Read
the relevant implementation:
[file references](../../src/artisan/schemas/artifact/file_ref.py) or
[large files](../../src/artisan/schemas/artifact/large_file.py).

A new external implementation must declare `EXTERNALLY_BACKED = True` and
exactly one real model field in `LOCATOR_FIELDS`. Keep the locator out of
`POLARS_SCHEMA` and the identity payload; the store owns the location relation.
Identity describes verified content, so relocating equal bytes preserves it.
Provide or inherit `verify_external_content()` and `_materialize_content()`
with bounded reads that check both digest and byte count and reject changed
content. Preserve the base integrity check when overriding verification.
Credentials and temporary capability URLs do not belong in durable locations.
If no public base fits, implement and test domain-owned I/O; do not import
private framework helpers or expand core APIs while scaffolding.

## Verify the authored type

Exercise behavior appropriate to the changed representation:

- Registration resolves the type key to the model; a draft has no ID.
- Finalization is deterministic and idempotent, including across origin steps.
  Changed content, name, extension, or semantic metadata changes identity.
  Invalid content-derived descriptors fail before finalization.
- `from_row(artifact.to_row())` preserves the ID and every persisted field,
  including metadata and any structured columns. Protected-field mutation
  fails; an ID-only reference cannot serialize as a hydrated row.
- Materializing into an existing temporary directory writes the original
  bytes. External types additionally verify relocation and reject changed
  bytes or byte counts.
- For a new type, use an importable temporary domain package and a minimal
  local pipeline with a fresh store. Its creator returns a draft and declares
  `infer_lineage_from={"inputs": []}` for a generative output. Confirm the step
  succeeds and pipeline `finalize()` reports `overall_success=True`.
  In a separate Python process, import the package before opening
  `ArtifactStore(delta_root)` from `artisan.storage`. Retrieve the committed ID
  with `get_artifact(id, hydrate=False)` and `get_artifact(id, hydrate=True)`;
  both must have the custom model type, with content absent only in the former.
  Materialize the hydrated artifact and compare bytes.

If the request includes a producing operation, consult
[operation-write](../operation-write/SKILL.md) for its lifecycle. Keep smoke
packages and stores in temporary directories; add domain tests alongside the
authored model rather than permanent sample types to Artisan core.
