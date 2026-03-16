# Create a New Artifact Type

Add a custom artifact type to the Artisan framework so the storage, staging,
caching, and provenance systems handle it automatically.

**Prerequisites:** [Artifacts and Content Addressing](../concepts/artifacts-and-content-addressing.md) (draft/finalize lifecycle, content addressing), familiarity with Pydantic models.

---

## Minimal working example

Two classes are all you need: a model and a type definition. Here they are in
full, for a hypothetical `DataRecordArtifact` that stores CSV sample data.

```python
# src/artisan/schemas/artifact/data_record.py
"""Data record artifact schema."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, ClassVar, Self

import polars as pl
from pydantic import Field

from artisan.schemas.artifact.base import Artifact
from artisan.schemas.artifact.common import get_compound_extension
from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.utils.filename import strip_extensions


class DataRecordArtifact(Artifact):
    """Data record artifact for CSV sample data."""

    POLARS_SCHEMA: ClassVar[dict[str, pl.DataType]] = {
        "artifact_id": pl.String,
        "origin_step_number": pl.Int32,
        "content": pl.Binary,
        "original_name": pl.String,
        "extension": pl.String,
        "size_bytes": pl.Int64,
        "record_count": pl.Int64,
        "metadata": pl.String,
        "external_path": pl.String,
    }

    artifact_type: str = Field(default="data_record", frozen=True)
    content: bytes | None = Field(default=None)
    original_name: str | None = Field(default=None)
    extension: str | None = Field(default=None)
    size_bytes: int | None = Field(default=None, ge=0)
    record_count: int | None = Field(default=None, ge=0)

    def _materialize_content(self, directory: Path) -> Path:
        if self.content is None:
            raise ValueError("Cannot materialize: artifact not hydrated")
        if self.original_name is None:
            raise ValueError("Cannot materialize: original_name not set")
        filename = f"{self.original_name}{self.extension or '.csv'}"
        path = directory / filename
        path.write_bytes(self.content)
        self.materialized_path = path
        return path

    @classmethod
    def draft(
        cls,
        content: bytes,
        original_name: str,
        step_number: int,
        record_count: int | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> DataRecordArtifact:
        return cls(
            artifact_id=None,
            origin_step_number=step_number,
            content=content,
            original_name=strip_extensions(original_name),
            extension=get_compound_extension(original_name),
            size_bytes=len(content),
            record_count=record_count,
            metadata=metadata or {},
        )

    def to_row(self) -> dict[str, Any]:
        return {
            "artifact_id": self.artifact_id,
            "origin_step_number": self.origin_step_number,
            "content": self.content,
            "original_name": self.original_name,
            "extension": self.extension,
            "size_bytes": self.size_bytes,
            "record_count": self.record_count,
            "metadata": json.dumps(self.metadata or {}),
            "external_path": self.external_path,
        }

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> Self:
        metadata_raw = row.get("metadata")
        return cls(
            artifact_id=row["artifact_id"],
            origin_step_number=row.get("origin_step_number"),
            content=row.get("content"),
            original_name=row.get("original_name"),
            extension=row.get("extension"),
            size_bytes=row.get("size_bytes"),
            record_count=row.get("record_count"),
            metadata=json.loads(metadata_raw) if metadata_raw else {},
            external_path=row.get("external_path"),
        )


class DataRecordTypeDef(ArtifactTypeDef):
    key = "data_record"
    table_path = "artifacts/data_records"
    model = DataRecordArtifact
```

That is the complete implementation. The rest of this guide breaks it down.

---

## Create the artifact model

Create a new file in `src/artisan/schemas/artifact/` (or your domain layer's
`schemas/artifact/` directory). The model must subclass `Artifact` and provide
these members:

| Member | Kind | Purpose |
|--------|------|---------|
| `POLARS_SCHEMA` | `ClassVar` | Column names and Polars types for the Delta Lake table |
| `artifact_type` | field | String discriminator with `frozen=True` |
| `draft()` | classmethod | Create a mutable artifact with `artifact_id=None` |
| `_materialize_content()` | method | Write content to disk, return the `Path` |
| `to_row()` | method | Serialize to a dict matching `POLARS_SCHEMA` |
| `from_row()` | classmethod | Deserialize from a dict back to the model |

The base `Artifact` class provides `finalize()`, `materialize_to()`, and
several fields your model inherits automatically. You do not need to redeclare
these inherited fields on your subclass, but you must include them in
`POLARS_SCHEMA` and `to_row()`/`from_row()` because they are stored as
Delta Lake columns.

### Inherited fields from the base class

The `Artifact` base class defines these fields that every artifact type shares:

| Field | Type | Purpose |
|-------|------|---------|
| `artifact_id` | `str \| None` | Content-addressed ID (32-char hex). `None` for drafts. |
| `artifact_type` | `str` | Type discriminator. Must be overridden with a default on each subclass. |
| `origin_step_number` | `int \| None` | Pipeline step that produced this artifact. |
| `metadata` | `dict[str, Any]` | Generic JSON-serializable metadata dict. |
| `external_path` | `str \| None` | Path to external content on disk. |
| `materialized_path` | `Path \| None` | Runtime-only path (excluded from serialization). |

The base class also sets `model_config = ConfigDict(extra="forbid")`, which
means Pydantic rejects any fields not declared on your model. This catches
typos in field names early.

### Set the artifact type

```python
artifact_type: str = Field(default="data_record", frozen=True)
```

The value is a plain string that uniquely identifies this type. `frozen=True`
prevents mutation after creation. Built-in types use `ArtifactTypes` constants
(e.g., `default=ArtifactTypes.METRIC`), but a plain string works identically
for custom types.

The base class validates that `artifact_type` is never set to
`ArtifactTypes.ANY`, which is a spec-only sentinel that cannot appear on
concrete artifacts.

### Define the Polars schema

```python
POLARS_SCHEMA: ClassVar[dict[str, pl.DataType]] = {
    "artifact_id": pl.String,
    "origin_step_number": pl.Int32,
    "content": pl.Binary,
    "original_name": pl.String,
    "extension": pl.String,
    "size_bytes": pl.Int64,
    "record_count": pl.Int64,
    "metadata": pl.String,
    "external_path": pl.String,
}
```

Every column written by `to_row()` must appear here. Column order determines
Parquet column order. All artifact types share the first two columns
(`artifact_id`, `origin_step_number`) and typically end with `metadata` and
`external_path`.

### Implement draft

`draft()` builds a mutable artifact with `artifact_id=None`:

```python
@classmethod
def draft(cls, content: bytes, original_name: str, step_number: int, ...) -> DataRecordArtifact:
    return cls(
        artifact_id=None,
        origin_step_number=step_number,
        content=content,
        original_name=strip_extensions(original_name),
        extension=get_compound_extension(original_name),
        size_bytes=len(content),
        ...
    )
```

Use `strip_extensions()` from `artisan.utils.filename` to extract the bare
filename stem and `get_compound_extension()` from
`artisan.schemas.artifact.common` to capture compound extensions like
`.tar.gz`.

### Understand finalize (base class)

You do not need to implement `finalize()`. The base `Artifact.finalize()`
method:

- Returns `self` if `artifact_id` is already set (idempotent)
- Calls `_finalize_content()` to get the bytes to hash
- Raises `ValueError` if `_finalize_content()` returns `None`
- Passes those bytes to `compute_artifact_id` (xxh3_128, returns a 32-character hex string)
- Sets `artifact_id` on the instance

The default `_finalize_content()` returns `getattr(self, "content", None)`. If
your artifact has a `content: bytes | None` field, finalization works out of
the box. For metadata-only types without a `content` field, override
`_finalize_content()` instead -- see
[Metadata-only types](#metadata-only-types-no-embedded-content).

### Implement serialization

`to_row()` returns a flat dict suitable for Parquet. `from_row()` reverses it.
The key rule: **JSON-encode any complex fields** (dicts, lists) as strings in
`to_row()` and decode them in `from_row()`.

```python
def to_row(self) -> dict[str, Any]:
    return {
        ...
        "metadata": json.dumps(self.metadata or {}),  # dict -> str
        "external_path": self.external_path,           # include inherited fields
    }

@classmethod
def from_row(cls, row: dict[str, Any]) -> Self:
    metadata_raw = row.get("metadata")
    return cls(
        ...
        metadata=json.loads(metadata_raw) if metadata_raw else {},  # str -> dict
        external_path=row.get("external_path"),
    )
```

Include the inherited fields (`artifact_id`, `origin_step_number`, `metadata`,
`external_path`) in both methods. The keys in the dict returned by `to_row()`
must match `POLARS_SCHEMA` exactly.

### Implement _materialize_content

The base class `_materialize_content()` raises `NotImplementedError`, so your
subclass must provide an implementation. Write the artifact content to a file
in the given directory, set `self.materialized_path`, and return the path:

```python
def _materialize_content(self, directory: Path) -> Path:
    if self.content is None:
        raise ValueError("Cannot materialize: artifact not hydrated")
    if self.original_name is None:
        raise ValueError("Cannot materialize: original_name not set")
    filename = f"{self.original_name}{self.extension or '.csv'}"
    path = directory / filename
    path.write_bytes(self.content)
    self.materialized_path = path
    return path
```

The base class `materialize_to()` rejects format conversion by default and
delegates to your `_materialize_content()`. For most artifact types,
implementing `_materialize_content()` is sufficient. Override
`materialize_to()` only if you need custom logic beyond writing content to
disk -- for example, `ExecutionConfigArtifact` overrides it to resolve
`{"$artifact": id}` reference patterns into filesystem paths before writing.

---

## Register the type definition

Add a three-line `ArtifactTypeDef` subclass. You can place it at the bottom of
your model file (as `DataArtifact` does) or in `registry.py` alongside the
framework types:

```python
class DataRecordTypeDef(ArtifactTypeDef):
    key = "data_record"
    table_path = "artifacts/data_records"
    model = DataRecordArtifact
```

When Python loads this class, `__init_subclass__` fires and:

- Validates that `key`, `table_path`, and `model` are set
- Validates that the model has `POLARS_SCHEMA`, `to_row`, and `from_row`
- Rejects duplicate keys (raises `ValueError` if another type def already uses the same key)
- Registers `"data_record"` in `ArtifactTypes` (so `ArtifactTypes.DATA_RECORD` works at runtime)
- Registers the type def in `ArtifactTypeDef._registry`

If any validation fails, you get an immediate `TypeError` or `ValueError` at
import time.

---

## Update exports

Add the new model to the package `__init__.py`:

```python
# src/artisan/schemas/artifact/__init__.py
from artisan.schemas.artifact.data_record import DataRecordArtifact

__all__ = [
    # ... existing exports
    "DataRecordArtifact",
]
```

The `ArtifactTypeDef` subclass must be importable for registration to happen.
If the type def lives in the same file as the model (the recommended pattern
for domain-layer types), importing the model is sufficient. If it lives in
`registry.py`, that module is already imported by the package init.

For domain-layer types (outside `artisan`), update your domain package's
`__init__.py` instead.

---

## Write tests

Cover these scenarios:

```python
# tests/artisan/schemas/test_data_record.py
import pytest

from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.schemas.artifact.data_record import DataRecordArtifact


SAMPLE_CSV = b"id,value\n1,hello\n2,world\n"


def test_draft_populates_fields():
    artifact = DataRecordArtifact.draft(
        content=SAMPLE_CSV,
        original_name="test.csv",
        step_number=1,
        record_count=2,
    )
    assert artifact.is_draft
    assert artifact.artifact_id is None
    assert artifact.original_name == "test"
    assert artifact.extension == ".csv"
    assert artifact.size_bytes == len(SAMPLE_CSV)
    assert artifact.record_count == 2


def test_finalize_computes_artifact_id():
    artifact = DataRecordArtifact.draft(
        content=SAMPLE_CSV, original_name="test.csv", step_number=1,
    ).finalize()
    assert artifact.is_finalized
    assert len(artifact.artifact_id) == 32


def test_finalize_is_idempotent():
    artifact = DataRecordArtifact.draft(
        content=SAMPLE_CSV, original_name="test.csv", step_number=1,
    ).finalize()
    first_id = artifact.artifact_id
    artifact.finalize()
    assert artifact.artifact_id == first_id


def test_materialize_writes_file(tmp_path):
    artifact = DataRecordArtifact.draft(
        content=SAMPLE_CSV, original_name="test.csv", step_number=1,
    ).finalize()
    path = artifact.materialize_to(tmp_path)
    assert path.exists()
    assert path.name == "test.csv"
    assert path.read_bytes() == SAMPLE_CSV


def test_round_trip_serialization():
    artifact = DataRecordArtifact.draft(
        content=SAMPLE_CSV,
        original_name="test.csv",
        step_number=1,
        record_count=2,
        metadata={"source": "test"},
    ).finalize()

    restored = DataRecordArtifact.from_row(artifact.to_row())

    assert restored.artifact_id == artifact.artifact_id
    assert restored.content == artifact.content
    assert restored.record_count == artifact.record_count
    assert restored.metadata == {"source": "test"}


def test_type_registered():
    type_def = ArtifactTypeDef.get("data_record")
    assert type_def.key == "data_record"
    assert type_def.table_path == "artifacts/data_records"
    assert type_def.model is DataRecordArtifact
```

---

## Common patterns

### Type-specific metadata fields

Put structured properties in dedicated model fields, not in the generic
`metadata` dict. This makes them queryable in the Delta Lake table.

```python
# Good: dedicated field, appears as a Parquet column
record_count: int | None = Field(default=None, ge=0)

# Avoid: buried in metadata, requires JSON parsing to query
metadata={"record_count": 42}
```

### Using JsonContentMixin for JSON-based artifacts

If your artifact stores JSON-encoded content (like `MetricArtifact` and
`ExecutionConfigArtifact` do), use the `JsonContentMixin` from
`artisan.schemas.artifact.common`. It provides a cached `values` property that
parses and returns the JSON content as a dict:

```python
from artisan.schemas.artifact.common import JsonContentMixin

class MyJsonArtifact(JsonContentMixin, Artifact):
    """Artifact storing JSON-encoded data."""

    content: bytes | None = Field(default=None)
    # ... other fields

    @classmethod
    def draft(cls, content: dict[str, Any], ...) -> MyJsonArtifact:
        encoded = json.dumps(content, sort_keys=True).encode("utf-8")
        return cls(artifact_id=None, content=encoded, ...)
```

After drafting or loading, access parsed values with `artifact.values` instead
of manually decoding `artifact.content`. The mixin caches the parsed result, so
repeated access is free. It raises `ValueError` if the artifact is not hydrated.

Note the MRO: list `JsonContentMixin` before `Artifact` in the class
definition so the mixin's methods are resolved first.

### Metadata-only types (no embedded content)

Some artifact types reference external data rather than storing content inline.
`FileRefArtifact` is the built-in example. These types have no `content` field,
so the default `_finalize_content()` (which calls `getattr(self, "content",
None)`) returns `None`, causing `finalize()` to raise `ValueError`. Override
`_finalize_content()` to hash a metadata record instead:

```python
def _finalize_content(self) -> bytes | None:
    if self.content_hash is None:
        return None
    return json.dumps(
        {"content_hash": self.content_hash, "path": self.path, "size_bytes": self.size_bytes},
        sort_keys=True,
    ).encode("utf-8")
```

The base class `finalize()` calls your `_finalize_content()` and hashes the
result. You do not override `finalize()` itself.

### Domain-layer types

Domain types can live in a separate package outside `artisan`. The pattern is
identical -- subclass `Artifact`, define an `ArtifactTypeDef`, and the
framework discovers it at import time via `__init_subclass__`.

Ensure the module containing your `ArtifactTypeDef` subclass is imported
somewhere during application startup. If it is never imported, the type will
not be registered.

### Placing the type definition

The framework supports two patterns:

- **In the model file** (like `DataArtifact` does): keeps model and
  registration together. Preferred for domain-layer types.
- **In `registry.py`** (like `MetricArtifact` does): groups all framework type
  definitions. Preferred for core framework types.

Both work identically. Pick whichever keeps your import graph cleaner.

---

## Common pitfalls

| Problem | Cause | Fix |
|---------|-------|-----|
| `TypeError` at import time | Model missing `POLARS_SCHEMA`, `to_row`, or `from_row` | Add the missing member to the model class |
| `ValueError: Duplicate artifact type key` | Two `ArtifactTypeDef` subclasses share the same `key` | Use a unique key string |
| `KeyError` when looking up the type | Type def class was never imported | Ensure the module is imported (add to `__init__.py`) |
| Data loss in round-trip | `to_row()` and `from_row()` are out of sync | Test with `from_row(artifact.to_row())` and compare all fields |
| Non-deterministic artifact IDs | JSON encoding without `sort_keys=True` | Always use `json.dumps(..., sort_keys=True)` for hash inputs |
| `POLARS_SCHEMA` mismatch | Schema columns don't match `to_row()` keys | Keep schema and `to_row()` in sync -- same keys, same order |
| `ValidationError: extra fields not permitted` | Field name typo in `from_row()` or `draft()` | The base class uses `ConfigDict(extra="forbid")` -- check field names match the model |
| `ValueError` on `artifact_type` | Used `ArtifactTypes.ANY` as a default | `ANY` is a spec-only sentinel; use a concrete type string |

---

## Verify

Confirm your type is registered:

```python
from artisan.schemas.artifact.registry import ArtifactTypeDef
from artisan.schemas.artifact.types import ArtifactTypes

type_def = ArtifactTypeDef.get("data_record")
assert type_def.model is DataRecordArtifact
assert "data_record" in ArtifactTypes
```

Run the tests:

```bash
pixi run -e dev test-unit -k test_data_record
```

---

## Files changed summary

| File | Change |
|------|--------|
| `src/.../artifact/data_record.py` | **New:** model class + type definition |
| `src/.../artifact/__init__.py` | Add `DataRecordArtifact` to exports |
| `tests/.../test_data_record.py` | **New:** unit tests |

No changes to the storage layer, staging functions, dispatch chains, or enum
definitions. The registry handles everything.

---

## Cross-references

- [Artifacts and Content Addressing](../concepts/artifacts-and-content-addressing.md) -- artifact identity, draft/finalize lifecycle, hydration
- [Storage and Delta Lake](../concepts/storage-and-delta-lake.md) -- how artifact tables are persisted
- [Writing Creator Operations](writing-creator-operations.md) -- using artifacts in operation postprocess
