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
from artisan.schemas.artifact.types import ArtifactTypes
from artisan.utils.filename import strip_extensions
from artisan.utils.hashing import compute_artifact_id


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

    _default_hydrate: ClassVar[bool] = True

    def materialize_to(self, directory: Path, *, format: str | None = None) -> Path:
        if format is not None:
            raise ValueError(f"DataRecordArtifact does not support format conversion (got {format!r})")
        if self.content is None:
            raise ValueError("Cannot materialize: artifact not hydrated")
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

    def finalize(self) -> DataRecordArtifact:
        if self.artifact_id is not None:
            return self
        if self.content is None:
            raise ValueError("Cannot finalize: artifact not hydrated")
        self.artifact_id = compute_artifact_id(self.content)
        return self

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

## Step 1: Create the artifact model

Create a new file in `src/artisan/schemas/artifact/` (or your domain layer's
`schemas/artifact/` directory). The model must subclass `Artifact` and implement
six members:

| Member | Kind | Purpose |
|--------|------|---------|
| `POLARS_SCHEMA` | `ClassVar` | Column names and Polars types for the Delta Lake table |
| `draft()` | classmethod | Create a mutable artifact with `artifact_id=None` |
| `finalize()` | method | Compute the content hash and set `artifact_id` |
| `materialize_to()` | method | Write content to disk, return the `Path` |
| `to_row()` | method | Serialize to a dict matching `POLARS_SCHEMA` |
| `from_row()` | classmethod | Deserialize from a dict back to the model |

### Set the artifact type

```python
artifact_type: str = Field(default="data_record", frozen=True)
```

The value is a plain string. `frozen=True` prevents mutation after creation.

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

### Implement draft and finalize

`draft()` builds a mutable artifact with `artifact_id=None`. `finalize()`
computes the content hash:

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

def finalize(self) -> DataRecordArtifact:
    if self.artifact_id is not None:
        return self  # idempotent
    if self.content is None:
        raise ValueError("Cannot finalize: artifact not hydrated")
    self.artifact_id = compute_artifact_id(self.content)
    return self
```

Use `compute_artifact_id` from `artisan.utils.hashing`. It takes `bytes` and
returns a 32-character hex string (xxh3_128). For JSON-based content, use
`json.dumps(data, sort_keys=True).encode("utf-8")` to ensure deterministic
hashing.

### Implement serialization

`to_row()` returns a flat dict suitable for Parquet. `from_row()` reverses it.
The key rule: **JSON-encode any complex fields** (dicts, lists) as strings in
`to_row()` and decode them in `from_row()`.

```python
def to_row(self) -> dict[str, Any]:
    return {
        ...
        "metadata": json.dumps(self.metadata or {}),  # dict -> str
    }

@classmethod
def from_row(cls, row: dict[str, Any]) -> Self:
    metadata_raw = row.get("metadata")
    return cls(
        ...
        metadata=json.loads(metadata_raw) if metadata_raw else {},  # str -> dict
    )
```

### Implement materialize_to

Write the artifact content to a file in the given directory:

```python
def materialize_to(self, directory: Path, *, format: str | None = None) -> Path:
    if format is not None:
        raise ValueError(f"... does not support format conversion (got {format!r})")
    if self.content is None:
        raise ValueError("Cannot materialize: artifact not hydrated")
    filename = f"{self.original_name}{self.extension or '.csv'}"
    path = directory / filename
    path.write_bytes(self.content)
    self.materialized_path = path
    return path
```

If your artifact type supports format conversion (e.g., `.json` to `.csv`),
handle the `format` parameter instead of rejecting it.

---

## Step 2: Register the type definition

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

1. Validates that `key`, `table_path`, and `model` are set
2. Validates that the model has `POLARS_SCHEMA`, `to_row`, and `from_row`
3. Registers `"data_record"` in `ArtifactTypes` (so `ArtifactTypes.DATA_RECORD` works)
4. Registers the type def in `ArtifactTypeDef._registry`

If any validation fails, you get an immediate `TypeError` at import time.

---

## Step 3: Update exports

Add the new model to the package `__init__.py`:

```python
# src/artisan/schemas/artifact/__init__.py
from artisan.schemas.artifact.data_record import DataRecordArtifact

__all__ = [
    # ... existing exports
    "DataRecordArtifact",
]
```

For domain-layer types (outside `artisan`), update your domain package's
`__init__.py` instead.

---

## Step 4: Write tests

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

### Metadata-only types (no embedded content)

Some artifact types reference external data rather than storing content inline.
`FileRefArtifact` is the built-in example. For metadata-only types, hash a
metadata record instead of raw content:

```python
def finalize(self) -> MyRefArtifact:
    if self.artifact_id is not None:
        return self
    metadata_json = json.dumps(
        {"path": self.path, "size_bytes": self.size_bytes},
        sort_keys=True,
    ).encode("utf-8")
    self.artifact_id = compute_artifact_id(metadata_json)
    return self
```

### Domain-layer types

Domain types can live in a separate package outside `artisan`. The pattern is
identical -- subclass `Artifact`, define an `ArtifactTypeDef`, and the
framework discovers it at import time.

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

## Cross-References

- [Artifacts and Content Addressing](../concepts/artifacts-and-content-addressing.md) -- artifact identity, draft/finalize lifecycle, hydration
- [Storage and Delta Lake](../concepts/storage-and-delta-lake.md) -- how artifact tables are persisted
- [Writing Creator Operations](writing-creator-operations.md) -- using artifacts in operation postprocess
