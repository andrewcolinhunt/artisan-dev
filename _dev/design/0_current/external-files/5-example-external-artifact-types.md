# Design: Example External-Content Artifact Types

**Date:** 2026-04-04
**Status:** Draft

---

## Problem

The external-content infrastructure (Design 2) adds `files_root` and promotes
`external_path` to a functional content pointer, but the framework has no
concrete external-content artifact types. Domain-specific types (e.g.,
SilentStructureArtifact for protein design) live in external repos and cannot
appear in framework tutorials.

Tutorials and tests need built-in example types that demonstrate two distinct
external-content patterns:

**Many-to-one (appendable bundle):** One file contains many independently
addressable records. Each artifact stores metadata in Delta and a shared
`external_path` pointing to the bundle file. Real-world analogs: silent files,
FASTA/FASTQ, SDF, JSONL datasets. Key property: the file is appendable --
workers can write records incrementally, then consolidation concatenates worker
files into one.

**One-to-one (large standalone file):** One artifact = one file too large for
a Parquet binary column. Delta stores only metadata (hash, size, name);
`external_path` points to the actual file in `files_root`. Real-world analogs:
model weights (PyTorch checkpoints, safetensors), embedding matrices, large
simulation outputs, HDF5 datasets.

Without these, Tutorial B (External File Storage) and Tutorial A (Post-Step
Consolidation) cannot demonstrate the features end-to-end.

---

## Prior Art Survey

### `Artifact` base class (`schemas/artifact/base.py`)

`external_path: str | None` is present on every artifact, persisted in every
Delta table, and round-trips through `to_row()`/`from_row()`. Currently
informational only -- no production code reads from it for content retrieval.
`_default_hydrate: ClassVar[bool] = True` controls whether the storage layer
loads full content or ID-only stubs.

`_finalize_content()` default returns `getattr(self, "content", None)`. Types
without a `content` field must override it. `_materialize_content()` raises
`NotImplementedError` by default.

**Reuse:** Both new types inherit from `Artifact`. They override
`_finalize_content()` and `_materialize_content()`, following the FileRefArtifact
pattern.

### `FileRefArtifact` (`schemas/artifact/file_ref.py`)

Closest existing type to both new types. Stores `path`, `content_hash`,
`size_bytes` without a `content: pl.Binary` column. `_finalize_content()`
hashes a JSON dict of `{content_hash, path, size_bytes}`. `_materialize_content()`
copies the referenced file into the sandbox.

**Key difference:** FileRefArtifact models **user-managed** files -- Artisan
never creates, moves, or deletes the referenced file. The `path` field stores
a user-owned filesystem path. The new types model **Artisan-managed** files
that live in `files_root` and whose lifecycle is tied to the pipeline.

**Reuse:** The `_finalize_content()` pattern (hash metadata JSON, not file
bytes) is the template. The `to_row()`/`from_row()`/`POLARS_SCHEMA` structure
is reused exactly. No shared base class needed -- the overlap is structural,
not behavioral.

### `ArtifactTypeDef` registry (`schemas/artifact/registry.py`)

Auto-registration via `__init_subclass__`. Each type provides `key`,
`table_path`, `model`. The storage layer dispatches through the registry --
a new type gets its own Delta table with zero storage changes.

**Reuse:** Both new types register through this mechanism. No changes needed.

### Example operations (`operations/examples/`)

`DataGenerator` is the generative pattern: no inputs, writes files to
`execute_dir` in execute(), creates artifact drafts from `file_outputs` in
postprocess(). `DataTransformer` is the transform pattern: materializes inputs,
transforms files, creates output drafts.

**Reuse:** New generator operations follow the same lifecycle structure. The
key difference: external-content operations write to `files_dir` (not
`execute_dir`) and create drafts from `memory_outputs` (not `file_outputs`),
since the files live outside the sandbox.

### `ExecuteInput` (`schemas/specs/input_models.py`)

Frozen dataclass with `execute_dir`, `inputs`, `log_path`, `metadata`. Does
not expose `files_root` or `step_number`.

**Gap:** Creator operations that produce external files need a directory
within `files_root` to write to. Currently no mechanism to pass this.

### Curator pattern (`operations/curator/`)

`IngestFiles` is the abstract curator base for file processing. Curators
receive `inputs: dict[str, pl.DataFrame]` and `artifact_store: ArtifactStore`
in `execute_curator()`. Curators already have access to `files_root` through
`artifact_store.files_root` (Design 2).

**Reuse:** The consolidation curator follows the `IngestFiles` pattern:
hydrate input artifacts, process files, return `ArtifactResult` with new
artifacts.

### `InputSpec.materialize` (`schemas/specs/input_spec.py`)

When `materialize=False`, artifacts are not written to disk -- they are
passed in-memory with full field access. `DataTransformerConfig` already
uses this pattern.

**Reuse:** External-content input specs use `materialize=False`. Operations
read from `artifact.external_path` directly.

### `files_root` infrastructure (Design 2)

`PipelineConfig.files_root` defaults to `delta_root.parent / "files"`.
`RuntimeEnvironment.files_root_path` carries it to workers.
`ArtifactStore.files_root` stores it for curators.

**Gap:** The value reaches curators (via `artifact_store.files_root`) but
not creator operations (not in `ExecuteInput`).

---

## Design

### Threading `files_dir` to Creator Operations

Add an optional `files_dir: Path | None` field to `ExecuteInput`:

```python
@dataclass(frozen=True)
class ExecuteInput:
    execute_dir: Path
    inputs: dict[str, Any] = field(default_factory=dict)
    log_path: Path | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    files_dir: Path | None = None
```

The creator executor constructs a per-worker subdirectory and passes it:

```python
# In run_creator_lifecycle(), during setup phase:
if runtime_env.files_root_path is not None:
    files_dir = (
        runtime_env.files_root_path
        / str(unit.step_number)
        / "workers"
        / execution_run_id
    )
    files_dir.mkdir(parents=True, exist_ok=True)
else:
    files_dir = None

# Later, when building ExecuteInput:
execute_input = ExecuteInput(
    inputs=prepared_inputs,
    execute_dir=execute_dir,
    log_path=log_path,
    files_dir=files_dir,
)
```

**Why `files_dir` not `files_root`:** The operation should not construct
directory paths. The executor owns the directory structure
(`files_root/{step}/workers/{execution_run_id}/`). The operation gets a
ready-made directory to write into. Different workers get different
subdirectories -- no filename collisions.

**Why per-`execution_run_id`:** Each execution unit runs in its own worker.
Multiple units for the same step produce separate subdirectories. The
consolidation curator reads from all `workers/*/` subdirectories.

**Backward compatible:** `files_dir` defaults to `None`. Existing operations
ignore it. `ExecuteInput` is frozen, so the new field is safe.

### `RecordBundleArtifact`

JSONL-based appendable bundle. Each artifact represents one record within
a shared JSONL file.

```python
class RecordBundleArtifact(Artifact):
    """Artifact representing one record in a JSONL bundle file.

    Many RecordBundleArtifacts share the same external_path (the JSONL
    file). Each is addressed by record_id within the file.
    """

    POLARS_SCHEMA: ClassVar[dict[str, pl.DataType]] = {
        "artifact_id": pl.String,
        "origin_step_number": pl.Int32,
        "record_id": pl.String,
        "content_hash": pl.String,
        "size_bytes": pl.Int64,
        "original_name": pl.String,
        "extension": pl.String,
        "metadata": pl.String,
        "external_path": pl.String,
    }

    artifact_type: str = Field(default="record_bundle", frozen=True)
    record_id: str | None = Field(
        default=None,
        description="Unique identifier for this record within the bundle.",
    )
    content_hash: str | None = Field(
        default=None,
        description="Hash of this record's JSON content.",
    )
    size_bytes: int | None = Field(
        default=None,
        ge=0,
        description="Size of this record's JSON line in bytes.",
    )
    original_name: str | None = Field(
        default=None,
        description="Record key for lineage inference (stem only).",
    )
    extension: str | None = Field(
        default=None,
        description="File extension (.jsonl typically).",
    )
```

**No `content` column.** Record data lives in the JSONL file. Delta stores
only per-record metadata that is queryable without touching the file.

**`_finalize_content()`:** Hashes metadata including `external_path`, so the
same record in a per-worker file and in the consolidated file produces
distinct `artifact_id`s:

```python
def _finalize_content(self) -> bytes | None:
    if self.content_hash is None:
        return None
    return json.dumps(
        {
            "content_hash": self.content_hash,
            "record_id": self.record_id,
            "external_path": self.external_path,
        },
        sort_keys=True,
    ).encode("utf-8")
```

**`_materialize_content()`:** Extracts the specific record from the JSONL
file and writes it as a standalone JSON file. This is expensive for large
bundles -- the default consumption pattern is `InputSpec(materialize=False)`:

```python
def _materialize_content(self, directory: Path) -> Path:
    if self.external_path is None:
        msg = "Cannot materialize: external_path not set"
        raise ValueError(msg)
    record = self._read_record()
    filename = f"{self.artifact_id}.json"
    path = directory / filename
    path.write_text(json.dumps(record, indent=2))
    self.materialized_path = path
    return path

def _read_record(self) -> dict[str, Any]:
    """Read this record from the JSONL bundle by record_id."""
    with open(self.external_path) as f:
        for line in f:
            record = json.loads(line)
            if record.get("record_id") == self.record_id:
                return record
    msg = f"Record {self.record_id} not found in {self.external_path}"
    raise ValueError(msg)
```

**`draft()`:**

```python
@classmethod
def draft(
    cls,
    record_id: str,
    content_hash: str,
    size_bytes: int,
    step_number: int,
    external_path: str,
    original_name: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> RecordBundleArtifact:
```

**Registration:**

```python
class RecordBundleTypeDef(ArtifactTypeDef):
    key = "record_bundle"
    table_path = "artifacts/record_bundles"
    model = RecordBundleArtifact
```

### `LargeFileArtifact`

Single large file per artifact. Content is too large for a Parquet binary
column.

```python
class LargeFileArtifact(Artifact):
    """Artifact representing a large file stored externally.

    Content lives at external_path. Delta stores only metadata
    (hash, size, name). For files too large to embed in Parquet:
    model weights, embedding matrices, simulation outputs.
    """

    POLARS_SCHEMA: ClassVar[dict[str, pl.DataType]] = {
        "artifact_id": pl.String,
        "origin_step_number": pl.Int32,
        "content_hash": pl.String,
        "size_bytes": pl.Int64,
        "original_name": pl.String,
        "extension": pl.String,
        "metadata": pl.String,
        "external_path": pl.String,
    }

    artifact_type: str = Field(default="large_file", frozen=True)
    content_hash: str | None = Field(
        default=None,
        description="Hash of the file bytes.",
    )
    size_bytes: int | None = Field(
        default=None,
        ge=0,
        description="File size in bytes.",
    )
    original_name: str | None = Field(
        default=None,
        description="Human-readable filename stem.",
    )
    extension: str | None = Field(
        default=None,
        description="File extension (e.g., .bin, .npy, .pt).",
    )
```

**No `content` column.** The whole point: a 500MB model checkpoint should not
be a cell in a Parquet row.

**`_finalize_content()`:** Hashes metadata. Includes `external_path` so the
same file at different locations produces distinct artifact IDs:

```python
def _finalize_content(self) -> bytes | None:
    if self.content_hash is None:
        return None
    return json.dumps(
        {
            "content_hash": self.content_hash,
            "external_path": self.external_path,
        },
        sort_keys=True,
    ).encode("utf-8")
```

**`_materialize_content()`:** Copies the file from `external_path` to the
sandbox. Uses `artifact_id` as filename (per Design 1):

```python
def _materialize_content(self, directory: Path) -> Path:
    if self.external_path is None:
        msg = "Cannot materialize: external_path not set"
        raise ValueError(msg)
    source = Path(self.external_path)
    filename = f"{self.artifact_id}{self.extension or ''}"
    dest = directory / filename
    shutil.copy2(str(source), str(dest))
    self.materialized_path = dest
    return dest
```

**`draft()`:**

```python
@classmethod
def draft(
    cls,
    content_hash: str,
    size_bytes: int,
    step_number: int,
    external_path: str,
    original_name: str | None = None,
    extension: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> LargeFileArtifact:
```

**Registration:**

```python
class LargeFileTypeDef(ArtifactTypeDef):
    key = "large_file"
    table_path = "artifacts/large_files"
    model = LargeFileArtifact
```

### Comparison to `FileRefArtifact`

| | FileRefArtifact | RecordBundleArtifact | LargeFileArtifact |
|---|---|---|---|
| **Who creates the file** | User, before pipeline | Operation, during pipeline | Operation, during pipeline |
| **Where it lives** | Arbitrary user path | `files_root/{step}/` | `files_root/{step}/` |
| **Who manages lifecycle** | User | Artisan | Artisan |
| **Content pointer** | `path` (own field) | `external_path` (base) | `external_path` (base) |
| **Many-to-one** | No (1 artifact = 1 file) | Yes (N artifacts = 1 JSONL) | No (1 artifact = 1 file) |
| **Appendable** | N/A | Yes (JSONL append) | N/A |
| **Consolidation** | N/A | Concatenate worker files | N/A |
| **Default consumption** | `materialize=True` (copy) | `materialize=False` (read path) | `materialize=False` (read path) |

### Example Operations

#### `RecordBundleGenerator`

Generative creator that produces a JSONL file with N records. Demonstrates
the appendable bundle pattern.

```python
class RecordBundleGenerator(OperationDefinition):
    name = "record_bundle_generator"

    inputs: ClassVar[dict] = {}
    outputs: ClassVar[dict[str, OutputSpec]] = {
        "records": OutputSpec(
            artifact_type="record_bundle",
            infer_lineage_from={"inputs": []},
        ),
    }

    class Params(BaseModel):
        count: int = Field(default=10, ge=1)
        fields_per_record: int = Field(default=5, ge=1)
        seed: int | None = None

    params: Params = Params()

    def execute(self, inputs: ExecuteInput) -> dict[str, Any]:
        if inputs.files_dir is None:
            msg = "files_dir required for RecordBundleGenerator"
            raise ValueError(msg)

        rng = random.Random(self.params.seed)
        output_path = inputs.files_dir / "records.jsonl"
        records_meta: list[dict[str, Any]] = []

        with output_path.open("w") as f:
            for i in range(self.params.count):
                record = {
                    "record_id": f"rec_{i:06d}",
                    "values": {
                        f"field_{j}": round(rng.gauss(0, 1), 6)
                        for j in range(self.params.fields_per_record)
                    },
                }
                line = json.dumps(record, sort_keys=True)
                f.write(line + "\n")
                records_meta.append({
                    "record_id": record["record_id"],
                    "content_hash": compute_content_hash(line.encode()),
                    "size_bytes": len(line.encode()),
                })

        return {
            "output_path": str(output_path),
            "records": records_meta,
        }

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        output_path = inputs.memory_outputs["output_path"]
        records = inputs.memory_outputs["records"]

        drafts = [
            RecordBundleArtifact.draft(
                record_id=rec["record_id"],
                content_hash=rec["content_hash"],
                size_bytes=rec["size_bytes"],
                step_number=inputs.step_number,
                external_path=output_path,
                original_name=rec["record_id"],
            )
            for rec in records
        ]

        return ArtifactResult(
            success=True,
            artifacts={"records": drafts},
        )
```

#### `LargeFileGenerator`

Generative creator that produces large binary files. Demonstrates the
one-to-one pattern where each file is too large for Delta.

```python
class LargeFileGenerator(OperationDefinition):
    name = "large_file_generator"

    inputs: ClassVar[dict] = {}
    outputs: ClassVar[dict[str, OutputSpec]] = {
        "files": OutputSpec(
            artifact_type="large_file",
            infer_lineage_from={"inputs": []},
        ),
    }

    class Params(BaseModel):
        count: int = Field(default=1, ge=1)
        file_size_bytes: int = Field(
            default=1_000_000,
            ge=1,
            description="Approximate size of each generated file.",
        )
        seed: int | None = None

    params: Params = Params()

    def execute(self, inputs: ExecuteInput) -> dict[str, Any]:
        if inputs.files_dir is None:
            msg = "files_dir required for LargeFileGenerator"
            raise ValueError(msg)

        rng = random.Random(self.params.seed)
        files_meta: list[dict[str, Any]] = []

        for i in range(self.params.count):
            filename = f"output_{i:05d}.bin"
            output_path = inputs.files_dir / filename
            # Generate deterministic pseudo-random binary data
            data = rng.randbytes(self.params.file_size_bytes)
            output_path.write_bytes(data)
            files_meta.append({
                "path": str(output_path),
                "content_hash": compute_content_hash(data),
                "size_bytes": len(data),
                "original_name": f"output_{i:05d}",
                "extension": ".bin",
            })

        return {"files": files_meta}

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        files = inputs.memory_outputs["files"]

        drafts = [
            LargeFileArtifact.draft(
                content_hash=f["content_hash"],
                size_bytes=f["size_bytes"],
                step_number=inputs.step_number,
                external_path=f["path"],
                original_name=f["original_name"],
                extension=f["extension"],
            )
            for f in files
        ]

        return ArtifactResult(
            success=True,
            artifacts={"files": drafts},
        )
```

#### `ConsolidateRecordBundles`

Curator that concatenates per-worker JSONL files into a single combined
file. Natural `post_step` target.

```python
class ConsolidateRecordBundles(OperationDefinition):
    name = "consolidate_record_bundles"

    inputs: ClassVar[dict[str, InputSpec]] = {
        "records": InputSpec(
            artifact_type="record_bundle",
            materialize=False,
        ),
    }
    outputs: ClassVar[dict[str, OutputSpec]] = {
        "records": OutputSpec(
            artifact_type="record_bundle",
            infer_lineage_from={"inputs": ["records"]},
        ),
    }

    hydrate_inputs: ClassVar[bool] = True

    def execute_curator(
        self,
        inputs: dict[str, pl.DataFrame],
        step_number: int,
        artifact_store: ArtifactStore,
    ) -> ArtifactResult:
        record_ids = inputs["records"]["artifact_id"].to_list()
        artifacts = artifact_store.get_artifacts_by_type(
            record_ids, "record_bundle"
        )

        # Find distinct worker files
        worker_files: set[str] = set()
        for art in artifacts.values():
            if art.external_path:
                worker_files.add(art.external_path)

        # Concatenate into combined file
        combined_path = (
            artifact_store.files_root
            / str(step_number)
            / "combined.jsonl"
        )
        combined_path.parent.mkdir(parents=True, exist_ok=True)
        with open(combined_path, "w") as out:
            for worker_file in sorted(worker_files):
                with open(worker_file) as f:
                    out.write(f.read())

        # Create new artifacts pointing to combined file
        drafts: list[RecordBundleArtifact] = []
        for art in artifacts.values():
            drafts.append(
                RecordBundleArtifact.draft(
                    record_id=art.record_id,
                    content_hash=art.content_hash,
                    size_bytes=art.size_bytes,
                    step_number=step_number,
                    external_path=str(combined_path),
                    original_name=art.original_name,
                )
            )

        return ArtifactResult(
            success=True,
            artifacts={"records": drafts},
        )
```

**Consolidation produces new artifacts:** Because `external_path` is included
in `_finalize_content()`, consolidated artifacts get new `artifact_id`s (they
point to the combined file, not the worker file). Lineage traces them back to
the per-worker artifacts via the curator's `infer_lineage_from` configuration.

**Pipeline usage with `post_step`:**

```python
step = pipeline.run(
    RecordBundleGenerator,
    params={"count": 100},
    post_step=ConsolidateRecordBundles,
)
# step.output("records") -> consolidated artifacts
```

### Consumption Pattern

Operations that consume external-content artifacts use
`InputSpec(materialize=False)` and read from `external_path` directly:

```python
class ProcessRecordBundle(OperationDefinition):
    inputs: ClassVar[dict[str, InputSpec]] = {
        "records": InputSpec(
            artifact_type="record_bundle",
            materialize=False,
        ),
    }

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        records = inputs.input_artifacts["records"]
        # All artifacts share the same external_path (one JSONL file)
        bundle_path = records[0].external_path
        record_ids = [r.record_id for r in records]
        return {"bundle_path": bundle_path, "record_ids": record_ids}
```

### Content Hashing Helper

Both operations need to hash content for `content_hash`. A thin wrapper
around the existing `compute_artifact_id`:

```python
def compute_content_hash(content: bytes) -> str:
    """Compute xxh3_128 hash of content bytes."""
    return compute_artifact_id(content)
```

This reuses `artisan.utils.hashing.compute_artifact_id` which already wraps
`xxhash.xxh3_128(content).hexdigest()`. No new hashing code.

---

## Scope

### PR: `files_dir` threading

| File | Change |
|------|--------|
| `src/artisan/schemas/specs/input_models.py` | Add `files_dir: Path \| None = None` to `ExecuteInput` |
| `src/artisan/execution/executors/creator.py` | Construct `files_dir` from `runtime_env.files_root_path`, pass to `ExecuteInput` |

### PR: Artifact types and operations

| File | Change |
|------|--------|
| `src/artisan/schemas/artifact/record_bundle.py` | New: `RecordBundleArtifact` with `POLARS_SCHEMA`, `draft()`, `_finalize_content()`, `_materialize_content()`, `_read_record()`, `to_row()`, `from_row()`, `RecordBundleTypeDef` |
| `src/artisan/schemas/artifact/large_file.py` | New: `LargeFileArtifact` with `POLARS_SCHEMA`, `draft()`, `_finalize_content()`, `_materialize_content()`, `to_row()`, `from_row()`, `LargeFileTypeDef` |
| `src/artisan/operations/examples/record_bundle_generator.py` | New: `RecordBundleGenerator` operation |
| `src/artisan/operations/examples/large_file_generator.py` | New: `LargeFileGenerator` operation |
| `src/artisan/operations/curator/consolidate_record_bundles.py` | New: `ConsolidateRecordBundles` curator |

---

## Testing

| Test file | Coverage |
|-----------|----------|
| `tests/artisan/schemas/artifact/test_record_bundle.py` | Schema round-trip, `_finalize_content()` includes `external_path`, `draft()` factory, `_read_record()` extraction, `_materialize_content()` writes JSON, distinct `artifact_id` for same record at different paths |
| `tests/artisan/schemas/artifact/test_large_file.py` | Schema round-trip, `_finalize_content()` includes `external_path`, `draft()` factory, `_materialize_content()` copies file, distinct `artifact_id` for same content at different paths |
| `tests/artisan/execution/test_files_dir_threading.py` | `ExecuteInput.files_dir` default None, creator executor constructs per-worker directory, directory created under `files_root/{step}/workers/{run_id}` |
| `tests/artisan/operations/test_record_bundle_generator.py` | Generates JSONL with correct record count, records have `record_id` and `content_hash`, artifacts share `external_path`, `files_dir` is used |
| `tests/artisan/operations/test_large_file_generator.py` | Generates binary files of specified size, artifacts have correct `content_hash` and `size_bytes`, one file per artifact |
| `tests/artisan/operations/test_consolidate_record_bundles.py` | Concatenates multiple worker JSONL files, new artifacts point to combined file, new artifact IDs (external_path changed), record count preserved |

---

## Open Questions

- **`random.randbytes()` availability:** `random.Random.randbytes()` was added
  in Python 3.9. If the project supports older versions, use
  `rng.getrandbits(8 * n).to_bytes(n, 'big')` instead.

- **Consolidation idempotency:** If the consolidation curator runs twice
  (re-run after failure), it overwrites `combined.jsonl`. The second run
  produces artifacts with the same `artifact_id`s (same content, same path).
  Is this acceptable or should the curator check for existing output?

- **Worker cleanup:** After consolidation, per-worker files in
  `files_root/{step}/workers/` are orphaned. Should the consolidation curator
  delete them, or leave cleanup to a separate utility?

---

## Related Docs

- `1-artifact-id-materialization.md` -- Materialization now uses `artifact_id`
  for filenames (Design 1). LargeFileArtifact's `_materialize_content()` follows
  this convention.
- `2-external-content-artifacts.md` -- `files_root` infrastructure that these
  types build on.
- `3-post-step-sugar.md` -- `ConsolidateRecordBundles` is the natural
  `post_step` target.
