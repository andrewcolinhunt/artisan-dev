# Cloud Deployment: Options Exploration

Thorough enumeration of options for each design decision in
`cloud-deployment.md`. The goal is divergent thinking — enumerate all
viable approaches before converging.

---

## The I/O Map

Before exploring options, here's what actually touches the filesystem in
the worker→staging→commit pipeline. This determines the surface area of
each option.

**Worker-local only (no change needed for cloud):**
- Sandbox creation: `mkdir` in working_root (always /tmp on cloud)
- Input materialization: artifact content → files in sandbox
- Execute phase: operation writes files to sandbox
- Output snapshot: `glob` inside sandbox

**Worker → Orchestrator transfer (staging — must change):**
- Worker writes Parquet files to `staging_root/{step}/{shard}/{run_id}/`
- Worker optionally fsyncs for NFS visibility
- Orchestrator polls for sentinel files (NFS verification)
- Orchestrator rglobs and reads staged Parquet
- Orchestrator rmtrees staging after commit

**Orchestrator → Worker transfer (dispatch — must change):**
- Orchestrator pickles units to `staging_root/_dispatch/`
- Workers read pickle from same path

**Delta Lake access (may need to change):**
- Workers read artifacts from `delta_root` via `pl.scan_delta(str(path))`
  during input hydration
- Orchestrator reads/writes Delta Lake via `pl.scan_delta()` /
  `df.write_delta()` / `DeltaTable(str(path))`
- All Delta reads use `table_path.exists()` as a guard

**Key observation:** `pl.scan_delta()`, `df.write_delta()`, and
`DeltaTable()` already accept S3/GCS URIs via `deltalake-rs`. The only
thing preventing remote Delta Lake is that we pass `pathlib.Path` objects
and call `.exists()` on them. If we pass URI strings instead, Delta Lake
access works on cloud storage today.

---

## Decision 1: Storage Abstraction

This is the highest-impact decision. Every cloud backend depends on it.

The problem decomposes into three independent sub-problems:

- **Unit transport:** How do pickled ExecutionUnits get from orchestrator
  to workers?
- **Staging transport:** How do staged Parquet files get from workers to
  orchestrator?
- **Store access:** How do workers read from Delta Lake for input
  hydration?

These don't have to use the same mechanism. Different abstractions may be
appropriate for each.

### Option A: TransferStrategy (narrow — staging + units only)

A focused abstraction that handles only the transfer of ephemeral data
between orchestrator and workers. The worker sandbox stays local. Delta
Lake access is a separate concern.

```python
class TransferStrategy(ABC):
    """Moves ephemeral data between orchestrator and workers."""

    def upload_units(self, local_path: Path) -> str:
        """Upload pickled units, return a URI workers can fetch."""

    def download_units(self, uri: str, local_path: Path) -> None:
        """Download pickled units to worker-local path."""

    def upload_staging(self, local_dir: Path) -> str:
        """Upload staged Parquet dir, return a URI."""

    def download_staging(self, uri: str, local_dir: Path) -> None:
        """Download staged Parquet to orchestrator-local dir."""
```

Implementations: `LocalTransfer` (no-op — paths are already accessible),
`S3Transfer`, `GCSTransfer`.

**What changes:** `_save_units()` and `_load_units()` go through the
strategy. `parquet_writer.py` writes locally, then the strategy uploads.
`StagingManager` downloads before reading. ~6 call sites.

**What doesn't change:** StagingArea, StagingManager internal logic,
DeltaCommitter, ArtifactStore, worker sandbox, all Path-based code
inside the worker.

**Strengths:**
- Smallest refactor surface — staging and dispatch code is ~200 lines
- Workers keep full POSIX semantics in their sandbox
- Clean separation: ephemeral transfer vs durable storage
- Easy to implement and test incrementally

**Weaknesses:**
- Double I/O for cloud: worker writes to /tmp, uploads to S3; orchestrator
  downloads from S3, reads from /tmp
- Doesn't solve Delta Lake access for workers (separate solution needed)
- Two abstractions emerge: TransferStrategy for staging, something else
  for Delta Lake

**Best for:** Getting to cloud fast with minimal risk. Good first step
even if a broader abstraction follows later.

---

### Option B: StorageProvider (broad — abstract all file operations)

Replace `pathlib.Path` operations throughout the storage layer with a
provider interface. Every read, write, list, exists, and mkdir goes
through the provider.

```python
class StorageProvider(ABC):
    """Abstract filesystem for staging and store access."""

    def read_bytes(self, key: str) -> bytes: ...
    def write_bytes(self, key: str, data: bytes) -> None: ...
    def read_parquet(self, key: str) -> pl.DataFrame: ...
    def write_parquet(self, key: str, df: pl.DataFrame) -> None: ...
    def list_keys(self, prefix: str) -> list[str]: ...
    def exists(self, key: str) -> bool: ...
    def mkdir(self, key: str) -> None: ...
    def rmtree(self, key: str) -> None: ...
    def join(self, *parts: str) -> str: ...
```

Implementations: `LocalStorageProvider` (delegates to pathlib.Path),
`S3StorageProvider` (delegates to boto3/s3fs).

**What changes:** StagingArea, StagingManager, DeltaCommitter,
ArtifactStore, parquet_writer, staging_verification — every component
that touches the filesystem. RuntimeEnvironment path fields become
strings. ~50+ call sites.

**What doesn't change:** Worker sandbox (always local), operation code.

**Strengths:**
- Single abstraction covers everything
- No double I/O — workers write directly to S3 staging
- Delta Lake access and staging use the same provider
- Clean, consistent API throughout the storage layer

**Weaknesses:**
- Large refactor surface (50+ call sites across 8+ files)
- Must handle behavioral differences: S3 has no mkdir, no atomic rename,
  eventual consistency, no glob (only prefix listing)
- Leaky abstraction risk: POSIX and object store semantics differ
  fundamentally (directories are real vs virtual, atomic ops, listing
  consistency)
- NFS-specific code (fsync, cache invalidation) becomes
  provider-specific, adding conditional logic

**Best for:** A clean long-term architecture. But high risk as a first
step — touching the entire storage layer at once.

---

### Option C: URI-first (strings everywhere, leverage existing libraries)

All storage references become URI strings. Local paths become
`file:///path` or just `/path`. S3 becomes `s3://bucket/key`. The
framework passes strings instead of `Path` objects. Libraries that
already support URIs (`deltalake-rs`, `polars`, `pyarrow`) handle the
actual I/O.

```python
# RuntimeEnvironment
delta_root: str        # "/data/delta" or "s3://bucket/delta"
staging_root: str      # "/data/staging" or "s3://bucket/staging"
working_root: str      # always local: "/tmp/artisan"
```

For operations that need POSIX semantics (mkdir, glob, rmtree), a thin
utility layer dispatches based on URI scheme:

```python
def storage_exists(uri: str) -> bool:
    if uri.startswith("s3://"):
        return _s3_exists(uri)
    return Path(uri).exists()

def storage_list(prefix: str) -> list[str]:
    if prefix.startswith("s3://"):
        return _s3_list(prefix)
    return [str(p) for p in Path(prefix).rglob("*")]
```

**What changes:** RuntimeEnvironment fields become `str`. Every
`.exists()` call becomes `storage_exists()`. Every `rglob()` becomes
`storage_list()`. `pl.scan_delta()` already takes strings — just stop
converting to `str(Path(...))`. ~30 call sites where `Path` objects are
constructed or `.exists()` is checked.

**What doesn't change:** `pl.read_parquet()`, `df.write_parquet()`,
`pl.scan_delta()`, `df.write_delta()` — these already accept URI
strings. Worker sandbox stays local Path.

**Strengths:**
- Leverages the fact that our core libraries (Polars, delta-rs) already
  handle S3/GCS URIs natively
- Minimal new abstraction — just utility functions, not a class hierarchy
- Delta Lake access works immediately (just pass the URI)
- Staging writes work immediately (`df.write_parquet("s3://...")`)
- No double I/O — workers write directly to cloud storage

**Weaknesses:**
- URI scheme dispatch in utility functions is ad-hoc — could proliferate
- No clear "provider" object to configure credentials, region, etc.
- Workers writing directly to S3 means no local-first staging (every
  write is a network call)
- Directory operations (list, glob, rmtree) need custom implementation
  per scheme

**Best for:** Pragmatic middle ground. Leverages existing library
capabilities without a large abstraction layer.

---

### Option D: fsspec (standard filesystem abstraction library)

Use the `fsspec` library (filesystem spec) as the abstraction layer.
fsspec is the Python standard for filesystem abstraction, used by
PyArrow, Polars, delta-rs, Dask, and most data tools. It provides a
`AbstractFileSystem` interface with implementations for local, S3, GCS,
Azure, HDFS, and 40+ other backends.

```python
import fsspec

# Local
fs = fsspec.filesystem("file")
fs.exists("/data/staging/step_0")
fs.ls("/data/staging/step_0")

# S3
fs = fsspec.filesystem("s3", key="...", secret="...")
fs.exists("bucket/staging/step_0")
fs.ls("bucket/staging/step_0")

# Both
with fs.open("bucket/staging/file.parquet", "wb") as f:
    df.write_parquet(f)
```

The framework would thread an `fsspec.AbstractFileSystem` instance
through RuntimeEnvironment (or a wrapper config). All Path-based code
would be rewritten to use the fs instance.

**What changes:** Same surface area as Option B (50+ call sites), but
using a standard library instead of a custom ABC. RuntimeEnvironment
carries an fs config (scheme + credentials). StagingArea, StagingManager,
etc. accept an fs instance.

**What doesn't change:** Worker sandbox (always local fs). Operations.

**Strengths:**
- Industry standard — used by Flyte, Dask, PyArrow
- 40+ filesystem implementations already exist
- Polars and delta-rs interop via `storage_options` dicts
- Credential management handled by fsspec
- Testing: `fsspec.filesystem("memory")` for in-memory tests

**Weaknesses:**
- Same large refactor surface as Option B
- fsspec API is different from pathlib.Path — can't be a drop-in
- fsspec is another dependency (though it's already a transitive dep
  of pyarrow/deltalake)
- Some fsspec implementations have quirks (S3 trailing slashes, GCS
  consistency)

**Best for:** If we're going to do a broad refactor anyway, using the
standard library is better than inventing our own.

---

### Option E: Return-value staging (eliminate staging filesystem for cloud)

For cloud backends, workers don't write to a staging filesystem at all.
Instead, they return staged data through the DispatchHandle transport.

Currently: worker → Parquet files on disk → orchestrator reads files
Proposed: worker → serialized DataFrames → DispatchHandle.collect() →
orchestrator commits

```python
@dataclass
class StagingPayload:
    """Everything a worker produces, ready for commit."""
    execution_run_id: str
    artifacts: dict[str, pl.DataFrame]   # table_name → rows
    index: pl.DataFrame
    edges: pl.DataFrame
    execution_record: pl.DataFrame
    success: bool
    error: str | None
```

The worker's `parquet_writer.py` would have two modes:
- Filesystem mode (local/SLURM): write Parquet files as today
- Return mode (cloud): accumulate DataFrames in memory, return as payload

The DispatchHandle returns `list[StagingPayload]` instead of
`list[dict]`.

**What changes:** `parquet_writer.py` gains a return-value mode.
`execute_unit_task` returns `StagingPayload` instead of `dict`.
`DeltaCommitter` accepts DataFrames directly instead of reading from
staging. DispatchHandle collect() returns typed payloads. ~15 call sites.

**What doesn't change:** StagingArea/StagingManager for shared-FS
backends. Worker sandbox. Delta Lake commit logic (just receives
DataFrames from a different source).

**Strengths:**
- Eliminates staging filesystem for cloud entirely — no S3 staging
  directory, no upload/download, no cleanup
- Data flows through the dispatch mechanism, which is already
  backend-specific (Modal returns values, Ray returns via object store)
- Simplest cloud worker: receive unit, execute, return results
- No credential management for staging storage on workers
- Natural fit for Modal `.map()` (return values) and Ray (object store)

**Weaknesses:**
- Large payloads in return values (DataArtifact content can be MB+)
- Some backends have return size limits (Lambda: 6MB sync response)
- Diverges the code path: filesystem staging for shared-FS backends,
  return-value staging for cloud backends
- Workers must hold all staged data in memory until return (no streaming)
- SLURM can't use this (results must survive worker process exit → disk)

**Best for:** Serverless backends (Modal, Lambda, Cloud Run) where there's
no shared storage and the dispatch mechanism already handles return
values. Not a universal solution — shared-FS backends still need
filesystem staging.

---

### Option F: Layered approach (different mechanism per concern)

Recognize that unit transport, staging, and store access are different
problems. Use the simplest mechanism for each:

| Concern | Shared-FS backends | Cloud backends |
|---------|-------------------|----------------|
| **Unit transport** | Pickle on shared FS | Embed in DispatchHandle (handle carries units internally) |
| **Staging** | Parquet on shared FS | Return-value via DispatchHandle (Option E) |
| **Store access** | Local Delta Lake path | URI string to remote Delta Lake (delta-rs native) |
| **Sandbox** | Local /tmp | Local /tmp |

The DispatchHandle becomes responsible for both unit delivery and result
collection. For shared-FS backends, it writes/reads pickle files and
Parquet files on disk (wrapping current behavior). For cloud backends, it
serializes units into its native transport and receives results back.

```python
class DispatchHandle(ABC):
    def dispatch(self, units: list[ExecutionUnit], runtime_env: RuntimeEnvironment) -> None:
        """Send units to workers. Handle owns the transport."""

    def collect(self) -> list[StagingPayload]:
        """Get staged results back. Handle owns the transport."""
```

Note: `dispatch()` takes units directly (not a file path). The handle
decides how to transport them. `collect()` returns typed payloads (not
dicts). The handle decides how to receive them.

For Delta Lake, RuntimeEnvironment's `delta_root_path` becomes a string
that can be a local path or an S3 URI. Workers pass it to
`pl.scan_delta()` which handles both.

**What changes:**
- DispatchHandle interface evolves (takes units, returns typed payloads)
- `_save_units()` / `_load_units()` move inside handle implementations
- `execute_unit_task` returns `StagingPayload` instead of `dict`
- RuntimeEnvironment `delta_root_path` becomes `str`
- `ArtifactStore` / `ProvenanceStore` drop `.exists()` guard for URI
  compatibility (or use a `storage_exists()` helper)
- DeltaCommitter gains a direct-DataFrame path alongside staged-file path

**What doesn't change:** StagingArea/StagingManager (still used by
shared-FS backends inside their handles). Worker sandbox. Operations.

**Strengths:**
- Each concern uses the natural mechanism for its context
- DispatchHandle is the single integration point for cloud backends —
  no separate transfer strategy to configure
- Delta Lake access leverages delta-rs native URI support (zero new code)
- Clean migration path: refactor handles first, then add cloud handles
- Shared-FS backends barely change (handles wrap existing code)

**Weaknesses:**
- DispatchHandle has more responsibility than the current design doc
  envisions (transport + lifecycle)
- The StagingPayload type couples the handle interface to staging
  internals
- Large payloads in memory for cloud backends (same as Option E)

**Best for:** Long-term architecture where the DispatchHandle is the
primary abstraction for backend differences, not just a lifecycle
manager.

---

### Option G: Backend-owned staging (maximum backend autonomy)

Each backend fully owns how staging works. The framework contract is:
"dispatch units, get results back." How data moves is entirely internal
to the backend implementation.

```python
class BackendBase(ABC):
    def create_dispatch_handle(
        self,
        resources: ResourceConfig,
        execution: ExecutionConfig,
        step_number: int,
        job_name: str,
    ) -> DispatchHandle:
        """Backend owns everything: transport, staging, collection."""
```

The DispatchHandle's `collect()` returns committed-ready DataFrames.
The orchestrator never touches staging files — it receives DataFrames
and commits them to Delta Lake.

- LocalHandle: uses ProcessPool, workers write Parquet to local disk,
  collect() reads and returns DataFrames
- SlurmHandle: uses sbatch, workers write Parquet to NFS, collect()
  reads and returns DataFrames
- ModalHandle: uses .map(), workers return DataFrames through Modal,
  collect() returns them
- K8sHandle: uses K8s Jobs, workers write to S3, collect() downloads
  and returns DataFrames

**What changes:** DispatchHandle.collect() returns DataFrames.
DeltaCommitter gains a "commit from DataFrames" path (no staging read).
Step executor doesn't interact with staging at all — just passes
DataFrames from handle to committer.

**What doesn't change:** The staging implementation (StagingArea,
StagingManager, parquet_writer) still exists and is used internally by
shared-FS handles. Operations. Worker sandbox.

**Strengths:**
- Maximum flexibility per backend
- Clean orchestrator: dispatch → collect → commit
- No universal storage abstraction needed
- Each backend can optimize its data path (Modal returns in-memory,
  SLURM uses NFS, K8s uses S3)

**Weaknesses:**
- Duplicates staging logic across backends (each handle must implement
  "read staged data and return DataFrames")
- Harder to test (each backend has its own staging path)
- New backends have more to implement

**Best for:** If backends are genuinely so different that a unified
abstraction is more hindrance than help.

---

### Option H: Pre-hydrated units (workers never access the store)

The orchestrator fully materializes inputs before dispatch. Workers
receive everything they need — no store access required. Staged results
come back through the DispatchHandle.

```
Orchestrator:
  resolve inputs → read artifact content from Delta Lake →
  embed content in ExecutionUnit (or sidecar payload) →
  dispatch via handle

Worker:
  receive unit with embedded content →
  materialize from embedded bytes (not from store) →
  run operation →
  return staged results via handle

Orchestrator:
  collect results → commit to Delta Lake
```

The ExecutionUnit gains an optional `materialized_content` field:

```python
class ExecutionUnit(BaseModel):
    operation: OperationDefinition
    inputs: dict[str, list[str]]  # role → artifact IDs
    materialized_content: dict[str, bytes] | None = None  # artifact_id → content
    # ... other fields
```

For shared-FS backends, `materialized_content` is None (workers read
from the store as today). For cloud backends, the orchestrator populates
it before dispatch.

**What changes:** ExecutionUnit gains optional content field.
Orchestrator pre-hydrates content for cloud backends. Worker
materialization code checks unit content before store access. ~10 call
sites.

**What doesn't change:** Everything for shared-FS backends. StagingArea,
StagingManager, DeltaCommitter. Operations.

**Strengths:**
- Workers are completely stateless — receive data, compute, return data
- No Delta Lake access from workers at all
- No storage credentials needed on workers
- No remote Delta Lake setup
- Workers don't need `deltalake` or `pyarrow` for store access (smaller
  container images)
- Natural fit for the `materialize=False` path (content is already bytes)

**Weaknesses:**
- ExecutionUnit size grows (carries artifact content)
- For large artifacts (MB-GB), pickle files become very large
- Transfer overhead: content is serialized into the unit, sent to worker,
  deserialized, then materialized to disk. Three copies.
- Doesn't scale for operations with many large input artifacts
- Orchestrator must read all input content before dispatching any units
  (higher memory pressure)

**Best for:** Operations with small inputs (KB-MB) on serverless backends
where simplicity matters more than transfer efficiency. Bioinformatics
sequence files, config artifacts, small datasets.

---

### Option I: Sidecar payload (unit stays small, content travels separately)

Like Option H, but artifact content is uploaded separately rather than
embedded in the unit. The unit carries IDs; a companion payload carries
content.

```
Orchestrator:
  batch artifact IDs into ExecutionUnit (small) →
  read artifact content → upload to object store →
  dispatch unit via handle (unit includes content URI)

Worker:
  receive unit + content URI →
  download content from object store →
  materialize from downloaded bytes →
  run operation →
  upload results to object store →
  return result metadata via handle

Orchestrator:
  collect result metadata →
  download results from object store →
  commit to Delta Lake
```

```python
class ExecutionUnit(BaseModel):
    # ... existing fields
    content_uri: str | None = None  # "s3://bucket/dispatch/step_3/unit_0/"
```

**What changes:** Orchestrator exports input artifacts to object store
before dispatch. Unit carries a content URI. Worker downloads content
instead of reading Delta Lake. Worker uploads results instead of writing
to staging FS. ~15 call sites.

**What doesn't change:** Unit stays small. Shared-FS backends unchanged
(content_uri is None). Operations.

**Strengths:**
- Unit stays small (IDs + URI, not embedded content)
- Workers don't need Delta Lake access
- Content transfer is parallelizable (multi-part upload/download)
- Object store acts as the transfer medium (same as TransferStrategy
  but with clearer boundaries)
- Works for large artifacts

**Weaknesses:**
- Three storage locations: Delta Lake (durable), object store (transfer),
  worker /tmp (scratch)
- Orchestrator must export artifacts before each dispatch (adds latency)
- Object store cleanup needed after commit
- Workers need S3 credentials (for download + upload)

**Best for:** Cloud backends with large artifacts where embedding
content in the unit is impractical.

---

### Comparison Matrix

| Option | Refactor size | Double I/O | Solves DL access | Cloud-native | Shared-FS impact |
|--------|--------------|-----------|------------------|-------------|-----------------|
| A. TransferStrategy | Small (~6 sites) | Yes | No | Partial | None |
| B. StorageProvider | Large (~50 sites) | No | Yes | Yes | Large refactor |
| C. URI-first | Medium (~30 sites) | No | Yes | Yes | Medium refactor |
| D. fsspec | Large (~50 sites) | No | Yes | Yes | Large refactor |
| E. Return-value | Medium (~15 sites) | No | N/A | Yes | None (parallel path) |
| F. Layered | Medium (~20 sites) | No | Yes (URI) | Yes | Small |
| G. Backend-owned | Small (framework) | No | Backend decides | Yes | None |
| H. Pre-hydrated | Small (~10 sites) | Content in unit | Eliminated | Yes | None |
| I. Sidecar payload | Medium (~15 sites) | Upload/download | Eliminated | Yes | None |

---

## Decision 3: Backend Traits

The traits system encodes behavioral differences between backends that
the framework needs to respond to. Current traits cover one axis
(shared filesystem). Cloud backends vary along more axes.

### Axes of variation

| Axis | LOCAL | SLURM | K8s+NFS | K8s+S3 | Modal+Vol | Modal+S3 | Batch | Lambda | Ray |
|------|-------|-------|---------|--------|-----------|----------|-------|--------|-----|
| Shared FS | no | yes | yes | no | yes | no | no | no | depends |
| Worker identity | none | env var | env var | env var | none | none | env var | none | none |
| Code delivery | shared | shared | image | image | image+mount | image+mount | image | image | package |
| Store access | local | NFS | NFS | S3 | volume | S3 | S3 | S3 | depends |
| GPU | no | yes | yes | yes | yes | yes | yes | no | yes |
| Timeout | none | soft | none | none | configurable | configurable | soft | hard 15m | none |
| Worker lifecycle | pool | per-job | per-pod | per-pod | ephemeral | ephemeral | per-container | ephemeral | task/actor |
| Cancellation | kill pool | scancel | delete job | delete job | cancel call | cancel call | cancel job | - | cancel task |

Not all of these need to be traits. The question is: which differences
require different **framework behavior** (code paths in the orchestrator
or worker)?

### Which traits drive framework behavior?

| Trait | Framework behavior it drives |
|-------|------------------------------|
| `shared_filesystem` | Staging verification, NFS fsync, data transfer |
| `worker_id_env_var` | How workers identify their index in a job array |
| `staging_method` | Whether staging goes through FS, return value, or object store |
| `has_store_access` | Whether workers can read Delta Lake (affects input delivery) |
| `supports_cancellation` | Whether the handle implements cancel() |
| `ephemeral_workers` | Whether model caching across invocations is possible |

Axes that are NOT traits (just backend configuration):
- GPU support → ResourceConfig validation
- Timeout → ResourceConfig.time_limit
- Code delivery → backend constructor config
- Cancellation mechanics → DispatchHandle internal

### Option A: Flat expansion (add fields to existing dataclasses)

```python
@dataclass(frozen=True)
class WorkerTraits:
    worker_id_env_var: str | None = None
    shared_filesystem: bool = False
    has_store_access: bool = True        # workers can read Delta Lake
    ephemeral_workers: bool = False      # no state across invocations

@dataclass(frozen=True)
class OrchestratorTraits:
    shared_filesystem: bool = False
    staging_verification_timeout: float = 60.0
```

**Strengths:** Simple, backwards compatible (new fields have defaults).
**Weaknesses:** Gets unwieldy as more backends are added.

### Option B: Storage model enum

Replace the boolean with a higher-level concept:

```python
class StagingModel(Enum):
    SHARED_FILESYSTEM = auto()  # LOCAL, SLURM, K8s+NFS, Modal+Vol
    OBJECT_STORE = auto()       # K8s+S3, Batch, Lambda
    RETURN_VALUE = auto()       # Modal, Ray (data comes back via handle)
    SELF_CONTAINED = auto()     # Content embedded in unit (pre-hydrated)

@dataclass(frozen=True)
class WorkerTraits:
    worker_id_env_var: str | None = None
    staging_model: StagingModel = StagingModel.SHARED_FILESYSTEM
    has_store_access: bool = True
```

**Strengths:** Higher-level abstraction — the framework switches behavior
based on staging model, not individual booleans. Extensible.
**Weaknesses:** Some backends could use multiple models (Modal can do
shared FS or return value). Enum may be too rigid.

### Option C: Capability set

Backends declare capabilities. The framework checks for specific
capabilities when making decisions.

```python
class Capability(Enum):
    SHARED_FILESYSTEM = auto()
    STORE_ACCESS = auto()
    ARRAY_JOBS = auto()
    GPU_SCHEDULING = auto()
    CANCELLATION = auto()
    LOG_STREAMING = auto()

class BackendBase(ABC):
    capabilities: ClassVar[frozenset[Capability]]
```

**Strengths:** Open-ended, easy to add new capabilities.
**Weaknesses:** Capabilities don't encode degree (shared_filesystem
is yes/no, but store_access could be local/remote/none). Checking
capabilities in framework code is verbose.

### Option D: Traits follow from storage decision

If Decision 1 lands on an approach where the DispatchHandle owns
transport (Options F or G), traits become simpler. The framework doesn't
need to know whether the filesystem is shared — the handle abstracts it.

```python
@dataclass(frozen=True)
class WorkerTraits:
    worker_id_env_var: str | None = None
    # That's it. Storage behavior is owned by the DispatchHandle.
```

**Strengths:** Minimal. The handle is the abstraction, not the traits.
**Weaknesses:** Some framework code outside the handle still needs to
know about storage (e.g., RuntimeEnvironment construction, failure log
paths).

### Option E: Keep minimal, expand on demand (YAGNI)

Don't expand traits proactively. When implementing each cloud backend,
add exactly the traits that backend needs. If no backend needs
`ephemeral_workers`, don't add it.

**Strengths:** No speculative design. Every trait is justified by a
concrete backend.
**Weaknesses:** May lead to inconsistent trait additions if not
coordinated.

---

## Decision 4: Delta Lake Location

Where does Delta Lake live when workers run on cloud infrastructure?

### Option A: Local-only (workers never access Delta Lake)

Delta Lake stays on the orchestrator's local disk. Workers get everything
they need from the ExecutionUnit (either pre-hydrated content or a
sidecar payload). Workers return results through the DispatchHandle.
Orchestrator commits locally.

**Strengths:** Simplest. No remote Delta Lake. No credentials on workers.
No concurrent access concerns.
**Weaknesses:** Workers can't do ad-hoc store queries. Orchestrator must
pre-hydrate all inputs (adds memory pressure and dispatch latency).

### Option B: Remote Delta Lake on object storage

`DeltaTable("s3://bucket/delta")` everywhere. Orchestrator and workers
both read/write via S3. RuntimeEnvironment carries S3 URIs.

**Strengths:** Universal access. Workers hydrate inputs directly. Hybrid
pipelines work (different backends share the same store). `deltalake-rs`
already supports this.
**Weaknesses:** Orchestrator writes must be coordinated (only
orchestrator writes, workers only read). S3 latency for metadata queries.
S3 costs for list/get operations. Need S3 credentials everywhere.
`table_path.exists()` checks don't work for S3 URIs — need a different
guard.

### Option C: Local-write, remote-read

Orchestrator writes Delta Lake locally (or to S3). Workers read from S3
but never write. The orchestrator syncs local Delta tables to S3 after
each commit (or writes directly to S3).

**Strengths:** Preserves local write performance. Workers have read
access for input hydration. No concurrent write concerns.
**Weaknesses:** Sync latency between commits and worker reads. Two copies
of the store (local + S3). Complexity of keeping them in sync.

### Option D: Remote Delta Lake, local cache

Delta Lake lives on S3. Orchestrator caches table metadata locally for
fast queries. Reads hit the cache first, falling through to S3. Writes go
to S3 directly.

**Strengths:** Single source of truth. Fast local reads. Workers and
orchestrator use the same URIs.
**Weaknesses:** Cache invalidation. delta-rs doesn't have built-in
caching (it re-reads the log on each operation). Would need a custom
caching layer.

### Option E: Delta Lake on S3, workers get read-only snapshot

Before dispatching a step, the orchestrator exports the relevant input
artifacts to lightweight Parquet files (not full Delta tables) on S3.
Workers read from the snapshot. The orchestrator commits to the full
Delta Lake.

```python
# Orchestrator before dispatch:
input_artifacts = store.get_artifacts(artifact_ids)
snapshot_uri = upload_snapshot(input_artifacts, s3_prefix)

# Worker:
artifacts = read_snapshot(snapshot_uri)  # simple Parquet read, not Delta
```

**Strengths:** Workers don't need delta-rs. Snapshot is minimal (only
relevant artifacts, not full tables). Simple worker implementation.
**Weaknesses:** Snapshot export adds latency. Another storage location to
manage. Snapshot must include enough context for input materialization.

### Option F: Workers proxy through orchestrator

Workers access the store through an API running on the orchestrator's
network. The orchestrator exposes a lightweight read-only artifact
service.

**Strengths:** Workers need no store access or credentials. Orchestrator
controls all access. Single source of truth.
**Weaknesses:** Network latency. Orchestrator becomes a bottleneck.
Requires a running service (complexity). Doesn't work if workers can't
reach the orchestrator's network.

### Option G: Decision depends on backend (configurable)

Different backends use different strategies:

- K8s + NFS PVC: local Delta Lake on shared FS (same as SLURM)
- Modal + Volume: Delta Lake on Modal Volume
- AWS Batch + S3: Delta Lake on S3
- Lambda: pre-hydrated units (no store access)
- Ray: Delta Lake on whatever the cluster shares

The backend declares its storage strategy. The framework adapts.

**Strengths:** Each backend uses the natural approach. No one-size-fits-all.
**Weaknesses:** More complexity in the framework. Must be tested per
backend.

---

## Decision 5: Worker Image Strategy

How do cloud workers get artisan + operation code?

### Option A: Pre-built container image (baked)

Users build a Docker image with artisan framework + their operation code
+ all dependencies. Backend config references the image.

```python
ModalBackend(image="myregistry/artisan-ops:latest")
K8sBackend(image="myregistry/artisan-ops:v2.1")
```

**Strengths:** Reproducible. Fast startup (no dynamic install). Standard
practice (Nextflow, Flyte, Dagster). Image layer caching.
**Weaknesses:** Rebuild on every code change. Registry management. Image
size (GPU deps can be 10+ GB). Slow iteration during development.

### Option B: Base image + code upload (Metaflow pattern)

A base `artisan-worker` image has the framework + heavy deps. User
operation source code is tarballed, uploaded to cloud storage, and
extracted in the container at startup.

```python
# At dispatch time:
code_uri = upload_code_package(operations_dir)

# Container entrypoint:
download_and_extract(code_uri)
import artisan.worker  # starts listening for units
```

**Strengths:** Fast iteration (no image rebuild for code changes). Base
image is reused across projects. Only code changes trigger upload.
**Weaknesses:** Startup latency (download + extract). Code package must
include all source files. Dependencies must be in the base image.

### Option C: Base image + code mount (Modal pattern)

Modal's `Mount.from_local_dir()` mounts local directories into the
container at runtime. No upload step — Modal handles the transfer.

```python
ModalBackend(
    image=modal.Image.pip_install("artisan"),
    mounts=[modal.Mount.from_local_dir("./src", remote_path="/app/src")],
)
```

**Strengths:** Instant code updates. No image rebuild. Modal caches
mounts. Similar: Kubernetes hostPath mounts for dev, though not for
production.
**Weaknesses:** Modal-specific. Not portable to other backends. Mount
staleness (cached version may lag).

### Option D: Cloudpickle serialization (ship the operation)

Use cloudpickle to serialize the operation class itself. Workers
deserialize and execute it. No operation source code in the image.

```python
# Orchestrator:
serialized_op = cloudpickle.dumps(MyOperation)
unit = ExecutionUnit(operation=serialized_op, ...)

# Worker:
op = cloudpickle.loads(unit.operation)
op.execute(...)
```

**Strengths:** Zero image management for operations. Operations can be
defined in notebooks, scripts, anywhere. Instant code changes. This is
how Modal works internally.
**Weaknesses:** cloudpickle can't serialize C extensions or complex
native objects. Unpickling untrusted code is a security concern. Fragile
across Python versions. Operation dependencies must still be in the image.

### Option E: Environment declaration (Flyte ImageSpec pattern)

Operations declare their dependencies. The framework builds or selects
the appropriate image automatically.

```python
class MyOperation(OperationDefinition):
    image = ImageSpec(
        packages=["torch==2.0", "biopython"],
        cuda="12.1",
    )
```

At dispatch time, the framework hashes the spec, checks a registry for
an existing image, and builds one if needed. Content-hash dedup avoids
unnecessary rebuilds.

**Strengths:** Declarative. Operations are self-contained. Automatic
dedup. No manual image management.
**Weaknesses:** Build system complexity. Slow first build. Not all deps
are pip-installable. Framework takes on image build responsibility.

### Option F: No framework management (document the pattern)

The framework doesn't manage images. Users provide a container image URI
as backend config. The project provides a Dockerfile template and
documentation.

```dockerfile
# Provided template
FROM python:3.11-slim
RUN pip install artisan
COPY src/ /app/src/
ENV PYTHONPATH=/app
```

**Strengths:** Zero framework complexity. Users have full control. Works
with any CI/CD pipeline. No opinions about registries, build tools, etc.
**Weaknesses:** More work for users. No validation that operations are
importable. Easy to get wrong (missing deps, wrong Python version).

---

## Open Questions (expanded)

### Result contract

Currently `list[dict]` with informal keys. Options:

**(A) Typed dataclass:**
```python
@dataclass
class UnitResult:
    success: bool
    error: str | None
    item_count: int
    execution_run_ids: list[str]
    worker_log: str | None = None
```

**(B) Tagged union:**
```python
UnitResult = Success(run_ids, item_count) | Failure(error, run_ids) | Cancelled()
```

**(C) Part of StagingPayload (if Decision 1 picks E/F/G):**
The result type includes both metadata AND staged data, eliminating the
separate staging read.

### Resource escalation on retry

**(A) Callable fields:**
```python
ResourceConfig(memory_gb=lambda attempt: 8 * attempt)
```
Problem: ResourceConfig is a Pydantic model used in cache keys — lambdas
break serialization and hashing.

**(B) Escalation profile (separate from ResourceConfig):**
```python
RetryConfig(
    max_retries=3,
    escalation={"memory_gb": "2x", "time_limit": "1.5x"},
)
```
Keeps ResourceConfig clean. Escalation is applied at dispatch time.

**(C) Per-attempt resource list:**
```python
RetryConfig(
    resources_per_attempt=[
        ResourceConfig(memory_gb=8),
        ResourceConfig(memory_gb=16),
        ResourceConfig(memory_gb=32),
    ]
)
```
Explicit. No magic multipliers.

**(D) Backend auto-escalation:**
The backend detects OOM/timeout failures and automatically retries with
more resources. No user configuration.

### Prefect as optional dependency

**(A) Optional extras:** `pip install artisan[prefect]`. Non-Prefect
backends work without it. Prefect is only needed for local/SLURM.

**(B) Replace Prefect for local backend:** Rewrite LocalBackend using raw
`ProcessPoolExecutor` (Artisan already wraps it in
`SIGINTSafeProcessPoolTaskRunner`). Keep Prefect only for SLURM via
submitit.

**(C) Full removal:** Replace both backends. DispatchHandle provides the
same interface without Prefect. Local uses ProcessPool directly. SLURM
uses submitit directly (which Prefect-submitit wraps anyway).

---

## What's next

This document enumerates options. The next step is to converge: pick one
option per decision, document the rationale, and update
`cloud-deployment.md` with the chosen approach.

Decisions are not fully independent — some combinations are more natural:

| If Decision 1 is... | Then Decision 3... | Then Decision 4... |
|---------------------|-------------------|-------------------|
| A (TransferStrategy) | Needs `has_store_access` trait | Must solve DL access separately |
| C (URI-first) | Minimal trait changes | B or C (remote DL via URI) |
| E (Return-value) | Needs `staging_model` trait | A (local-only DL) or H (pre-hydrated) |
| F (Layered) | D (traits follow from handle) | G (per-backend) |
| G (Backend-owned) | D or E (minimal traits) | G (per-backend) |
| H (Pre-hydrated) | Needs `has_store_access` trait | A (local-only, eliminated) |
