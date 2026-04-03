# Design: Cloud Deployment

**Status:** Draft — core decisions pending

---

## Problem

Artisan runs on local machines and SLURM clusters. Both share a POSIX
filesystem. Cloud backends (Kubernetes, Modal, AWS Batch, Lambda, Ray) do
not. Two things block cloud deployment:

- **Storage is POSIX-coupled.** `RuntimeEnvironment` paths are
  `pathlib.Path`. Staging, commit, and unit dispatch all assume local
  filesystem access. Workers on cloud infrastructure can't read or write
  these paths.

- **Dispatch is Prefect-coupled.** Both backends use `_build_prefect_flow()`
  to create a Prefect `@flow` with a `TaskRunner`. Cloud backends like
  Modal and Lambda have no Prefect task runner. The interface *allows*
  returning any callable, but the only shared helper assumes Prefect.

Everything else is cloud-ready: the backend ABC, traits system,
`ExecutionUnit` serialization, content-addressed artifacts, the
orchestrator-worker split, and operations themselves.

---

## Scope

This design covers the minimum changes to make Artisan pipelines run on
cloud compute. It does **not** cover:

- Streaming/async pipeline execution
- Multi-cloud orchestrator deployment (orchestrator stays local)
- Cost optimization or autoscaling
- A CLI for building worker images

---

## Current Architecture

```
Orchestrator (local Python process)
    ├── Resolve inputs (Delta Lake on POSIX)
    ├── Batch into ExecutionUnits
    ├── Pickle units to shared filesystem
    ├── backend.create_flow() → Prefect @flow callable
    ├── step_flow(units_path, runtime_env) → list[dict]
    │       ├── LOCAL:  ProcessPool (same machine)
    │       └── SLURM:  sbatch job array (shared NFS)
    ├── Workers read units from shared filesystem
    ├── Workers write staged Parquet to shared filesystem
    ├── Orchestrator reads staged Parquet
    └── Commit atomically to Delta Lake
```

Key coupling points in the current code:

| Component | File | Coupling |
|-----------|------|----------|
| `RuntimeEnvironment` | `schemas/execution/runtime_environment.py` | 3 `Path` fields |
| `_save_units()` | `orchestration/engine/dispatch.py` | Writes pickle to `staging_root / "_dispatch"` |
| `_load_units()` | `orchestration/engine/dispatch.py` | Reads pickle from local `Path` |
| `_build_prefect_flow()` | `orchestration/backends/base.py` | Constructs Prefect `@flow` |
| `StagingArea` | `storage/io/staging.py` | `Path.mkdir()`, `write_parquet(path)` |
| `StagingManager` | `storage/io/staging.py` | `Path.glob()`, `read_parquet(path)` |
| `DeltaCommitter` | `storage/io/commit.py` | `DeltaTable(path)` |
| `ArtifactStore` | `storage/core/artifact_store.py` | `DeltaTable(path)` |
| Worker sandbox | `execution/context.py` | `Path.mkdir()`, local scratch dirs |

---

## Target Architecture

```
Orchestrator (local Python process)
    ├── Resolve inputs (Delta Lake — local or remote)
    ├── Batch into ExecutionUnits
    ├── Transfer units via StorageProvider
    │       ├── LOCAL/SLURM:  write pickle to shared filesystem
    │       └── Cloud:        upload pickle to S3/GCS
    ├── Dispatch via DispatchHandle
    │       ├── LOCAL:       ProcessPool
    │       ├── SLURM:       sbatch job array
    │       ├── Kubernetes:  K8s Job (Prefect or direct)
    │       ├── Modal:       .map() (native)
    │       └── AWS Batch:   SubmitJob
    ├── Workers download units via StorageProvider
    ├── Workers execute locally (sandbox in /tmp)
    ├── Workers upload staged Parquet via StorageProvider
    ├── Orchestrator downloads staged Parquet via StorageProvider
    └── Commit to Delta Lake (local or remote via delta-rs)
```

Operations are unchanged. The worker sandbox is always local (`/tmp`).
The framework handles data transfer transparently.

---

## Design Decisions

### Decision 1: Storage Abstraction

**The question:** How should the framework abstract the difference between
POSIX paths and cloud object storage?

**Options:**

**(A) TransferStrategy — upload/download around existing paths.**
Workers always use local `/tmp` paths. The framework uploads/downloads
units and staged files between the orchestrator and workers. Existing
path-based code is untouched inside the sandbox.

```python
class TransferStrategy(ABC):
    def upload_units(self, local_path: Path) -> str: ...
    def download_units(self, uri: str, local_path: Path) -> None: ...
    def upload_staging(self, local_dir: Path) -> str: ...
    def download_staging(self, uri: str, local_dir: Path) -> None: ...
```

Pros: minimal code changes, workers keep POSIX semantics internally.
Cons: double I/O (download to /tmp, process, upload from /tmp). Doesn't
help Delta Lake access from workers.

**(B) StorageProvider — abstract file operations.**
Replace `pathlib.Path` operations with a provider interface. Provider has
local and cloud implementations. Workers call `provider.read()` /
`provider.write()` instead of `Path.read_bytes()` / `Path.write_bytes()`.

```python
class StorageProvider(ABC):
    def read_bytes(self, key: str) -> bytes: ...
    def write_bytes(self, key: str, data: bytes) -> None: ...
    def write_parquet(self, key: str, df: pl.DataFrame) -> None: ...
    def read_parquet(self, key: str) -> pl.DataFrame: ...
    def list_keys(self, prefix: str) -> list[str]: ...
    def exists(self, key: str) -> bool: ...
```

Pros: unified interface, works for staging and Delta Lake access.
Cons: larger refactor surface (StagingArea, StagingManager, DeltaCommitter).

**(C) Hybrid — TransferStrategy for staging, URI support for Delta Lake.**
Workers use local paths for sandbox I/O (untouched). Staging transfer
uses a TransferStrategy. Delta Lake tables accept URI strings (already
supported by `deltalake-rs`: `DeltaTable("s3://bucket/delta")`). The
`RuntimeEnvironment` path fields become `str | Path`.

Pros: minimal worker-side changes, leverages delta-rs S3 support.
Cons: two abstractions instead of one.

**Status:** Open — needs decision.

---

### Decision 2: Dispatch Abstraction

**Decision: DispatchHandle.** Already designed in
`_dev/design/1_future/dispatch_handle.md`. The interface
(`dispatch`/`is_done`/`collect`/`cancel` + blocking `run()` convenience
method) is well-specified with implementation sketches for Local and SLURM
backends, state machine semantics, cancellation wiring to
`PipelineManager`, and a file-by-file scope of changes.

The cross-framework research validates this: Flyte's agent/connector
pattern uses the same `create`/`get`/`delete` lifecycle, and Prefect's
own task runner model breaks down for serverless backends that need
infrastructure provisioning per invocation.

Cloud backends implement `DispatchHandle` directly against native APIs:
- Modal: `dispatch()` calls `modal.Function.map()`, `collect()` gathers
- Kubernetes: `dispatch()` creates a K8s Job, `is_done()` polls status
- AWS Batch: `dispatch()` calls `SubmitJob`, `is_done()` polls
- Ray: `dispatch()` submits `ray.remote()` tasks, `collect()` calls
  `ray.get()`

**Status:** Decided. Implementation plan in `dispatch_handle.md`.

---

### Decision 3: Backend Traits

**The question:** Are the current traits sufficient for cloud backends?

Current traits encode one distinction: shared filesystem (yes/no). Cloud
backends vary along more axes.

**Proposed expansion (drawing from Snakemake's CommonSettings):**

```python
@dataclass(frozen=True)
class WorkerTraits:
    worker_id_env_var: str | None = None
    shared_filesystem: bool = False
    requires_code_deployment: bool = False    # code must be in container image
    requires_storage_passthrough: bool = False # storage config forwarded to workers
```

`requires_code_deployment` would trigger validation that operations are
importable in the target container image. `requires_storage_passthrough`
would include storage credentials in the RuntimeEnvironment.

**Status:** Low urgency — can be added incrementally as backends need them.

---

### Decision 4: Delta Lake Location

**The question:** Where does Delta Lake live when workers are on cloud
infrastructure?

**Options:**

**(A) Local Delta Lake, staging transfer only.**
Delta Lake stays on the orchestrator's local disk. Workers don't read
Delta Lake directly — inputs are materialized into the ExecutionUnit or
transferred alongside it. Staged outputs are transferred back. The
orchestrator commits locally.

Pros: no remote Delta Lake setup, simplest. Cons: workers can't do cache
lookups or hydrate artifacts not in their unit.

**(B) Remote Delta Lake on object storage.**
`DeltaTable("s3://bucket/delta")` — already supported by `deltalake-rs`.
Workers and orchestrator both read/write via S3. `RuntimeEnvironment`
paths become URIs.

Pros: workers have full store access, hybrid pipelines work. Cons:
concurrent writes need coordination, latency for metadata queries.

**(C) Local Delta Lake for orchestrator, read-only remote access for workers.**
Orchestrator writes locally (or to S3). Workers read from S3 for input
hydration but write staged outputs to a transfer location. The
orchestrator commits.

Pros: preserves atomic commit pattern, workers can hydrate inputs.
Cons: workers need read credentials, still need transfer for staging.

**Status:** Open — depends on Decision 1.

---

### Decision 5: Worker Image Strategy

**The question:** How do cloud workers get artisan + operation code?

**Options:**

**(A) Pre-built container image.**
Users build a Docker image with artisan and their operations installed.
The backend references this image. Standard in Nextflow, Flyte, Dagster.

Pros: reliable, reproducible. Cons: rebuild on every code change.

**(B) Base image + code mount/upload.**
A base `artisan-worker` image has the framework and heavy deps. Operation
source code is uploaded to cloud storage and downloaded at container
startup (Metaflow pattern) or mounted (Modal `Mount.from_local_dir()`).

Pros: fast iteration during development. Cons: adds startup latency,
mount availability varies by backend.

**(C) Both — configurable per backend.**
Production uses pre-built images. Development uses base image + code
upload. The backend exposes a `code_delivery` config.

**Status:** Low urgency — operational concern, not a framework blocker.
Document the pattern; don't build tooling yet.

---

## Implementation Phases

Each phase is independently shippable.

### Phase 1: DispatchHandle

Decouple dispatch from Prefect. Implement the handle interface. Wrap
existing Prefect-based backends in handles. This is a prerequisite for
Modal and other non-Prefect backends, and independently valuable for
cancellation support.

**Scope:** `BackendBase`, `LocalBackend`, `SlurmBackend`, `step_executor`.

### Phase 2: Storage Abstraction

Introduce the storage abstraction (whichever option is chosen). Implement
local (passthrough) and S3 providers. Modify worker dispatch and
orchestrator collection to use the provider.

**Scope:** New `StorageProvider` or `TransferStrategy`, modifications to
staging and dispatch code, `RuntimeEnvironment` path handling.

### Phase 3: Kubernetes Backend

First cloud backend. Uses shared filesystem via NFS PVC (storage model A
from the analysis). Closest to SLURM — validates the dispatch abstraction
with minimal storage changes.

### Phase 4: Modal Backend

First serverless backend. Uses Modal Volume (shared FS) or S3 transfer.
Requires DispatchHandle (Phase 1) for non-Prefect dispatch. Validates the
storage abstraction with a real cloud backend.

### Phase 5: AWS Batch / Lambda

Object-storage-only backends. Requires both DispatchHandle and storage
abstraction. Validates the full cloud story.

---

## Open Questions

- ~~**Result contract typing.**~~ **Decided:** `UnitResult` frozen
  dataclass. See `dispatch-handle.md` for the type definition.

- **Resource escalation on retry.** Nextflow and Snakemake support
  `memory = base * attempt`. Should `ResourceConfig` support callables?
  Or is this a backend-level concern?

- **Concurrency caps for cloud.** Metaflow has `--max-workers` and
  `--max-num-splits`. `ExecutionConfig.max_workers` exists but isn't
  enforced for all backends. Should cloud backends have explicit caps?

- **Prefect as optional dependency.** Non-Prefect backends (Modal, Lambda,
  Ray) mean Prefect isn't required for all dispatch paths. Should it
  become optional?

---

## References

- `_dev/analysis/cloud/cloud_compute_backends.md` — detailed gap analysis
  and backend designs
- `_dev/analysis/cloud/cloud-compute-integration.md` — Modal integration
  and cross-provider portability
- `_dev/analysis/cloud/other-frameworks/synthesis.md` — cross-framework
  comparison and recommendations
- `_dev/analysis/cloud/hamilton-cloud-execution.md` — Hamilton lessons
