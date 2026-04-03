# Design: Cloud Deployment

**Date:** 2026-03-24 (updated 2026-04-03)
**Status:** Active

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
    |-- Resolve inputs (Delta Lake on POSIX)
    |-- Batch into ExecutionUnits
    |-- Pickle units to shared filesystem
    |-- backend.create_flow() -> Prefect @flow callable
    |-- step_flow(units_path, runtime_env) -> list[dict]
    |       |-- LOCAL:  ProcessPool (same machine)
    |       +-- SLURM:  sbatch job array (shared NFS)
    |-- Workers read units from shared filesystem
    |-- Workers write staged Parquet to shared filesystem
    |-- Orchestrator reads staged Parquet
    +-- Commit atomically to Delta Lake
```

Key coupling points in the current code:

| Component | File | Coupling |
|-----------|------|----------|
| `RuntimeEnvironment` | `schemas/execution/runtime_environment.py` | 4 `Path` fields |
| `_save_units()` | `orchestration/engine/dispatch.py` | Writes pickle to `staging_root / "_dispatch"` |
| `_load_units()` | `orchestration/engine/dispatch.py` | Reads pickle from local `Path` |
| `_build_prefect_flow()` | `orchestration/backends/base.py` | Constructs Prefect `@flow` |
| `StagingArea` | `storage/io/staging.py` | `Path.mkdir()`, `write_parquet(path)` |
| `StagingManager` | `storage/io/staging.py` | `Path.glob()`, `read_parquet(path)` |
| `DeltaCommitter` | `storage/io/commit.py` | `DeltaTable(path)` |
| `ArtifactStore` | `storage/core/artifact_store.py` | `DeltaTable(path)` |
| Worker sandbox | `execution/context/sandbox.py` | `Path.mkdir()`, local scratch dirs |

---

## Target Architecture

```
Orchestrator (local Python process)
    |-- Resolve inputs (Delta Lake -- local or S3/GCS via URI)
    |-- Batch into ExecutionUnits
    |-- Dispatch via DispatchHandle (handle owns unit transport)
    |       |-- LOCAL:       ProcessPool (units via multiprocessing pickle)
    |       |-- SLURM:       sbatch (units pickled to shared FS)
    |       |-- Kubernetes:  K8s Job (units pickled to S3/NFS via fsspec)
    |       |-- Modal:       .map() (units as function arguments)
    |       +-- AWS Batch:   SubmitJob (units pickled to S3 via fsspec)
    |-- Workers run inside operation's container image
    |-- Workers execute locally (sandbox in /tmp)
    |-- Workers write staged Parquet via fsspec (local FS or S3/GCS)
    |-- Orchestrator reads staged Parquet via fsspec
    +-- Commit to Delta Lake (local or S3/GCS via delta-rs URI support)
```

Operations are unchanged. The worker sandbox is always local (`/tmp`).
The framework handles storage and dispatch transparently.

---

## Design Decisions

### Storage: fsspec

**Designed in:** `cloud-storage-design.md`

All storage operations go through fsspec. Local runs use
`LocalFileSystem`. Cloud runs use `S3FileSystem` or `GCSFileSystem`.
Staging, commit, and store access code are the same regardless of
backend.

`StorageConfig` (protocol + non-sensitive options) is embedded in
`RuntimeEnvironment`. Each component creates an `fs` instance on demand
via `storage_config.filesystem()`. Credentials come from the execution
environment (IAM roles, env vars, service accounts) — never stored in
config.

Delta Lake access uses URI strings + `storage_options` dicts, which
Polars and delta-rs already support natively.

Workers and orchestrator both access Delta Lake and staging via the same
URI scheme. No separate transfer mechanism.

### Dispatch: DispatchHandle

**Designed in:** `dispatch-handle.md`

`DispatchHandle` replaces the Prefect `@flow` callable with a
lifecycle interface: `dispatch()` / `is_done()` / `collect()` /
`cancel()`, plus a blocking `run()` convenience method.

Unit transport is handle-owned. Each handle decides how to deliver
`ExecutionUnit` objects to workers: multiprocessing pickle (local),
shared filesystem pickle (SLURM, K8s+NFS), function arguments (Modal),
object store (Ray). The orchestrator no longer calls `_save_units()`.

Prefect remains a core dependency. Local and SLURM handles continue
to use Prefect task runners internally. Cloud handles implement
`DispatchHandle` directly against native APIs without Prefect.

### Worker Image: Worker Runs Inside the Operation's Container

On local and SLURM, operations call external tools via `run_command()`,
which wraps commands in `docker run` or `apptainer exec` on the host.
This two-layer model (artisan worker process + tool container) relies on
a container runtime being available on the host.

Cloud workers ARE containers — no Docker daemon is available inside
them. Instead, the dispatch handle launches workers directly inside the
operation's container image. The operation's `docker.image` field from
its `Environments` config tells the handle which image to use. Since
tools are already present in the container, the operation runs with
`active="local"` — no nested container invocation.

**Image requirements:** The operation's Docker image must include:

- The artisan framework (`pip install artisan`)
- The operation's Python code (the `OperationDefinition` subclass)
- The operation's tool dependencies (external binaries, libraries)

**Development iteration:** Modal supports `Mount.from_local_dir()` to
upload operation source code into the container at dispatch time,
avoiding image rebuilds during development.

**Composites on cloud:** Collapsed composites run multiple operations
sequentially in a single worker. If those operations need different tool
containers, this conflicts with the one-image-per-worker model. Two
options:

- **Expand the composite** — each operation runs as its own step with
  its own container. The performance cost is small on cloud (S3 staging
  adds ~500ms per intermediate step, not per artifact).
- **Shared image** — if all operations in a composite use the same
  image (or a combined image with all tools), collapsing works naturally.

No framework changes needed — the dispatch handle picks the image from
the operation's environment config. Expansion is the default; collapsing
is available when images align.

### Backend Traits: No Expansion

The existing two traits are sufficient:

- `worker_id_env_var: str | None` — how workers identify themselves
- `shared_filesystem: bool` — drives NFS-specific behavior (staging
  verification, fsync)

Cloud backends set `shared_filesystem=False`. Storage credentials
are handled by the environment (IAM roles, service accounts), not by
traits. Code deployment is a user responsibility (they control the
Docker image), not a framework concern.

### Concurrency: Configurable Caps

`ExecutionConfig.max_workers` must be enforced across all backends.
Cloud backends with per-invocation billing (Modal, Lambda) make this a
cost concern, not just a resource concern. Each dispatch handle respects
`max_workers` when dispatching units.

---

## Implementation Phases

Each phase is independently shippable.

### Phase 1: DispatchHandle

Decouple dispatch from Prefect. Implement the handle interface. Wrap
existing Prefect-based backends in handles. Prerequisite for cloud
backends and independently valuable for cancellation support.

**Scope:** `dispatch-handle.md` has the full file-by-file plan.

### Phase 2: Storage Abstraction

Introduce fsspec as the unified storage layer. `cloud-storage-design.md`
specifies a 4-phase internal rollout: foundation (StorageConfig) →
storage layer (StagingArea, StagingManager, DeltaCommitter, ArtifactStore)
→ Delta read sites (44 `scan_delta` calls) → RuntimeEnvironment field
rename. 21 files total.

**Scope:** `cloud-storage-design.md` has the full migration inventory.

### Phase 3: Kubernetes Backend

First cloud backend. K8s Jobs with either NFS PVC
(`shared_filesystem=True`) or S3 (`shared_filesystem=False`). Closest to
SLURM — validates the dispatch abstraction with a familiar execution
model. Worker image specified in operation's `docker.image` field.

### Phase 4: Modal Backend

First serverless backend. `dispatch()` calls `modal.Function.map()`.
Workers run inside the operation's Docker image. Staging via fsspec to
S3. Validates the full cloud story: non-Prefect dispatch, remote
storage, container-per-operation model.

### Phase 5: AWS Batch

`dispatch()` calls `SubmitJob`, `is_done()` polls `DescribeJobs`.
S3 for staging and Delta Lake. Same container-per-operation model.
Validates the design with a third cloud backend.

---

## Open Questions

- **Partial commit on cancellation.** Should a cancelled step commit the
  units that succeeded before cancellation? Current behavior: incomplete
  step results in nothing committed, resume re-runs the entire step.
  (See `dispatch-handle.md`.)

- **Return-value staging.** For serverless backends, the fsspec path
  writes to S3 and reads back from S3, adding ~100ms per unit in API
  latency. Workers could instead return staged DataFrames through the
  dispatch handle, bypassing S3. Deferred — the fsspec path is correct
  and uniform. (See `cloud-storage-design.md`.)

---

## References

- `dispatch-handle.md` — DispatchHandle interface, state machine,
  backend implementations, cancellation wiring
- `cloud-storage-design.md` — fsspec storage layer, StorageConfig,
  migration inventory, rollout phases
- `_dev/analysis/cloud/cloud_compute_backends.md` — gap analysis
  and backend designs
- `_dev/analysis/cloud/cloud-compute-integration.md` — Modal integration
  and cross-provider portability
- `_dev/analysis/cloud/other-frameworks/synthesis.md` — cross-framework
  comparison and recommendations
