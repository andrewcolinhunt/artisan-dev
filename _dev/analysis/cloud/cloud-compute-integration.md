# Cloud Compute Integration: Modal, Serverless, and HPC

Analysis of how Artisan pipelines should integrate with cloud compute
providers (Modal in particular) alongside existing HPC (SLURM) and local
backends. Includes cross-provider portability analysis.

---

## Context

Artisan's backend abstraction cleanly separates pipeline orchestration from
compute dispatch. Two backends exist today:

- **LocalBackend**: ProcessPoolTaskRunner, single machine
- **SlurmBackend**: submitit job arrays, shared NFS, multi-node HPC

The goal is to support cloud GPU providers — specifically Modal — so that the
same operation code runs on local machines, HPC clusters, and serverless cloud
infrastructure without modification.

### Constraints

- Primary inference workload: bioinformatics (sequence/structure files, KB-MB)
- Modal is the current cloud provider; others may follow
- Local iteration is required for development
- Operations must be backend-agnostic ("write once, run anywhere")

---

## Current Architecture

### Backend Abstraction

All backends implement `BackendBase` (two abstract methods):

- `create_flow(resources, execution, step_number, job_name)` — returns a
  callable `(units_path, runtime_env) -> list[dict]`
- `capture_logs(results, staging_root, failure_logs_root, operation_name)` —
  post-dispatch log capture

Plus class-level traits:

- `WorkerTraits`: `worker_id_env_var`, `shared_filesystem`
- `OrchestratorTraits`: `shared_filesystem`, `staging_verification_timeout`

### Key Insight

`create_flow()` returns a **plain callable**. It doesn't have to be a Prefect
flow. The step executor just calls it:

```python
step_flow = backend.create_flow(...)
results = step_flow(units_path=str(units_path), runtime_env=runtime_env)
```

This means a Modal backend can return a function that uses Modal's dispatch
internally, without involving Prefect task runners at all.

### Execution Flow

```
Orchestrator (local or remote):
  resolve inputs → batch → cache check → dispatch → collect → commit → provenance

Workers (backend-determined):
  deserialize unit → load artifacts → execute operation → stage results
```

### Dual Input Delivery Model

Operations control how they receive input artifacts via `InputSpec`:

```python
# Filesystem path (HPC-native: shared NFS, local disk)
InputSpec(artifact_type="data", materialize=True)
# → artifact.materialized_path → Path on disk

# In-memory bytes (cloud-native: no filesystem dependency)
InputSpec(artifact_type="data", materialize=False)
# → artifact.content → bytes in memory

# ID-only (curators: no content loaded at all)
InputSpec(artifact_type="data", hydrate=False)
# → artifact.artifact_id only
```

The full lifecycle adapts to the chosen mode:

| Phase | `materialize=True` | `materialize=False` |
|---|---|---|
| **Setup** | Writes artifact content to `materialized_inputs/` | Skips filesystem write |
| **Preprocess** | Reads `artifact.materialized_path` (Path) | Reads `artifact.content` (bytes) |
| **Execute** | Works with filesystem paths | Works with in-memory data |
| **Postprocess** | Creates drafts from `file_outputs` (Paths) | Creates drafts from `memory_outputs` |

An operation using `materialize=False` for all inputs and returning results
via `memory_outputs` is **entirely filesystem-independent**. Data flows as
bytes through the lifecycle — serializable and shippable to any remote
execution target.

Existing example: `DataTransformerConfig` uses `materialize=False` to receive
input datasets in memory, builds config dicts purely in Python, and returns
them via `memory_outputs`. No filesystem I/O in the operation itself.

Most current operations use `materialize=True` because development has been
on HPC with shared NFS. But the in-memory path is a first-class framework
feature, not an afterthought.

### Resource Configuration

Operations declare needs via `ResourceConfig`:

- `cpus`, `memory_gb`, `gpus`, `time_limit`, `extra`

Backends map these to infrastructure-specific parameters (SLURM gres, Modal
GPU specs, etc.).

---

## Modal vs Traditional Backends

### Fundamental Differences

| Aspect | SLURM / Ray / Dask | Modal |
|---|---|---|
| Infrastructure | User-managed clusters | Serverless, zero config |
| Worker lifecycle | Long-running | Ephemeral containers per call |
| Code delivery | Shared filesystem | Container image + mounts |
| Data access | Shared NFS | Volumes (explicit commit/reload) |
| Parallelism | Task runner dispatches to workers | `.map()` / `.remote()` across containers |
| GPU allocation | Scheduler-managed | Decorator: `gpu="A100"` |
| Prefect integration | Task runner (distributes tasks within flow) | Work pool (runs entire flow on Modal) |

### Why Modal Doesn't Fit the Task Runner Model

There is no Modal task runner for Prefect. The Prefect-Modal integration runs
entire flows on Modal infrastructure as a work pool — it doesn't distribute
individual tasks the way SlurmTaskRunner does. This means the standard
`_build_prefect_flow()` template doesn't apply.

### The Packaging Question

SLURM workers share a filesystem with the orchestrator — code, dependencies,
and data are all available via NFS. Modal containers are isolated — code must
be in the container image or mounted, and data must be on a Volume or cloud
storage.

Modal's `Mount.from_local_dir()` solves the code delivery problem without
rebuilding images. Heavy dependencies (torch, bio libraries) live in a cached
base Image; operation source code is mounted fresh each run.

### The Storage Question

Artisan's execution model:

```
Workers stage results (Parquet) → Orchestrator commits to Delta Lake
```

Options for Modal:

| Storage | Mechanism | Tradeoff |
|---|---|---|
| Modal Volume | Mount in containers, API from orchestrator | Write-once/read-many fits Parquet. Explicit commit/reload semantics. |
| S3/GCS | delta-rs has native S3 support | Location-agnostic. Higher latency. Config overhead. |
| Orchestrator on Modal | Volume mounted as filesystem everywhere | Cleanest, but all orchestration runs on Modal. |

---

## Design Options

### Option A: ModalBackend (backend handles everything)

Operations are completely backend-agnostic. The ModalBackend wraps execution
in Modal functions transparently.

```python
class ModalBackend(BackendBase):
    name = "modal"

    def create_flow(self, resources, execution, step_number, job_name):
        gpu_spec = self._resolve_gpu(resources)
        configured_fn = modal_execute_unit.options(
            gpu=gpu_spec,
            memory=resources.memory_gb * 1024,
            cpu=resources.cpus,
        )

        def modal_flow(units_path, runtime_env):
            units = _load_units(Path(units_path))
            results = list(configured_fn.map(
                [serialize(u) for u in units],
                [serialize(runtime_env)] * len(units),
            ))
            return [deserialize(r) for r in results]

        return modal_flow
```

Where `modal_execute_unit` is a `@modal.function` that calls the same
executor logic (run_creator_flow, etc.) that Local and SLURM use.

**Strengths:**

- Same operation code everywhere — true backend agnosticism
- Leverages existing executor logic entirely
- ResourceConfig maps naturally to Modal's GPU/memory/CPU options
- `Function.options()` allows per-step resource configuration
- Code mounting avoids slow image rebuilds during development

**Weaknesses:**

- Volume commit/reload semantics differ from filesystem — staging logic needs
  adaptation
- Operation dependencies must be in the base image
- Cold start latency per step (mitigated by Modal's container caching)
- Orchestrator needs Volume access for commit phase — either runs on Modal too,
  or uses the Volume Python API

### Option B: Routing Backend (GPU to cloud, CPU local)

A composite backend that routes operations based on resource requirements:

```python
backend = RoutingBackend(
    gpu=ModalBackend(gpu="A100"),
    cpu=LocalBackend(),
)
```

GPU-requiring operations go to Modal. Everything else (ingestion, filtering,
merging) runs locally. This reflects the economic reality: GPU time is
expensive, CPU orchestration is cheap.

Note: Artisan already runs curator operations (Filter, Merge) locally in
subprocesses regardless of backend. The routing backend extends this principle
to all non-GPU operations.

**Strengths:**

- Only pays for cloud compute when using GPUs
- Orchestration and lightweight ops stay fast and local
- Naturally fits the dev-to-production workflow

**Weaknesses:**

- Artifacts must be accessible from both local and Modal — requires shared
  storage (Volume or S3)
- Two backends to configure

### Option C: Compute Kernel Extraction

Operations define a pure compute function separately from orchestration logic.

**Key insight:** The framework's existing `materialize=False` path already
provides the foundation for this. An operation using in-memory inputs works
entirely with bytes and dicts — no filesystem coupling. The compute kernel
pattern formalizes this by explicitly separating the GPU-intensive compute
from the framework orchestration:

```python
class StructurePrediction(OperationDefinition):
    name = "structure_prediction"
    resources = ResourceConfig(gpus=1, memory_gb=16)

    class InputRole(StrEnum):
        sequences = auto()

    inputs = {
        InputRole.sequences: InputSpec(
            artifact_type="data",
            materialize=False,  # In-memory — no filesystem dependency
        ),
    }

    class OutputRole(StrEnum):
        structures = auto()

    outputs = {
        OutputRole.structures: OutputSpec(
            artifact_type="data",
            infer_lineage_from={"inputs": ["sequences"]},
        ),
    }

    @staticmethod
    def compute(sequences: list[str]) -> list[dict]:
        """Pure compute — no framework deps, serializable."""
        model = load_model("esm-fold")
        return [model.predict(seq) for seq in sequences]

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        # Extract data from in-memory artifacts
        return {
            "sequences": [
                parse_fasta(a.content)
                for a in inputs.input_artifacts["sequences"]
            ]
        }

    def execute(self, inputs: ExecuteInput) -> dict[str, Any]:
        # On local/SLURM: compute() runs here directly
        # On cloud: backend could intercept and route compute() remotely
        results = self.compute(inputs.inputs["sequences"])
        return {"structures": results}

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        drafts = [
            DataArtifact.draft(
                content=serialize_structure(s),
                original_name=f"structure_{i}.pdb",
                step_number=inputs.step_number,
            )
            for i, s in enumerate(inputs.memory_outputs["structures"])
        ]
        return ArtifactResult(success=True, artifacts={"structures": drafts})
```

This operation is already fully functional with the existing lifecycle and
`materialize=False`. The `compute()` method is a static method with no
framework dependencies — just raw Python with ML libraries.

A `CloudComputeAdapter` could intercept `compute()` and route it remotely:

- Local / SLURM: calls `compute()` directly (already on compute node)
- Modal: ships `compute` to a Modal container via `.remote()`
- Ray: `ray.remote(compute).remote(sequences)`

Alternatively, with Option A (full backend), this same operation runs
unmodified on Modal because `materialize=False` means no filesystem
dependency in the operation itself — only the framework's sandbox/staging
layer touches the filesystem.

**Strengths:**

- Clean serialization boundary — compute function has no framework deps
- Easy to containerize (small dependency surface)
- Works naturally with Modal, Ray, or any remote execution model
- Operations remain portable
- Builds on existing `materialize=False` — not a new concept, just a pattern
- Preprocess/postprocess stay on the orchestrator (cheap, no GPU)

**Weaknesses:**

- Formalizing the compute extraction requires a `context.run_compute()` hook
  or similar mechanism to intercept the call
- Not all operations split cleanly (multi-phase, stateful, filesystem-dependent)
- Model lifecycle (warm containers) needs separate handling — a static method
  reloads the model on every invocation (see Model Lifecycle section)

### Option D: Model Registry

For inference-heavy workloads, make models first-class:

```python
artisan.register_model(
    "esm-fold",
    loader=lambda: ESMFold.from_pretrained("facebook/esmfold_v1"),
    resources=ResourceConfig(gpus=1, memory_gb=16),
)

class StructurePrediction(OperationDefinition):
    def execute(self, context, inputs):
        model = context.load_model("esm-fold")
        results = [model.predict(seq) for seq in inputs.sequences]
        return self.create_artifact(data=results)
```

`context.load_model()` resolves per backend:

- Local: loads into local GPU memory, caches across invocations
- Modal: returns a proxy that calls a Modal function with the model pre-loaded
- SLURM: loads on worker node, potentially cached via `@enter` pattern

**Strengths:**

- Natural for inference workloads — models are the shared resource
- Centralizes model lifecycle (loading, caching, versioning)
- Modal naturally supports this via `@modal.cls` with `@enter` for model loading

**Weaknesses:**

- Domain-specific — only useful for model-based inference
- Doesn't generalize to non-model compute (data transforms, custom algorithms)
- Adds infrastructure (registry, proxy objects)

### Option E: Operations Call Modal Directly

Operations use Modal functions within their execute method:

```python
class StructurePrediction(OperationDefinition):
    resources = ResourceConfig(cpus=1, memory_gb=2)  # lightweight

    def execute(self, context, inputs):
        results = list(predict_structure.map(inputs.sequences))
        return self.create_artifact(data=results)
```

Where `predict_structure` is a separately-deployed Modal function.

**Strengths:**

- Simplest implementation — zero framework changes
- Modal does what it's designed to do (serve inference)
- Works today

**Weaknesses:**

- Operations become Modal-aware — not backend-agnostic
- Requires separately maintaining Modal deployments
- Pays for Modal even during local development (always calls remote)
- Doesn't work offline or on HPC

### Option F: Burst Backend

Default to local execution, overflow to cloud when resources are insufficient:

```python
backend = BurstBackend(
    primary=LocalBackend(),
    overflow=ModalBackend(gpu="A100"),
    burst_when=lambda res: res.gpus > available_local_gpus(),
)
```

**Strengths:**

- Natural dev-to-scale workflow
- Minimal cloud cost for small runs
- Graceful scaling

**Weaknesses:**

- Same storage accessibility challenges as routing backend
- "When to burst" heuristics can be surprising
- Testing the burst path requires deliberate triggers

---

## Comparison Matrix

| Criterion | A: Full Backend | B: Routing | C: Kernel | D: Model Registry | E: Direct Call | F: Burst |
|---|---|---|---|---|---|---|
| Backend-agnostic ops | Yes | Yes | Yes | Yes | No | Yes |
| Framework changes | Medium | Small | Medium | Large | None | Medium |
| Local dev works | Yes | Yes | Yes | Yes | No (calls Modal) | Yes |
| HPC compatible | N/A (separate) | Yes | Yes | Yes | No | Yes |
| Cloud-optimized | Good (with `materialize=False`) | Good | Best | Good | Best | Moderate |
| New concepts | None | Routing config | Compute hook | Model registry | None | Burst policy |
| Container weight | Full Artisan + deps | Depends on sub-backend | Compute deps only | Compute deps + registry | None (user manages) | Depends |
| Multi-provider cost | ~200 lines each | Orthogonal | ~30 lines each | ~30 lines each | N/A | Depends |
| Filesystem-dependent ops | Yes | Yes | No (falls back to A) | No | N/A | Yes |
| In-memory ops | Yes | Yes | Yes (optimal) | Yes | Yes | Yes |

---

## Cross-Provider Portability Analysis

The design options above were initially evaluated against Modal. This section
stress-tests them against multi-provider deployment: what happens when you
need to support Modal, AWS Batch, GCP, RunPod, Ray, or future providers
simultaneously?

### Option A across providers

Each provider gets a bespoke `BackendBase` subclass:

```
LocalBackend    → ProcessPoolTaskRunner
SlurmBackend    → submitit job arrays
ModalBackend    → modal.Function.map() + Volume
AWSBatchBackend → boto3 submit_job() + S3
RunPodBackend   → RunPod API + network storage
GCPBatchBackend → google.cloud.batch + GCS
```

Each backend must independently solve three problems:

| Problem | Modal | AWS Batch | GCP Batch |
|---|---|---|---|
| Code delivery | Image + Mount | ECR image | Artifact Registry image |
| Data access | Volume | S3 | GCS |
| Job dispatch | `.map()` | `submit_job()` | `create_job()` |
| GPU config | `gpu="A100"` | Resource requirements | Accelerators |
| Result collection | Return value | Poll + S3 | Poll + GCS |

Every implementation is ~300 lines of bespoke code. They share `BackendBase`
but nothing else.

Worse: **the full Artisan framework must run inside each provider's
container.** That means building and maintaining provider-specific images with
Artisan + all dependencies. A torch version bump means rebuilding images
across Modal, ECR, Artifact Registry, etc.

**Verdict:** Viable for 1-2 providers. Maintenance cost grows linearly.

### Option B (Routing) across providers

Routing is orthogonal — it composes whatever backends exist:

```python
RoutingBackend(gpu=ModalBackend(), cpu=LocalBackend())
RoutingBackend(gpu=AWSBatchBackend(), cpu=LocalBackend())
```

Good pattern regardless, but doesn't reduce the per-provider implementation
cost. It just decides which backend to call.

### Option A across providers (revisited with in-memory path)

The in-memory input delivery model (`materialize=False`) significantly
changes the Option A picture. When operations use in-memory inputs:

- The container still needs Artisan, but the operation's filesystem
  footprint shrinks to just the framework's sandbox/staging machinery
- Input data is hydrated from Delta Lake as bytes, not materialized to disk
- Output data flows through `memory_outputs`, not `file_outputs`

This means the Delta Lake store is the only shared-state requirement. On
SLURM, that's NFS. On Modal, that's a Volume or S3. The operation itself
has no filesystem opinions.

**For operations using `materialize=False`**: Option A works across providers
with minimal friction. The container needs Artisan + deps, but the operation
is already cloud-friendly.

**For operations using `materialize=True`**: The container needs a writable
local filesystem (always available — container scratch) plus Delta Lake
access. Still works, just heavier.

### Option C (Compute Kernel) across providers

The serialization boundary changes what the provider adapter needs to do.

**With Option A**, the cloud container runs the full Artisan execution
lifecycle:

```
Cloud container needs:
  artisan + delta-rs + pyarrow + operation deps + model weights

Cloud container does:
  deserialize ExecutionUnit → load artifacts from store → setup → preprocess
  → execute → postprocess → stage results → return metadata
```

**With Option C**, the cloud container runs a plain Python function:

```
Cloud container needs:
  operation compute deps only (torch, bio libs)
  NO artisan, NO delta-rs, NO pyarrow

Cloud container does:
  receive (function, data) → call function(data) → return results
```

The orchestrator handles everything else — artifact loading, staging,
provenance, caching — locally. The cloud just does compute.

This makes the **provider adapter dramatically simpler:**

```python
class CloudComputeAdapter(ABC):
    """Ship a function + args to a cloud provider, get results back."""

    @abstractmethod
    def run(self, fn: Callable, args: tuple, resources: ResourceConfig) -> Any: ...

    @abstractmethod
    def map(self, fn: Callable, args_list: list[tuple], resources: ResourceConfig) -> list: ...


class ModalAdapter(CloudComputeAdapter):
    def map(self, fn, args_list, resources):
        modal_fn = self._wrap(fn, resources)
        return list(modal_fn.map(args_list))


class RayAdapter(CloudComputeAdapter):
    def map(self, fn, args_list, resources):
        remote_fn = ray.remote(fn).options(num_gpus=resources.gpus)
        return ray.get([remote_fn.remote(*a) for a in args_list])
```

Each adapter is ~30 lines, not ~300. You're shipping a function, not
orchestrating an execution lifecycle.

**The container image problem also shrinks.** Instead of "Artisan + all
framework deps + operation deps," it's just "operation compute deps." A single
`torch + transformers` image works across Modal, AWS, GCP — no
provider-specific framework concerns baked in.

### Data transfer implications

The tradeoff with Option C: the orchestrator must serialize input data to the
compute function and deserialize results back. For KB-MB artifacts
(sequences, structures), this is negligible. For multi-GB artifacts (images,
video, large datasets), this becomes a bottleneck and the full-operation
approach (A) with direct store access is better.

Note: `DataArtifact.content` stores raw bytes embedded in the Delta Lake
Parquet table. `FileRefArtifact` stores only a pointer (`path` +
`content_hash`) — the actual bytes live at the original filesystem location.
Operations consuming `FileRefArtifact` inputs inherently need filesystem
access to the original files, which limits them to Option A or HPC backends.

### The hybrid: operations choose their path

The framework already supports both modes. Operations that use
`materialize=False` are cloud-ready. Operations that use `materialize=True`
need filesystem access. The backend can handle both:

```python
# Cloud-friendly: in-memory inputs, compute kernel, memory outputs
class InferenceOp(OperationDefinition):
    inputs = {
        "sequences": InputSpec(artifact_type="data", materialize=False),
    }

    @staticmethod
    def compute(sequences: list[str]) -> list[dict]:
        model = load_model("esm-fold")
        return [model.predict(seq) for seq in sequences]

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        return {"sequences": [parse(a.content) for a in inputs.input_artifacts["sequences"]]}

    def execute(self, inputs: ExecuteInput) -> dict[str, Any]:
        return {"results": self.compute(inputs.inputs["sequences"])}

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        # Creates drafts from memory_outputs — no file_outputs needed
        ...


# HPC-native: materialized inputs, filesystem I/O, file outputs
class ToolWrapperOp(OperationDefinition):
    inputs = {
        "dataset": InputSpec(artifact_type="data", materialize=True),
    }

    def preprocess(self, inputs: PreprocessInput) -> dict[str, Any]:
        return {"paths": [a.materialized_path for a in inputs.input_artifacts["dataset"]]}

    def execute(self, inputs: ExecuteInput) -> Any:
        for path in inputs.inputs["paths"]:
            run_external_tool(path, inputs.execute_dir / "output.dat")
        return None

    def postprocess(self, inputs: PostprocessInput) -> ArtifactResult:
        # Creates drafts from file_outputs
        ...
```

Both are valid operations using the existing API. The backend determines
where they run:

- `InferenceOp` (in-memory): works with any backend, including lightweight
  cloud adapters (Option C) or full backends (Option A)
- `ToolWrapperOp` (filesystem): requires backends with filesystem access
  (Local, SLURM, or full cloud backend with Volume/store mount)

### Cross-provider summary

| | A: Full backend | C: Compute kernel | Hybrid (A + C) |
|---|---|---|---|
| Per-provider work | ~300 lines, bespoke | ~30 lines, thin adapter | Both paths |
| Container image | Full Artisan + all deps | Compute deps only | Per-operation |
| N-provider cost | Linear, expensive | Linear, cheap | Cheap for kernel ops |
| Data transfer | Direct store access | Serialized through transport | Per-operation |
| Large artifacts | Good | Bottleneck | Falls back to A |
| Operation API change | None | Adds `compute()` + `run_compute()` | Optional `compute()` |
| Framework coupling | High (Artisan in container) | Low (plain function) | Per-operation |

---

## Model Lifecycle and Warm Containers

For inference operations, the model is typically the heaviest resource:
multi-GB weights, 10-60 seconds to load. How models are loaded and kept warm
varies across backends.

### The problem with stateless compute kernels

A `@staticmethod compute()` reloads the model on every invocation:

```python
@staticmethod
def compute(sequences: list[str]) -> list[dict]:
    model = load_model("esm-fold")  # 2.5 GB, 30s load — paid every call
    return [model.predict(seq) for seq in sequences]
```

With `.map()` across N Modal containers, each container loads the model
independently. This is N x 30s of cold start.

### Per-backend warm model patterns

| Backend | Pattern | Mechanism |
|---|---|---|
| Local | Process pool keeps workers alive | Model cached in process memory |
| SLURM | Job array workers persist | Model loaded once per worker |
| Modal | `@modal.cls` + `@enter` | Model loaded once per container, container stays warm |
| Ray | Actor with `__init__` | Model in actor state, reused across calls |

### Options for handling model lifecycle

**Within the existing framework:** Operations can implement model caching
internally (module-level singletons, `functools.lru_cache`, etc.). This works
across all backends but is ad-hoc.

**With a compute class pattern:** The kernel becomes a class with
setup/teardown, mapping naturally to provider lifecycle:

```python
class Compute:
    def setup(self):
        self.model = load_model("esm-fold")

    def run(self, sequence: str) -> dict:
        return self.model.predict(sequence)
```

Maps to: Modal `@modal.cls`/`@enter`, Ray actor, local persistent worker.

**With a model registry (Option D):** Framework manages model lifecycle
centrally. Most infrastructure, but cleanest separation. Worth considering
if many operations share the same models.

**Recommendation:** Start with internal caching (simplest). Graduate to the
compute class pattern if cold starts become a measurable bottleneck. Only
build a model registry if multiple operations share models and lifecycle
management becomes a maintenance burden.

---

## Recommendations

### Recommended path: A (full backend) first, C (compute kernel) when needed

The framework's existing dual input delivery model (`materialize=True` vs
`materialize=False`) already supports both HPC-native and cloud-native
operation patterns. The recommended path leverages this:

**Phase 1: ModalBackend (Option A) + RoutingBackend (Option B)**

Build a full `ModalBackend` that ships ExecutionUnits to Modal containers.
Operations using `materialize=False` (in-memory) will work with minimal
friction — the container runs the same `run_creator_flow()` as Local/SLURM.

```python
# Production: GPU ops go to Modal, CPU ops stay local
pipeline.run(backend=RoutingBackend(
    gpu=ModalBackend(image="artisan-bio:latest", volume="artisan-store"),
    cpu=LocalBackend(),
))

# Local dev: everything local
pipeline.run(backend="local")

# HPC: existing SLURM backend, unchanged
pipeline.run(backend="slurm")
```

Why start here:

- Zero operation API changes — existing operations work as-is
- Operations using `materialize=False` are already cloud-friendly (no
  filesystem coupling in the operation itself)
- Operations using `materialize=True` also work (container scratch disk +
  Volume for Delta Lake)
- `create_flow()` returns a callable using Modal `.map()` — simple
  implementation
- `Function.options()` configures GPU/memory per step from ResourceConfig
- Code mounting (`Mount.from_local_dir()`) avoids slow image rebuilds

Engineering work:

- `ModalBackend` implementation (~100-200 lines)
- `RoutingBackend` that delegates based on `ResourceConfig.gpus`
- Volume/S3 adapter for Delta Lake access from Modal containers
- Base image with Artisan + heavy deps (torch, bio libs)

**Phase 2: Encourage `materialize=False` for new cloud-targeted operations**

As new inference operations are written, default to `materialize=False` for
inputs where the data is small (KB-MB). This makes them cloud-efficient
(no filesystem I/O) while remaining fully functional on Local/SLURM.

Operations wrapping external tools that require filesystem paths continue
to use `materialize=True` — these naturally run on HPC where shared NFS is
available.

**Phase 3: Compute kernel extraction (Option C) for multi-provider**

If a second cloud provider is needed (Ray, AWS Batch, etc.), introduce the
`CloudComputeAdapter` interface and `context.run_compute()` for operations
that define a `compute()` method. This enables ~30-line adapters per
provider for in-memory operations, without requiring Artisan in every
container.

Operations without `compute()` continue using the full-backend path (A).

```python
# Multi-provider routing
RoutingBackend(
    gpu_kernel=ComputeKernelBackend(adapter=ModalAdapter(...)),
    gpu_full=ModalBackend(image="artisan-full:latest", ...),
    cpu=LocalBackend(),
)
```

**Phase 4: Model lifecycle optimization (when needed)**

If model cold starts become a measurable bottleneck, introduce either:
- A compute class pattern with `setup()`/`run()` lifecycle (maps to Modal
  `@modal.cls`/`@enter`, Ray actors, etc.)
- A model registry for operations sharing the same models

### What to defer

- **Option E (direct Modal calls)**: breaks backend agnosticism. Use only as
  a temporary bridge before the ModalBackend exists.
- **Option F (burst)**: adds complexity over explicit routing. Revisit if the
  dev-to-scale transition becomes painful.
- **Kubernetes / SkyPilot**: overkill unless multi-cloud or team growth
  demands it.

---

## Open Questions

- **Orchestrator location**: Does the orchestrator run locally or on Modal?
  With routing, the orchestrator stays local (handles non-GPU ops and
  commits). For the full backend (A), the orchestrator needs Delta Lake
  access to commit staged results — either via Modal Volume API from local,
  or by running the orchestrator on Modal too.
- **Volume vs S3**: Modal Volumes have explicit commit/reload semantics.
  S3 via delta-rs is location-agnostic but higher latency. Volumes are
  optimized for write-once/read-many with large files, which fits Parquet.
  S3 is better if you need the same store accessible from SLURM and Modal.
- **FileRefArtifact on cloud**: FileRefArtifact stores filesystem pointers
  to external files. Operations ingesting FileRefs need those paths
  accessible. On HPC this is NFS. On cloud, the original files would need
  to be on a Volume or cloud storage. May need a FileRef resolver that
  adapts paths per backend.
- **Image management**: Base image with Artisan + heavy deps (torch, bio
  libraries). Modal's `Image.pip_install_from_pyproject()` can automate
  this. Code mounting handles operation source without image rebuilds.
- **Cold start impact**: For pipelines with many short steps, container
  cold starts may dominate. Routing mitigates by keeping non-GPU steps
  local. Benchmarking needed for GPU steps.
- **Prefect's role**: The ModalBackend bypasses Prefect's task runner layer
  (returns a plain callable from `create_flow()`). Prefect's value shifts
  to flow-level concerns (tracking, UI, retry). Evaluate whether this
  justifies its presence for Modal-only deployments.
