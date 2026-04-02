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

Operations define a pure compute function separately from orchestration logic:

```python
class StructurePrediction(OperationDefinition):
    resources = ResourceConfig(gpus=1, memory_gb=16)

    @staticmethod
    def compute(sequences: list[str]) -> list[dict]:
        """Pure compute — no framework deps, serializable."""
        model = load_model("esm-fold")
        return [model.predict(seq) for seq in sequences]

    def execute(self, context, inputs):
        sequences = [a.data["seq"] for a in inputs.artifacts]
        results = context.run_compute(self.compute, sequences)
        return [self.create_artifact(data=r) for r in results]
```

`context.run_compute()` routes based on backend:

- Local / SLURM: calls directly (already on compute node)
- Modal: ships `compute` to a Modal container via `.remote()`
- Ray: `ray.remote(compute).remote(sequences)`

**Strengths:**

- Clean serialization boundary — compute function has no framework deps
- Easy to containerize (small dependency surface)
- Works naturally with Modal, Ray, or any remote execution model
- Operations remain portable

**Weaknesses:**

- Adds a concept: compute kernel vs execute method
- Forces a separation that may feel artificial for simple operations
- Some operations don't have a clean pure-function extraction (stateful,
  multi-phase)

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

| Criterion | A: ModalBackend | B: Routing | C: Kernel | D: Model Registry | E: Direct Call | F: Burst |
|---|---|---|---|---|---|---|
| Backend-agnostic ops | Yes | Yes | Yes | Yes | No | Yes |
| Framework changes | Medium | Medium | Medium | Large | None | Medium |
| Local dev works | Yes | Yes | Yes | Yes | No (calls Modal) | Yes |
| HPC compatible | N/A (separate backend) | Yes | Yes | Yes | No | Yes |
| Modal-optimized | Moderate | Moderate | Good | Good | Best | Moderate |
| New concepts | None | Routing config | Compute kernel | Model registry | None | Burst policy |
| Storage complexity | High | High | Low-Medium | Medium | None | High |
| Inference-specific | No | No | No | Yes | Yes | No |

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

### Option C (Compute Kernel) across providers

This is where the difference is stark. The serialization boundary changes
what the provider adapter needs to do.

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
provenance, caching — locally. The cloud just does the compute.

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

### The hybrid: optional kernel extraction

Not every operation has a clean pure-function extraction. Some operations
read multiple artifacts in complex ways, have multi-phase execution, or use
framework APIs during compute. For these, the kernel pattern doesn't apply.

Solution: make kernel extraction optional. The backend detects which path
to use:

```python
# WITH compute kernel → lightweight cloud path (~30-line adapter)
class InferenceOp(OperationDefinition):
    resources = ResourceConfig(gpus=1, memory_gb=16)

    @staticmethod
    def compute(sequences: list[str]) -> list[dict]:
        """Pure function — no framework deps."""
        model = load_model("esm-fold")
        return [model.predict(seq) for seq in sequences]

    def execute(self, context, inputs):
        sequences = [a.data["seq"] for a in inputs.artifacts]
        results = context.run_compute(self.compute, sequences)
        return [self.create_artifact(data=r) for r in results]


# WITHOUT kernel → full operation path (~300-line backend)
class ComplexOp(OperationDefinition):
    def execute(self, context, inputs):
        # Multi-phase, uses framework APIs
        # Backend ships the entire operation to the cloud container
        ...
```

Backend behavior:

- Has `compute()` method → use lightweight `CloudComputeAdapter` (portable)
- No `compute()` method → fall back to full provider-specific backend (A)

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

## Recommendations

### Recommended path: C (compute kernel) + B (routing), with A as fallback

The compute kernel pattern (Option C) provides the strongest foundation for
multi-provider portability. Combined with routing (Option B) to keep non-GPU
work local, and with a full-backend fallback (Option A) for operations that
can't cleanly extract a kernel.

**Phase 1: Compute kernel + routing**

Introduce `context.run_compute()` and the `CloudComputeAdapter` interface.
Build a Modal adapter first, since Modal is the current target.

```python
# Production: GPU ops go to Modal, CPU ops stay local
pipeline.run(backend=RoutingBackend(
    gpu=ComputeKernelBackend(adapter=ModalAdapter(image="bio:latest")),
    cpu=LocalBackend(),
))

# Local dev: everything runs locally
pipeline.run(backend="local")

# HPC: existing SLURM backend, unchanged
pipeline.run(backend="slurm")
```

Key properties:

- Operations with `compute()` use the lightweight adapter path — ~30 lines
  per provider, compute deps only in the container, no Artisan in the image
- Operations without `compute()` run via the standard backend path
- On SLURM/local, `context.run_compute()` is a direct call (no serialization)
- Routing keeps non-GPU steps local and cheap
- Adding a new provider means writing one small adapter class

Engineering work:

- `context.run_compute()` implementation in ExecutionContext
- `CloudComputeAdapter` ABC + `ModalAdapter` (~30 lines)
- `ComputeKernelBackend` that wraps adapters as a `BackendBase`
- `RoutingBackend` that delegates based on `ResourceConfig`

**Phase 2: Full ModalBackend for complex operations**

If some operations can't extract a clean kernel, build a full `ModalBackend`
(Option A) as a fallback. This requires Artisan in the container image and
Modal Volume access, but only applies to operations that need it.

The routing backend can compose both:

```python
RoutingBackend(
    gpu_with_kernel=ComputeKernelBackend(adapter=ModalAdapter(...)),
    gpu_without_kernel=ModalBackend(image="artisan-full:latest", ...),
    cpu=LocalBackend(),
)
```

**Phase 3: Additional providers**

Adding Ray, AWS Batch, or any other provider means writing a
`CloudComputeAdapter` subclass (~30 lines each). The compute kernel path
makes this cheap. If a provider also needs the full-backend fallback, that's
a larger effort — but only for operations that require it.

**Phase 4: Model registry (optional)**

If multiple operations share inference models, centralize model lifecycle.
This enables Modal's `@modal.cls` / `@enter` pattern for warm model
instances and avoids redundant loading. Only build this when model loading
becomes a measurable bottleneck.

### What to defer

- **Option E (direct Modal calls)**: breaks backend agnosticism. Use only as
  a temporary bridge before the adapter exists.
- **Option F (burst)**: adds complexity over explicit routing. Revisit if the
  dev-to-scale transition becomes painful.
- **Kubernetes / SkyPilot**: overkill unless multi-cloud or team growth
  demands it.

---

## Open Questions

- **Orchestrator location**: Does the orchestrator run locally or on Modal?
  With the compute kernel approach, the orchestrator stays local (it handles
  artifacts, the cloud just runs functions). For full-backend operations (A
  fallback), the orchestrator needs artifact store access — either via Volume
  API or by also running on Modal.
- **Volume vs S3**: Only relevant for the full-backend fallback (A). The
  compute kernel path (C) doesn't need shared storage — data flows through
  function arguments and return values. For A, Modal Volumes have explicit
  commit/reload semantics while S3 via delta-rs is location-agnostic but
  higher latency.
- **Serialization**: The compute kernel path requires function + args to be
  serializable (cloudpickle for Modal/Ray, JSON for REST-based providers).
  Need to establish serialization conventions and size limits.
- **Image management**: For the kernel path, images only need compute deps
  (lighter, more stable). For the full-backend fallback, images need the full
  Artisan stack. CI/CD can rebuild on dependency changes.
- **Cold start impact**: For pipelines with many short steps, container cold
  starts may dominate. Benchmarking needed. The routing backend mitigates
  this by keeping non-GPU steps local.
- **Prefect's role**: With compute kernels bypassing Prefect's task runner,
  Prefect's value shifts to flow-level concerns (tracking, UI, retry).
  Evaluate whether this justifies its presence in the stack.
