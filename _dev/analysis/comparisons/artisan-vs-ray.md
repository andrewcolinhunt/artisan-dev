# Artisan vs Ray: Comparative Analysis

A deep comparison of Artisan's design against the Ray ecosystem (Ray Core, Ray
Data, Ray Train, Ray Tune, Ray Serve, Ray Workflows).

---

## The Fundamental Divergence

**Ray is compute-centric. Artisan is artifact-centric.**

Ray's primary abstraction is the *task* — a unit of computation that produces
transient results. Data flows through tasks as ephemeral intermediates,
identified by opaque `ObjectRef`s, stored in volatile shared memory, and garbage
collected when no longer referenced.

Artisan's primary abstraction is the *artifact* — an immutable, content-addressed
data object with identity, provenance, and persistence. Computation exists to
produce artifacts. Every artifact is durably stored in Delta Lake, linked to its
parents via a provenance graph, and addressable by its content hash across runs,
pipelines, and time.

This isn't a matter of degree — it's a different category of tool. Ray answers
"how do I distribute this computation?" Artisan answers "what was produced, from
what, by what process, and can I trust it?"

---

## Where Artisan Adds Value That Ray Cannot

### Content-Addressed Artifact Identity

Artisan assigns every artifact a deterministic ID via `xxh3_128` hashing of its
content. Identical content always produces the same ID, regardless of when,
where, or how it was produced.

Ray has no equivalent. `ObjectRef`s are opaque, location-based identifiers
assigned at creation time. Two identical computations on the same data produce
two separate objects with different refs. There is no deduplication, no
cross-run identity, and no way to ask "have I computed this before?"

**Why this matters:**

- **Natural deduplication**: Re-running a pipeline with the same inputs
  produces the same artifact IDs. No duplicate data is stored.
- **Cross-run caching**: If a prior execution produced an artifact with the
  same content hash, Artisan skips re-execution automatically — at both the
  step level (skip entire steps) and the execution-unit level (skip individual
  batches within a step).
- **Portable identity**: An artifact ID is meaningful outside the cluster,
  outside the session, and outside the pipeline that created it. It's a
  permanent, content-derived name.

### Full Provenance Tracking

Artisan maintains a complete, persistent, queryable provenance graph:

- **Artifact edges** (`artifact_edges` Delta table): source → target derivation
  relationships with denormalized types, roles, group IDs for co-input tracking,
  and step-boundary flags for composite internals.
- **Execution edges** (`execution_edges` Delta table): which artifacts each
  execution consumed and produced, with direction and role metadata.
- **Execution records** (`executions` Delta table): operation name, parameters,
  user overrides, timestamps, worker ID, backend, success/failure, tool output,
  SLURM logs.

This provenance is **enforced at class definition time**. Every creator operation
must declare `infer_lineage_from` on each output, specifying which inputs it
derives from. Missing declarations raise `TypeError` at import — not at runtime,
not as a warning, not as a best practice. If you write an operation, you declare
its lineage.

Ray has "lineage" — but it's a completely different thing. Ray's lineage is an
internal fault-tolerance mechanism for re-executing tasks to reconstruct evicted
objects. It is ephemeral (lost when the cluster dies), non-queryable, and not
designed for scientific reproducibility or audit. Anyscale added OpenLineage-
based tracking in 2025, but it is a commercial platform feature, not part of
open-source Ray.

**Why this matters:**

- **Reproducibility**: Given any artifact, walk the provenance graph backward to
  find every ancestor, every operation, every parameter configuration. This is
  the complete audit trail for how a result was produced.
- **Selective re-execution**: Change one operation's code, and the provenance
  graph tells you exactly which downstream artifacts are invalidated.
- **Multi-input lineage**: Artisan's `group_by` strategies (LINEAGE, ZIP,
  CROSS_PRODUCT) use the provenance graph itself to pair artifacts from
  different branches — aligning by shared ancestry rather than arbitrary
  position.
- **Visualization**: Micro-level graphs (every artifact and execution) and
  macro-level graphs (step-level overview) render the provenance as navigable
  Graphviz diagrams.

### Durable Pipeline Orchestration

Artisan provides a first-class pipeline abstraction with:

- **Imperative step construction**: `pipeline.run()` / `pipeline.submit()` with
  explicit wiring via `OutputReference` objects.
- **Branching**: A single step's output feeds multiple downstream steps.
- **Diamond patterns**: Multiple branches reconverge via `Merge` or multi-role
  inputs.
- **Step-level caching**: Deterministic `step_spec_id` (hash of operation,
  step number, params, upstream specs) enables skipping entire steps.
- **Execution-level caching**: Per-batch `execution_spec_id` (hash of operation,
  input artifact IDs, params) enables skipping individual batches.
- **Resumption**: `PipelineManager.resume()` restores state from the `steps`
  Delta table.
- **Cancellation**: SIGINT/SIGTERM handlers propagate through the execution
  graph with checked cancel events between phases.

Ray's closest equivalent was **Ray Workflows** — which was **deprecated in
March 2025** and is scheduled for removal. Users are now directed to external
orchestrators (Flyte, Airflow, Prefect). Without Workflows, Ray has no built-in
durable pipeline execution.

Ray Data handles linear streaming chains but has known issues with fan-out
parallelism (ray-project/ray#54430) and has no first-class merge, diamond
pattern, or multi-input support.

### Creator/Curator Architecture

Artisan splits operations into two fundamentally different categories:

- **Creators** produce new artifact content via a sandboxed
  preprocess → execute → postprocess lifecycle. They can be dispatched to SLURM.
  They write output files to a sandbox directory. They support batching,
  parallelism, and remote execution.
- **Curators** manipulate artifact *references* without creating new content.
  They operate on Polars DataFrames of artifact IDs. They always run locally
  in a subprocess. Filter, Merge, IngestFiles, IngestData, IngestPipelineStep,
  and InteractiveFilter are all curators.

Ray has no equivalent distinction. All tasks are treated uniformly. This means
metadata operations (filtering, merging, routing) pay the same serialization and
scheduling overhead as heavy compute operations. More importantly, there's no
framework-level guarantee that a "filter" operation doesn't accidentally mutate
data — the creator/curator split enforces this architecturally.

### Composites with Dual Execution Modes

Artisan's `CompositeDefinition` enables reusable operation compositions with
two execution modes:

- **Collapsed**: All internal operations run in-process on a single worker.
  Artifacts pass in-memory. One pipeline step, one commit. Ideal for
  lightweight chains that don't need individual step tracking.
- **Expanded**: Each internal operation becomes its own pipeline step with
  independent batching, caching, backend selection, and resource allocation.
  Ideal when internal operations have different resource requirements.

The same composite definition works in both modes — the user chooses at
pipeline construction time (`pipeline.run(MyComposite)` vs
`pipeline.expand(MyComposite)`).

Ray has no equivalent. Ad-hoc composition (calling remote functions from remote
functions) is possible but has no reuse model, no formal input/output contract,
and no framework-enforced lineage.

### HPC-Native Execution

Artisan treats SLURM as a first-class execution backend, not a container to
run inside:

- **Native job submission**: Each pipeline step submits its own `sbatch`
  command with appropriate resource requests.
- **Multi-allocation pipelines**: Step A can request 4 GPU nodes; step B can
  request 16 CPU nodes. Each step gets its own SLURM allocation.
- **SLURM array jobs**: Batching maps to SLURM job arrays, leveraging SLURM's
  native scheduling and accounting.
- **Resource mapping**: `ResourceConfig` fields (`partition`, `time_limit`,
  `mem_gb`, `cpus_per_task`, `gpus`) map directly to SLURM parameters.
- **Shared filesystem staging**: Workers write Parquet files to NFS staging
  directories with `fsync`; the orchestrator verifies visibility with
  NFS-aware polling before committing to Delta Lake.
- **Intra-allocation design**: Design docs detail `srun --exclusive` within
  `salloc` for fine-grained GPU/CPU multiplexing within a single allocation.

Ray takes the opposite approach: it starts a long-running cluster inside a
SLURM allocation, runs its own scheduler in parallel with SLURM, and manages
all resources internally. This means:

- Single allocation for the entire Ray cluster lifetime.
- Ray's scheduler duplicates SLURM's job — two systems managing the same GPUs.
- No SLURM job dependencies, array jobs, or per-step resource variation.
- Cluster dies when the allocation ends; no cross-allocation persistence.

### Typed Operations with Validated Contracts

Artisan operations declare typed input/output contracts at class definition
time:

```python
class ScoreOp(OperationDefinition):
    name = "score"
    class InputRole(StrEnum):
        DATA = "data"
    class OutputRole(StrEnum):
        METRICS = "metrics"
    inputs = {"data": InputSpec(artifact_type="data")}
    outputs = {"metrics": OutputSpec(
        artifact_type="metric",
        infer_lineage_from={"inputs": ["data"]},
    )}
```

Missing enums, missing lifecycle methods, missing lineage declarations — all
raise `TypeError` at import time. Input types are validated before dispatch.
Output types are validated before commit. The framework guarantees that if an
operation runs, its inputs and outputs conform to the declared contract.

Ray's `@ray.remote` decorator adds no type validation. Functions accept
anything and return anything. Type errors surface as runtime failures deep
inside distributed execution.

### Staging/Commit Architecture

Workers in Artisan never write directly to Delta Lake. They write Parquet files
to a staging directory, sharded by step and execution run. The orchestrator
(single writer) commits staged files to Delta Lake with:

- Content-addressed deduplication (anti-join on artifact IDs)
- Ordered commits for referential integrity (content → index → provenance →
  executions)
- Crash recovery via idempotent `recover_staged()` (deduplicated re-commit)
- Optional compaction and Z-ORDER clustering

This design avoids Delta Lake transaction conflicts on shared filesystems,
tolerates worker failures (uncommitted staging files are simply not committed),
and enables batch commit of many workers' results in one atomic operation.

Ray's object store is in-memory and volatile. There is no commit protocol, no
crash recovery, and no persistence layer.

---

## Where Ray Has Capabilities Artisan Doesn't

| Capability | Ray | Artisan |
|------------|-----|---------|
| **Elastic auto-scaling** | Ray autoscaler adds/removes nodes based on load | Fixed allocations per step |
| **Actor model** | Long-running stateful actors for services, streaming | No stateful actors |
| **GPU scheduling** | Fine-grained GPU sharing, fractional GPUs, placement groups | Delegates GPU allocation to SLURM |
| **Model serving** | Ray Serve for production inference endpoints | Not a serving framework |
| **Hyperparameter tuning** | Ray Tune with ASHA, PBT, Bayesian optimization | No built-in tuning |
| **Streaming execution** | Ray Data processes data larger than cluster memory | Batch-oriented |
| **Zero-copy data sharing** | Arrow-based shared memory object store | Disk-based staging |
| **Training coordination** | PyTorch DDP/FSDP/DeepSpeed integration | Not a training framework |
| **Community/ecosystem** | Large open-source community, extensive integrations | Early-stage project |

---

## Complementarity: Where They Could Work Together

Ray and Artisan operate at different layers and could theoretically complement
each other:

- **Ray as an execution backend for Artisan**: Replace or supplement the SLURM
  backend with a `RayBackend` that dispatches `ExecutionUnit`s as Ray tasks.
  This would give Artisan access to Ray's GPU scheduling, auto-scaling, and
  zero-copy data sharing while preserving Artisan's artifact identity,
  provenance, and caching.
- **Artisan as an orchestration layer for Ray**: Use Artisan's pipeline
  abstraction, content-addressed caching, and provenance tracking to
  orchestrate Ray-based compute steps. This is essentially what Flyte does
  with Ray today — but with stronger artifact guarantees.

The key insight: Ray manages *compute*. Artisan manages *data and its history*.
These are orthogonal concerns that can coexist.

---

## Comparison with Closer Competitors

While Ray is a common reference point, the tools that overlap more with
Artisan's design space are:

| Feature | Artisan | Flyte | Pachyderm | DVC | Ray |
|---------|---------|-------|-----------|-----|-----|
| Content-addressed artifacts | Yes (xxh3_128) | No (URI-based) | Yes (content hash) | Yes (hash-based) | No |
| Full provenance graph | Yes (artifact + execution edges) | Partial (task-level) | Yes (commit-based) | Partial (pipeline-level) | No |
| HPC/SLURM native | Yes (sbatch/srun) | Via plugins | No | No | Cluster-inside-allocation |
| Two-level caching | Yes (step + execution) | Yes (task-level) | Implicit (content hash) | Yes (hash-based) | No |
| Typed operation contracts | Yes (import-time validation) | Yes (Flyte types) | No | No | No |
| Composites (reusable subgraphs) | Yes (collapsed/expanded) | Yes (subworkflows) | No | No | No |
| Creator/curator split | Yes | No | No | No | No |
| Pipeline branching/merging | Yes (first-class) | Yes | Yes (DAG) | Yes (DAG) | No (deprecated) |
| Delta Lake storage | Yes | No (blob store) | No (custom) | No (git/cloud) | No |
| Interactive filtering | Yes (notebook-native) | No | No | No | No |

Artisan's unique combination is: **content-addressed artifacts + enforced
provenance + HPC-native execution + typed operation contracts + creator/curator
split**. No existing tool provides all of these together.

---

## Summary

Artisan and Ray solve fundamentally different problems. Ray is a distributed
compute engine — it excels at scheduling GPU workloads, scaling training across
nodes, and streaming data through transformation pipelines. But it treats data
as transient, provides no persistent identity for results, and has no
provenance system.

Artisan is an artifact-centric pipeline framework — it excels at tracking what
was produced, from what, by what process, and whether it needs to be recomputed.
Every result is permanently identified, every derivation is recorded, and
computation is automatically skipped when the answer already exists.

The core value propositions that Artisan provides and Ray does not:

- **You can always answer "where did this result come from?"** — full
  provenance graph from any artifact back to its sources.
- **You never compute the same thing twice** — content-addressed caching at
  both step and execution-unit granularity.
- **Your pipeline survives across sessions** — durable Delta Lake storage with
  crash recovery, not volatile in-memory objects.
- **Your operations have contracts** — typed inputs, typed outputs, enforced
  lineage declarations, validated at import time.
- **Your HPC resources are used efficiently** — each step gets its own SLURM
  allocation with appropriate resources, rather than one monolithic cluster.
- **Your metadata operations are architecturally separated** — curators can't
  accidentally create or corrupt data; creators can't silently skip lineage.

These are not features that can be bolted onto Ray. They are architectural
choices that permeate every layer of Artisan's design.
