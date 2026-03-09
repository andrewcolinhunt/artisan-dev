# Glossary

Key terms used throughout the Artisan documentation.

---

(glossary-acid)=
## ACID

Atomicity, Consistency, Isolation, Durability — the four properties that
guarantee reliable database transactions. Delta Lake provides ACID transactions
over Parquet files, ensuring that concurrent worker writes and partial failures
never corrupt the shared artifact store.

---

(glossary-artifact)=
## Artifact

An immutable, content-addressed data node identified by the xxh3\_128 hash of
its content. Artifacts are the fundamental data units flowing through pipelines.
Once finalized, an artifact's ID is a permanent commitment to its exact content.

---

(glossary-content-addressing)=
## Content addressing

A storage strategy where data is identified by the hash of its content rather
than by location or name. In Artisan, `artifact_id = xxh3_128(content)`, meaning
identical content always produces the same ID. This enables automatic
deduplication and deterministic caching.

---

(glossary-creator-operation)=
## Creator operation

An operation subclass that runs heavy computation (external tools, GPU work)
through a three-phase lifecycle: `preprocess`, `execute`, `postprocess`.
Creators produce new artifacts from inputs. See
[Operations Model](../concepts/operations-model.md).

---

(glossary-curator-operation)=
## Curator operation

A lightweight operation subclass that routes, filters, or merges artifacts
without heavy computation. Curators implement a single `execute_curator()` method
and run in-process on the orchestrator, with no worker dispatch or sandboxing. See
[Operations Model](../concepts/operations-model.md).

---

(glossary-delta-lake)=
## Delta Lake

An open-source storage layer that brings ACID transactions to Parquet files.
Artisan uses Delta Lake as its backing store for all artifacts, provenance, and
execution records. It provides transactional writes, time travel, and partition
pruning without requiring an external database server.

---

(glossary-execution-unit)=
## Execution unit

The work package dispatched to a worker. An `ExecutionUnit` carries a fully
configured operation instance, a batch of input artifact IDs, the cache key, and
the step number. The number of artifacts per unit is controlled by
`execution.artifacts_per_unit`.

---

(glossary-hydration)=
## Hydration

The process of loading an artifact's full content from storage. `hydrate=True`
(default) loads all fields including content bytes. `hydrate=False` loads only
the artifact ID and type, which is sufficient for passthrough operations like
Filter and Merge that route artifacts without reading their content.

---

(glossary-input-spec)=
## InputSpec

A declarative specification on an operation class that describes one named
input: its artifact type, whether it is required, and how it should be
materialized (hydrated or ID-only). Defined in the operation's `inputs`
class variable keyed by role name.

---

(glossary-nfs)=
## NFS

Network File System, a distributed filesystem protocol common on HPC clusters.
When workers run on different cluster nodes, Artisan calls `fsync()` on staged
files to ensure NFS close-to-open consistency before the orchestrator reads them.

---

(glossary-operation)=
## Operation

A Python class that consumes artifacts and produces artifacts. Operations
declare typed input and output specifications and are either creators (heavy
computation with a three-phase lifecycle) or curators (lightweight routing).
See [Operations Model](../concepts/operations-model.md).

---

(glossary-output-reference)=
## OutputReference

A lightweight reference to a previous step's output, created by
`pipeline.output("step_name", "role")`. Used to wire steps together without
moving data. The framework resolves references to concrete artifact IDs when
the downstream step executes.

---

(glossary-output-spec)=
## OutputSpec

A declarative specification on an operation class that describes one named
output: its artifact type and lineage inference strategy
(`infer_lineage_from`). Defined in the operation's `outputs` class variable
keyed by role name.

---

(glossary-parquet)=
## Parquet

A columnar file format optimized for analytical queries. Delta Lake tables are
composed of Parquet files. Artisan stages worker results as Parquet files before
committing them atomically to Delta Lake.

---

(glossary-pipeline)=
## Pipeline

A directed acyclic graph (DAG) of steps managed by `PipelineManager`. Steps
are added by calling `pipeline.run()` (blocking) or `pipeline.submit()`
(non-blocking) and wired together via output references.

---

(glossary-provenance)=
## Provenance

The record of where data came from and how it was produced. Artisan maintains
dual provenance: *execution provenance* (what computation happened) and
*artifact provenance* (which specific input produced which specific output).

---

(glossary-stem-matching)=
## Stem matching

The default algorithm for inferring artifact provenance (parent–child
relationships). It strips file extensions from input and output filenames, then
checks if the output stem starts with the input stem. Digit boundary protection
prevents `design_1` from matching `design_10`. Exactly one match is required;
zero or multiple matches result in an orphan artifact.

---

(glossary-step-result)=
## StepResult

The object returned by `pipeline.run()`. Contains metadata about the step
execution: success status, artifact counts, and output references for wiring
into downstream steps.
