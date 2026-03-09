# Orientation

A quick map of the system before you build anything. This page covers how the
documentation is organized and the five key abstractions you will encounter
throughout.

---

## How these docs are organized

The documentation provides four kinds of content, each designed for a different
situation:

| Kind | When to use it | What you'll find |
|------|---------------|------------------|
| **Tutorials** | "I want to learn" | Interactive notebooks that walk you through complete examples step by step. Start here if you're new. |
| **How-to Guides** | "I need to do X" | Focused recipes for specific tasks. Assumes you already know the basics. |
| **Concepts** | "I want to understand why" | Design decisions, mental models, and architecture. No code to run — explanation only. |
| **Reference** | "I need to look something up" | API signatures, schema tables, parameter lists. Structured for quick lookup, not reading. |

These four kinds are kept separate on purpose. Tutorials don't stop to explain
architecture. How-to guides don't teach theory. Reference pages don't include
design rationale. When you need context that isn't on the page you're reading,
you'll find a link to the right place.

> **Rule of thumb:** Start with **Tutorials** to build intuition, reach for
> **How-to Guides** when you have a concrete task, read **Concepts** when
> something feels like magic, and consult **Reference** when you need exact
> details.

---

## Five Key Abstractions

### Artifacts

An **artifact** is a content-addressed data node. The framework computes a
unique ID from the artifact's content (`xxh3_128` hash), so identical content
always produces the same ID. This enables automatic deduplication and
deterministic caching. Artifacts follow a draft/finalize pattern: drafts are
mutable while being assembled, and once finalized the content hash makes any
tampering detectable.

Artisan provides four built-in types: `DATA`, `METRIC`, `CONFIG`, and
`FILE_REF`. Custom artifact types can be registered by domain layers.

> **Deep dive:** [Artifacts and Content Addressing](../concepts/artifacts-and-content-addressing.md)

### Operations

An **operation** is a Python class that consumes artifacts and produces
artifacts. Operations declare typed input and output specifications and know
nothing about orchestration or infrastructure.

There are two kinds:

- **Creator operations** run heavy computation (external tools, GPU work) in a
  three-phase lifecycle — preprocess, execute, postprocess — each running in its
  own filesystem-isolated working directory.
- **Curator operations** perform lightweight metadata work (filtering, merging,
  ingesting) in a single `execute_curator` method call, skipping sandboxing
  entirely.

> **Deep dive:** [Operations Model](../concepts/operations-model.md)

### Pipelines

A **pipeline** is a directed acyclic graph (DAG) of steps. You build one by
calling `pipeline.run(Operation, ...)` to add steps, wiring outputs from
earlier steps into inputs of later ones. `PipelineManager` handles dispatch,
caching, and atomic commits.

```python
pipeline = PipelineManager.create(name="example", delta_root=..., staging_root=...)

step0 = pipeline.run(DataGenerator, params={"count": 4})
step1 = pipeline.run(DataTransformer, inputs={"dataset": step0.output("datasets")})
result = pipeline.finalize()
```

> **Deep dive:** [Execution Flow](../concepts/execution-flow.md)

### Provenance

The framework captures **dual provenance** automatically:

- **Execution provenance** records what computation happened -- which operation
  ran, when, with what parameters, and whether it succeeded.
- **Artifact provenance** records which input artifacts produced which output
  artifacts, forming a derivation graph across the entire pipeline.

You never need to wire provenance manually. The framework infers it from
filename stem matching during execution.

> **Deep dive:** [Provenance System](../concepts/provenance-system.md)

### Storage

All pipeline data lives in **Delta Lake** tables, giving you ACID transactions,
time travel, and efficient queries via Polars. Artifact content is stored
directly in Delta table columns (binary data, JSON, or tabular rows depending on
the type), while `FILE_REF` artifacts store references to files at their original
paths. Workers stage results as Parquet files during execution, and the
orchestrator commits them atomically to the Delta tables.

```python
import polars as pl
df = pl.read_delta(str(delta_root / "artifacts" / "index"))
```

> **Deep dive:** [Storage and Delta Lake](../concepts/storage-and-delta-lake.md)

---

## Where to Go Next

| Goal | Page |
|------|------|
| Install the project | [Installation](installation.md) |
| Build your first pipeline hands-on | [Your First Pipeline](first-pipeline.md) |
| Learn the framework in depth | [Concepts](../concepts/index.md) |
| Browse interactive notebooks | [Tutorials](../tutorials/index.md) |
