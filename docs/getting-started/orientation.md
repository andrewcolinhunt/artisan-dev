# Orientation

This page explains the mental model behind Artisan — how the documentation is
organized and the five key abstractions you will encounter throughout.

---

## How these docs are organized

The documentation provides four kinds of content, each designed for a different
situation:

| Kind              | When to use it                | What you'll find                                                                                      |
| ----------------- | ----------------------------- | ----------------------------------------------------------------------------------------------------- |
| **Tutorials**     | "I want to learn"             | Interactive notebooks that walk you through complete examples step by step. Start here if you're new. |
| **How-to Guides** | "I need to do X"              | Focused recipes for specific tasks. Assumes you already know the basics.                              |
| **Concepts**      | "I want to understand why"    | Design decisions, mental models, and architecture. No code to run — explanation only.                 |
| **Reference**     | "I need to look something up" | API signatures, schema tables, parameter lists. Structured for quick lookup, not reading.             |

These four kinds are kept separate on purpose. Tutorials don't stop to explain
architecture. How-to guides don't teach theory. Reference pages don't include
design rationale. When you need context that isn't on the page you're reading,
you'll find a link to the right place.

> **Rule of thumb:** Start with **Tutorials** to build intuition, reach for
> **How-to Guides** when you have a concrete task, read **Concepts** when
> something feels like magic, and consult **Reference** when you need exact
> details.

---

## Five key abstractions

### Artifacts

An **artifact** is a content-addressed data node. The framework computes a
unique ID from the artifact's content (`xxh3_128` hash), so identical content
always produces the same ID. This enables automatic deduplication and
deterministic caching. Artifacts follow a draft/finalize pattern: drafts are
mutable while being assembled, and once finalized the content hash makes any
tampering detectable.

Artisan provides four built-in artifact types:

| Type       | Class                     | Purpose                                                   |
| ---------- | ------------------------- | --------------------------------------------------------- |
| `DATA`     | `DataArtifact`            | Tabular data (CSV content stored as bytes)                |
| `METRIC`   | `MetricArtifact`          | Computed measurements (JSON-serializable key-value pairs) |
| `CONFIG`   | `ExecutionConfigArtifact` | Execution configuration snapshots (JSON)                  |
| `FILE_REF` | `FileRefArtifact`         | References to files at their original paths on disk       |

Custom artifact types can be registered by domain layers through the
`ArtifactTypeDef` registry.

> **Deep dive:**
> [Artifacts and Content Addressing](../concepts/artifacts-and-content-addressing.md)

### Operations

An **operation** is a Python class that consumes artifacts and produces
artifacts. Operations declare typed input and output specifications via
`InputSpec` and `OutputSpec`, and know nothing about orchestration or
infrastructure.

There are two kinds:

- **Creator operations** run computation (external tools, GPU work) in a
  three-phase lifecycle — `preprocess`, `execute`, `postprocess` — each running
  in its own filesystem-isolated working directory.
- **Curator operations** perform metadata work (filtering, merging, ingesting)
  in a single `execute_curator` method call, skipping sandboxing entirely.

The framework ships with example creators (`DataGenerator`, `DataTransformer`,
`MetricCalculator`) and built-in curators (`Filter`, `Merge`, `IngestFiles`,
`IngestData`, `IngestPipelineStep`). There is also an `InteractiveFilter` tool
for exploratory filtering in notebooks, though it is not a pipeline operation.

When operations are tightly coupled — for example, always running transform
immediately followed by scoring — you can bundle them into a **composite**. A
`CompositeDefinition` declares its own inputs and outputs and wires internal
operations together via a `compose()` method, creating a reusable building block
that the pipeline can execute as a single step (collapsed) or as separate steps
(expanded).

> **Deep dive:** [Operations Model](../concepts/operations-model.md) |
> [Composites and Composition](../concepts/composites-and-composition.md)

### _Pipelines_

_A **pipeline** is a directed acyclic graph (DAG) of steps. You build one by
calling_ `pipeline.run()` _to execute steps and_ `pipeline.output(name, role)`
_to wire outputs from earlier steps into inputs of later ones._
`PipelineManager` _handles dispatch, caching, and atomic commits._

```python
# PipelineManager is the main entry point for defining and running pipelines.
# Example operations ship with artisan for learning and testing.
from artisan.orchestration import PipelineManager
from artisan.operations.examples import DataGenerator, DataTransformer

# Create a pipeline. delta_root stores the Delta Lake tables (artifacts,
# provenance, step state); staging_root holds temporary worker output
# before it is atomically committed.
pipeline = PipelineManager.create(
    name="example",
    delta_root="runs/delta",
    staging_root="runs/staging",
)

# Bind pipeline.output to a short alias. Calling output(step_name, role)
# returns a lazy OutputReference. No data moves until the downstream
# step actually executes.
output = pipeline.output

# Step 0: generate 4 CSV datasets (no inputs, this is a "creator" step).
# params override the operation's default Params; here we ask for 4 files.
pipeline.run(DataGenerator, name="generate", params={"count": 4})

# Step 1: transform every dataset produced by "generate".
# The input role "dataset" matches DataTransformer.inputs, and the output
# role "datasets" matches DataGenerator.outputs. Artisan validates that
# the artifact types are compatible (both are "data").
pipeline.run(
    DataTransformer,
    name="transform",
    inputs={"dataset": output("generate", "datasets")},
)

# Finalize waits for any async steps, shuts down the executor, and
# returns a summary dict with per-step success counts and timings.
result = pipeline.finalize()
```

_Each_ `run()` _call executes a step, and_ `output("step_name", "role")`
_creates a lazy reference that connects one step's outputs to another step's
inputs. Steps can run on different backends —_ `"local"` _for in-process
execution or_ `"slurm"` _for cluster dispatch — configured per-pipeline or
per-step._

> **\*Deep dive:** [Execution Flow](../concepts/execution-flow.md)\*

### Provenance

The framework captures **dual provenance** automatically:

- **Execution provenance** records what computation happened — which operation
  ran, when, with what parameters, and whether it succeeded.
- **Artifact provenance** records which input artifacts produced which output
  artifacts, forming a derivation graph across the entire pipeline.

You never need to wire provenance manually. Each operation declares lineage
relationships through `infer_lineage_from` on its output specs, and the
framework resolves them at runtime using filename stem matching to pair inputs
with outputs.

> **Deep dive:** [Provenance System](../concepts/provenance-system.md)

### Storage

All pipeline data lives in **Delta Lake** tables, giving you ACID transactions,
time travel, and efficient queries via Polars. Artifact content is stored
directly in Delta table columns — binary bytes for data artifacts, JSON content
serialized as bytes for metrics and configs — while `FILE_REF` artifacts store
path references to files on disk. Workers stage results as Parquet files during
execution, and the orchestrator commits them atomically to the Delta tables.

```python
import polars as pl

# Read the artifact index — every artifact in the pipeline
df = pl.read_delta(str(delta_root / "artifacts" / "index"))
```

> **Deep dive:** [Storage and Delta Lake](../concepts/storage-and-delta-lake.md)

---

## Where to go next

| Goal                               | Page                                       |
| ---------------------------------- | ------------------------------------------ |
| AI-assisted development            | [Using Claude Code](using-claude-code.md)  |
| Build your first pipeline hands-on | [Tutorials](../tutorials/index.md)         |
| Learn the framework in depth       | [Concepts](../concepts/index.md)           |
| Task-oriented recipes              | [How-to Guides](../how-to-guides/index.md) |
