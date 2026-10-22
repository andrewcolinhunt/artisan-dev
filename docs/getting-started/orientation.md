# Orientation

Artisan connects reusable operations through typed artifacts. It records
execution history and declared lineage so you can inspect results and use their
relationships in later computations.

---

## How these docs are organized

| Kind | When to use it | What you'll find |
|------|---------------|------------------|
| **Tutorials** | Learn by running examples | Interactive notebooks; start here if you're new |
| **How-to Guides** | Complete a specific task | Recipes that assume you know the basics |
| **Concepts** | Understand behavior and tradeoffs | Explanations of the framework's design |
| **Reference** | Look up a fact | Terminology, public APIs, and links to source and docstrings |

---

## Core concepts

### Artifacts

An **artifact** is data identified by its content. Artisan computes its ID from
its type, canonical payload, and metadata that defines its identity. Identical
artifacts produce the same ID; equal bytes in different types remain distinct.
You can edit a draft until finalization records its durable state, allowing later
changes to be detected.

Artisan provides six built-in artifact types:

| Type | Class | Purpose |
|------|-------|---------|
| `data` | `DataArtifact` | Tabular data (CSV content stored as bytes) |
| `metric` | `MetricArtifact` | Computed measurements (JSON-serializable key-value pairs) |
| `config` | `ExecutionConfigArtifact` | JSON parameters, optionally containing artifact references |
| `file_ref` | `FileRefArtifact` | Verified references to files at local or object-store locations |
| `large_file` | `LargeFileArtifact` | References to large files whose bytes remain outside Delta tables |
| `appendable` | `AppendableArtifact` | Individual records stored in a shared JSONL file |

Custom artifact types use the `ArtifactTypeDef` registry. Configure their
application modules so workers and readers can load the registrations; see
[creating artifact types](../how-to-guides/creating-artifact-types.md).

See [Artifacts and Content Addressing](../concepts/artifacts-and-content-addressing.md)
for identity and finalization rules.

### Operations

An **operation** is a Python class that consumes artifacts and produces
artifacts. Operations declare typed input and output specifications via
`InputSpec` and `OutputSpec`. Runner settings control where creator workers run;
compute settings control where their execute phase runs.

There are two kinds:

- **Creator operations** run computation (external tools, GPU work) in a
  three-phase lifecycle: `preprocess`, an execute function or command, and
  `postprocess`. Each execution uses a working directory for its files.
- **Curator operations** perform metadata work (filtering, merging,
  ingesting) through `execute_curator` in a local subprocess, without a creator
  working directory.

The framework ships with example creators (`DataGenerator`, `DataTransformer`,
`MetricCalculator`) and built-in curators (`Filter`, `Merge`, `IngestFiles`,
`IngestData`, `IngestPipelineStep`). There is also an `InteractiveFilter` tool
for exploratory filtering in notebooks, though it is not a pipeline operation.

Bundle operations you repeatedly run together, such as transform followed by
scoring, into a **composite**. A `CompositeDefinition` declares inputs and outputs
and connects its operations through `compose()`. Each internal operation runs
as its own pipeline step.

See [Operations Model](../concepts/operations-model.md) and
[Composites and Composition](../concepts/composites-and-composition.md).

### Pipelines

A **pipeline** is a directed acyclic graph (DAG) of steps. `PipelineManager`
handles dispatch, caching, and publishing completed results:

```python
from artisan.orchestration import PipelineManager
from artisan.operations.examples import DataGenerator, DataTransformer

pipeline = PipelineManager.create(
    name="example",
    delta_root="runs/delta",
    staging_root="runs/staging",
)
output = pipeline.output

pipeline.run(DataGenerator, name="generate", params={"count": 4})
pipeline.run(
    DataTransformer,
    name="transform",
    inputs={"dataset": output("generate", "datasets")},
)
result = pipeline.finalize()
```

Each `run()` call executes a step, and `output("step_name", "role")` creates a
lazy reference that connects one step's outputs to another step's inputs. Core
uses the native `"local"` process runner; optional packages provide runner
instances for cluster dispatch. Configure either per pipeline or per step.

See [Execution Flow](../concepts/execution-flow.md) for how a step runs.

### Provenance

The framework records **dual provenance**:

- **Execution provenance** records what computation happened — which operation
  ran, when, with what parameters, and whether it succeeded.
- **Artifact provenance** records which input artifacts produced which output
  artifacts, forming a derivation graph across the entire pipeline.

Operations declare required parent roles through `derives_from` and supply
exact parents for every derived output in `ArtifactResult.lineage`. Artisan
validates and records those declarations. Root outputs explicitly have no
parents. Optional matching helpers are called by operations, never executors.

See [Provenance System](../concepts/provenance-system.md) for lineage declarations
and queries.

### Storage

Artisan stores artifact records, execution history, and provenance in **Delta
Lake** tables. Small payloads live in those tables; externally backed artifact
types retain verified references to separate files.

Workers stage results as Parquet files. The orchestrator writes the affected
tables and records when the complete logical commit is accepted. Each table has
its own Delta transaction; readers use the logical completion record to avoid
exposing incomplete writes across tables.

Use Artisan's inspection helpers and `ArtifactStore` to read accepted results.
For example, after running the pipeline above:

```python
from artisan.visualization import inspect_step

inspect_step(
    "runs/delta",
    step_number=1,
    pipeline_run_id=pipeline.config.pipeline_run_id,
)
```

Selecting a run includes the outputs it reused from earlier runs. See
[Inspect Pipeline Results](../how-to-guides/inspecting-provenance.md) for run
selection and provenance queries.

For queries with validated inputs and size-limited responses, use
[local evidence inspection](../how-to-guides/inspecting-evidence.md) from Python
or the CLI. The installed operating guide helps an agent select runs, interpret
missing evidence, and apply explicit assessment or comparison criteria.

See [Storage and Delta Lake](../concepts/storage-and-delta-lake.md) for persistence
and recovery guarantees.

---

## Where to go next

| Goal | Page |
|------|------|
| Agent-assisted development | [Using Coding Agents](using-coding-agents.md) |
| Build your first pipeline hands-on | [Tutorials](../tutorials/index.md) |
| Learn the framework in depth | [Concepts](../concepts/index.md) |
| Task-oriented recipes | [How-to Guides](../how-to-guides/index.md) |
