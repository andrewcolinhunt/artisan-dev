# Tutorials

Work through these notebooks to build pipelines, inspect their results, and write
reusable operations. Complete [Installation](../getting-started/installation.md)
first, including the source checkout and Jupyter kernel. Some tutorials read
fixtures or companion modules from that checkout.

Follow the sections below for the **local learning path**. You can skip all three
cloud tutorials until you have the required accounts and credentials. They are
listed separately at the end in prerequisite order.

Each notebook creates a named directory under `runs/`. Rerunning its
`tutorial_setup` cell deletes that tutorial's previous results unless the call
sets `clean=False`. The caching examples show when to preserve a previous run.

## Getting started

- [First Pipeline](01-getting-started/01-first-pipeline.ipynb) (~15 min) — Build and inspect a seven-step pipeline
- [Exploring Results](01-getting-started/02-exploring-results.ipynb) (~15 min) — Read committed artifacts, metrics, lineage, and timings

## Pipeline design

- [Sources and Sequencing](02-pipeline-design/01-sources-and-sequencing.ipynb) (~10 min) — Generate or ingest data and connect a linear pipeline
- [Branching and Merging](02-pipeline-design/02-branching-and-merging.ipynb) (~10 min) — Compare transformations and combine their outputs
- [Metrics and Filtering](02-pipeline-design/03-metrics-and-filtering.ipynb) (~10 min) — Score artifacts and select those that meet a threshold
- [Multi-Input Operations](02-pipeline-design/04-multi-input-operations.ipynb) (~15 min) — Pair datasets with inputs derived from them
- [Name-Based Pairing](02-pipeline-design/05-name-based-pairing.ipynb) (~15 min) — Pair independent streams by filename stem
- [Diamonds and Iteration](02-pipeline-design/06-diamonds-and-iteration.ipynb) (~15 min) — Combine branches and repeat a refinement sequence
- [Composites](02-pipeline-design/07-composites.ipynb) (~15 min) — Reuse, override, and nest provided operation compositions

## Caching

- [Resume and Caching](03-caching/01-resume-and-caching.ipynb) (~15 min) — Resume a run and choose which previous results qualify for reuse
- [Skipping the Cache](03-caching/02-skip-cache.ipynb) (~10 min) — Force one step or a whole pipeline to execute again

## Batching

- [Batching and Performance](04-batching/01-batching-and-performance.ipynb) (~20 min) — Group artifacts into units and pack units into worker tasks

The optional cloud batching comparison is listed under Cloud tutorials below.

## Errors and control

- [Step Overrides](05-errors-and-control/01-step-overrides.ipynb) (~15 min) — Change parameters, names, batching, pairing, and failure handling for individual steps
- [Error Handling in Practice](05-errors-and-control/02-error-visibility.ipynb) (~15 min) — Inspect runtime failures, partial results, and skipped steps
- [Pipeline Cancellation](05-errors-and-control/03-pipeline-cancellation.ipynb) (~10 min) — Request cancellation, inspect its outcome, and rerun

## Storage

- [Storage Layout and Logging](06-storage/01-storage-and-logging.ipynb) (~15 min) — Inspect persistent results, temporary files, and session logs
- [External File Storage](06-storage/02-external-file-storage.ipynb) (~10 min) — Store one artifact per file or several records in a shared file

## Compute routing

- [Compute Routing](07-compute-backends/01-compute-routing.ipynb) (~15 min) — Distinguish the step runner from the execute-phase provider, using local examples

## Analysis

- [Provenance Graphs](08-analysis/01-provenance-graphs.ipynb) (~10 min) — Read macro graphs, micro graphs, and cumulative snapshots
- [Lineage Tracing](08-analysis/02-lineage-tracing.ipynb) (~15 min) — Find ancestors and descendants in code
- [Interactive Filter](08-analysis/03-interactive-filter.ipynb) (~15 min) — Explore metrics, choose thresholds, and commit the selection
- [Timing Analysis](08-analysis/04-timing-analysis.ipynb) (~10 min) — Find slow steps and inspect their execution timings

## Writing operations

- [Writing an Operation](09-writing-operations/01-writing-an-operation.ipynb) (~20 min) — Build a creator and verify its output at two batch sizes
- [Writing a Composite](09-writing-operations/02-writing-a-composite.ipynb) (~20 min) — Design and verify a reusable filtered output
- [Co-Produced Outputs](09-writing-operations/03-co-produced-outputs.ipynb) (~15 min) — Produce datasets and metrics with output-to-output lineage

## Cloud tutorials (optional)

These tutorials make billable cloud calls. Start with Running on Modal, which
sets up the account, authentication, and endpoint used by the batching example.
The object-storage tutorial also needs a bucket, storage credentials, and its
own endpoint deployment.

1. [Running on Modal](07-compute-backends/04-modal-execution.ipynb) (~15 min) — Deploy a command operation, compare local and remote execution, and inspect remote markers
2. [Per-Artifact Batch Execute](04-batching/02-batch-execute.ipynb) (~10 min) — Compare concurrent and sequential endpoint calls within the same unit size
3. [Delivering Outputs to Object Storage](07-compute-backends/05-modal-r2-outputs.ipynb) (~10 min) — Deliver remote outputs to an allowed S3-compatible prefix, verify them, and clean up

## Related documentation

- [Concepts](../concepts/index.md) — Understand the execution, storage, and provenance models
- [How-to Guides](../how-to-guides/index.md) — Apply a technique to your own pipeline
- [Python API](../reference/python-api.md) — Find current public classes and their docstrings
