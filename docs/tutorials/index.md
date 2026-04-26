# Tutorials

Tutorials are hands-on, learning-oriented guides. Work through them from start
to finish in a Jupyter notebook. Each tutorial builds on concepts from the
previous ones.

**Recommended order:** Top-to-bottom in the sidebar. Categories are
numbered (`01-` … `09-`) so disk order matches reading order.

## Getting Started

Build your first pipeline and explore the results.

- [First Pipeline](01-getting-started/01-first-pipeline.ipynb) (~15 min) -- Build and run a seven-step pipeline with example operations
- [Exploring Results](01-getting-started/02-exploring-results.ipynb) (~15 min) -- Query Delta Lake tables and inspect artifacts

## Pipeline Design

Reusable patterns for common pipeline topologies, each with provenance graph
illustrations.

- [Sources and Sequencing](02-pipeline-design/01-sources-and-sequencing.ipynb) (~10 min) -- Linear pipelines and data ingestion
- [Branching and Merging](02-pipeline-design/02-branching-and-merging.ipynb) (~10 min) -- Fan-out and fan-in patterns
- [Metrics and Filtering](02-pipeline-design/03-metrics-and-filtering.ipynb) (~10 min) -- Score-based filtering with auto-discovered metrics
- [Multi-Input Operations](02-pipeline-design/04-multi-input-operations.ipynb) (~15 min) -- Operations that consume multiple input roles
- [Diamonds and Iteration](02-pipeline-design/05-diamonds-and-iteration.ipynb) (~15 min) -- Diamond DAGs, output lineage, and iterative refinement
- [Composites](02-pipeline-design/06-composites.ipynb) (~15 min) -- Compose operations into reusable units with collapsed or expanded execution

## Caching

When to compute, when to reuse a previous result.

- [Resume and Caching](03-caching/01-resume-and-caching.ipynb) (~15 min) -- List runs, resume pipelines, and step-level caching
- [Skipping the Cache](03-caching/02-skip-cache.ipynb) (~10 min) -- Force re-execution of cached steps

## Batching

How to group artifacts into work units.

- [Batching and Performance](04-batching/01-batching-and-performance.ipynb) (~20 min) -- Two-level batching model, BatchStrategy fields, and tuning guidelines
- [Per-Artifact Batch Execute](04-batching/02-batch-execute.ipynb) (~15 min) -- Custom batch grouping for fine-grained dispatch

## Errors and Control

Step overrides, error visibility, and pipeline cancellation.

- [Step Overrides](05-errors-and-control/01-step-overrides.ipynb) (~15 min) -- All `pipeline.run()` override parameters: params, name, execution, backend, and more
- [Error Handling in Practice](05-errors-and-control/02-error-visibility.ipynb) (~15 min) -- Runtime failures, failure logs, FailurePolicy, and empty input cascades
- [Pipeline Cancellation](05-errors-and-control/03-pipeline-cancellation.ipynb) (~10 min) -- Cancel pipelines with `cancel()` or Ctrl+C, inspect results, and re-run

## Storage

Where artifacts live on disk and how to manage external files.

- [Storage Layout and Logging](06-storage/01-storage-and-logging.ipynb) (~15 min) -- Three-path model, Delta Lake tables, directory layout, and logging configuration
- [External File Storage](06-storage/02-external-file-storage.ipynb) (~15 min) -- One-to-one and many-to-one external content patterns

## Compute Backends

Choosing where the execute() phase runs.

- [Compute Routing](07-compute-backends/01-compute-routing.ipynb) (~15 min) -- Route execute() to local or remote compute targets independently of the dispatch backend
- [Running on SLURM](07-compute-backends/02-slurm-execution.ipynb) (~10 min) -- Run operations on a SLURM cluster
- [Running Inside a SLURM Allocation](07-compute-backends/03-slurm-intra-execution.ipynb) (~10 min) -- Dispatch work via srun within an existing SLURM allocation
- [Running on Modal](07-compute-backends/04-modal-execution.ipynb) (~15 min) -- Route operations to Modal containers with GPU, automatic sandbox transport, and tool shipping

## Analysis

Provenance visualization, diagnostics, and interactive analysis tools.

- [Provenance Graphs](08-analysis/01-provenance-graphs.ipynb) (~10 min) -- Macro and micro provenance graph rendering
- [Lineage Tracing](08-analysis/02-lineage-tracing.ipynb) (~15 min) -- Trace ancestors and descendants programmatically
- [Interactive Filter](08-analysis/03-interactive-filter.ipynb) (~15 min) -- Explore metric distributions and commit filter thresholds interactively
- [Timing Analysis](08-analysis/04-timing-analysis.ipynb) (~10 min) -- Profile step and execution performance with PipelineTimings

## Writing Operations

Build custom operations and composites from scratch.

- [Writing an Operation](09-writing-operations/01-writing-an-operation.ipynb) (~15 min) -- Build a custom operation from scratch
- [Writing a Composite](09-writing-operations/02-writing-a-composite.ipynb) (~20 min) -- Build a reusable CompositeDefinition that composes multiple operations
- [Co-Produced Outputs](09-writing-operations/03-co-produced-outputs.ipynb) (~15 min) -- Author an operation with two output roles linked by `infer_lineage_from`

## Cross-references

- [Concepts](../concepts/index.md) -- Design decisions and architecture explanations behind what the tutorials demonstrate
- [Reference](../reference/index.md) -- API details and schema specifications for the types used in tutorials
