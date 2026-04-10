# Tutorials

Tutorials are hands-on, learning-oriented guides. Work through them from start
to finish in a Jupyter notebook. Each tutorial builds on concepts from the
previous ones.

**Recommended order:** Start with Getting Started (in order), then Pipeline
Design, then Execution, then Analysis, then Writing Operations.

## Getting Started

Build your first pipeline and explore the results.

- [First Pipeline](getting-started/01-first-pipeline.ipynb) (~15 min) -- Build and run
  a seven-step pipeline with example operations
- [Exploring Results](getting-started/02-exploring-results.ipynb) (~15 min) -- Query
  Delta Lake tables and inspect artifacts

## Pipeline Design

Reusable patterns for common pipeline topologies, each with provenance graph
illustrations.

- [Sources and Sequencing](pipeline-design/01-sources-and-sequencing.ipynb) (~10 min) -- Linear
  pipelines and data ingestion
- [Branching and Merging](pipeline-design/02-branching-and-merging.ipynb) (~10 min) --
  Fan-out and fan-in patterns
- [Metrics and Filtering](pipeline-design/03-metrics-and-filtering.ipynb) (~10 min) --
  Score-based filtering with auto-discovered metrics
- [Multi-Input Operations](pipeline-design/04-multi-input-operations.ipynb) (~15 min) --
  Operations that consume multiple input roles
- [Diamonds and Iteration](pipeline-design/05-diamonds-and-iteration.ipynb) (~15 min) --
  Diamond DAGs, output lineage, and iterative refinement
- [Composable Operations](pipeline-design/06-composable-operations.ipynb) (~15 min) --
  Compose operations into reusable units with collapsed or expanded execution

## Execution

Running pipelines, caching, batching, error handling, and step-level
configuration.

- [Run vs Submit](execution/01-run-vs-submit.ipynb) (~10 min) -- Blocking vs
  non-blocking step execution
- [Resume and Caching](execution/02-resume-and-caching.ipynb) (~15 min) --
  List runs, resume pipelines, and step-level caching
- [Batching and Performance](execution/03-batching-and-performance.ipynb) (~20 min) --
  Two-level batching model, ExecutionConfig fields, and tuning guidelines
- [Error Handling in Practice](execution/04-error-visibility.ipynb) (~15 min) --
  Runtime failures, failure logs, FailurePolicy, and empty input cascades
- [Storage Layout and Logging](execution/05-storage-and-logging.ipynb) (~15 min) --
  Three-path model, Delta Lake tables, directory layout, and logging configuration
- [Step Overrides](execution/06-step-overrides.ipynb) (~15 min) -- All
  `pipeline.run()` override parameters: params, name, execution, backend, and more
- [SLURM Execution](execution/07-slurm-execution.ipynb) (~10 min) -- Run
  operations on a SLURM cluster
- [Pipeline Cancellation](execution/08-pipeline-cancellation.ipynb) (~10 min) --
  Cancel pipelines with `cancel()` or Ctrl+C, inspect results, and re-run
- [SLURM Intra-Allocation](execution/10-slurm-intra-execution.ipynb) (~10 min) --
  Dispatch work via srun within an existing SLURM allocation
- [Compute Routing](execution/13-compute-routing.ipynb) (~15 min) -- Route
  execute() to local or remote compute targets independently of the dispatch backend

## Analysis

Provenance visualization, diagnostics, and interactive analysis tools.

- [Provenance Graphs](analysis/01-provenance-graphs.ipynb) (~10 min) -- Macro
  and micro provenance graph rendering
- [Lineage Tracing](analysis/02-lineage-tracing.ipynb) (~15 min) -- Trace
  ancestors and descendants programmatically
- [Interactive Filter](analysis/03-interactive-filter.ipynb) (~15 min) --
  Explore metric distributions and commit filter thresholds interactively
- [Timing Analysis](analysis/04-timing-analysis.ipynb) (~10 min) -- Profile
  step and execution performance with PipelineTimings

## Writing Operations

Build custom operations and composites from scratch.

- [Writing an Operation](writing-operations/01-writing-an-operation.ipynb) (~15 min) --
  Build a custom operation from scratch
- [Writing a Composite](writing-operations/02-writing-a-composite.ipynb) (~20 min) --
  Build a reusable CompositeDefinition that composes multiple operations

## Cross-references

- [Concepts](../concepts/index.md) -- Design decisions and architecture explanations behind what the tutorials demonstrate
- [Reference](../reference/index.md) -- API details and schema specifications for the types used in tutorials
