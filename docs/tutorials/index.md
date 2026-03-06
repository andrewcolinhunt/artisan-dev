# Tutorials

Tutorials are hands-on, learning-oriented guides. Work through them from start
to finish in a Jupyter notebook. Each tutorial builds on concepts from the
previous ones.

**Recommended order:** Start with First Steps (in order), then Pipeline
Patterns, then Working with Results, then Execution and Tuning.

## First Steps

Build your first pipeline, explore results, and learn about local vs
submitted execution.

- [First Pipeline](getting-started/01-first-pipeline.ipynb) (~15 min) -- Build and run
  a seven-step pipeline with example operations
- [Exploring Results](getting-started/02-exploring-results.ipynb) (~15 min) -- Query
  Delta Lake tables and inspect artifacts
- [Run vs Submit](getting-started/03-run-vs-submit.ipynb) (~10 min) -- Blocking vs
  non-blocking step execution
- [SLURM Execution](getting-started/04-slurm-execution.ipynb) (~10 min) -- Run
  operations on a SLURM cluster
- [Writing an Operation](getting-started/05-writing-an-operation.ipynb) (~15 min) --
  Build a custom operation from scratch

## Pipeline Patterns

Reusable patterns for common pipeline topologies, each with provenance graph
illustrations.

- [Sources and Chains](pipeline-patterns/01-sources-and-chains.ipynb) (~10 min) -- Linear
  pipelines and data ingestion
- [Branching and Merging](pipeline-patterns/02-branching-and-merging.ipynb) (~10 min) --
  Fan-out and fan-in patterns
- [Metrics and Filtering](pipeline-patterns/03-metrics-and-filtering.ipynb) (~10 min) --
  Score-based filtering with auto-discovered metrics
- [Multi-Input Operations](pipeline-patterns/04-multi-input-operations.ipynb) (~15 min) --
  Operations that consume multiple input roles
- [Advanced Patterns](pipeline-patterns/05-advanced-patterns.ipynb) (~15 min) -- Batching,
  diamond DAGs, output lineage, and iterative refinement
- [Error Visibility](pipeline-patterns/06-error-visibility.ipynb) (~10 min) -- Graceful
  skip on empty filter, skip inspection, and passthrough_failures
- [Composable Operations](pipeline-patterns/07-composable-operations.ipynb) (~15 min) --
  Chain operations into a single step with in-memory artifact passing

## Working with Results

Provenance visualization, diagnostics, and interactive analysis tools.

- [Provenance Graphs](working-with-results/01-provenance-graphs.ipynb) (~10 min) -- Macro
  and micro provenance graph rendering
- [Resume and Caching](working-with-results/02-resume-and-caching.ipynb) (~15 min) --
  List runs, resume pipelines, and step-level caching
- [Interactive Filter](working-with-results/03-interactive-filter.ipynb) (~15 min) --
  Explore metric distributions and commit filter thresholds interactively
- [Timing Analysis](working-with-results/04-timing-analysis.ipynb) (~10 min) -- Profile
  step and execution performance with PipelineTimings

## Execution and Tuning

Deep dives into batching, scheduling, and step-level configuration.

- [Batching and Performance](execution-and-tuning/01-batching-and-performance.ipynb) (~20 min) --
  Two-level batching model, ExecutionConfig fields, and tuning guidelines
- [Step Overrides](execution-and-tuning/02-step-overrides.ipynb) (~15 min) -- All
  `pipeline.run()` override parameters: params, name, execution, backend, and more

## Cross-References

- [Concepts](../concepts/index.md) -- Design decisions and architecture explanations behind what the tutorials demonstrate
- [Reference](../reference/index.md) -- API details and schema specifications for the types used in tutorials
