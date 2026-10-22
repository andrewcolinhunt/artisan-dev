# Concepts

Start with [Architecture Overview](architecture-overview.md) to understand how
Artisan runs operations and stores results. The pages below explain each
component's behavior and limits.

## System architecture

- [Architecture Overview](architecture-overview.md) -- High-level system
  structure and component relationships
- [Operations Model](operations-model.md) -- Why Creators and Curators, the
  three-phase execution model, and the spec system
- [Composites and Composition](composites-and-composition.md) -- Reusable
  compositions of operations that expand into real pipeline steps
- [Artifacts and Content Addressing](artifacts-and-content-addressing.md) --
  Immutable artifacts and hash-based identity
- [Pipeline Configuration](pipeline-configuration.md) -- Orchestrator-level
  settings captured by PipelineConfig

## Data and tracking

- [Provenance System](provenance-system.md) -- Dual provenance: execution
  and artifact lineage tracking
- [Execution Flow](execution-flow.md) -- Dispatch, execute, commit lifecycle
- [Storage and Delta Lake](storage-and-delta-lake.md) -- Why Delta Lake for
  artifact persistence

## Design rationale

- [Design Principles](design-principles.md) -- Motivating goals and the
  design decisions behind the framework
- [Error Handling](error-handling.md) -- Failure policies, diagnostics, and
  output availability

## Cross-references

- [Tutorials](../tutorials/index.md) -- Hands-on walkthroughs that demonstrate these concepts in practice
- [How-to Guides](../how-to-guides/index.md) -- Task-oriented instructions that apply these concepts
- [Python API](../reference/python-api.md) -- Public entry points and links to source definitions and docstrings
