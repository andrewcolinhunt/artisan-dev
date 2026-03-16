# Concepts

These pages explain the ideas, design decisions, and trade-offs behind the
Artisan framework. Start with the Architecture Overview for the big picture,
then explore topics relevant to your work.

## System Architecture

- [Architecture Overview](architecture-overview.md) -- High-level system
  structure and component relationships
- [Operations Model](operations-model.md) -- Why Creators and Curators, the
  three-phase execution model, and the spec system
- [Composites and Composition](composites-and-composition.md) -- Reusable
  compositions of operations with collapsed and expanded execution
- [Artifacts and Content Addressing](artifacts-and-content-addressing.md) --
  Immutable artifacts and hash-based identity

## Data and Tracking

- [Provenance System](provenance-system.md) -- Dual provenance: execution
  and artifact lineage tracking
- [Execution Flow](execution-flow.md) -- Dispatch, execute, commit lifecycle
- [Storage and Delta Lake](storage-and-delta-lake.md) -- Why Delta Lake for
  artifact persistence

## Design Rationale

- [Design Principles](design-principles.md) -- Motivating goals and the
  design decisions behind the framework
- [Error Handling](error-handling.md) -- Fail-fast philosophy and error
  propagation

## Cross-references

- [Tutorials](../tutorials/index.md) -- Hands-on walkthroughs that demonstrate these concepts in practice
- [How-to Guides](../how-to-guides/index.md) -- Task-oriented instructions that apply these concepts
- [Reference](../reference/index.md) -- API details and schema specifications for the types described here
