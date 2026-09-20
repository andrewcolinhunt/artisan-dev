# How-to Guides

Recipes for building operations, configuring pipelines, and working with results.
Each guide explains the task, its prerequisites, and how to check the outcome.

## Operations

- [Writing Creator Operations](writing-creator-operations.md) -- Build
  operations that produce new artifacts
- [Writing Curator Operations](writing-curator-operations.md) -- Build
  operations that filter, merge, or transform collections
- [Writing Composite Operations](writing-composite-operations.md) -- Group
  tightly coupled operations into reusable composites
- [Creating Artifact Types](creating-artifact-types.md) -- Define custom
  artifact types for your domain

## Pipelines

- [Building a Pipeline](building-a-pipeline.md) -- Create, wire, and execute
  a pipeline from scratch

## Configuration

- [Configuring Execution](configuring-execution.md) -- Resource allocation,
  batching, and runner configuration
- [Deploy Tool Endpoints](deploying-tool-endpoints.md) -- Deploy command operations
  and configure object-store input/output delivery
- [Debug a Recorded Execution](debugging-executions.md) -- Replay one unit and
  retain diagnostic evidence
- [Op Container Images](op-container-images.md) -- Build, resolve, and pin
  the image an operation runs in, for Modal and external harnesses
- [Configuring S3-Compatible Storage](configuring-s3.md) -- Point Delta
  Lake, staging, and inputs at S3, MinIO, or any S3-compatible backend

## Results

- [Connect an MCP Client](connecting-mcp.md) -- Discover operations and inspect
  runs and failure logs through the read-only MCP server
- [Inspecting Provenance](inspecting-provenance.md) -- Query lineage and trace
  artifact history
- [Exporting Results](exporting-results.md) -- Export accepted run outputs and materialize artifacts

## Cross-references

- [Concepts](../concepts/index.md) -- Understand the design decisions behind the tasks in these guides
- [Reference](../reference/index.md) -- Public entry points, source contracts, and terminology
- [Tutorials](../tutorials/index.md) -- Hands-on walkthroughs that introduce the framework interactively
