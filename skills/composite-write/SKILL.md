---
name: composite-write
description: Write, scaffold, or review an Artisan CompositeDefinition that connects creators under one execution and persistence boundary. Use for reusable operation compositions; use pipeline-write when intermediate steps need independent persistence or recovery.
---

# Write an Artisan Composite

Implement the requested composition with the installed public API. Inspect the
child operations' role, parameter, and lineage contracts before wiring them.
Use [Write Composite Operations](../../docs/how-to-guides/writing-composite-operations.md)
for the maintained example and [CompositeDefinition Reference](../../docs/reference/composite-definition.md)
for public entry points. In an installed application without this source tree,
consult the corresponding published documentation and installed docstrings.

## Choose the boundary

A composite runs creators and nested composites sequentially within each worker
unit. It has one accepted result and cache boundary. Its children do not become
pipeline steps. If an intermediate needs independent persistence, reuse, or
recovery, make it a separate pipeline submission instead.

Store-bound curators such as `Filter`, `Merge`, and ingestion operations stay at
the pipeline level. `compose()` cannot read a store or uncommitted child results.
Creator children must declare static input roles and cannot use NAME or LINEAGE
grouping inside the composite.

## Declare the plan

1. Define `CompositeDefinition`, matching `InputRole`/`OutputRole` enums and
   typed `inputs`/`outputs`. Declare algorithm parameters through a nested
   Pydantic `Params` model and matching `params` field when needed.
2. Implement `compose(self, ctx: CompositeContext) -> None` with `ctx.input`,
   `ctx.call`, and `ctx.output`. Import these types from `artisan.composites`.
3. Pass scientific inputs and effective parameters to each `ctx.call`. Use a
   unique `name` when it makes repeated calls easier to identify.
4. Bind every declared public output exactly once. An ordered list of references
   concatenates streams and preserves duplicate occurrences. Only optional
   outputs may be explicitly empty.

References contain wiring, not artifact values or futures. Loops and conditions
may depend on parameters, but not on future output contents. Keep composition
deterministic and free of provisioning, command execution, and filesystem I/O.
Do not attach child runner, cache, retry, batching, or persistence settings.
Base scientific wiring on `Params`, not on execution-default fields.

## Select what survives

Choose the intended public outputs explicitly. Child operations retain their
ordinary lineage declarations; leave composite `OutputSpec.derives_from` unset
instead of repeating those declarations. Unexported intermediates remain
temporary unless a retained artifact's semantic-reference
policy requires support content.

Export both a structure and its metric when their direct relationship should
survive. A history metric can remain an ordinary metric associated with the
final structure; it does not require persisting every intermediate. Preserve
exact occurrence identity when the same artifact appears more than once.

See [Composites and Composition](../../docs/concepts/composites-and-composition.md)
for projection and retention rules. Do not invent identifier rewriting or infer
lineage from filenames.

## Execute and verify

Submit through the same `pipeline.run(Definition, ...)` or
`pipeline.submit(Definition, ...)` used for operations. Runner placement,
resources, grouping, batching, cache, and failure policies belong to that outer
submission. Each command child may declare its own canonical tool `Environment`;
preparation remains explicit through the selected runner.

Declare `runner_resources`, `batch_strategy`, and `group_by` defaults on a
composite when it needs reusable outer settings. Submission overrides patch only
the supplied fields; omitted fields retain the declaration. The composite author
chooses its worker allocation without combining child resource defaults. These
outer defaults do not apply when the composite is nested through `ctx.call`:
nested composites expand within their parent's boundary.

Wire the returned result or future through `.output(role)`, just as for an
operation. The reference identifies its exact source attempt and store. A saved
result selected with `pipeline.get_step(step_number, pipeline_run_id=...)` uses
the same interface; cross-run inputs require the same authoritative store.

For local authoring checks, use `ExecutionSession` from `artisan.execution` as a
context manager and call `session.run(Definition, inputs=..., params=...)`.
Inputs are finalized artifacts with local carriers. The session retains temporary
artifacts across sequential calls and performs no store writes or provisioning.
See [Prepare Execute Environments](../../docs/how-to-guides/execute-environments.md)
when a child needs prepared software.

Verify the requested behavior with a small real invocation: selected output roles,
ordered duplicates, optional empties, and relevant lineage/reference relationships.
For a persisted pipeline test, check one outer step and its accepted output
membership. Do not test independent child cache entries or fabricate committed
artifacts to make the example pass.
