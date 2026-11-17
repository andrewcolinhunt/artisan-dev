# CompositeDefinition Reference

Import composite types from `artisan.composites`. Use the
[writing guide](../how-to-guides/writing-composite-operations.md) for a runnable
example and [Composites and Composition](../concepts/composites-and-composition.md)
for execution and persistence behavior.

## Public entry points

| Interface | Purpose |
| --- | --- |
| `CompositeDefinition` | Declares roles, parameters, outer execution defaults, and `compose()` wiring |
| `CompositeContext.input` | References a declared input role |
| `CompositeContext.call` | Declares a creator or nested composite call |
| `CompositeCall.output` | References a call's declared output role |
| `CompositeContext.output` | Binds a public role to one or more ordered streams |
| `CompositeRef` | Immutable reference scoped to its composition |
| `PipelineManager.run` / `submit` | Executes and persists the outer boundary |
| `PipelineManager.get_step` | Loads a saved result handle from a run in the same store |
| `Executor.run` | Invokes a definition on local artifacts without persistence |

`CompositeContext` is supplied by Artisan during compilation. Author code does
not construct it or pass a pipeline into it. Exact signatures and validation
rules live in the installed docstrings; see [Python API Lookup](python-api.md).

## Wiring and results

Inside `compose`, use only plan references. Every output role is bound once;
an ordered sequence concatenates compatible streams. Parameters configure the
selected child definition. Duplicate call names, foreign references, invalid
roles, and incompatible artifact types fail during compilation.

Outside the composite, `StepResult.output(role)` and `StepFuture.output(role)`
provide source-bound references for downstream steps. Completed outputs can
also feed another run in the same store. There is no separate composite
result or submission method. Placement, persistence, cache, and retry belong to
the submitted boundary, not the individual `ctx.call` declarations.

A composite's authored resource, batching, and grouping defaults apply when it
is the outer boundary. Supplied submission fields override those defaults.
Nested composites expand transparently and do not reapply their outer defaults.
See [declaring defaults](../how-to-guides/writing-composite-operations.md#declare-step-defaults).
