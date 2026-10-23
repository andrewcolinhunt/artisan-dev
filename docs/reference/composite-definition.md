# CompositeDefinition Reference

Import composite types from `artisan.composites` and submit them through
`artisan.orchestration.PipelineManager`. The
[writing guide](../how-to-guides/writing-composite-operations.md) contains a
complete example; [Composites and Composition](../concepts/composites-and-composition.md)
explains the execution model and override precedence.

Exact signatures, fields, and defaults live in the source and docstrings. Use
[Python API](python-api.md) for ways to inspect the installed version.

---

## Public entry points

| Entry point | Purpose | Definition |
|-------------|---------|------------|
| `CompositeDefinition` | Declares roles, parameters, and `compose()` wiring | [Source](https://github.com/dexterity-systems/artisan/blob/main/src/artisan/composites/base/composite_definition.py) |
| `CompositeContext` | Connects declared inputs, child operations, and exposed outputs | [Source](https://github.com/dexterity-systems/artisan/blob/main/src/artisan/composites/base/composite_context.py) |
| `CompositeStepHandle` | References a child operation or nested composite's outputs | [Source](https://github.com/dexterity-systems/artisan/blob/main/src/artisan/composites/base/results.py) |
| `CompositeRef` | Carries wiring references within `compose()` | [Source](https://github.com/dexterity-systems/artisan/blob/main/src/artisan/schemas/composites/composite_ref.py) |
| `CompositeResult` | Exposes mapped outputs and waits for child completion | [Source](https://github.com/dexterity-systems/artisan/blob/main/src/artisan/composites/base/results.py) |
| `PipelineManager.run_composite()` / `submit_composite()` | Expands a composite into ordinary pipeline steps | [Source](https://github.com/dexterity-systems/artisan/blob/main/src/artisan/orchestration/pipeline_manager.py) |

Repository links show the published branch. Installed docstrings describe the
version you are running.

---

## Wiring and results

Inside `compose()`, `ctx.input(role)` references a declared input.
`ctx.run(...)` submits a child operation or nested composite, and the returned
handle's `.output(role)` supplies wiring for another child. `ctx.output(role,
ref)` maps an internal output onto the composite's public output contract.

Outside the composite, `CompositeResult.output(role)` provides an
`OutputReference` for pipeline wiring. See
[How a composite executes](../concepts/composites-and-composition.md#how-a-composite-executes)
for child persistence and caching.

## Submission and overrides

`submit_composite()` returns after composition and child submission, which can
block during preparation. Call the result's `.wait()` or use `run_composite()`
to wait for every child. See
[Submission and waiting](../concepts/composites-and-composition.md#submission-and-waiting).

Execution overrides provide defaults for child steps. See
[Composite-level overrides](../concepts/composites-and-composition.md#composite-level-overrides-are-step-defaults)
for precedence and
[Forward execution overrides](../how-to-guides/writing-composite-operations.md#forward-execution-overrides)
for an example.

---

## See also

- [Composites and Composition](../concepts/composites-and-composition.md) — grouping, persistence, and placement
- [Writing Composite Operations](../how-to-guides/writing-composite-operations.md) — implementation and validation
- [Composable Operations Tutorial](../tutorials/02-pipeline-design/07-composites.ipynb) — interactive examples
- [Python API](python-api.md) — public modules and installed definitions
