# Coding Conventions

Use these conventions when changing Artisan or writing an extension. See
[contributing](https://github.com/dexterity-systems/artisan/blob/main/CONTRIBUTING.md)
for environment setup and pull requests.

## Naming and organization

Use `snake_case` for files, directories, functions, and variables; `PascalCase`
for classes; and `UPPER_SNAKE_CASE` for constants. Name directories after their
responsibility, such as `execution/context` or `storage/cache`.

Keep closely related models together when they form one contract. Otherwise,
prefer one primary class per file. Start with a flat package and introduce a
sub-package when several files share a distinct responsibility. Avoid new
abstractions for a single caller.

Place domain operations in their own downstream package. Framework operations
belong under `operations/curator`, generic demonstration operations under
`operations/examples`, and new storage or execution behavior in the layer that
owns it. External scheduler providers belong in separate packages.

The [architecture overview](../concepts/architecture-overview.md) explains the
layer boundaries. Keep dependencies flowing toward lower layers. A schema
should not import an orchestrator, and a domain operation should not depend on
framework storage or dispatch internals.

## Public imports

Public packages provide a docstring, curated re-exports, and an explicit
`__all__`. Downstream code and published examples import from these supported
packages:

```python
from artisan.operations.base import OperationDefinition, PerArtifact
from artisan.schemas import DataArtifact, InputSpec, OutputSpec
```

Use the [Python API index](../reference/python-api.md) to find the package that
owns a workflow. Internal modules may import implementation modules directly;
they do not need to re-export every type.

When adding a public symbol, identify its supported use and update
`tests/test_public_api.py`. Do not expose a model from several facades solely
because it exists. Keep internal helpers private.

## Code style

- Use type annotations on function signatures and Google-style docstrings for
  public behavior.
- Keep functions focused. Split them when doing so makes the responsibility
  clearer, rather than to meet a strict line count.
- Use comments to explain decisions that the code does not make apparent.
- Validate inputs early and raise specific exceptions with useful context.
- Remove obsolete code when changing an interface; do not add compatibility
  shims for removed names.
- Use Ruff for formatting and import order:

```bash
pixi run --locked -e dev fmt
```

## Operation contracts

Operations declare typed input and output roles and implement one execution
shape: a creator's command or Python function, or a curator's routing behavior.
Use a nested `Params` model for algorithm parameters. Keep parameter descriptions
with that model so schema discovery and users see the same explanation.

Use the authoring guides for working examples and validation requirements:

- [Creator operations](../how-to-guides/writing-creator-operations.md)
- [Curator operations](../how-to-guides/writing-curator-operations.md)
- [Composite operations](../how-to-guides/writing-composite-operations.md)
- [Artifact types](../how-to-guides/creating-artifact-types.md)

The framework validates role declarations, parameter models, lineage contracts,
and required outputs. Authoring tests should include a successful result, invalid
inputs, and the operation's meaningful failure cases. For per-artifact creators,
include more than one input per execution unit so batching mistakes are visible.

## External step-runner providers

Core supplies the local runner. A scheduler or other lifecycle runner is a
separate package that imports its contract from
`artisan.orchestration.runner_api`. Users pass a configured `RunnerBase`
instance to a pipeline; core does not register external providers by string or
reconstruct provider objects from stored configuration.

A provider implements a `RunnerBase` subclass with its name, traits, validation,
and `create_lifecycle_router`. Its `LifecycleRouter` owns submission and
collection for one step:

- `_dispatch` checks submission preconditions and starts asynchronous collection
  through `_start_background`.
- Collection returns one `UnitResult` per submitted `ExecutionUnit`, in submission
  order. Return the result model, not a dictionary with selected fields.
- `cancel` is thread-safe, idempotent, and limited to the handles this router
  submitted. Its acknowledgement must reflect what the provider can prove.
- A partial submission failure still settles a result for every original unit
  and cancels only the handles already created by that submission.

Use `pack_units` and `execute_unit_batch` at the worker boundary. Apply
`validate_batch_results` to returned batches and `failure_results_for_units` to
job-level failures. These helpers preserve result cardinality and execution
identity when one batch fails. The base router owns `dispatch`, `is_done`,
`collect`, and `run`; providers implement its hooks instead of replacing that
state machine.

Inspect the current contract directly:

```python
from artisan.orchestration.runner_api import LifecycleRouter, RunnerBase, UnitResult

help(RunnerBase)
help(LifecycleRouter)
help(UnitResult)
```

Provider tests should cover serialized worker execution, ordered batch results,
partial submission, malformed responses, cancellation races, and log delivery.
A successful submission alone does not prove that remote workers produced usable
results.

Stored configuration retains the provider's name. Resuming a pipeline with an
external default runner requires the configured provider instance again.
Curators still execute in an isolated local subprocess. For the distinction
between lifecycle runners and tool compute, see
[configuring execution](../how-to-guides/configuring-execution.md).

## Error handling

Operations should raise useful exceptions when execution cannot continue or
return the appropriate unsuccessful operation result for an expected failure.
Do not catch an exception merely to discard it. Include relevant operation,
artifact, or input context in the error.

Framework code must preserve the original failure if recording also fails.
When collecting concurrent work, one failed future must not erase completed
siblings. Use the runner helpers above instead of duplicating dispatch or
failure-recording logic in examples and providers.

Keep these outcomes distinct:

- Invalid configuration or an invalid API call may raise before execution.
- A failed execution produces a terminal step result and available diagnostic
  evidence according to the failure policy.
- Cancellation requires evidence from the runner; a request alone is not proof
  that work stopped.

See [error handling](../concepts/error-handling.md) for user-visible outcomes and
[debugging executions](../how-to-guides/debugging-executions.md) for diagnosis.

## Docstrings

Document purpose, parameters that need explanation, returned values, and relevant
exceptions beside their implementation. Use Google-style `Args`, `Returns`,
and `Raises` sections where applicable. Explain constraints and meaning without
restating every type annotation in prose.

When an API changes, update its docstring in the same change. Do not maintain
another copy of its signature or complete parameter list in a guide. See
[writing documentation](writing-docs.md#keep-one-authoritative-description).

## Testing

Tests mirror source structure under `tests/artisan/`; pipeline integration tests
live under `tests/integration/`. Use `test_<function>_<scenario>` names and cover
observable behavior, edge cases, and relevant errors.

Markers distinguish pipeline integration from resource requirements. The
[development task list](../getting-started/using-pixi.md#dev-environment) describes
which suites need Docker, object storage, or Modal credentials. Run the checks
appropriate to the change and the repository's required pre-PR checks.
