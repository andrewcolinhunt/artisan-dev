---
name: operation-write
description: Write, scaffold, or review an Artisan creator or curator OperationDefinition, including role contracts, lifecycle methods, exact lineage, and command environment declarations. Use composite-write to connect existing creators and artifact-write when defining a new artifact type.
---

# Write an Artisan Operation

Implement the requested transformation using existing artifact types and public
interfaces where possible. Inspect a relevant installed operation or repository
example before writing. Start with the maintained
[creator guide](../../docs/how-to-guides/writing-creator-operations.md) or
[curator guide](../../docs/how-to-guides/writing-curator-operations.md), then check
installed docstrings for the exact lifecycle arguments. Do not duplicate a full
signature or configuration catalog in the new operation.

## Choose the lifecycle

- A **creator** transforms artifacts through preprocess, execute, and postprocess.
  Implement either native `execute_function` or argv-producing `execute_command`.
- A **curator** selects or ingests content against a captured store view through
  `execute_curator`. It returns `PassthroughResult`, `ArtifactResult`, or the
  supported `ArtifactStreamResult` contract.

Do not mix curator and creator methods. Both use the pipeline's selected worker
runner. Preprocess, postprocess, and native Python run in that worker; a command
may use a separate declared tool environment. Curators remain top-level pipeline
operations, outside scientific composites and standalone `Executor.run`.

## Define a reviewable contract

Declare `name`, matching role enums and typed input/output specs, then parameters
and lifecycle methods. Role enum values must match the spec keys. Parameters
belong in a nested Pydantic `Params` model and matching `params` field; document
them with field descriptions or an Attributes section. Omit both when unnecessary.

For each output, decide whether it is required and which parent roles it uses.
Creators and artifact-producing curators declare `derives_from` explicitly:
input parents, co-produced output parents, or `{"inputs": []}` for roots.
Every declared parent role needs an actual parent, even if that input role is
optional. Passthrough outputs preserve existing identity and use their existing
contract instead of redrafting content.

Use existing public `InputSpec`, `OutputSpec`, and result models from
`artisan.schemas`. Consult their installed docstrings for materialization,
association, and output requirements rather than guessing option names.

## Keep each phase focused

Preprocess adapts input artifacts into plain prepared values. Wrap aligned
per-artifact values in `PerArtifact`; ordinary lists are shared values. Preserve
input order and occurrence indices alongside paths and other prepared data.
Declare `per_artifact_dispatch = False` only for intentional whole-batch work.
Per-artifact execute calls run within the assigned worker; a command may start
its own tool process. This setting does not create additional provider workers.

Execute performs the calculation. Native code receives `ExecuteInput`; command
code returns a literal argv list from its prepared input mapping. Use managed
commands, not a shell string or an unmanaged subprocess lifecycle. Write files
within the provided execution directory and return values for postprocess.

Postprocess drafts artifacts and names exact parents. Prefer
`inputs.ref(role, index=...)` when repeated occurrences are possible; an artifact
ID alone is ambiguous when the same content occurs twice. Use explicit
output-to-output references for co-produced relationships. Names and paths never
select lineage. Return drafts and let the framework finalize identity.

For declared associated artifacts, use the phase input's association accessors.
The captured membership, including an empty set, is part of the input contract.
Do not replace it with an ambient store query.

## Declare software without provisioning

For commands, use `Environment.from_pixi(...)` from `artisan.schemas` and keep the
manifest, lockfile, activation scripts, and supported local package inputs in the
application distribution. `name=` selects a named Pixi environment. A declaration
performs no installation. See
[Prepare Execute Environments](../../docs/how-to-guides/execute-environments.md).

The runner's environment supplies worker Python and native libraries. An
operation's `environment` supplies its command software; it does not move native
`execute_function` to another interpreter. `execute_as_tool=True` is the supported
Python child-process option and requires its importable operation and runtime
packages in the tool environment. Provider placement belongs to the pipeline,
not an operation-level compute selector or endpoint recipe.

Prepare missing software explicitly through the selected runner before execution.
Keep runtime weights/data and sensitive storage/runtime configuration out of the
canonical software recipe. An undeclared tool environment intentionally inherits
the worker; a missing declared target must fail instead of falling back.

An operation can declare `runner_resources`, `batch_strategy`, and `group_by`
defaults for its submitted boundary. Sparse submission overrides retain omitted
fields. A composite enclosing the operation chooses its own worker allocation;
child resource requests are not combined automatically. Prepare with the same
effective resource overrides when the runner's deployment depends on them.

## Package and verify

Put application definitions in importable installed modules. Supply trusted
initializer modules through `application_modules` to pipeline create/resume and
runner preparation when registrations are needed. Workers verify that code's
software coverage before decoding live calls; a driver-only import or a stored
module name is insufficient authorization. See
[application loading](../../docs/how-to-guides/configuring-execution.md#load-application-definitions-in-every-process).

Test pure scientific logic directly when useful, then check the actual lifecycle.
Use `Executor.run` for local creator/composite authoring without a store,
or a small pipeline for curator and durable acceptance behavior. Keep the executor
open while using temporary carriers. Test relevant empty/required outputs,
parameter errors, duplicate input occurrences, and exact lineage. Use real
admission and accepted fixtures for persistence checks; mocked rows do not prove
producer authority. Report separately any untested provider or external-tool path.
