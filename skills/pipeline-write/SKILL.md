---
name: pipeline-write
description: Write or update an Artisan pipeline script that submits operations or composites, wires exact outputs, and configures runner preparation, resources, caching, and lifecycle. Use operation-write for a missing transformation and pipeline-operate to inspect an existing run.
---

# Write an Artisan Pipeline

Build the requested workflow from existing operations where possible. If an
operation catalog is available, inspect names, input/output roles, and parameter
contracts there; otherwise use installed definitions and examples. Do not infer
role names from class names.

Use [Build a Pipeline](../../docs/how-to-guides/building-a-pipeline.md) for maintained
examples and [Configure Execution](../../docs/how-to-guides/configuring-execution.md)
for policies and resources. In an installed application, use the published guides
and installed public docstrings instead of requiring repository source paths.

## Choose durable boundaries

A pipeline submission is one operation or composite boundary. Use a composite
when creators should share a worker and temporary intermediates; use separate
steps for independently retained and recoverable results. Submit both with
`pipeline.run` or `pipeline.submit`. There is no separate composite submission API.
Curators such as ingestion, `Filter`, and `Merge` remain top-level steps.

Before execution, identify the operations, role wiring, software, runner,
store/staging/files roots, and required runtime inputs. Preserve explicit user
choices. Do not turn a local script request into a provider deployment.

## Prepare software explicitly

Use `LocalRunner` from `artisan.orchestration.runners` or an installed provider's
actual runner instance. `LocalRunner()` observes the current Python environment;
it does not claim a locked software identity, so persistent cache reuse is disabled.
Pass a canonical worker `Environment` when qualified prepared software is needed.
Operation declarations independently select their command environments.

Call the chosen runner's preparation methods for the definitions and effective
parameters before execution when software must be built or installed. Normal
run/submit/resolve does not provision. See
[Prepare Execute Environments](../../docs/how-to-guides/execute-environments.md).
Use provider documentation for its preparation, credentials, storage access, and
resource requirements. Core does not reconstruct arbitrary provider instances.

Keep runtime data and model weights separate from dependency declarations. Supply
credentials through the supported live storage/runtime configuration, not durable
operation params or output labels. Remote workers need access to the exact same
authoritative store and managed files; never invent a local mirror fallback.

## Create, connect, and close

Import `PipelineManager` from `artisan.orchestration`. Create a run with explicit
`name`, `delta_root`, and `staging_root`; supply working/files roots and storage
configuration when needed. Prefer a context manager, or call `finalize()` after
the last submission. Do not delete scratch while ownership remains unresolved.

Wire named roles with `StepResult.output(role)` or `StepFuture.output(role)`.
`pipeline.output(name, role, step_number=...)` is useful for an existing named step;
select a position explicitly when repeated names would be ambiguous. References
carry their store and exact upstream attempt from creation; they do not follow
later retries or get reinterpreted using the receiving run's step numbers.

Use a completed result from another run directly when it belongs to the same
authoritative store. To retrieve a saved result, use
`pipeline.get_step(step_number, pipeline_run_id=...)`, then its `.output(role)`.
Lookup is read-only and does not resume or recover the source run. Different-store
inputs require an explicit import. A pending source owned by another manager is
not automatically waited on: wait on its future before submitting the consumer.

`run` waits for a `StepResult`. `submit` freezes the request and returns a
`StepFuture`; `.result()` obtains its result while its output references can already
be wired downstream. Check terminal status and accepted outputs rather than
assuming a returned future or process exit means success.
For curator roles discovered during execution, use `future.result().output(role)`;
the future itself exposes only the roles declared at submission.

Raw file paths enter through ingestion. Registration captures full managed bytes
before curator admission. Creators consume artifact inputs or upstream references,
not unregistered paths. For cross-store ingestion, use the supported source
storage/files-root settings; do not infer remote credentials or alter identifiers
behind semantic references.

An absent boundary differs from an explicitly empty input list. Empty required
work can skip and stop later pipeline work; validate roles/types before relying on
that behavior. An optional empty output is a valid result until a consumer selects
it. Preserve ordered duplicates unless the selected curator deliberately deduplicates.

## Apply execution controls at the boundary

Choose `step_runner`, `runner_resources`, grouping, `batch_strategy`, cache policy,
and failure policy on the outer submission or its documented defaults. In
particular, `artifacts_per_unit` partitions scientific inputs;
`units_per_worker` packs those units in order. Packing does not create child
persistence boundaries or change accepted scientific membership.

Operations and composites can declare their own resource, batch, and grouping
defaults. Supplied resource/batch fields override only those fields; omitted
fields retain the definition defaults. Use the same effective resources for
preparation when the runner's prepared profile depends on them. Nested composites
do not reapply their outer defaults or allocate additional workers.

Do not put runner, cache, retry, or persistence settings on `ctx.call` inside a
composite. Keep authored command environments on the child operations. Native,
Docker, and Apptainer are runner launch choices over the same canonical source.

Use `skip_cache=True` for an intentional recomputation. Observed software and
ineligible operations still execute even when a cache policy permits reuse.
Cancellation is a request: unresolved worker ownership must remain visible and
must not be interpreted as accepted output or safe scratch removal.

## Resume and verify

Use `create` for a new run, including rerunning a script against existing content.
Use `resume` with an explicit run ID when appending to a particular run. Resume
restores verified current results and rejects unresolved historical ownership;
selected recovery does not invent success or abandon unknown work. Supply a
configured external/default runner again when the recorded configuration requires
it. Recorded names alone do not authorize constructing providers or importing apps.

Pass trusted installed initializer modules through `application_modules` for
application types. They are needed in fresh workers and relevant inspection,
repair, and replay entry points, not only in the driver's imports.

Validate a small representative path before scaling: expected status, output
roles/cardinality, required lineage, asynchronous wiring, and closure. Use public
inspection readers for accepted results. Distinguish local/source tests from
installed or remote provider qualification, and report external dependencies that
were not exercised. For temporary-only creator experimentation, use
`ExecutionSession.run` instead of fabricating a store or pipeline receipt.
