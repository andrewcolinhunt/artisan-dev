# Pipeline Configuration

`PipelineConfig` holds a run's identity, storage roots, trusted application
modules, and execution defaults. Workers receive the settings they need through
`RuntimeEnvironment`. An operation declares its compute provider separately;
a step can override that choice for one invocation.

## What the configuration captures

The configuration separates run-wide settings from step-specific choices:

| Concern | Run-wide configuration |
| --- | --- |
| Identity | A human-readable name and a `pipeline_run_id` that identifies the run. |
| Storage | Delta, staging, and managed external-file roots, plus filesystem and Delta storage options. Worker sandboxes always use a local working root. |
| Application definitions | Trusted installed modules that register custom operations and artifact types. Workers load the same resolved selection. |
| Execution defaults | The default lifecycle runner, failure policy, and cache policy. Steps and composites can override these defaults. |
| Recovery and debugging | Whether startup recovers finished staging, whether committed staging and sandboxes are retained, and whether caches are bypassed. |

Compute providers belong to operation configuration and explicit step overrides.
A local lifecycle worker can therefore send its execute phase to Modal without
changing the rest of the pipeline. See
[Configure Execution](../how-to-guides/configuring-execution.md) for runner,
compute, and policy choices.

Cache policy changes which prior outcomes are accepted, not computation
identity. Resume retains accepted steps and applies its configured policy to
new submissions. Recovery separately controls whether startup commits eligible
finished work; see [Staging preservation and recovery](storage-and-delta-lake.md#staging-preservation).

For current fields, types, and defaults, use the `PipelineConfig` entry in
[Python API](../reference/python-api.md).

## Why the configuration is frozen

Once constructed, configuration fields cannot be reassigned. This prevents
changing a run's roots or defaults while workers are using copies of them.
Build a new configuration and manager when you need different run-wide settings.

The frozen model does not freeze external state or recursively protect every
nested dictionary. Treat nested storage options as read-only too. Credentials,
external files, and deployed endpoints can change independently of the model.

## Load trusted application definitions

`PipelineManager.create()` and `resume()` combine `ARTISAN_LOAD_MODULES` with
explicit `application_modules`, then pass the resolved selection to workers.
Direct `PipelineConfig` construction uses only its explicit selection.

Recorded module names are audit information, not permission to import code.
Supply trusted modules again when inspecting, repairing, resuming, or replaying
an application store. Missing registration is a configuration error; it does not
mean the stored data is corrupt. See
[Load application definitions](../how-to-guides/configuring-execution.md#load-application-definitions-in-every-process).

(where-the-run-id-comes-from)=
## Where the run id comes from

`PipelineManager.create(...)` generates a fresh run ID that includes the pipeline
name. `resume()` selects an existing run. A directly constructed
`PipelineConfig` leaves `pipeline_run_id` blank unless you supply it; use a fresh
ID when creating a new run through that path.

For local storage, `files_root` defaults to a sibling `files` directory beside
`delta_root`. Cloud configurations need an explicit external-file root. Filesystem
and Delta clients have separate option dictionaries because their credential
and endpoint settings use different names. See
[Configure S3-Compatible Storage](../how-to-guides/configuring-s3.md) for an example.

## Related pages

- [Building a Pipeline](../how-to-guides/building-a-pipeline.md) — create a manager
  and submit steps.
- [First Pipeline tutorial](../tutorials/01-getting-started/01-first-pipeline.ipynb)
  — run a complete local example.
- [Storage and Delta Lake](storage-and-delta-lake.md) — how roots, staging, and
  accepted results relate.
