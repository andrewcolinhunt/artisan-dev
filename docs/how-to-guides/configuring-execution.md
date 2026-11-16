# Configure Execution

Choose a runner for worker placement, declare tool software on operations, and
set batching and persistence policies on pipeline submissions. A submitted
composite uses the same controls as a single operation.

**Prerequisites:** [Build a Pipeline](building-a-pipeline.md).

## Run a local step with a worker limit

This complete example uses built-in operations and a temporary store:

```python
from pathlib import Path
from tempfile import TemporaryDirectory

from artisan.operations.examples import DataGenerator, DataTransformer
from artisan.orchestration import PipelineManager, StepStatus
from artisan.visualization import inspect_step

with TemporaryDirectory() as directory:
    root = Path(directory)
    with PipelineManager.create(
        name="local_configuration",
        delta_root=str(root / "delta"),
        staging_root=str(root / "staging"),
    ) as pipeline:
        source = pipeline.run(DataGenerator, params={"count": 4, "seed": 7})
        transformed = pipeline.run(
            DataTransformer,
            inputs={"dataset": source.output("datasets")},
            params={"variants": 1, "seed": 7},
            batch_strategy={"artifacts_per_unit": 2, "max_workers": 2},
        )
        assert transformed.status is StepStatus.SUCCEEDED, transformed.error
        outputs = inspect_step(
            str(root / "delta"),
            transformed.step_number,
            pipeline_run_id=pipeline.config.pipeline_run_id,
        )
        assert outputs.height == 4
```

The transform has two execution units with two inputs each. Each unit has its
own persistence and cache result. `max_workers` limits concurrent workers. Use
persistent paths when you want to keep the results after the example ends.

## Load application definitions in every process

Put custom definitions in an installed application package. Pipeline workers
require importable code covered by their installed software; classes defined in
`__main__` or only in a notebook are not portable worker definitions.

If your package registers artifact types, pass its trusted registration module
through `PipelineManager.create(..., application_modules=("my_application",))`.
`ARTISAN_LOAD_MODULES` also selects trusted application modules. These choices
travel to the workers. Store readers need the same registrations:

```python
from artisan.registry import bootstrap_applications
from artisan.storage import ArtifactStore

bootstrap_applications(("my_application",))  # Your installed package.
store = ArtifactStore("runs/delta")
```

CLI store readers accept repeatable `--load-module` arguments. Resume and replay
require trusted configuration again: stored module names cannot authorize
imports. Missing registrations are configuration errors; install and load the
required definitions before trying to repair a store.

## Choose a step runner

The built-in default is `LocalRunner()`, which uses the current Python
installation. You can pass a configured runner instance to
`PipelineManager.create(..., default_step_runner=runner)` or override placement
with `pipeline.run(..., step_runner=runner)`. Only `"local"` is a built-in runner
name; provider instances carry their own configuration.

A runner controls the whole worker lifecycle, including preprocess and
postprocess. Modal and Slurm providers use the same submitted operation or
composite contract. Install and configure the matching provider package before
using its runner. Prepare worker and tool software explicitly through the runner;
ordinary submission does not install environments or deploy services.

### Allocate resources at the right layer

`runner_resources` describes a worker allocation. A scheduler provider translates
it to its resource request. The local runner validates supported requests and
GPU availability but does not reserve CPU or memory. Provider-specific `extra`
options need their matching runner. For an existing operation `MyOp`, inputs,
and configured runner, the call shape is:

```python
step = pipeline.run(
    MyOp,
    inputs=inputs,
    step_runner=runner,
    runner_resources={"cpus": 4, "memory_gb": 16, "time_limit": "01:00:00"},
)
```

A tool's `environment` controls its command process. It does not select worker
placement or remote hardware. See [Prepare Execute Environments](execute-environments.md).

## Control batching

`artifacts_per_unit` partitions the inputs. `units_per_worker` packs compatible
units into a worker that processes them sequentially. For 100 inputs, a unit size
of 10 and a worker pack size of 2 gives 10 units in five worker assignments:

```python
step = pipeline.run(
    MyOp,
    inputs=inputs,
    batch_strategy={
        "artifacts_per_unit": 10,
        "units_per_worker": 2,
        "max_workers": 2,
    },
)
```

This fragment continues with your operation and inputs. Increasing worker packing
reduces dispatch overhead without merging the units' cache or persistence results.
Larger units increase the amount of work affected by a unit failure.

A unit can contain several execute calls. Creators normally dispatch once per
input, slicing values marked with [`PerArtifact`](writing-creator-operations.md#creator-preprocess)
and sharing unmarked values. Set `per_artifact_dispatch=False` on a creator
that intentionally executes on its full prepared batch.

## Set and override operation defaults

Operations can declare `runner_resources`, `batch_strategy`, and `tool` defaults.
Submission overrides use the fields you supply, as mappings or typed models.
Omitted fields keep their defaults. `None` as the whole option means no override;
an explicit `None` inside a patch resets a nullable field. Resource `extra`
mappings are supplied as a whole, so include the entries you want to keep.

Composites take placement, resources, batching, and policies at their outer
submission. Their `ctx.call` declarations contain wiring and scientific parameters.
See [composite submission](writing-composite-operations.md#forward-execution-overrides).

## Select an external tool environment

Declare `environment = Environment.from_pixi(...)` on a command operation.
Choose Pixi, Docker, or Apptainer through the configured runner's `tool_backend`.
The [environment guide](execute-environments.md) covers preparation and launchers.
Operations without a separate declaration intentionally inherit worker software.

### Keep managed commands in the foreground

Use Artisan's `run_command` for commands that belong to an execution. The worker
retains ownership of managed descendants and waits for their cleanup before
accepting success. Join operation threads and nested commands before returning
from a phase. Direct subprocesses or external services outside the managed
contract do not become owned merely because an operation started them.

Cancellation can be confirmed, rejected because work already finished, or
unknown when closure cannot be established. A provider acknowledgement alone
is not proof that computation stopped. Uncertain work retains its evidence and
prevents later science from using that unresolved worker. Independently completed
units retain their results.

## Set failure policy

`FailurePolicy.CONTINUE` allows independent units to finish and keeps their
successful results. `FailurePolicy.FAIL_FAST` stops further work after a failure,
while retaining failure evidence and already accepted sibling results. Check the
returned `StepResult.status`; ordinary operation failure is recorded as a failed
result rather than becoming a caller exception solely because of this policy.

## Set cache policy

Persistent cache reuse requires a prepared worker environment. The default
`LocalRunner()` observes the current interpreter and runs without persistent
reuse; see [environment preparation](execute-environments.md).

Whole-step reuse and individual successful-unit reuse are separate:

| Policy | Eligible whole-step result |
| --- | --- |
| `CachePolicy.ALL_SUCCEEDED` | A succeeded attempt |
| `CachePolicy.STEP_COMPLETED` | A succeeded or partial attempt |

Use the enum from `artisan.schemas`, at pipeline creation or per submission.
Failed, cancelled, skipped, and diagnostic replay attempts are not whole-step
cache candidates. A partial whole-step hit keeps its partial status and exposes
its accepted outputs.

### Inherit policy through composites

A composite has the policy of its outer submission. Children have no separate
cache policy or checkpoint. Choose separate pipeline steps when you want to reuse
an intermediate independently.

### Retry unsuccessful execution units

With two successful units and one failure, `STEP_COMPLETED` can reuse the partial
step. `ALL_SUCCEEDED` instead reuses eligible successful units and runs the missing
unit again. Neither policy treats a failed unit as successful.

### Bypass caching and resume

Use `skip_cache=True` to force execution. A definition with `cacheable=False`
disables reuse for its submitted boundary, including an outer composite containing
it. Software identity, scientific parameters, input occurrences, and the compiled
composition determine eligibility; worker packing does not change scientific
identity. Resume retains already accepted outcomes and uses the supplied policy
for new submissions.

## Submit work while keeping the caller available

`pipeline.submit` freezes the call and records a pending step, then returns a
`StepFuture`. Its `.output(role)` can wire a downstream submission. Call `.result()`
for its `StepResult`, or use `pipeline.run` to submit and wait in one call. Use
the manager as a context manager so owned workers are closed on exit.

## Keep working files for diagnosis

Set `working_root` and `preserve_working=True` when creating a pipeline to retain
local scratch. Use `preserve_staging=True` separately for staged results. These
settings preserve evidence; they do not keep processes running after closure.

### Recovering from crashes

Resume the selected run to recover its previously claimed results. Creating a
new run does not scan orphan staging and claim arbitrary old output. Recovery
can finish a result whose acceptance claim was already recorded; it cannot
turn an unclaimed late result into success after cancellation.

With other writers stopped, inspect the store before applying repair:

```bash
artisan store repair --delta-root runs/delta --staging-root runs/staging
artisan store repair --delta-root runs/delta --staging-root runs/staging --apply
```

Supply `--files-root` for separately configured external-file storage and
`--storage-config` for an explicit storage configuration file. Report mode is
read-only. Apply mode finishes eligible claimed publications and reports unresolved
evidence; it does not invent missing claims or relabel an older store format.
