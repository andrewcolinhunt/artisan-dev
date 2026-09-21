# Configure Execution

Control worker concurrency, batching, resources, caching, and failure handling.
Start with local execution; use an optional runner for cluster dispatch and a
[tool endpoint](deploying-tool-endpoints.md) for remote execute phases.

**Prerequisites:** [Build a Pipeline](building-a-pipeline.md). See
[Execution Flow](../concepts/execution-flow.md) for the worker lifecycle.

## Run a local step with a worker limit

This example uses built-in operations and needs no cluster or cloud account:

```python
from artisan.operations.examples import DataGenerator, DataTransformer
from artisan.orchestration import PipelineManager, StepStatus
from artisan.visualization import inspect_step

pipeline = PipelineManager.create(
    name="local_configuration",
    delta_root="runs/delta",
    staging_root="runs/staging",
)
source = pipeline.run(DataGenerator, params={"count": 4, "seed": 7})
transformed = pipeline.run(
    DataTransformer,
    inputs={"dataset": source.output("datasets")},
    params={"variants": 1, "seed": 7},
    batch_strategy={"artifacts_per_unit": 2, "max_workers": 2},
)
assert transformed.status is StepStatus.SUCCEEDED
outputs = inspect_step(
    "runs/delta",
    transformed.step_number,
    pipeline_run_id=pipeline.config.pipeline_run_id,
)
assert outputs.height == 4
pipeline.finalize()
```

The transform has two execution units with two inputs each. `max_workers` caps
concurrent worker tasks. Successful-item counts describe processed inputs (or
source executions), so inspect accepted outputs when verifying artifact counts.

## Choose a step runner

The native local runner uses a process pool. Set `batch_strategy.max_workers`
per step to limit it. To use SLURM, install and configure the optional
`artisan-submitit` package on a cluster:

```python
from artisan_submitit import SlurmRunner

pipeline.run(
    DataTransformer,
    inputs={"dataset": source.output("datasets")},
    step_runner=SlurmRunner(),
    runner_resources={
        "cpus": 4,
        "memory_gb": 16,
        "time_limit": "01:00:00",
        "extra": {"slurm_partition": "cpu"},
    },
)
```

Pass `default_step_runner=SlurmRunner()` to `PipelineManager.create` to select
that provider for the pipeline. Use `SlurmIntraRunner()` inside an existing
`salloc` or `sbatch` allocation when the provider should launch with `srun`.
Provider installation and scheduler options belong to the provider's own docs.

### Allocate resources at the right layer

`runner_resources` describes the lifecycle worker. A cluster runner translates
it to a scheduler allocation. The local runner uses GPU requests to assign
devices and constrain concurrency; it does not reserve CPU or memory like a
cluster scheduler. Runner-specific `extra` settings need their matching runner.

`compute_provider` selects where execute runs. Ordinary Python function
operations run in the lifecycle worker. Command operations can route execute to
a deployed Modal endpoint while preprocess and postprocess stay in the worker:

```python
pipeline.run(MyCommandOp, inputs=..., compute_provider="modal")
```

The operation must declare a Modal configuration and have a compatible endpoint
deployed first. `ComputeResources` declares deployment hardware, while
`ModalComputeConfig` supplies the image, runtime, and transport settings.
Changing GPU, CPU, or memory for an existing endpoint requires redeployment;
a per-step `compute_resources` override does not resize the deployed worker.
See [Deploy Tool Endpoints](deploying-tool-endpoints.md).

## Control batching

Use two settings to divide work:

- `artifacts_per_unit` groups inputs into a logical execution unit, which shares
  preprocess and postprocess work.
- `units_per_worker` groups units into a worker task, reducing task submission
  overhead. A process-pool worker can handle more than one task over its life.

For example, 100 inputs with `artifacts_per_unit=10` form 10 units. With
`units_per_worker=2`, those become five worker tasks:

```python
pipeline.run(
    DataTransformer,
    inputs={"dataset": source.output("datasets")},
    batch_strategy={
        "artifacts_per_unit": 10,
        "units_per_worker": 2,
        "max_workers": 2,
    },
)
```

A unit is not necessarily one execute call. By default, creators dispatch
execute once per input and collect the results for postprocess. Their preprocess
output uses [`PerArtifact`](writing-creator-operations.md#implement-preprocess)
for values that the framework should slice for each call; raw lists are shared.
An operation that intentionally processes a batch in one call sets
`per_artifact_dispatch=False`. That call receives the full prepared values,
with `PerArtifact` wrappers unwrapped into lists.

Increase batching for short tasks after measuring overhead. Larger units also
increase the amount of work affected by a unit failure. See
[Execution Flow](../concepts/execution-flow.md) and the
[batching tutorial](../tutorials/04-batching/01-batching-and-performance.ipynb).

## Set and override operation defaults

Operations can declare resources and batching on the class. At a call site,
pass only the values you want to change:

```python
from artisan.schemas import RunnerResources, ComputeResources

pipeline.run(MyOp, inputs=..., runner_resources={"memory_gb": 32})
```

The operation supplies resource, batching, tool, environment, and compute
settings. The pipeline supplies the default step runner, failure policy, and
cache policy. A composite can supply defaults for its children; an explicit
child setting takes precedence. See
[composite overrides](writing-composite-operations.md#forward-execution-overrides).

### Patch configuration without replacing defaults

All model-valued step options use the same patch behavior:
`runner_resources`, `batch_strategy`, `environment`, `tool`,
`compute_provider`, and `compute_resources`.

You can pass either a dict or the corresponding typed model. Both forms use
the fields you supplied as the patch, including values that equal the model's
schema default:

```python
# These are equivalent, even though RunnerResources.cpus defaults to 1.
pipeline.run(operation=MyOp, inputs=..., runner_resources={"cpus": 1})
pipeline.run(
    operation=MyOp,
    inputs=...,
    runner_resources=RunnerResources(cpus=1),
)
```

Fields you omit retain the operation's declared values. Explicit `None` inside
a patch resets an optional field instead of falling back to the operation
default:

```python
# Both clear an operation-level ComputeResources(gpu="A100") default.
pipeline.run(operation=MyOp, inputs=..., compute_resources={"gpu": None})
pipeline.run(
    operation=MyOp,
    inputs=...,
    compute_resources=ComputeResources(gpu=None),
)
```

Nested non-empty mappings merge recursively, so updating one environment
variable preserves its siblings. Scalars, lists, `None`, and empty mappings
replace the inherited value:

```python
# Preserve every existing variable except MODE.
environment = {
    "active": "docker",
    "docker": {"env": {"MODE": "production"}},
}

# Clear the inherited env mapping.
environment = {"active": "docker", "docker": {"env": {}}}
```

An empty root patch such as `runner_resources={}` supplies no fields and is a
no-op. Top-level `None` also means no override; use `None` inside a patch to
reset an optional field.

---

## Select an external tool environment

Command operations declare their binary with `ToolSpec` and available runtimes
with `Environments`. Select a declared runtime by name, or patch its settings:

```python
pipeline.run(MyCommandOp, inputs=..., environment="local")
pipeline.run(
    MyCommandOp,
    inputs=...,
    tool={"executable": "tool-v2"},
    environment={
        "active": "apptainer",
        "apptainer": {"image": "/tools/tool-v2.sif"},
    },
)
```

Include `active` when configuring a provider in a dict. A string selects a
provider already configured on the operation. Invalid or unconfigured targets
fail before cache lookup.

Container mounts use tuples, such as `("/data", "/data", "ro")` for a read-only
mount or `("/scratch", "/scratch")` for a writable mount. They are not
colon-delimited strings. Runtime specs also accept environment variables.
See [Write Creator Operations](writing-creator-operations.md#command-operations-external-tools)
for a complete command wrapper, and [Python API](../reference/python-api.md)
for supported configuration types and their source contracts.

## Set failure policy

The default `FailurePolicy.CONTINUE` allows independent units to finish,
retains their successful outputs, and records failures. Use `FAIL_FAST` when
you want the step to stop after a failure:

```python
from artisan.schemas import FailurePolicy

step = pipeline.run(MyOp, inputs=..., failure_policy=FailurePolicy.FAIL_FAST)
```

Fail-fast returns a failed step result after preserving failure evidence and
already completed sibling work. That failed result exposes no downstream output
roles. Check the returned status; ordinary execution failures do not become an
exception merely because you selected fail-fast. See
[Error Handling](../concepts/error-handling.md) for durable outcomes and
cancellation behavior.

## Set cache policy

Cache policy controls which previous terminal step qualifies as a whole-step
cache hit. Set a pipeline default, then override individual steps as needed:

```python
from artisan.schemas import CachePolicy

pipeline = PipelineManager.create(..., cache_policy=CachePolicy.ALL_SUCCEEDED)
pipeline.run(
    operation=MyOp,
    inputs=...,
    cache_policy=CachePolicy.STEP_COMPLETED,
)
```

| Policy | Behavior |
|--------|----------|
| `CachePolicy.ALL_SUCCEEDED` (default) | Cache hit only for a `succeeded` attempt |
| `CachePolicy.STEP_COMPLETED` | Cache hit for a `succeeded` or `partial` attempt |

Failed, cancelled, and skipped attempts never qualify under either policy. The
difference is whether a `partial` attempt counts as a hit.

Use `STEP_COMPLETED` when you want to skip re-running a step that mostly
succeeded, even if a few artifacts failed. `submit()` accepts the same argument.
Pass a `CachePolicy` member; strings such as `"step_completed"` are rejected.
Omitting the argument or passing `None` inherits the default.

### Inherit policy through composites

`run_composite()` and `submit_composite()` set defaults for their children:

```python
pipeline.run_composite(MyComposite, inputs=..., cache_policy=CachePolicy.STEP_COMPLETED)

# Inside compose(), require complete success for this child:
ctx.run(MyOp, inputs=..., cache_policy=CachePolicy.ALL_SUCCEEDED)

# Set a different default for an entire nested subtree:
ctx.run(InnerComposite, inputs=..., cache_policy=CachePolicy.ALL_SUCCEEDED)
```

A leaf uses its explicit policy, then the nearest explicit enclosing composite
policy, then the pipeline default. `None` continues that inheritance through
nested composites. A deeper child can override either enum value again.

### Retry unsuccessful execution units

Whole-step caching and individual execution caching are separate. Suppose a
step had three independent units, with two successes and one failure:

- `STEP_COMPLETED` accepts the partial step without executing any units. The
  result remains `partial`, including its failure count and error; downstream
  steps receive its successful outputs.
- `ALL_SUCCEEDED` rejects that partial whole-step hit, reuses the two successful
  units, and retries the failed unit. Neither policy reuses a failed unit as a
  successful execution.

The current consumer's policy selects the newest eligible prior step. The
source's policy does not restrict reuse. Changing policy preserves artifact
IDs, step spec IDs, and execution spec IDs.

### Bypass caching and resume

Step `skip_cache=True` or pipeline `skip_cache=True` bypasses both cache layers.
A child's explicit `skip_cache=False` can replace a composite default of
`True`, but cannot disable pipeline-wide bypass. An operation declaring
`cacheable=False` also bypasses both layers regardless of policy or skip flags.
Diagnostic replay attempts are excluded from ordinary cache candidates under
both policies.

Each new attempt records its resolved policy, including cache hits, preparation
failures, skips, and cancellations. `PipelineManager.resume(cache_policy=...)`
sets the default for subsequent submissions. Previously accepted steps retain
their outcomes and recorded policies, including partial cache hits. Omitting
the resume argument uses `ALL_SUCCEEDED` for new steps; it does not recover a
pipeline default from prior child policies.

---

## Submit work while keeping the caller available

`run()` waits for a step result. `submit()` returns a future after synchronous
input preparation and any wait for referenced predecessors. It lets the caller
continue while execution runs; it does not promise immediate return or parallel
execution of independent steps. The current manager serializes step execution.

```python
future = pipeline.submit(MyOp, inputs=...)
# Perform other caller-side work here.
step = future.result()
pipeline.finalize()
```

## Keep working files for diagnosis

Choose an explicit local working root and preserve it when investigating an
operation:

```python
pipeline = PipelineManager.create(
    name="debug",
    delta_root="runs/delta",
    staging_root="runs/staging",
    working_root="runs/working",
    preserve_working=True,
)
```

Without an explicit working root, sandboxes use the system temporary directory.
Use `preserve_staging=True` separately to keep staged Parquet files after they
commit. Uncommitted staging is retained even without that flag, including after
cancellation. See [Staging preservation](../concepts/storage-and-delta-lake.md#staging-preservation).

### Recovering from crashes

Confirm that no other orchestrator or repair process is writing the store.
Then rerun your pipeline with `PipelineManager.create()` and the same roots.
By default, startup recovers validated finished work before checking caches.
Use `recover_staging=False` when you need to inspect the old evidence without
startup recovery; add `preserve_staging=True` to retain files after recovery.

For an explicit report that includes finished, uncommitted executions:

```bash
artisan store repair --delta-root runs/delta --staging-root runs/staging \
  --recover-staging
```

Report mode does not change either root. Inspect affected execution and plan IDs.
To apply recovery while keeping the staging evidence:

```bash
artisan store repair --delta-root runs/delta --staging-root runs/staging \
  --recover-staging --preserve-staging --apply
```

Supply `--files-root` if the store uses a separately configured managed external
files root. Without `--recover-staging`, repair only handles recorded plans.
Incomplete or ineligible evidence can remain in a report after successful
recovery; the CLI reports unresolved evidence with a nonzero exit status.
Corrupt or conflicting evidence requires diagnosis and blocks pipeline startup.

Use `--abandon ID --reason ...` only after deciding that a particular
logical commit should never finish. This retains its evidence and excludes it
from recovery; abandonment and staging recovery are separate commands.
See [Crash recovery](../concepts/storage-and-delta-lake.md#crash-recovery) for
eligibility, history, and restart-versus-resume behavior.

## Inspect commands from an execution

Use `inspect_commands(delta_root, execution_run_id)` to read the arguments,
environment wrapper, outcome, and evidence completeness recorded for a unit.
The [execution debugging guide](debugging-executions.md#inspect-commands-from-an-execution)
covers command evidence, credentials, and diagnostic replay together.

## Related guides

- [Deploy Tool Endpoints](deploying-tool-endpoints.md) — deployment, credentials,
  and object-store transport.
- [Compute Routing](../tutorials/07-compute-backends/01-compute-routing.ipynb) —
  a runnable introduction to runner and compute choices.
- [Python API](../reference/python-api.md) — configuration models and exact
  method contracts.
