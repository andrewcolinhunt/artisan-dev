# Configure Execution

How to control where operations run, what resources they get, and how work
is batched — from local development through production SLURM.

**Prerequisites:** [Operations Model](../concepts/operations-model.md),
[Building a Pipeline](building-a-pipeline.md)

---

## Minimal working example

A pipeline running one step locally and one on SLURM with GPU resources:

```python
from artisan.orchestration import Backend, PipelineManager
from myops import PreprocessOp, InferenceOp

pipeline = PipelineManager.create(
    name="example",
    delta_root="runs/delta",
    staging_root="runs/staging",
)
output = pipeline.output

pipeline.run(operation=PreprocessOp, name="preprocess", params={"count": 100})

pipeline.run(
    operation=InferenceOp,
    name="inference",
    inputs={"dataset": output("preprocess", "dataset")},
    backend=Backend.SLURM,
    resources={"gpus": 1, "memory_gb": 32, "extra": {"partition": "gpu"}},
    execution={"artifacts_per_unit": 1},
)
```

The rest of this guide breaks down each knob.

---

## Step 1: Choose a compute backend

Every step runs on a compute backend. Set it at the step level:

```python
from artisan.orchestration import Backend

pipeline.run(operation=MyOp, inputs=..., backend=Backend.SLURM)
```

| Backend | How it runs | When to use |
|---------|-------------|-------------|
| `Backend.LOCAL` (default) | Thread pool on your machine | Development, testing, lightweight ops |
| `Backend.SLURM` | SLURM job array on cluster | Production, GPU work, HPC |

For `LOCAL`, you can cap the number of concurrent workers per step:

```python
pipeline.run(operation=MyOp, inputs=..., execution={"max_workers": 8})
```

---

## Step 2: Configure resources

Pass a `resources` dict to override resource allocation for a step:

```python
pipeline.run(
    operation=MyOp,
    inputs=...,
    backend=Backend.SLURM,
    resources={
        "gpus": 1,
        "memory_gb": 32,
        "time_limit": "04:00:00",
        "cpus": 4,
        "extra": {"partition": "gpu"},
    },
)
```

### ResourceConfig fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `cpus` | `int` | `1` | CPU cores per task |
| `memory_gb` | `int` | `4` | Memory in GB |
| `gpus` | `int` | `0` | Number of GPUs requested |
| `time_limit` | `str` | `"01:00:00"` | Wall-clock time limit (HH:MM:SS) |
| `extra` | `dict` | `{}` | Backend-specific settings (e.g., `{"partition": "gpu"}`) |

`ResourceConfig` is portable across backends — each backend translates these
fields to its native format. Use `extra` for backend-specific settings like
SLURM partition or account.

Step-level `resources` merge with operation defaults — you only need to specify
the fields you want to override.

---

## Step 3: Control batching

Batching determines how many artifacts each worker processes. This is the main
lever for tuning throughput.

```python
pipeline.run(
    operation=MyOp,
    inputs=...,
    execution={"artifacts_per_unit": 10},
)
```

With 100 input artifacts and `artifacts_per_unit=10`, the framework creates
10 execution units, each processing a batch of 10 artifacts.

### Two-level batching

Batching happens at two levels:

```
100 artifacts
    │
    │  artifacts_per_unit = 10
    ▼
10 execution units (logical work packages)
    │
    │  units_per_worker = 2
    ▼
5 SLURM jobs (each runs 2 units sequentially)
```

**Level 1 — `artifacts_per_unit`**: How many artifacts each execution unit
processes. Set this based on your operation's workload: 1 for GPU inference
(one structure per job), 50–100 for fast metrics calculations.

**Level 2 — `units_per_worker`**: How many execution units a single SLURM job
runs sequentially. Use this to amortize job startup overhead without changing
your operation's batch logic.

### ExecutionConfig fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `artifacts_per_unit` | `int` | `1` | Artifacts per execution unit |
| `units_per_worker` | `int` | `1` | Execution units per SLURM job |
| `max_workers` | `int \| None` | `None` | Cap on concurrent workers |
| `max_artifacts_per_unit` | `int \| None` | `None` | Cap on step-level `artifacts_per_unit` overrides |
| `estimated_seconds` | `float \| None` | `None` | Runtime hint (informational) |
| `job_name` | `str \| None` | `None` | Custom SLURM job name (defaults to operation name) |

---

## Step 4: Set operation-level defaults

Operations can declare their own default resources and execution config so you
don't repeat the same overrides at every step:

```python
from artisan.operations.base import OperationDefinition
from artisan.schemas.operation_config.resource_config import ResourceConfig
from artisan.schemas.execution.execution_config import ExecutionConfig

class GpuInference(OperationDefinition):
    name = "gpu_inference"

    resources: ResourceConfig = ResourceConfig(
        gpus=1,
        memory_gb=32,
        time_limit="02:00:00",
        extra={"partition": "gpu"},
    )

    execution: ExecutionConfig = ExecutionConfig(
        artifacts_per_unit=1,
        estimated_seconds=600.0,
    )

    # ... lifecycle methods ...
```

Step-level overrides merge on top of these defaults. For example, to give a
specific step more memory without changing other settings:

```python
pipeline.run(operation=GpuInference, inputs=..., resources={"memory_gb": 64})
# gpus, time_limit, extra keep their operation defaults
```

### Override precedence

```
Pipeline defaults (PipelineManager.create)
    └── Operation defaults (class fields)
            └── Step overrides (pipeline.run kwargs)   ← wins
```

---

## Step 5: Configure external tools and environments

Operations that wrap external tools declare two things: a `ToolSpec` (the
binary/script to invoke) and an `Environments` configuration (the runtime
that wraps the command):

```python
from pathlib import Path
from artisan.operations.base import OperationDefinition
from artisan.schemas.operation_config.tool_spec import ToolSpec
from artisan.schemas.operation_config.environments import Environments
from artisan.schemas.operation_config.environment_spec import ApptainerEnvironmentSpec

class ToolAOp(OperationDefinition):
    name = "tool_a"

    tool: ToolSpec = ToolSpec(
        executable=Path("run_tool_a.sh"),
        interpreter="bash",
    )

    environments: Environments = Environments(
        active="apptainer",
        apptainer=ApptainerEnvironmentSpec(
            image=Path("/tools/tool_a.sif"),
            gpu=True,
            binds=[
                (Path("/data/weights"), Path("/weights")),
                (Path("/scratch"), Path("/scratch")),
            ],
        ),
    )

    # ... lifecycle methods ...
```

Override tool or environment settings at the step level:

```python
pipeline.run(
    operation=ToolAOp,
    inputs=...,
    tool={"executable": "run_tool_a_v2.sh"},
    environment={"apptainer": {"image": "/tools/tool_a_v2.sif"}},
)
```

The `binds` field takes a list of `(host_path, container_path)` tuples — not
colon-delimited strings.

### Environment spec types

| Spec | Use case |
|------|----------|
| `ApptainerEnvironmentSpec` | Apptainer/Singularity containers (HPC) |
| `DockerEnvironmentSpec` | Docker containers |
| `LocalEnvironmentSpec` | Local execution, optional virtualenv |
| `PixiEnvironmentSpec` | Pixi-managed environments |

All specs share a base `EnvironmentSpec` with an `env` dict for extra
environment variables. `ToolSpec` declares the `executable`, optional
`interpreter`, and optional `subcommand`.

---

## Step 6: Set failure policy

Control what happens when some artifacts fail within a step:

```python
from artisan.schemas.enums import FailurePolicy

# Pipeline-wide default
pipeline = PipelineManager.create(..., failure_policy=FailurePolicy.CONTINUE)

# Step-level override
pipeline.run(operation=MyOp, inputs=..., failure_policy=FailurePolicy.FAIL_FAST)
```

| Policy | Behavior |
|--------|----------|
| `FailurePolicy.CONTINUE` (default) | Commit successful artifacts, record failures, continue pipeline |
| `FailurePolicy.FAIL_FAST` | Stop the step immediately on any failure |

`CONTINUE` is the default because in large runs (thousands of artifacts), a
single malformed input should not discard thousands of successful results.
Failures are always recorded for diagnosis.

---

## Common patterns

### Development: inspectable sandboxes

During development, you can make the working directory visible and persistent:

```python
pipeline = PipelineManager.create(
    ...,
    working_root="runs/working",
    preserve_working=True,
)
```

This writes sandboxes to `runs/working/` instead of `$TMPDIR` and keeps them
after execution completes, so you can inspect input materialization and output
files.

For production, omit `working_root` — the default uses `$TMPDIR` (typically
node-local SSD on SLURM clusters), which avoids shared filesystem contention.

### Debugging: preserve staging files

```python
pipeline = PipelineManager.create(..., preserve_staging=True)
```

Keeps the raw Parquet files workers produce before commit. Useful for diagnosing
staging or commit issues.

### Tuning SLURM throughput

For operations with fast per-artifact execution (< 1 second), increase
`artifacts_per_unit` to reduce job overhead:

```python
pipeline.run(
    operation=FastMetrics,
    inputs=...,
    backend=Backend.SLURM,
    execution={"artifacts_per_unit": 100, "units_per_worker": 5},
)
```

For GPU operations, keep `artifacts_per_unit=1` and let SLURM handle
parallelism via job arrays.

### Custom SLURM parameters

Use `extra` for backend-specific parameters not covered by `ResourceConfig`:

```python
pipeline.run(
    operation=MyOp,
    inputs=...,
    resources={
        "extra": {
            "partition": "gpu",
            "constraint": "a100",
            "account": "my_allocation",
            "exclude": "node[001-003]",
        }
    },
)
```

---

## Common pitfalls

| Problem | Cause | Fix |
|---------|-------|-----|
| SLURM jobs OOM-killed | Default `memory_gb=4` too low | Set `resources={"memory_gb": 32}` or add to operation defaults |
| Thousands of tiny SLURM jobs | `artifacts_per_unit=1` on a fast operation | Increase `artifacts_per_unit` to batch work |
| `binds` validation error | Using `"/host:/container"` strings | Use tuple pairs: `[("/host", "/container")]` |
| Step ignores `resources` | Forgot `backend=Backend.SLURM` | Resources only apply to SLURM steps |
| Workers contend on shared filesystem | Default `working_root` on NFS | Omit `working_root` — default uses `$TMPDIR` (node-local) |

---

## Verify

Confirm your configuration works by running a small test:

```python
step = pipeline.run(operation=MyOp, inputs=..., backend=Backend.LOCAL)
assert step.success
print(f"Processed {step.succeeded_count} artifacts")
```

Then switch to `Backend.SLURM` for production. Check SLURM job logs if
failures occur — the job name format is `s{step_number}_{operation_name}`.

---

## Cross-references

- [Execution Flow](../concepts/execution-flow.md) — Dispatch, execute, commit lifecycle
- [SLURM Execution Tutorial](../tutorials/getting-started/04-slurm-execution.ipynb) — Interactive SLURM walkthrough
- [Writing Creator Operations](writing-creator-operations.md) — Declaring operation-level defaults
