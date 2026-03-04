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
    resources={"partition": "gpu", "gres": "gpu:1", "mem_gb": 32},
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

For `LOCAL`, the maximum number of concurrent workers defaults to 4. Override it
at pipeline creation:

```python
pipeline = PipelineManager.create(..., max_workers_local=8)
```

Or per-operation via `execution={"max_workers": 8}` at the step level.

---

## Step 2: Configure SLURM resources

Pass a `resources` dict to override SLURM resource allocation for a step:

```python
pipeline.run(
    operation=MyOp,
    inputs=...,
    backend=Backend.SLURM,
    resources={
        "partition": "gpu",
        "gres": "gpu:1",
        "mem_gb": 32,
        "time_limit": "04:00:00",
        "cpus_per_task": 4,
    },
)
```

### ResourceConfig fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `partition` | `str` | `"cpu"` | SLURM partition |
| `gres` | `str \| None` | `None` | GPU/resource spec (e.g., `"gpu:1"`, `"gpu:a100:2"`) |
| `cpus_per_task` | `int` | `1` | CPU cores per task |
| `mem_gb` | `int` | `4` | Memory in GB |
| `time_limit` | `str` | `"01:00:00"` | Wall-clock time limit (HH:MM:SS) |
| `extra_slurm_kwargs` | `dict` | `{}` | Arbitrary SLURM parameters not covered above |

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
        partition="gpu",
        gres="gpu:1",
        mem_gb=32,
        time_limit="02:00:00",
    )

    execution: ExecutionConfig = ExecutionConfig(
        artifacts_per_unit=1,
        estimated_seconds=600.0,
    )

    # ... lifecycle methods ...
```

Step-level overrides merge on top of these defaults. For example, to give a
specific step more memory without changing the partition:

```python
pipeline.run(operation=GpuInference, inputs=..., resources={"mem_gb": 64})
# partition, gres, time_limit keep their operation defaults
```

### Override precedence

```
Pipeline defaults (PipelineManager.create)
    └── Operation defaults (class fields)
            └── Step overrides (pipeline.run kwargs)   ← wins
```

---

## Step 5: Configure containers (external tools)

Operations that wrap external tools (e.g., ToolA, ToolC) declare a
container configuration as a `ClassVar`:

```python
from typing import ClassVar
from pathlib import Path
from artisan.operations.base import OperationDefinition
from artisan.schemas.operation_config.command_spec import ApptainerCommandSpec
from artisan.utils.external_tools import ArgStyle

class ToolAOp(OperationDefinition):
    uses_external_tool: ClassVar[bool] = True
    command: ClassVar[ApptainerCommandSpec] = ApptainerCommandSpec(
        image=Path("/tools/tool_a.sif"),
        script=Path("run_tool_a.sh"),
        arg_style=ArgStyle.ARGPARSE,
        gpu=True,
        binds=[
            (Path("/data/weights"), Path("/weights")),
            (Path("/scratch"), Path("/scratch")),
        ],
    )
```

Override container settings at the step level:

```python
pipeline.run(
    operation=ToolAOp,
    inputs=...,
    command={
        "image": "/tools/tool_a_v2.sif",
        "binds": [("/data/new_weights", "/weights"), ("/scratch", "/scratch")],
    },
)
```

The `binds` field takes a list of `(host_path, container_path)` tuples — not
colon-delimited strings.

### Container spec types

| Spec | Use case |
|------|----------|
| `ApptainerCommandSpec` | Apptainer/Singularity containers (HPC) |
| `DockerCommandSpec` | Docker containers |
| `LocalCommandSpec` | Local scripts, optional virtualenv |
| `PixiCommandSpec` | Pixi-managed environments |

All specs share a base: `script` (path to entrypoint), `arg_style` (`HYDRA` or
`ARGPARSE`), and optional `subcommand` / `interpreter` fields.

---

## Step 6: Set failure policy

Control what happens when some artifacts fail within a step:

```python
# Pipeline-wide default
pipeline = PipelineManager.create(..., failure_policy="continue")

# Step-level override
pipeline.run(operation=MyOp, inputs=..., failure_policy="fail_fast")
```

| Policy | Behavior |
|--------|----------|
| `"continue"` (default) | Commit successful artifacts, record failures, continue pipeline |
| `"fail_fast"` | Stop the step immediately on any failure |

`"continue"` is the default because in large runs (thousands of artifacts), a
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

Use `extra_slurm_kwargs` for parameters not covered by `ResourceConfig`:

```python
pipeline.run(
    operation=MyOp,
    inputs=...,
    resources={
        "extra_slurm_kwargs": {
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
| SLURM jobs OOM-killed | Default `mem_gb=4` too low | Set `resources={"mem_gb": 32}` or add to operation defaults |
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
