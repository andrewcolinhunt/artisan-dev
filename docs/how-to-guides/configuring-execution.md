# Configure Execution

How to control where operations run, what resources they get, and how work
is batched — from local development through optional cluster runners.

**Prerequisites:** [Operations Model](../concepts/operations-model.md),
[Building a Pipeline](building-a-pipeline.md)

**Key types:** `Runner`, `RunnerBase`, `RunnerResources`, `BatchStrategy`, `ToolSpec`,
`Environments`, `CachePolicy`, `FailurePolicy`, `ComputeProvider`,
`ModalComputeConfig`, `ComputeResources`

---

## Minimal working example

A pipeline running one step locally and one on SLURM with GPU resources. The
SLURM runner comes from the optional `artisan-submitit` package:

```python
from artisan.orchestration import PipelineManager
from artisan_submitit import SlurmRunner
from myops import PreprocessOp, InferenceOp

pipeline = PipelineManager.create(
    name="example",
    delta_root="runs/delta",
    staging_root="runs/staging",
)

pipeline.run(operation=PreprocessOp, name="preprocess", params={"count": 100})

pipeline.run(
    operation=InferenceOp,
    name="inference",
    inputs={"dataset": pipeline.output("preprocess", "dataset")},
    step_runner=SlurmRunner(),
    runner_resources={"gpus": 1, "memory_gb": 32, "extra": {"partition": "gpu"}},
    batch_strategy={"artifacts_per_unit": 1},
)
```

The rest of this guide breaks down each option.

---

## Choose a step runner

Every step runs on a step runner. Set it per step or as a pipeline-wide
default:

```python
from artisan.orchestration import Runner
from artisan_submitit import SlurmIntraRunner, SlurmRunner

# Pipeline-wide default
pipeline = PipelineManager.create(..., default_step_runner=SlurmRunner())

# Step-level override
pipeline.run(operation=MyOp, inputs=..., step_runner=Runner.LOCAL)
```

| Step runner | How it runs | When to use |
|-------------|-------------|-------------|
| `Runner.LOCAL` (default) | Process pool on your machine | Development, testing, lightweight ops |
| `SlurmRunner()` (`artisan-submitit`) | SLURM job array on cluster | Production, GPU work, HPC |
| `SlurmIntraRunner()` (`artisan-submitit`) | `srun` within an existing SLURM allocation | Interactive `salloc` sessions, zero queue wait |

For `SlurmIntraRunner`, you must be inside an existing SLURM allocation
(`salloc` or `sbatch`). Work is distributed via `srun` with no queue wait:

```python
pipeline.run(
    operation=MyOp,
    inputs=...,
    step_runner=SlurmIntraRunner(),
    runner_resources={"gpus": 1, "cpus": 4, "memory_gb": 16},
)
```

For `LOCAL`, you can cap the number of concurrent workers per step:

```python
pipeline.run(operation=MyOp, inputs=..., batch_strategy={"max_workers": 8})
```

The default process pool size is 4.

---

## Configure compute routing

Compute routing controls where the execute phase runs, independently of
the step runner. Set it per step or as a pipeline-wide default:

```python
from artisan.schemas.operation_config.compute import ComputeProvider, ModalComputeConfig
from artisan.schemas.operation_config.compute_resources import ComputeResources

# Pipeline-wide default
pipeline = PipelineManager.create(..., default_compute_provider="local")

# Step-level override (string shorthand)
pipeline.run(operation=MyOp, inputs=..., compute_provider="modal")

# Step-level override (dict with inline config)
pipeline.run(
    operation=MyOp,
    inputs=...,
    compute_provider={"active": "modal"},
    compute_resources={"gpu": "A100", "memory_gb": 32},
)
```

| Compute target | How it runs | When to use |
|----------------|-------------|-------------|
| `"local"` (default) | Direct call inside the worker | Development, testing, CPU-only ops |
| `"modal"` | Call the tool's deployed Modal endpoint | GPU work, cloud burst, isolated environments |

The modal provider runs **command ops** only — operations declaring a
`ToolSpec` + `execute_command()` instead of `execute_function()` — and requires the
tool's endpoint to be deployed first:

```bash
artisan modal deploy <operation-name>
```

Hardware fields (`gpu`, `cpu`, `memory_gb`, `timeout`) live on
`ComputeResources` so the same hardware spec applies to any future compute
provider; `ModalComputeConfig` carries Modal-specific transport and
runtime concerns only.

### ComputeResources fields

Container hardware allocated by the compute provider for each call. All
fields are `None` by default — the provider's own default applies.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `gpu` | `str \| None` | `None` | GPU type (e.g. `"A10G"`, `"A100"`, `"H100"`). |
| `cpu` | `float \| None` | `None` | Fractional CPU cores. Modal's default is **0.125** — set this explicitly for non-trivial CPU work (pandas, numpy, shell-invoked tools). Fractional values like `0.5` and `2.5` are valid. |
| `memory_gb` | `int \| None` | `None` | Container memory in GB. Modal's default is `8`. |
| `timeout` | `int \| None` | `None` | Per-call timeout in seconds. Modal's default is `3600`. |

### ModalComputeConfig fields

Modal-specific provider configuration. Hardware fields (`gpu`, `cpu`,
`memory_gb`, `timeout`) live on `ComputeResources` (above), not here.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `image` | `str` | `ARTISAN_WORKER_IMAGE` | Container image for the Modal function. |
| `retries` | `int` | `3` | Retries on preemption. |
| `min_containers` | `int` | `0` | Containers kept warm at zero traffic. Set to expected batch parallelism to eliminate cold starts; `0` means scale-to-zero. |
| `max_containers` | `int \| None` | `None` | Upper bound on concurrent worker containers — caps per-artifact fan-out on large batches. |
| `scaledown_window` | `int \| None` | `None` | Seconds a container idles before shutdown. Modal's default is 60s; max 1200s. |
| `image_registry_secret` | `str \| None` | `None` | Name of a Modal Secret holding `REGISTRY_USERNAME` / `REGISTRY_PASSWORD` for pulling private images. |
| `secrets` | `list[str]` | `[]` | Names of Modal Secrets to inject into the runtime environment (e.g. `["hf-read", "aws-s3"]`). Created via `modal secret create ...`. Distinct from `image_registry_secret`, which authenticates the image pull only. |
| `volumes` | `dict[str, str]` | `{}` | Mount path → volume name (e.g. `{"/weights": "foundry-weights"}`). Each volume is resolved via `modal.Volume.from_name(name, create_if_missing=True, version=2)` — surviving across cold starts is the point. |
| `env` | `dict[str, str]` | `{}` | Environment variables set inside the container (e.g. `{"HF_XET_HIGH_PERFORMANCE": "1"}`). Applied as an image layer; cache hits survive as long as the dict is stable. |
| `local_python_sources` | `list[str]` | `[]` | Top-level Python package names overlaid onto the **worker** image, shadowing the image's baked versions — dev-mode iteration only (`artisan modal deploy --overlay` appends). Default `[]`: op code is baked into the image. The endpoint image carries no artisan — it validates requests against the op's `Params` JSON schema baked in at deploy time. |
| `endpoint_url` | `str \| None` | `None` | Base URL of an externally-deployed tool endpoint. `None` resolves the Artisan-deployed app `artisan-tool-<op.name>` via the Modal SDK. |
| `auth_secret` | `str \| None` | `None` | Env-var prefix for the proxy-auth token pair (`<prefix>_TOKEN_ID` / `<prefix>_TOKEN_SECRET`). `None` uses `MODAL_PROXY`. |
| `poll_interval` | `float` | `2.0` | Seconds between `/result` polls while a tool job runs. |
| `max_concurrent_calls` | `int` | `64` | Client-side cap on concurrent endpoint calls per unit — the execute router fans one thread per artifact up to this bound. The server-side sibling is `max_containers`. |
| `output_store` | `str \| None` | `None` | Object-store prefix (`s3://bucket/prefix`) to deliver tool outputs under, sent per request. `None` returns outputs inline (100 MB bound). See *Object-store output delivery* below. |

The worker image must carry everything the op needs — tool binaries,
artisan, and the op's own module are baked in (see the op-container-images
guide); `local_python_sources` overlays dev-host source on top for
iteration.

#### GPU op with weights, secrets, and runtime env

Typical pattern: a private image, a secret for HF auth, weights on a warm
Modal Volume, and one runtime env var. The config lives on the operation
class — deploy reads it from there, never from an instance:

```python
class GpuInference(OperationDefinition):
    name = "gpu_inference"
    tool = ToolSpec(executable="inference")
    compute_provider = ComputeProvider(
        modal=ModalComputeConfig(
            image="ghcr.io/your-org/foundry-artisan:latest",
            image_registry_secret="ghcr-pat",
            secrets=["hf-read"],
            volumes={"/weights": "foundry-weights"},
            env={"HF_XET_HIGH_PERFORMANCE": "1"},
            max_containers=50,
        ),
    )
    compute_resources = ComputeResources(gpu="A100", cpu=4.0, memory_gb=64, timeout=7200)
    ...
```

```bash
artisan modal deploy gpu_inference
```

```python
pipeline.run(operation=GpuInference, inputs=..., compute_provider="modal")
```

Hardware is part of the deployed worker — changing `ComputeResources`
means redeploying; a per-step `compute_resources` override does not
reconfigure an already-deployed endpoint.

### Authentication

The deployed endpoint requires Modal proxy-auth tokens (dashboard →
*Proxy Auth Tokens*). Recommended setup — a gitignored `.env` at the repo
root, copied from the committed template:

```bash
cp .env.example .env
# MODAL_PROXY_TOKEN_ID=wk-...
# MODAL_PROXY_TOKEN_SECRET=ws-...
```

Discovery order: process environment variables first (CI injects secrets
this way and always wins), then the nearest `.env` walking up from the
working directory — so Jupyter kernels and cron jobs work without
shell-inherited exports. Override the variable prefix per op via
`ModalComputeConfig.auth_secret`. Missing tokens fail fast with the setup
instructions in the error, before any network call.

### Transport limits

Input files ship inline in the submit request and outputs return as a
tar — bounded at 100 MB per direction. Inputs that already live on object
storage pass their `s3://` URI by reference (no re-upload, no bound — see
*Object-store input delivery* below), and outputs can be delivered to an
object store with no size bound — see *Object-store output delivery*
below. Large static data (model weights) belongs on Modal Volumes
(`ModalComputeConfig.volumes`), not in the request.

External binaries (compiled tools), artisan, and the op's Python module
must all be baked into the worker image; `local_python_sources` overlays
dev-host source for iteration.

(object-store-input-delivery)=
### Object-store input delivery

An input artifact whose bytes already live in an object store (a
`LargeFileArtifact`, or any file-backed artifact produced on a cloud
backend) crosses to the worker **by reference** on an endpoint step: the
client sends the `s3://` URI, and the worker fetches it with its own
ambient credentials. The client never downloads it and never inlines it,
so the 100 MB inline bound does not apply — a 2 GB MSA database crosses
the same way a 1 MB PDB does. Nothing changes for local execution, and
nothing changes in your op: `preprocess` still reads
`artifact.materialized_path` (now the URI). This is automatic — there is
no flag to set.

The worker reads inputs with the **same Modal Secret** that delivers
outputs, so scope that Secret's IAM policy to grant **read** on the input
buckets as well as write on the output prefixes.

:::{warning}
**R2 / custom-endpoint footgun — a required deploy step.** For a
custom-endpoint store (Cloudflare R2, MinIO), the Secret must carry
**`AWS_ENDPOINT_URL`** alongside `AWS_ACCESS_KEY_ID` /
`AWS_SECRET_ACCESS_KEY`. botocore reads it, so a bare `s3://bucket/key`
resolves to your store. **With only the key pair, an `s3://` fetch
silently targets AWS**, not R2 — and fails. This is the same env var the
output-delivery path relies on; one Secret carries creds **and** endpoint
for both directions.
:::

A fetch failure — a missing object, a denied read, absent credentials, or
an unreachable/typo'd `AWS_ENDPOINT_URL` — surfaces as an
`INPUT_RESOLUTION_FAILED` / `CHECK_INPUT` envelope **before** the tool
runs, so there is no partial work to clean up. Fix the ref or the Secret
and resubmit.

A local input larger than 100 MB on an endpoint step still fails at the
inline cap (v1): host it in object storage first — a cloud-backend
pipeline does this automatically via `files_root`.

(object-store-output-delivery)=
### Object-store output delivery

The 100 MB output bound applies only to inline returns. Set
`output_store` to deliver outputs of any size to an object store
instead. The destination is **request data**: every caller of one
deployed endpoint picks its own, per run, with no redeploy.

```python
op = FoldComplex(
    compute_provider=ComputeProvider(
        active="modal",
        modal=ModalComputeConfig(
            image="ghcr.io/your-org/boltz-worker:0.4",
            secrets=["aws-s3"],                    # worker upload credentials
            output_store="s3://your-bucket/runs",  # this caller's choice
        ),
    )
)
```

The worker tars the outputs, uploads
`<output_store>/<op-name>/<uuid>.tar.gz` with its own credentials, and
the `/result` manifest carries the URI plus a presigned GET URL (7-day
expiry, matching Modal's result retention). The artisan client fetches
the tarball straight from the store; `/download` 307-redirects to the
same URL for curl-style consumers — one plain HTTP GET, no AWS
credentials. Omit `output_store` and behavior is exactly the inline
mode above.

Operational notes:

- **Credentials.** Prefix-mode uploads run with the worker's Modal
  Secret (`secrets=["aws-s3"]`). Use long-lived IAM user keys — STS
  session credentials cap presign lifetime below 7 days — and scope the
  key's IAM policy to the prefixes callers may target: that policy is
  the access-control surface for worker-identity writes.
- **Lifecycle.** Artisan never deletes delivered tarballs. Pair
  destination prefixes with a bucket lifecycle policy (≥ 7 days,
  matching presign and result expiry).
- **Caller-owned buckets.** A caller outside the worker's IAM universe
  either grants the worker's principal `s3:PutObject` on its prefix via
  bucket policy, or skips shared credentials entirely with a presigned
  PUT (below).
- **Redeploy to enable.** An endpoint deployed before this feature
  ignores the field and silently falls back to inline delivery.

(presigned-puts-and-external-consumers-capability-mode)=
#### Presigned PUTs and external consumers (capability mode)

`output_store` rides `/submit` as a plain form field, so a consumer
with no artisan installation can direct delivery. Two forms,
discriminated by scheme: an object-store prefix (`s3://…`, the worker's
credentials write) or a presigned PUT URL (`https://…`) the caller
mints for its own bucket — the worker PUTs the tarball through it and
no store credentials cross the boundary in either direction:

```bash
# mint a presigned PUT with your own credentials, e.g. boto3:
#   s3.generate_presigned_url("put_object", Params={"Bucket": ..., "Key": ...})
curl -X POST "$ENDPOINT/submit" \
  -H "Modal-Key: $TOKEN_ID" -H "Modal-Secret: $TOKEN_SECRET" \
  -F params='{"contigs": "10-20"}' \
  -F output_store="$PRESIGNED_PUT_URL"
# → {"call_id": "..."}

curl "$ENDPOINT/result?call_id=$CALL_ID" -H "Modal-Key: ..." -H "Modal-Secret: ..."
# → {"status": "done",
#    "manifest": {"stored": {"uri": "https://...", "presigned_url": null}}}
# the tarball is in your bucket — fetch it with your own credentials;
# /download answers 409 (the endpoint cannot serve what it never held)
```

Mint **SigV4** URLs (boto3/botocore default to legacy SigV2 query auth
unless configured with `Config(signature_version="s3v4")` — R2 and
modern AWS buckets reject SigV2 with 401) with default (host-only)
signed headers — the worker adds an explicit `Content-Length` and
nothing else. A single presigned PUT is bounded by S3's 5 GiB
per-object limit; prefix mode multiparts transparently and has no such
bound. Presigned PUT URLs are per-request
wire data: `ModalComputeConfig.output_store` rejects them at
import time, and the artisan client always uses prefix mode.

Inputs compose: input-ref URIs accept presigned GET URLs too, so a
fully credential-free deployment (presigned GETs in, presigned PUT out)
needs no object-store secret at all.

---

## Configure resources

Pass a `runner_resources` dict to override resource allocation for a step:

```python
pipeline.run(
    operation=MyOp,
    inputs=...,
    step_runner=SlurmRunner(),
    runner_resources={
        "gpus": 1,
        "memory_gb": 32,
        "time_limit": "04:00:00",
        "cpus": 4,
        "extra": {"partition": "gpu"},
    },
)
```

### RunnerResources fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `cpus` | `int` | `1` | CPU cores per task |
| `memory_gb` | `int` | `4` | Memory in GB |
| `gpus` | `int` | `0` | Number of GPUs requested |
| `time_limit` | `str` | `"01:00:00"` | Wall-clock time limit (HH:MM:SS) |
| `extra` | `dict` | `{}` | Runner-specific settings (e.g., `{"partition": "gpu"}`) |

`RunnerResources` is portable across step runners — each runner translates these
fields to its native format. Use `extra` for runner-specific settings like
SLURM partition or account.

Step-level `runner_resources` merge with operation defaults — you only need to
specify the fields you want to override.

### Runner resources vs compute resources

`runner_resources` describes resources for the step runner — CPUs, memory,
time limit, and provider-specific fields. A cluster provider uses these fields
to request the allocation that hosts the worker process.

`compute_resources` (a `ComputeResources` typed model or dict) describes the
remote container hardware that the worker actually runs on — GPU type,
fractional CPU cores, memory in GB, and per-call timeout. The worker reaches
into this hardware when it hands the operation off to the remote compute
provider.

Set both when a SLURM-dispatched worker should off-load the heavy compute step
to a Modal container — for example, `runner_resources={"cpus": 2, "memory_gb":
8}` to host the dispatcher and `compute_provider={"active": "modal"}` plus
`compute_resources={"gpu": "A100", "memory_gb": 64}` to run inference on the
GPU container.

---

## Control batching

Batching determines how many artifacts each worker processes. This is the main
lever for tuning throughput.

```python
pipeline.run(
    operation=MyOp,
    inputs=...,
    batch_strategy={"artifacts_per_unit": 10},
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
5 workers (each runs 2 units sequentially)
```

**Level 1 — `artifacts_per_unit`**: How many artifacts each execution unit
processes. Set this based on your operation's workload: 1 for GPU inference
(one artifact per job), 50–100 for fast metrics calculations.

**Level 2 — `units_per_worker`**: How many execution units a single worker
runs sequentially. Use this to amortize process, container, or scheduler startup
overhead without changing your operation's batch logic.

### BatchStrategy fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `artifacts_per_unit` | `int` | `1` | Artifacts per execution unit |
| `units_per_worker` | `int` | `1` | Execution units per worker invocation |
| `max_workers` | `int \| None` | `None` | Cap on concurrent workers |
| `max_artifacts_per_unit` | `int \| None` | `None` | Upper bound on artifacts per unit when using adaptive batching |
| `estimated_seconds` | `float \| None` | `None` | Expected wall-clock time per unit, used for scheduler hints |
| `job_name` | `str \| None` | `None` | Custom worker or scheduler job name (defaults to operation name) |

---

## Set operation-level defaults

Operations can declare their own default resources and execution config so you
don't repeat the same overrides at every step:

```python
from artisan.operations.base import OperationDefinition
from artisan.schemas.operation_config.runner_resources import RunnerResources
from artisan.schemas.execution.batch_strategy import BatchStrategy


class GpuInference(OperationDefinition):
    name = "gpu_inference"

    runner_resources: RunnerResources = RunnerResources(
        gpus=1,
        memory_gb=32,
        time_limit="02:00:00",
        extra={"partition": "gpu"},
    )

    batch_strategy: BatchStrategy = BatchStrategy(
        artifacts_per_unit=1,
        estimated_seconds=600.0,
    )

    # ... lifecycle methods ...
```

Step-level overrides merge on top of these defaults. For example, to give a
specific step more memory without changing other settings:

```python
pipeline.run(operation=GpuInference, inputs=..., runner_resources={"memory_gb": 64})
# gpus, time_limit, extra keep their operation defaults
```

### Override precedence

```
Pipeline defaults (PipelineManager.create)
    └── Operation defaults (class fields)
            └── Step overrides (pipeline.run kwargs)   ← wins
```

---

## Configure external tools and environments

Operations that wrap external tools declare two things: a `ToolSpec` (the
binary/script to invoke) and an `Environments` configuration (the runtime
that wraps the command):

```python
from artisan.operations.base import OperationDefinition
from artisan.schemas.operation_config.tool_spec import ToolSpec
from artisan.schemas.operation_config.environments import Environments
from artisan.schemas.operation_config.environment_spec import ApptainerEnvironmentSpec


class ToolAOp(OperationDefinition):
    name = "tool_a"

    tool: ToolSpec = ToolSpec(
        executable="run_tool_a.sh",
        interpreter="bash",
    )

    environments: Environments = Environments(
        active="apptainer",
        apptainer=ApptainerEnvironmentSpec(
            image="/tools/tool_a.sif",
            gpu=True,
            binds=[
                ("/data/weights", "/weights"),
                ("/scratch", "/scratch"),
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

When you pass a dict for `environment`, fields are deep-merged with the
operation's existing environment config. This means partial overrides work
without discarding other fields. To switch the active environment without
changing any spec fields, pass a string instead:

```python
pipeline.run(operation=ToolAOp, inputs=..., environment="local")
```

The `binds` field takes a list of `(host_path, container_path)` tuples — not
colon-delimited strings. To mount read-only (e.g. for a shared weights cache
the running op should not be able to modify), pass a 3-tuple
`(host_path, container_path, mode)` where `mode` is `"ro"`, `"rw"`, or any
other Docker / Apptainer-supported mode string:

```python
binds = [
    ("/data/weights", "/weights", "ro"),
    ("/scratch", "/scratch"),  # 2-tuple still works (default rw)
]
```

### ToolSpec fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `executable` | `str \| Path` | (required) | Path or name of the binary/script. Resolved via PATH if not absolute. |
| `interpreter` | `str \| None` | `None` | Interpreter prefix (e.g., `"bash"`, `"python -u"`) |
| `subcommand` | `str \| None` | `None` | Subcommand inserted after the executable |

### Environment spec types

| Spec | Use case | Key fields |
|------|----------|------------|
| `ApptainerEnvironmentSpec` | Apptainer/Singularity containers (HPC) | `image` (str), `gpu`, `binds` (2- or 3-tuple) |
| `DockerEnvironmentSpec` | Docker containers | `image` (str), `gpu`, `binds` (2- or 3-tuple) |
| `LocalEnvironmentSpec` | Local execution, optional virtualenv | `venv_path` |
| `PixiEnvironmentSpec` | Pixi-managed environments | `pixi_environment`, `manifest_path` |

All specs share a base `EnvironmentSpec` with an `env` dict for extra
environment variables.

### String, dict, or typed model — pick one

Both `environment` and `compute_provider` accept three shapes. Pick the form
that matches what you want to do.

`environment`:

```python
# String form (select active provider only):
pipeline.submit(MyOp, environment="docker")

# Dict form (configure provider — must set 'active'):
pipeline.submit(MyOp, environment={"active": "docker", "docker": {"image": "myimg:latest"}})

# Typed-model form (autocomplete + validation):
pipeline.submit(MyOp, environment=Environments(active="docker", docker=DockerEnvironmentSpec(image="myimg:latest")))
```

`compute_provider`:

```python
# String form (select active provider only):
pipeline.submit(MyOp, compute_provider="modal")

# Dict form (configure provider — must set 'active'). Hardware fields go
# on compute_resources, not the provider's modal block:
pipeline.submit(
    MyOp,
    compute_provider={"active": "modal", "modal": {"image": "ghcr.io/your-org/img:latest"}},
    compute_resources={"gpu": "A100", "memory_gb": 32},
)

# Typed-model form (autocomplete + validation):
pipeline.submit(
    MyOp,
    compute_provider=ComputeProvider(
        active="modal",
        modal=ModalComputeConfig(image="ghcr.io/your-org/img:latest"),
    ),
    compute_resources=ComputeResources(gpu="A100", memory_gb=32),
)
```

Passing a dict that configures a non-active provider (e.g.
`environment={"docker": {...}}` without `active="docker"`) leaves the active
provider unchanged — the `docker` block is merged in but stays unused. Set
`active="docker"` to switch providers.

---

## Set failure policy

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

## Set cache policy

Cache policy controls when a previously completed step qualifies as a cache
hit on re-run (e.g., when resuming a pipeline):

```python
from artisan.schemas.enums import CachePolicy

pipeline = PipelineManager.create(..., cache_policy=CachePolicy.STEP_COMPLETED)
```

| Policy | Behavior |
|--------|----------|
| `CachePolicy.ALL_SUCCEEDED` (default) | Cache hit only when the step had zero execution failures |
| `CachePolicy.STEP_COMPLETED` | Cache hit for any completed step, regardless of execution failure count |

Both policies block caching when infrastructure errors (dispatch or commit
failures) occurred. The difference is whether partial-failure steps count as
hits.

Use `STEP_COMPLETED` when you want to skip re-running a step that mostly
succeeded, even if a few artifacts failed.

---

## Use non-blocking execution

`pipeline.run()` blocks until the step completes. For steps that can overlap
(e.g., independent branches), use `pipeline.submit()` to dispatch without
waiting:

```python
future = pipeline.submit(
    operation=BranchAOp,
    inputs={"data": pipeline.output("preprocess", "data")},
    step_runner=SlurmRunner(),
)

# Submit another step concurrently
pipeline.submit(
    operation=BranchBOp,
    inputs={"data": pipeline.output("preprocess", "data")},
    step_runner=SlurmRunner(),
)

# Downstream steps that depend on a submitted step automatically wait
pipeline.run(
    operation=MergeOp,
    inputs={
        "a": pipeline.output("branch_a", "result"),
        "b": pipeline.output("branch_b", "result"),
    },
)
```

`submit()` returns a `StepFuture`. The orchestrator tracks dependencies and
blocks downstream steps until their inputs are ready.

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

### Recovering from crashes

By default, `PipelineManager.create` commits leftover staging files from prior
crashed runs at pipeline initialization (`recover_staging=True`). To disable
this:

```python
pipeline = PipelineManager.create(..., recover_staging=False)
```

### Naming steps

By default, each step is named after the operation. Provide a custom `name` to
disambiguate when the same operation appears multiple times:

```python
pipeline.run(operation=ScoreOp, name="score_round1", inputs=...)
pipeline.run(operation=ScoreOp, name="score_round2", inputs=...)

# Reference by name
pipeline.output("score_round1", "scores")
```

### Tuning SLURM throughput

For operations with fast per-artifact execution (< 1 second), increase
`artifacts_per_unit` to reduce job overhead:

```python
pipeline.run(
    operation=FastMetrics,
    inputs=...,
    step_runner=SlurmRunner(),
    batch_strategy={"artifacts_per_unit": 100, "units_per_worker": 5},
)
```

For GPU operations, keep `artifacts_per_unit=1` and let SLURM handle
parallelism via job arrays.

### Custom SLURM parameters

Use `extra` for runner-specific parameters not covered by `RunnerResources`:

```python
pipeline.run(
    operation=MyOp,
    inputs=...,
    runner_resources={
        "extra": {
            "partition": "gpu",
            "constraint": "a100",
            "account": "my_allocation",
            "exclude": "node[001-003]",
        }
    },
)
```

### Disabling Delta Lake compaction

Each `run()` call compacts Delta Lake tables after commit. To skip compaction
(useful when running many small steps in sequence):

```python
pipeline.run(operation=MyOp, inputs=..., compact=False)
```

---

## Common pitfalls

| Problem | Cause | Fix |
|---------|-------|-----|
| SLURM jobs OOM-killed | Default `memory_gb=4` too low | Set `runner_resources={"memory_gb": 32}` or add to operation defaults |
| Thousands of tiny SLURM jobs | `artifacts_per_unit=1` on a fast operation | Increase `artifacts_per_unit` to batch work |
| `binds` validation error | Using `"/host:/container"` strings | Use tuple pairs: `[("/host", "/container")]` (or `("/host", "/container", "ro")` for read-only) |
| Step ignores scheduler-specific `runner_resources` | Forgot to pass `SlurmRunner()` | Scheduler-specific resources require the provider runner |
| Workers contend on shared filesystem | Default `working_root` on NFS | Omit `working_root` — default uses `$TMPDIR` (node-local) |
| GPU/extra resource warning on local | SLURM-specific resources on `Runner.LOCAL` | These are ignored locally — use `SlurmRunner()` or remove them |

---

## Verify

Confirm your configuration works by running a small test:

```python
step = pipeline.run(operation=MyOp, inputs=..., step_runner=Runner.LOCAL)
assert step.success
print(f"Processed {step.succeeded_count} artifacts")
```

Then switch to `SlurmRunner()` for production. Check SLURM job logs if
failures occur — the job name format is `s{step_number}_{operation_name}`.

---

## Cross-references

- [Execution Flow](../concepts/execution-flow.md) — dispatch, execute, commit lifecycle
- The `artisan-submitit` README — installing and configuring the optional SLURM provider
- [Writing Creator Operations](writing-creator-operations.md) — declaring operation-level defaults
- [Compute Routing Tutorial](../tutorials/07-compute-backends/01-compute-routing.ipynb) — interactive compute routing walkthrough
- [Running on Modal Tutorial](../tutorials/07-compute-backends/04-modal-execution.ipynb) — Modal-specific configuration and debugging
