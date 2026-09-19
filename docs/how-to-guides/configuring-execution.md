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
    runner_resources={"gpus": 1, "memory_gb": 32, "extra": {"slurm_partition": "gpu"}},
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
the step runner. Every operation has a class-level `ComputeProvider`; the base
operation defaults to local compute. Omit the step keyword to use the
operation's declaration, or pass `compute_provider` to select or patch a
provider for one invocation:

```python
from artisan.schemas import (
    ComputeProvider,
    ComputeResources,
    ModalComputeConfig,
    ToolEndpointDataPolicy,
)

# Use MyOp's declared compute provider
pipeline.run(operation=MyOp, inputs=...)

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
| `"local"` (`OperationDefinition` default) | Direct call inside the worker | Development, testing, CPU-only ops |
| `"modal"` | Call the tool's deployed Modal endpoint | GPU work, cloud burst, isolated environments |

For a composite invocation, `run_composite(..., compute_provider=...)` and
`submit_composite(...)` provide an explicit default for child steps. A
`CompositeContext.run(..., compute_provider=...)` value wins for that child.
When neither is supplied, the child operation keeps its class declaration.

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
| `endpoint_url` | `str \| None` | `None` | Absolute root URL of an externally deployed endpoint. `None` resolves `artisan-tool-<op.name>` through Modal. Custom URLs may not contain credentials, a path, query, or fragment. |
| `auth_secret` | `str \| None` | `None` | Env-var prefix for a proxy-auth token pair (`<prefix>_TOKEN_ID` / `<prefix>_TOKEN_SECRET`). For the built-in Modal endpoint, `None` uses `MODAL_PROXY`. A custom URL with `None` is unauthenticated; setting a prefix requires a complete pair and HTTPS. |
| `poll_interval` | `float` | `2.0` | Seconds between `/result` polls while a tool job runs. |
| `max_concurrent_calls` | `int` | `64` | Client-side cap on concurrent endpoint calls per unit — the execute router fans one thread per artifact up to this bound. The server-side sibling is `max_containers`. |
| `output_store` | `str \| None` | `None` | Object-store prefix (`s3://bucket/prefix`) to deliver tool outputs under, sent per request. `None` returns outputs inline (100 MB bound). See *Object-store output delivery* below. |
| `data_policy` | `ToolEndpointDataPolicy` | empty, default-deny | Deployment-owned input-read and output-write allowlists. Inline data needs no entry; every remote URI must match a baked S3 prefix or exact HTTP origin. |

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
    compute_resources = ComputeResources(
        gpu="A100", cpu=4.0, memory_gb=64, timeout=7200
    )
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
*Proxy Auth Tokens*). Create or edit the gitignored `.env` file at the repo
root with these keys, replacing the placeholders with your own tokens:

```dotenv
MODAL_PROXY_TOKEN_ID=wk-...
MODAL_PROXY_TOKEN_SECRET=ws-...
```

Discovery order: process environment variables first (CI injects secrets
this way and always wins), then the nearest `.env` walking up from the
working directory — so Jupyter kernels and cron jobs work without
shell-inherited exports. Override the variable prefix per op via
`ModalComputeConfig.auth_secret`. Missing tokens fail fast with the setup
instructions in the error, before any network call.

Endpoint selection and authentication are one decision. A built-in Modal
endpoint discovers `MODAL_PROXY`. A custom `endpoint_url` sends no
authentication and does not look up `MODAL_PROXY` unless `auth_secret`
explicitly names a token prefix. Authenticated endpoints must use HTTPS.
Control requests never follow redirects; an unexpected redirect is a
configuration error, not a new destination for the credentials.

### Transport limits

Input files ship inline in the submit request and outputs return as a
tar — bounded at 100 MB per direction. Eligible complete-file artifacts
that already live on object storage pass their `s3://` URI by reference
(no re-upload, no bound — see
*Object-store input delivery* below), and outputs can be delivered to an
object store without the 100 MB inline bound — see *Object-store output delivery*
below. Large static data (model weights) belongs on Modal Volumes
(`ModalComputeConfig.volumes`), not in the request.

External binaries (compiled tools), artisan, and the op's Python module
must all be baked into the worker image; `local_python_sources` overlays
dev-host source for iteration.

(object-store-input-delivery)=
### Object-store input delivery

An externally stored `FileRefArtifact` or `LargeFileArtifact` crosses to
the worker **by reference** on an endpoint step. The client sends its URI,
`content_digest`, and `size_bytes`; the worker authorizes the URI,
downloads the complete file, and verifies both integrity values before
the tool can see a local path. An `AppendableArtifact` still selects and
verifies its bounded record locally, then sends that record inline rather
than exposing the shared container URI. Nothing changes for local
execution or for the operation's `preprocess` implementation.

Remote input access is off by default. The operation's class-level config
must include every permitted S3 path-segment prefix or HTTP capability
origin before deployment:

```python
data_policy=ToolEndpointDataPolicy(
    input_allowlist=(
        "s3://your-bucket/reference-data",
        "https://downloads.example.com",
    ),
)
```

S3 matching is bucket- and segment-aware: allowing `s3://bucket/data`
does not allow `s3://bucket/database`. HTTP entries are exact origins;
signed paths and queries remain per-request capabilities. Other schemes,
including `file://`, are not endpoint transports.

Direct HTTP consumers encode the same contract as two role-keyed maps:

```text
input_uris={"dataset":"https://downloads.example.com/input.csv?sig=..."}
input_integrity={"dataset":{"content_digest":"<32 lowercase hex>","size_bytes":123}}
```

The maps must have exactly the same keys. `data_policy` is never a request
field; only the deployment owner can change it.

The worker reads S3 inputs with the **same Modal Secret** that delivers
S3 outputs, so scope that Secret's IAM policy to grant **read** on the
allowed input prefixes as well as write on the allowed output prefixes.
The policy and IAM scope are separate defenses: the policy constrains
caller-directed I/O, while the Secret supplies credentials.

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
`output_store` to bypass that inline bound and deliver outputs to S3. The requested
destination travels per call, but it must remain beneath an output prefix
baked into the endpoint deployment. Changing or widening that policy
requires a redeploy.

```python
class FoldComplex(OperationDefinition):
    compute_provider = ComputeProvider(
        modal=ModalComputeConfig(
            image="ghcr.io/your-org/boltz-worker:0.4",
            secrets=["aws-s3"],
            output_store="s3://your-bucket/runs",
            data_policy=ToolEndpointDataPolicy(
                output_allowlist=(
                    "s3://your-bucket/runs",
                    "https://your-bucket.s3.amazonaws.com",
                ),
            ),
        ),
    )
    ...

# Redeploy after defining or changing the policy:
# artisan modal deploy fold_complex
```

The worker tars the outputs, uploads
`<output_store>/<op-name>/<uuid>.tar.gz` with its own credentials, and
the `/result` manifest carries the URI plus a presigned GET URL (7-day
expiry, matching Modal's result retention). The artisan client fetches
the tarball straight from the store with one plain, non-redirecting HTTP
GET and no AWS credentials. The generated URL's exact origin must also be
in `output_allowlist`. `/download` exposes the same capability to raw
consumers as a redirect, but Artisan's client treats every control-plane
redirect as an error. Omit `output_store` and behavior is exactly the
inline mode above.

Operational notes:

- **Credentials.** Prefix-mode uploads run with the worker's Modal
  Secret (`secrets=["aws-s3"]`). Use long-lived IAM user keys — STS
  session credentials cap presign lifetime below 7 days — and scope the
  key's IAM policy to the prefixes the deployment allows.
- **Lifecycle.** Artisan never deletes delivered tarballs. Pair
  destination prefixes with a bucket lifecycle policy (≥ 7 days,
  matching presign and result expiry).
- **Caller-owned buckets.** The endpoint policy must allow the destination.
  A caller outside the worker's IAM universe
  either grants the worker's principal `s3:PutObject` on its prefix via
  bucket policy, or skips shared credentials entirely with a presigned
  PUT (below).
- **Redeploy to change access.** `data_policy` is baked into the worker;
  caller fields cannot add or widen its roots.

(presigned-puts-and-external-consumers-capability-mode)=
#### Presigned PUTs and external consumers (capability mode)

`output_store` rides `/submit` as a plain form field, so a consumer
with no artisan installation can request delivery within the deployment
policy. Two forms are accepted: an S3 prefix written with worker
credentials, or an HTTP(S) PUT capability minted by the caller. Capability
mode works only when that exact HTTP origin is in the deployment's
`output_allowlist`; the signed path and query remain request data:

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
per-object limit; prefix mode can use the filesystem's multipart upload.
Artisan's compressed-size, expanded-size, and member-count archive budgets
still apply to both modes. Presigned PUT URLs are per-request
wire data: `ModalComputeConfig.output_store` rejects them at
import time, and the artisan client always uses prefix mode.

Inputs compose: input-ref URIs accept presigned GET URLs whose origins are
in `input_allowlist`, and each ref must include `content_digest` and
`size_bytes`. A deployment using only authorized GET and PUT capabilities
needs no object-store secret.

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
        "extra": {"slurm_partition": "gpu"},
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
| `extra` | `dict` | `{}` | Runner-specific settings (e.g., `{"slurm_partition": "gpu"}`) |

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
from artisan.schemas import BatchStrategy, RunnerResources


class GpuInference(OperationDefinition):
    name = "gpu_inference"

    runner_resources: RunnerResources = RunnerResources(
        gpus=1,
        memory_gb=32,
        time_limit="02:00:00",
        extra={"slurm_partition": "gpu"},
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

### Override sources and precedence

An explicit `pipeline.run()` or `pipeline.submit()` value wins, but the value
it overrides depends on the setting:

| Setting | Default source | Explicit override |
|---------|----------------|-------------------|
| `step_runner` | `PipelineManager.create(default_step_runner=...)` | Step `step_runner` |
| `failure_policy` | `PipelineManager.create(failure_policy=...)` | Step `failure_policy` |
| `cache_policy` | Nearest composite default, then pipeline `cache_policy` | Step `cache_policy` |
| `compute_provider` | Operation class | Step `compute_provider` |
| `runner_resources`, `batch_strategy`, `environment`, `tool`, `compute_resources` | Operation class | Matching step keyword |
| `group_by` | Operation class | Step `group_by` |

Pipeline defaults do not provide an intermediate compute-routing layer. The
effective provider is the operation declaration patched by the explicit step
value, when present.

### Patch configuration without replacing defaults

All model-valued step options use the same patch behavior:
`runner_resources`, `batch_strategy`, `environment`, `tool`,
`compute_provider`, and `compute_resources`.

You can pass either a dict or the corresponding typed model. Both forms use
the fields you supplied as the patch, including values that equal the model's
schema default:

```python
# These are equivalent, even though RunnerResources.cpus defaults to 1.
pipeline.run(operation=GpuInference, inputs=..., runner_resources={"cpus": 1})
pipeline.run(
    operation=GpuInference,
    inputs=...,
    runner_resources=RunnerResources(cpus=1),
)
```

Fields you omit retain the operation's declared values. Explicit `None` inside
a patch resets an optional field instead of falling back to the operation
default:

```python
# Both clear an operation-level ComputeResources(gpu="A100") default.
pipeline.run(operation=GpuInference, inputs=..., compute_resources={"gpu": None})
pipeline.run(
    operation=GpuInference,
    inputs=...,
    compute_resources=ComputeResources(gpu=None),
)
```

Nested non-empty mappings merge recursively, so updating one environment
variable preserves its siblings. Scalars, lists, `None`, and empty mappings
replace the inherited value:

```python
# Preserve every existing variable except MODE.
environment={
    "active": "docker",
    "docker": {"env": {"MODE": "production"}},
}

# Clear the inherited env mapping.
environment={"active": "docker", "docker": {"env": {}}}
```

An empty root patch such as `runner_resources={}` supplies no fields and is a
no-op. Top-level `None` also means no override; use `None` inside a patch to
reset an optional field.

---

## Configure external tools and environments

Operations that wrap external tools declare two things: a `ToolSpec` (the
binary/script to invoke) and an `Environments` configuration (the runtime
that wraps the command):

```python
from artisan.operations.base import OperationDefinition
from artisan.schemas import ApptainerEnvironmentSpec, Environments, ToolSpec


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
    environment={
        "active": "apptainer",
        "apptainer": {"image": "/tools/tool_a_v2.sif"},
    },
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
that matches what you want to do. Dict and typed forms that supply the same
fields produce the same effective configuration. A string is shorthand for an
`active` patch and follows the same validation path.

`environment`:

```python
# String form (select active provider only):
pipeline.submit(MyOp, environment="docker")

# Dict form (configure provider — must set 'active'):
pipeline.submit(
    MyOp, environment={"active": "docker", "docker": {"image": "myimg:latest"}}
)

# Typed-model form (autocomplete + validation):
pipeline.submit(
    MyOp,
    environment=Environments(
        active="docker", docker=DockerEnvironmentSpec(image="myimg:latest")
    ),
)
```

`compute_provider`:

```python
# String form (select active provider only):
pipeline.submit(MyOp, compute_provider="modal")

# Dict form (configure provider — must set 'active'). Hardware fields go
# on compute_resources, not the provider's modal block:
pipeline.submit(
    MyOp,
    compute_provider={
        "active": "modal",
        "modal": {"image": "ghcr.io/your-org/img:latest"},
    },
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

Passing a dict that configures a non-active provider (for example,
`environment={"docker": {...}}` without `active="docker"`) raises before
dispatch. Set `active="docker"` in the patch to configure and select it. A
string selector must name a target already configured on the operation;
unknown or unconfigured targets also raise before cache lookup.

---

## Set failure policy

Control what happens when some artifacts fail within a step:

```python
from artisan.schemas import FailurePolicy

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
pipeline.run_composite(
    MyComposite, inputs=..., cache_policy=CachePolicy.STEP_COMPLETED
)

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

Pipeline startup never guesses ownership for leftover staging. First inspect
the immutable evidence without mutation:

```bash
artisan store repair --delta-root runs/delta --staging-root runs/staging
```

Replay validated incomplete plans with `--apply`. If a plan cannot be restored,
abandon that one logical commit explicitly with `--abandon ID --reason ...`.

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
            "slurm_partition": "gpu",
            "slurm_constraint": "a100",
            "slurm_account": "my_allocation",
            "slurm_exclude": "node[001-003]",
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

## Inspect commands from an execution

After an execution commits, read the commands it attempted:

```python
from artisan.visualization import inspect_commands

recording = inspect_commands(delta_root, execution_run_id)
for command in recording["commands"]:
    print(command["invocation"], command["sequence"], command["argv"], command["outcome"])
```

`requested_argv` contains the requested arguments; `argv` includes the actual
Local, Pixi, Docker, or Apptainer wrapper. Each record includes the worker's
working directory, declared tool, environment identity, outcome, and exit code.
Python-only executions have a complete, empty command list.

Read `invocation` as input dispatch order and `sequence` as command order within
that invocation. Endpoint calls can run concurrently; their positions do not
represent a shared clock or completion order. Recording covers `run_command`
and Artisan's invocation helpers, including preprocessing and postprocessing.
Direct `subprocess` calls and user-created threads without propagated context
are outside this capture scope.

### Check whether evidence is complete

Check `status` before interpreting an empty list. `missing_invocations` explains
transport failures, cancellation, and missing or invalid endpoint evidence.
`unavailable_reason="worker_evidence_unavailable"` means the orchestrator had no
worker evidence when it synthesized a failure. Neither case proves that the
worker launched no command. Endpoints without recording support require redeployment.

The entire recording is capped at 1 MiB. When it reaches that limit, Artisan
keeps an ordered prefix and reports `omitted_commands` and
`omitted_missing_invocations`; later, shorter entries do not skip the cutoff.
Omissions make the recording partial. Inspection reads committed execution
rows; it never reconstructs missing evidence from logs.

### Supply credentials safely

Prefer environment variables or recognizable credential flags such as `--token`
and `--password`. For opaque values, pass `sensitive_values` to `run_command`:

```python
from artisan.utils import run_command

result = run_command(
    environment,
    ["tool", "--custom-option", credential],
    cwd=work_dir,
    sensitive_values=(credential,),
)
```

Recording removes explicit environment values, inherited credential values,
recognized credential arguments, and URL capabilities from diagnostic copies.
`redacted_fields` identifies affected fields; `required_environment` lists known
variable names that may require fresh credentials. Subprocess arguments and
successful return values keep their original values. Raw tool output files and
live stdout are not secret-filtered; arbitrary unknown positional secrets cannot
be inferred reliably.

Use this evidence to diagnose a run. Stored arguments can contain placeholders,
and worker paths can disappear; command recording is not an executable replay
script. Execution replay reconstructs an operation separately.

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
from artisan.orchestration import StepStatus

step = pipeline.run(operation=MyOp, inputs=..., step_runner=Runner.LOCAL)
assert step.status is StepStatus.SUCCEEDED
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
