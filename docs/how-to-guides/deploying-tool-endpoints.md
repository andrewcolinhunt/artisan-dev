# Deploy Tool Endpoints

Deploy a command operation to Modal, invoke it from a pipeline, and configure
object-store delivery for files that exceed the inline transport limit.

You need a command operation, the `dexterity-artisan[modal]` extra, a configured
Modal account, and a worker image containing Artisan, the operation's module,
and its tools. See [Write Creator Operations](writing-creator-operations.md#command-operations-external-tools)
and [Op Container Images](op-container-images.md) before deploying a new operation.

## Declare deployment settings on the operation

Deployment reads class-level defaults. Set `compute_provider.modal` for the
image and runtime, and `compute_resources` for hardware. For example, these
fields belong on your command operation:

```python
from artisan.schemas import ComputeProvider, ComputeResources, ModalComputeConfig

compute_provider = ComputeProvider(
    modal=ModalComputeConfig(
        image="ghcr.io/your-org/inference:1.0",
        image_registry_secret="registry-pull",
        secrets=["model-access"],
        volumes={"/weights": "model-weights"},
        env={"MODEL_CACHE": "/weights"},
        max_containers=8,
    ),
)
compute_resources = ComputeResources(gpu="A100", cpu=4, memory_gb=32, timeout=3600)
```

The image-pull secret authenticates the registry. Runtime secrets are injected
inside workers. Volumes keep data such as model weights available across worker
starts. Choose limits for your workload; see the
[configuration model docstrings](../reference/python-api.md) for all fields.

Make the operation discoverable by installing its package and loading the module
when invoking the CLI:

```bash
ARTISAN_LOAD_MODULES=my_package.ops artisan modal deploy my_operation
```

The argument is the registered operation name. Deployment produces a Modal app
named `artisan-tool-<operation-name>`. For development, `--overlay my_package`
adds local source over the worker image. Production images should contain the
code they execute.

Changing hardware, image, runtime secrets, or data policy requires redeployment.
Per-step configuration does not resize or rebuild an existing endpoint.

## Authenticate calls and run a small step

Set proxy credentials in the process environment or a gitignored `.env` near
the working directory:

```dotenv
MODAL_PROXY_TOKEN_ID=your-token-id
MODAL_PROXY_TOKEN_SECRET=your-token-secret
```

Process environment values take precedence over `.env`. Built-in endpoint
lookup uses the `MODAL_PROXY` prefix; `ModalComputeConfig.auth_secret` selects
another prefix. A custom `endpoint_url` sends no proxy credentials unless you
explicitly set `auth_secret`. Authenticated URLs require HTTPS. Control requests
do not follow redirects.

After deployment, route a pipeline step to the operation's endpoint:

```python
step = pipeline.run(
    MyCommandOp,
    inputs=...,
    compute_provider="modal",
)
```

The lifecycle worker prepares inputs and handles outputs. Only execute runs at
the endpoint. Plain function operations cannot use this route; command operations
use `ToolSpec` plus `execute_command`, or `execute_as_tool=True`.

Check `step.status`, inspect the accepted outputs, and review Modal worker logs
for the matching app. [Running on Modal](../tutorials/07-compute-backends/04-modal-execution.ipynb)
provides a complete pipeline example.

## Choose how files travel

Inline requests and responses are bounded at 100 MB per direction. Keep large
static assets such as model weights in the worker image or a mounted volume.
Use object storage for large per-request inputs or outputs. Stored archives
remain subject to compressed-size, expanded-size, and member-count safety
budgets.

(object-store-input-delivery)=
### Object-store input delivery

Eligible externally stored `FileRefArtifact` and `LargeFileArtifact` inputs
travel by reference. The request carries a URI, digest, and byte count. The
worker authorizes the URI, downloads the full file, and verifies integrity before
invoking the tool. `AppendableArtifact` inputs instead send the selected,
locally verified record inline.

Remote reads are disabled until you declare allowed sources on the operation
before deploying:

```python
from artisan.schemas import ToolEndpointDataPolicy

data_policy = ToolEndpointDataPolicy(
    input_allowlist=(
        "s3://your-bucket/reference-data",
        "https://downloads.example.com",
    ),
)
```

Assign this to `ModalComputeConfig.data_policy`. S3 prefixes match path segments:
`.../data` does not grant access to `.../database`. HTTP entries are exact origins;
the signed path and query travel with the request. The worker also needs
credentials with read access to allowed S3 prefixes. Policy defines destinations
callers may request; credentials determine what the worker can access.

For an S3-compatible service such as R2 or MinIO, the worker secret must include
`AWS_ENDPOINT_URL` as well as credentials. Without the endpoint, an `s3://` URI
resolves against AWS. A failed input download or integrity check fails before
the tool runs, with `INPUT_RESOLUTION_FAILED` and `CHECK_INPUT` diagnostics.

A large local file still hits the inline limit. Store it externally first; a
cloud-backed pipeline can place creator outputs in `files_root`. See
[Configure S3-Compatible Storage](configuring-s3.md).

### Config files containing paths

Endpoint transport does not rewrite config file contents. Pass data files as
explicit input roles and build any path-bearing config inside `execute_command`,
where paths refer to worker-local files.

Materialized `ExecutionConfigArtifact` inputs containing `$artifact` references
are rejected for endpoint execution. Configs without references work. Local
execution still resolves those references into materialized paths.

(object-store-output-delivery)=
### Object-store output delivery

Set `output_store` to an S3 prefix allowed by the deployed operation's policy:

```python
compute_provider = ComputeProvider(
    modal=ModalComputeConfig(
        secrets=["object-store-access"],
        output_store="s3://your-bucket/runs",
        data_policy=ToolEndpointDataPolicy(
            output_allowlist=(
                "s3://your-bucket/runs",
                "https://your-bucket.s3.amazonaws.com",
            ),
        ),
    ),
)
```

The worker writes an output archive beneath the requested prefix. Its result
manifest contains the URI and a presigned GET URL, which the client uses to
retrieve the archive. Allow the actual URL's exact origin as well as the S3
prefix. The destination travels with each request, but callers cannot widen
the deployment's policy.

Scope worker credentials to the permitted prefixes. Delivered tarballs are not
automatically deleted; choose a bucket lifecycle that covers their required
retrieval period. Result retention and presigned access are bounded, and a
credential's earlier expiry can shorten access further.

### Deploy the R2 wait example

The repository includes a small operation for the
[R2 output tutorial](../tutorials/07-compute-backends/05-modal-r2-outputs.ipynb):
[r2_wait.py](../tutorials/07-compute-backends/r2_wait.py) subclasses `WaitTool` as
`R2WaitTool`, named `r2_wait_tool`. It declares the `r2-artisan` runtime secret,
the `artisan-tutorial` output prefix, and the R2 HTTP origin from your environment.
The notebook imports this same class, so deployment and invocation agree.

Set `ARTISAN_S3_BUCKET` and `ARTISAN_S3_ENDPOINT_URL` in your environment or
repository `.env`. Configure the `r2-artisan` Modal Secret with
`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`, and
`AWS_ENDPOINT_URL`. The latter must match the selected R2 endpoint. Use a worker
image with `s3fs` and CA certificates installed.

From the repository root, deploy the example:

```bash
cd docs/tutorials/07-compute-backends
ARTISAN_LOAD_MODULES=r2_wait PYTHONPATH=. pixi run --locked -e dev artisan modal deploy r2_wait_tool
```

The helper declares source overlays for `artisan` and `r2_wait`, so this is a
development deployment from the checkout. Keep the helper importable when
running the notebook. If you change the bucket, endpoint, or policy, redeploy
before invoking the notebook's step.

(presigned-puts-and-external-consumers-capability-mode)=
## Call an endpoint from an HTTP client

External clients can submit files and JSON parameters directly to the endpoint.
For remote inputs, send role-keyed `input_uris` and `input_integrity` maps with
identical keys; each integrity entry needs `content_digest` and `size_bytes`.
Only origins or prefixes already in the deployment's input policy are accepted.

An external client can also pass a presigned PUT URL in `output_store`. Its exact
HTTP origin must be allowed by the deployment. Use a URL signed for the intended
PUT with the required service signature and headers:

```bash
curl -X POST "$ENDPOINT/submit" \
  -H "Modal-Key: $TOKEN_ID" -H "Modal-Secret: $TOKEN_SECRET" \
  -F params='{"seconds": 1}' \
  -F 'files=@input.csv;filename=dataset' \
  -F output_store="$PRESIGNED_PUT_URL"
```

Poll `/result?call_id=...` using the returned call ID. In this mode, the caller
retrieves the archive from its own bucket; the endpoint cannot issue a GET
capability for a caller-owned PUT, and `/download` returns 409. The deployment
must still accept the operation's required inputs and parameters.

Presigned PUT URLs are per-request wire data. `ModalComputeConfig.output_store`
accepts S3 prefixes, so Artisan's pipeline client uses the prefix workflow above.
Capability-only deployments can use authorized GET and PUT URLs without a
worker object-store secret. Service upload limits and Artisan's archive budgets
still apply.

## Related guides

- [Configure Execution](configuring-execution.md) — lifecycle runners, batching,
  and invocation overrides.
- [Op Container Images](op-container-images.md) — build and inspect worker images.
- [Debug a Recorded Execution](debugging-executions.md) — command evidence and
  replay with retained endpoint files.
- [Python API](../reference/python-api.md) — configuration source and docstrings.
