# Op Container Images

Build a container image for a command operation, then use that image for a
Modal endpoint or a direct Docker invocation.

**Prerequisites:** A command operation in an importable package, Docker for
local builds, and registry access if you intend to publish the image. See
[Deploy Tool Endpoints](deploying-tool-endpoints.md) for endpoint configuration.

## The image-content contract

A production operation image should contain its tool binaries, dependencies,
Artisan, and operation package. Include the package in the Dockerfile so a
consumer can run it without source files from the deployment machine.

Keep required nonsecret tool settings in the image's `ENV` declarations. Supply
credentials at runtime. Modal adds its declared environment, secrets, and volume
mounts; a direct Docker caller must provide equivalent settings when the tool
needs them. Sharing an image does not make those runtime environments identical.

The build command expects `docker/<image-name>/Dockerfile`, where
`<image-name>` is the last path segment of the image reference without its tag
or digest. For `ghcr.io/dexterity-systems/artisan-worker:latest`, it uses
`docker/artisan-worker/Dockerfile`.

Install CA certificates in minimal runtime images and verify HTTPS access in the
actual worker environment. A local Docker smoke test alone does not verify
Modal's environment, credentials, or mounted files.

## Point an operation at its image

Add the image setting to your existing command operation. This fragment omits
its input/output declarations and execution methods. Both deployment and
`artisan op image` read the class-level setting:

```python
from artisan.operations.base import OperationDefinition
from artisan.schemas import ComputeProvider, ModalComputeConfig


class ProcessData(OperationDefinition):
    ...
    compute_provider = ComputeProvider(
        modal=ModalComputeConfig(image="ghcr.io/dexterity-systems/process-tool:0.3.1")
    )
```

The default image is `artisan-worker`. Custom operations still need their
package and dependencies installed in the selected image.

## Build the image

From the repo root, make the operation discoverable and build its image:

```bash
export ARTISAN_LOAD_MODULES=my_package.ops
artisan docker build process_data
```

The command resolves the declared image tag and builds its conventional
Dockerfile. Use a writable tag when building; select a registry digest after
publishing. When you are ready to publish the tagged image:

```bash
docker push "$(artisan op image process_data)"
```

## Resolve the image from a harness

An external script can resolve the operation's image:

```bash
IMAGE=$(artisan op image process_data)
docker run --rm -v /data/weights:/weights "$IMAGE" process --input ...
```

`--json` also reports the declared environment, volume names, secret references,
and hardware settings:

```bash
artisan op image process_data --json
```

For example, an operation that also declares a weights volume and an A100 GPU
would report those settings:

```json
{
  "image": "ghcr.io/dexterity-systems/process-tool:0.3.1",
  "env": {},
  "volumes": { "/weights": "model-weights" },
  "secrets": [],
  "gpu": "A100",
  "cpu": null,
  "memory_mb": null,
  "timeout": 3600
}
```

Where Modal mounts named volumes, a direct Docker caller can bind-mount local
directories at the same paths. Secret names in this output are references; the
caller must supply credentials through its own runtime.

Command operations wrapping an external tool use that tool's CLI. Operations
whose body is Python
([`execute_as_tool`](writing-creator-operations.md#execute-as-tool))
run through the Artisan CLI installed in the image:

```bash
IMAGE=$(artisan op image transform_data)
docker run --rm -v /data:/data "$IMAGE" artisan op run mypkg.ops:TransformData \
    --params '{"batch_size": 16}' --inputs '{"data": "/data/input.csv"}'
```

## Pin for production

The upstream Docker workflow publishes `artisan-worker` with `latest` and
`sha-<short>` tags when relevant image inputs change on `main`. The workflow
skips publishing from forks. A tag identifies a name in the registry; use the
resolved image digest when you need to pin the exact image bytes.

The image does not freeze mounted files, downloaded weights, credentials, or
other external state. Record and verify those inputs separately when they affect
results. Endpoint command-contract validation does not inspect image contents.

## Use source overlays during development

During development, overlay local package sources onto the worker image:

```bash
artisan modal deploy process_data --overlay artisan --overlay mypkg
```

The overlay replaces the image's packaged source for that deployment. You can
also declare `ModalComputeConfig(local_python_sources=[...])`; the built-in
example operations use this for checkout-based development. Remove overlays
from production declarations and rebuild the image with the intended code.

## Verify

```bash
export ARTISAN_LOAD_MODULES=artisan.operations.examples
artisan docker build wait_tool          # builds docker/artisan-worker/Dockerfile
docker run --rm "$(artisan op image wait_tool)" bash -c 'echo ok'
```

These commands verify image resolution, building, and container startup. Also
run the operation's actual tool with a small valid input to check its runtime
dependencies. Test the deployed endpoint separately if you will use Modal.

---

## Cross-references

- [Execution Flow](../concepts/execution-flow.md) -- how compute routing and
  remote execution fit the pipeline step lifecycle
- [Running on Modal](../tutorials/07-compute-backends/04-modal-execution.ipynb)
  -- deploy and run an op on Modal end-to-end
- [Configuring Execution](configuring-execution.md) -- set compute providers and
  resources on an op
