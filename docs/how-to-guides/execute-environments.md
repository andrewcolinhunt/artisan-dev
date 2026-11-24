# Prepare Execute Environments

Declare a command's software with `Environment.from_pixi(...)`, then prepare it
through the runner that will execute the operation. The Pixi manifest and lockfile
are the source for native Pixi installations, OCI images, and Apptainer images.
Provider settings choose where to run that software.

Managed command execution, including `execute_as_tool=True`, currently requires
Linux x86_64 for preparation and launch. Native Python workers also support
Linux aarch64.

## Declare a tool environment

Keep `pixi.toml` and `pixi.lock` beside your operation package and include them in
its distribution. In an existing command operation, add this field:

```python
from pathlib import Path

from artisan.schemas import Environment

environment: Environment = Environment.from_pixi(
    Path(__file__).resolve().with_name("pixi.toml"),
)
```

This is a class-body fragment; the operation still needs its input/output specs
and lifecycle methods. Pass `name="tools"` to select a named Pixi environment.
The declaration records a location without installing anything at import time.
Several operations can import the same `Environment` constant. They share one
prepared installation for that definition and target; operation names and
composite call paths do not create additional installations.

Put dependencies and activation settings in the Pixi project. Commit its lockfile.
Preparation captures the manifest, lockfile, referenced activation scripts, and
supported package inputs. Additional source files can be listed under
`[tool.dexterity-artisan.environment]` with `assets = ["relative/path"]`. Files must stay
within the project; source paths cannot traverse symlinks. Installable packages
and wheels are the portable packaging path.

The field applies to `execute_command()` and the child process used by
`execute_as_tool=True`. Preprocess and postprocess run in the Artisan worker.
A native `execute_function()` also runs in that worker and cannot switch Python
through this field. Python-as-tool operations need Artisan and the operation's
package installed in their tool environment.

## Prepare, then run

For your importable `Align` operation, prepare the software explicitly:

```python
from artisan.orchestration.runners import LocalRunner
from my_application.operations import Align  # Your installed operation package.

runner = LocalRunner()
runner.prepare(Align)
```

Then pass `step_runner=runner` to your pipeline submission, or use
`PipelineManager.create(..., default_step_runner=runner)`.

`prepare` installs or builds missing software. It prepares each distinct command
environment in a composite once and reuses existing compatible targets. If
parameters change which tools the composite uses, supply those parameters to
both preparation and submission.

For native Pixi preparation, set `PIXI_OFFLINE=true` to use packages already
available in the local cache. Preparation fails if required packages are
unavailable in offline mode.

Preparation uses the operation or composite's authored `runner_resources` by
default. A supplied `RunnerResources` model patches only its explicit fields,
matching submission. If you override the eventual allocation, pass the same
resource patch to preparation when the provider's prepared profile depends on it.
Nested children do not contribute separate worker allocations.

Preparation is reusable across operations, pipelines, and runner instances that
can access the same prepared cache or deployment. Another operation using packages
and tools from those same prepared targets needs no separate installation. A new
pipeline does not need to call `prepare` again. Repeating the call reuses completed
preparation.

Each submission resolves its distinct environments once, then binds every command
to those targets. Normal execution checks preparation records, required paths,
and runtime compatibility; it does not solve dependencies, install packages,
build images, or deploy a provider. Missing preparation requires an explicit
`prepare` call. There is no fallback to the worker's environment.

### Treat prepared software as immutable

Software identity comes from the manifest, lockfile, selected environment and
platform, referenced source files, and relevant build inputs. Changes to these
inputs require preparation of a new target. Submissions already resolved keep
their selected target.

Artisan trusts completed managed installations. It does not hash installed
package files or an entire Apptainer image during normal execution. Do not edit
installed files inside a prepared target; change its source definition and
prepare again. Arbitrary edits or corruption inside an installation may go
undetected. This contract does not change artifact-content verification or
result acceptance.

Older preparation records require preparation again with the matching updated
core and provider versions. This preparation change does not introduce another
results-store format change.

An operation with no `environment` declaration intentionally uses its worker's
software. `LocalRunner()` uses the current Python installation. For a separate
worker installation, pass another `Environment` as `LocalRunner(environment=...)`;
that project must include Artisan and the application packages. These two
declarations have different owners: the runner declares worker software, and
the operation declares command software. If both select the same native
environment, they share its installation and resolved target.

The default local runner observes the current interpreter without claiming a
locked dependency closure, so its results are not eligible for persistent cache
reuse. Use a prepared worker `Environment` when you need that guarantee.

## Choose Pixi, Docker, or Apptainer

`LocalRunner(tool_backend="pixi")` is the default. Container execution uses the
same operation declaration and an explicit launcher. This setup fragment assumes
the launchers and base image are already available:

```python
from pathlib import Path

from artisan.orchestration.runners import ApptainerRuntime, DockerRuntime, LocalRunner

docker = DockerRuntime(executable=Path("/usr/bin/docker"))
runner = LocalRunner(
    tool_backend="apptainer",
    docker=docker,
    apptainer=ApptainerRuntime(executable=Path("/usr/bin/apptainer")),
    base_image="python:3.13-slim",
)
```

Call `runner.prepare(Align)` with your operation. For Docker execution, use
`tool_backend="docker"` and omit `apptainer`. Preparation builds the OCI image
from the locked Pixi project; Apptainer preparation converts that image to SIF.
The command runs in the chosen tool environment while the worker retains the
Python lifecycle. There is no second dependency list in a Dockerfile.

Operations sharing an Apptainer target reuse the prepared SIF. The current
launcher uses `--unsquash`, which extracts that SIF into temporary storage for
each command. Preparation reuse avoids rebuilding the image; it does not remove
this extraction cost. Native Pixi commands share the installed prefix directly.

The current launcher checks require Docker client/server 28.5.1 and Apptainer
1.5.4. Container preparation needs a local Docker builder and an existing base
image. Running a prepared SIF needs Apptainer and access to the prepared files.
Check the installed runtime docstrings for supported host and GPU settings.

## Use a remote provider

A provider runner owns worker placement, hardware, credentials, and deployment.
Operations keep the same `Environment` declarations. Prepare the operation or
composite with that provider's runner, then pass the configured runner to
`pipeline.run(..., step_runner=runner)` or set it as the pipeline default.

Modal places the worker in its prepared image. Slurm places it in a scheduled
allocation and uses the site's selected tool launcher. Provider installation and
resource configuration belong to the provider package; a local preparation does
not establish that a remote node can access or execute those targets.

See [Configure Execution](configuring-execution.md) for batching and placement,
and [Write Composite Operations](writing-composite-operations.md) for operations
that share a worker while using different tool environments.
