# Prepare Tool Container Images

Tool containers are built from the same locked Pixi project used for native
execution. Declare that project with `Environment.from_pixi` on the operation;
select Docker or Apptainer on the runner. The complete preparation example is in
[Prepare Execute Environments](execute-environments.md#choose-pixi-docker-or-apptainer).

## Choose what belongs in the tool image

A command environment contains its executable, dependencies, and activation
settings. A Python operation using `execute_as_tool=True` also needs Artisan and
its own application package. Preprocess and postprocess dependencies belong in
the worker environment. Include application resources in their installed packages
or declare additional project assets in the Pixi manifest.

Credentials, mounted input data, and model weights outside the prepared software
remain runtime inputs. Building an image does not freeze their contents.

## Build explicitly

Configure `LocalRunner(tool_backend="docker", docker=..., base_image=...)` and
call `runner.prepare(Operation)`. The base image must already be available to the
configured local Docker daemon. Preparation builds the tool environment from the
manifest and lockfile and records its actual software identity.

For Apptainer, configure `tool_backend="apptainer"` and an `ApptainerRuntime` as
well. Preparation derives a SIF from the prepared OCI image. The execution host
needs the prepared SIF and the qualified Apptainer launcher; it does not solve
Pixi dependencies for each command.

Normal execution verifies the selected prepared target. A changed declaration,
missing image, or changed launcher requires explicit preparation or configuration
correction rather than an implicit pull or rebuild.

## Place the worker remotely

The tool image and the worker allocation have different responsibilities. Use
the selected provider runner to prepare and place the worker with its required
tool software. Modal deployment and Slurm scheduling belong to those providers.
See [Run Remote Workers](running-remote-workers.md).

## Verify

Run a small representative command through the pipeline and inspect its accepted
outputs. Check Python-as-tool imports when applicable. Verify GPU visibility and
cancellation on the actual runtime before relying on that configuration; a
successful build alone establishes neither.
