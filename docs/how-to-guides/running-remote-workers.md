# Run Remote Workers

Use a configured provider runner to place a whole operation or composite worker
on Modal or Slurm. The worker runs preprocess, native Python execution, and
postprocess. Command operations can use separate tool environments declared on
their definitions.

## Prepare software before submitting work

Install the provider version matching your Artisan version and the application
packages needed by your definitions. Declare worker software in a locked Pixi
project containing Artisan, the provider, and those packages. Tool dependencies
remain in the operations' own `Environment` declarations.

With the optional Modal provider installed and your account configured, this
fragment prepares an imported operation. Replace the project path and operation
with your application:

```python
from pathlib import Path

from artisan.schemas import Environment
from artisan_modal import ModalRunner
from my_application.operations import Align

runner = ModalRunner(
    environment=Environment.from_pixi(Path("worker/pixi.toml").resolve()),
    app_name="alignment-workers",
)
runner.prepare(Align)
```

Preparation can build an image and deploy an assignment entrypoint, so perform
it deliberately as setup. It is separate from pipeline execution. Pass the same
scientific parameters and hardware choices to preparation and submission when
they affect the selected software or deployed worker.

Once prepared, the same compatible image/deployment can serve other operations
and pipelines with covered requirements. Repeated preparation reuses it. Within
a Modal image, operations sharing an environment use one installation; on Slurm,
they share a node-visible prepared prefix or SIF. Different machines or images
may still need their own accessible copy of a target. The current Apptainer
launcher extracts its SIF for each command; see
[Prepare Execute Environments](execute-environments.md#choose-pixi-docker-or-apptainer)
for this runtime cost.

## Submit through the same pipeline API

Pass the configured runner to `pipeline.run(..., step_runner=runner)` or to
`PipelineManager.create(..., default_step_runner=runner)`. Operations and
composites use the same submission interface. No Modal fields belong on the
operation and no execute-only HTTP endpoint is required.

Remote workers need explicit shared storage for staged results and accepted
external files. The Modal provider uses an S3-compatible shared store; supply
its roots and storage configuration to the pipeline. Do not pass local controller
paths as remote storage or embed credentials in operation parameters. Provider
credentials and mounted datasets are runtime configuration, not dependencies in
the Pixi project.

For Slurm, configure the matching scheduler runner from `artisan-submitit`.
The runner owns partition selection, allocations, and node launch. The site's
chosen Pixi or Apptainer targets must be prepared and reachable on the nodes.
Validate against the site's actual launcher and filesystem configuration; a
local image build does not prove scheduled execution works.

## Choose worker packing and checkpoints

A composite unit already executes all its children in one worker. It can invoke
different tool environments without submitting another remote worker for each
child. `batch_strategy.units_per_worker` groups compatible units into an ordered
worker assignment. Each unit retains its own persistence and cache result.

Use separate pipeline steps when an intermediate result needs an independent
checkpoint. Use a composite when children should share an execution boundary
and only selected outputs should be retained.

## Cancellation and verification

Provider cancellation is a request. Artisan confirms cancellation only with
evidence that the owned work never started or has closed. An SDK acknowledgement,
a scheduler state, or a completed result future alone is insufficient. Earlier
accepted units remain available when a later unit fails; unresolved work keeps
its evidence and cannot be reused as a healthy worker.

Before using a new provider setup, run a small installed operation and check its
accepted outputs, tool activation, and cleanup. Exercise cancellation as well as
success. GPU availability and host driver integration need their own check.
See [Configure Execution](configuring-execution.md) and
[Prepare Execute Environments](execute-environments.md).
