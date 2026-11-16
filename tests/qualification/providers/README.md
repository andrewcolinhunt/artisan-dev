# Provider qualification fixtures

These retained feasibility probes exercise a prepared worker and two separately
installed command environments. They cover the prototype protocol described
below and use explicit resources outside ordinary pytest discovery.

For current production coverage, see the [environment qualification checks](../environments/README.md),
the [Modal provider tests](../../../packages/artisan-modal/tests/), and the
[remote worker guide](../../../docs/how-to-guides/running-remote-workers.md).

The worker receives an inert compatibility header plus serialized bytes. It
checks Python, serializer and schema packages, framework and application source,
the prepared lock, and tool selection before deserialization. The payload runs
native/pre/command/post phases, stages one external file and an associated metric,
then deletes its working copy. `accept_candidates.py` verifies those selected
bytes after worker exit and checks cancellation, a competing delivery, exact
cache results, and a different provider target with the same core and lock.

Use an already built candidate Artisan wheel. Every prefix must be installed in
its final location; do not copy or mount another checkout's installed environment.
`prepare.py` creates an isolated manifest and lock beneath the supplied directory.

```bash
pixi run --locked -e dev python tests/qualification/providers/prepare.py \
  /tmp/provider-probe --artisan-wheel "$PWD/dist/dexterity_artisan-0.2.0.dev0-py3-none-any.whl"
pixi run --locked -e dev python tests/qualification/providers/run_local.py \
  /tmp/provider-probe /tmp/provider-results
pixi run --locked -e dev python tests/qualification/providers/accept_candidates.py \
  /tmp/provider-acceptance /tmp/provider-results/first/delivery/candidate \
  /tmp/provider-results/redelivery/delivery/candidate \
  --descriptor /tmp/provider-results/first/software-descriptor.json
```

Use the actual launchers in [the container fixtures](../containers/README.md) for
OCI and Apptainer:

```bash
pixi run --locked -e dev python tests/qualification/containers/run.py docker \
  /tmp/docker-provider-results --native-prepared /tmp/provider-probe \
  --image IMAGE_ID --harness /path/to/frozen/qualification
pixi run --locked -e dev python tests/qualification/containers/run.py apptainer \
  /tmp/apptainer-provider-results --native-prepared /tmp/provider-probe \
  --apptainer /path/to/apptainer --sif /path/to/image.sif --image SOURCE_IMAGE_ID \
  --harness /path/to/frozen/qualification
```

The identity flags on `run_local.py` describe an already launched target; they
do not launch Docker or Apptainer. The software digest is a closed experimental
descriptor, not a claim that different physical runtimes are cache-equivalent.
Successful acceptance moves the candidate into
the test controller's evidence store and removes its delivery directory.

The qualified CPU stack uses Pixi 0.83.0, Python 3.12.12, cloudpickle 3.1.2,
Pydantic 2.12.5, Polars 1.37.1, Delta 1.4.0 and Arrow 26.0.0. Command prefixes install
packaging 24.2 and 25.0 separately. The worker also verifies the built Artisan
wheel's exact Python source and an application-owned schema's exact source.

`run_slurm.py` needs working scheduler commands, the prepared worker interpreter,
a `debug` partition, a shared output directory, and the actual scheduler image ID:

```bash
/path/to/prepared/.pixi/envs/worker/bin/python tests/qualification/providers/run_slurm.py \
  /path/to/prepared /path/to/shared/results --image-id sha256:ACTUAL_IMAGE_ID
/path/to/prepared/.pixi/envs/worker/bin/python tests/qualification/providers/run_step_cancellation.py \
  --output /path/to/shared/cancellation-results
```

The first script runs one batch job, one exact job step and one controlled
requeue. Each entry creates a fresh delivery and node-local scratch directory.
The second checks an actual provider cancellation call targeting one `job.step`,
its descendants, an unstarted queued launcher, and a surviving sibling. Install
the candidate `artisan-submitit` package into the prepared worker, using
`prepare.py --provider-root PATH` when preparing that environment. Run that
provider's existing integration tests against the candidate core as well.

Slurm 21.08.5 passed both launch modes, a real requeue, and exact-step sibling
isolation. Its accounting assigns Start==End to a cancelled held job; the provider
therefore reports FAILED/UNKNOWN and accepts no outputs. Preserve this nuance:
a passing baseline test can deliberately validate unknown cancellation rather
than prove the queued job never started. Requeue has a two-minute scheduler
delay; the probe allows a bounded three-minute wait.

```bash
pixi run --locked -e dev python tests/qualification/providers/run_modal.py \
  dist/dexterity_artisan-0.2.0.dev0-py3-none-any.whl /tmp/modal-provider-results
```

This uses the current configured Modal account with SDK 1.4.1 and explicit image
builder 2025.06. It creates an ephemeral app and Volume, never deploys an app,
and authors at most eight CPU inputs: one container, one CPU, 512 MiB and a
60-second input timeout, with retries zero. Actual cancellation caused provider
redelivery even with retries zero; every entry therefore gets a new delivery UUID.

Modal Volume rejects the local publication primitive `os.link`. The fixture
seals locally, uploads and commits the payloads, then uploads and commits the
seal. One input deliberately omits that seal. The controller downloads bytes
after the app context closes, before closing the ephemeral Volume. Verify them:

```bash
pixi run --locked -e dev python tests/qualification/providers/accept_candidates.py \
  /tmp/modal-provider-acceptance /tmp/modal-provider-results/volume/first-*/candidate \
  /tmp/modal-provider-results/volume/sibling-*/candidate \
  --descriptor /tmp/modal-provider-results/software-descriptor.json \
  --partial /tmp/modal-provider-results/volume/partial-*/candidate
```

The final CPU probes verified that selected bytes remain readable after worker
exit, cancellation and losing deliveries cannot replace a winner, partial
uploads cannot populate cache, and changing only the physical provider target
prevents cache reuse. Child environments preserve explicit literals, activation,
scratch, a synthetic selected credential and ownership token while excluding
worker Python and loader paths. These probes do not establish GPU support.

Keep live resource scope bounded and raw runtime IDs/logs outside the repository.
Container startup must not recursively change ownership of a mounted shared
cache: set the host UID/GID explicitly and place cache mounts outside startup
ownership traversals. Installed environments must remain independent.
