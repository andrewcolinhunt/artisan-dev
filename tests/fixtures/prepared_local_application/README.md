# Prepared local application fixture

This package supplies an application-defined metric model, native operations and
two command operations for installed-wheel qualification. The command tools use
`packaging` 24.2 and 25.0 in separate environments from one canonical Pixi
manifest. The scientific sequence maps an input value of 5 to 16, exports a file
and an ordinary metric, and stages them before worker scratch is removed.

`create_profile.py --destination /absolute/owned/directory` copies checkout source
and this application into a new source snapshot. It pins the worker dependencies
to versions installed in the development interpreter. Run it from the checkout's
locked development environment. It copies source files only; it never copies an
installed environment. The resulting `source/pixi.toml` defines `worker`, `tool-a`
and `tool-b`; explicitly lock that manifest with the qualified Pixi release,
then call `prepare_native_environment` for each declaration using an owned cache
root. Explicitly call `prepare_command_witness(cache_root)` for the command
launcher capability. Save the returned worker binding outside the prepared workspace.

The Linux x86_64 command witness uses a bundled, locked Pixi compiler and sysroot
during explicit preparation. It records the build environment, selected compiler
inputs, build recipe and resulting static binary. Lookup and execution verify the
existing binary without requiring the build environment. Tool environments do not
need Python to run the witness.

Preparation downloads and installs software. Select an owned location with enough
space for all prefixes and configure task-owned Pixi and uv caches when shared
cache writes are inappropriate. Prefixes must be installed at their final paths.
The execution probe performs no installation or rebuilding.

Run the installed worker's Python as the controller so both sides use the same
ordinary core and application wheels:

```sh
/path/to/prepared/worker/bin/python -I -m prepared_local_application.probe \
  --worker /absolute/worker-binding.json \
  --manifest /absolute/profile/source/pixi.toml \
  --cache /absolute/prepared-targets \
  --pixi /absolute/pixi \
  --work /absolute/new-probe-directory \
  --report /absolute/probe-report.json
```

The probe measures the installed worker Python, core and application schema
before submitting live objects. It verifies native phase placement; activated
tool isolation; literal runtime values; exact output lineage; sealed file
hydration after worker exit; redelivery; changed numeric parameters without
rebuilding; and Python/core/application/payload mismatch rejection before a
harmless deserialization canary can run.

It also exercises sequential units in one actual worker process, distinct unit
scratch/delivery/seals, different synthetic runtime values and their restoration,
an earlier success followed by failure, and command cancellation with a surviving
packed sibling. A native Python phase waiting inside the shared process cannot
be forcibly stopped independently. Its per-unit cancellation remains `UNKNOWN`
while that phase runs; the probe releases it and checks that the next unit still
runs. These private launchers do not reserve CPU or memory and do not accept
staged results into a durable store.

## Container tools

Use a fresh source profile to qualify an installed native worker with OCI and
derived SIF command tools. `prepare_containers.py` prepares the worker and both
tools from the same `source/pixi.toml` and lock. The core is installed only in the
native worker; each tool image contains its selected environment.

```bash
pixi run -e dev python tests/fixtures/prepared_local_application/create_profile.py \
  --destination /absolute/owned/container-profile
pixi lock --manifest-path /absolute/owned/container-profile/source/pixi.toml
pixi run -e dev python tests/fixtures/prepared_local_application/prepare_containers.py \
  --profile /absolute/owned/container-profile \
  --base-image sha256:YOUR_EXISTING_BASE_IMAGE_DIGEST \
  --apptainer /absolute/apptainer

/path/to/prepared/worker/bin/python -I -m prepared_local_application.container_probe docker \
  --worker /absolute/owned/container-profile/worker-binding.json \
  --manifest /absolute/owned/container-profile/source/pixi.toml \
  --cache /absolute/owned/container-profile/targets \
  --pixi /absolute/pixi \
  --work /tmp/artisan-installed-docker \
  --report /tmp/artisan-installed-docker.json
```

Repeat the last command with `apptainer`, new work/report paths,
`--apptainer /absolute/apptainer`, and
`--temporary-root /absolute/owned/extraction-scratch`. The prepared worker prefix
is printed during preparation and recorded in `worker-binding.json`.

The container probe expects the same scientific results as native execution:
16 and 18 after a numeric-only parameter change, two distinct activated tool
interpreters, native phases in the worker, and exact file/metric associations.
It hydrates the sealed file after worker exit and scratch removal. The same
prepared software serves both numeric cases. Container and native software
identities remain distinct. No preparation occurs during this probe.

## Persisted admissions and controller execution

The application metric has its own registered type and content table. This lets
an installed worker hydrate its exact model from persisted admission data.
`admitted_probe` uses the private local runner and finite-step controller to
publish admissions, launch worker packs, accept sealed results and read current
step outputs.

```sh
/path/to/prepared/worker/bin/python -I -m prepared_local_application.admitted_probe pixi \
  --manifest /absolute/profile/source/pixi.toml \
  --cache /absolute/prepared-targets \
  --pixi /absolute/pixi \
  --work /absolute/new-controller-proof \
  --report /absolute/controller-proof.json
```

The command calls explicit preparation first. It then disables preparation entry
points before running the controller, so a cache lookup or execution that tries
to install, build or qualify software fails the proof. It checks two sequential
units in one worker process, ordered values of 16 and 18, current cache reuse,
a numeric change using the same software identity, and downstream localization
of accepted external files. A separate packed pair checks partial success when
one native operation raises a confirmed scientific failure, including diagnostic
redaction and owner cleanup.

For Docker command tools, replace `pixi` with `docker` and supply
`--docker /absolute/docker --base-image sha256:YOUR_EXISTING_BASE_IMAGE_DIGEST`.
The worker remains native. Preparation may reuse exact existing tool images;
it never copies an installed prefix. Use a new work directory and report path
for each backend. This probe does not qualify native worker GPU allocation or
classify an ambiguous launcher exit as a confirmed tool failure.

## Shared computation and publication

`run_shared_execution.py` compares one deterministic creator through a direct
call, an executor session, a configured callback, a prepared pipeline worker and
a configured callback inside that worker. All five paths must return value `6`
for input `3`, with the same artifact ID and an explicit edge from the input.
The pipeline records three steps: the seed and two parent executions. The callback
creates no extra step. Direct and session calls create no store.

Create a fresh profile from the checkout, then run:

```bash
pixi lock --manifest-path /absolute/profile/source/pixi.toml
pixi install --locked --manifest-path /absolute/profile/source/pixi.toml -e worker
pixi run --locked --manifest-path /absolute/profile/source/pixi.toml -e worker \
  python tests/fixtures/prepared_local_application/run_shared_execution.py \
  --profile /absolute/profile \
  --cache /absolute/prepared-targets \
  --output /absolute/new-shared-execution-proof
```

Run these commands from the checkout. The driver explicitly prepares the pipeline
worker before execution and writes `proof.json` after all assertions pass. The
controller uses the profile's installed environment; the pipeline uses a separate
prepared prefix from the same source. This CPU proof does not require model weights
or a GPU.
