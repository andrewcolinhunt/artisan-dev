# Bounded GPU qualification

These supplements test one allocated A10 with a four-byte CUDA driver-API
kernel: copy 41 to the device, increment it to 42, copy it back, and free the
device memory and context. They install no toolkit, compiler, PyTorch or other
framework. Each command has a 20-second subprocess timeout.

`run_gpu.py` uses the same constructed command environment as the CPU sequence.
It preserves allocated GPU visibility, scratch, a synthetic selected credential
and ownership token while excluding accidental Python and loader paths. Preserve
an absent `CUDA_VISIBLE_DEVICES` variable as absent: replacing it with an empty
value masks every GPU. The initial cloud probe exposed this difference; the
corrected helper passed both explicit device 0 and absent visibility cases.

For an already prepared native tool environment on an assigned idle GPU:

```bash
pixi run --locked -e dev python tests/qualification/providers/run_gpu.py \
  /path/to/prepared/.pixi/envs/tool-a/bin/python --scratch /tmp/native-gpu-probe
```

For Docker or Apptainer, launch the real container with its GPU support enabled
and mount this fixture directory read-only. Use the prepared interpreter and
tool prefix inside that target; the command does not install missing software:

```bash
docker run --rm --gpus device=0 --entrypoint \
  /opt/provider-probe/.pixi/envs/worker/bin/python \
  --mount type=bind,src="$PWD/tests/qualification/providers",dst=/probe,readonly \
  IMAGE_ID /probe/run_gpu.py /opt/provider-probe/.pixi/envs/tool-a/bin/python \
  --script /probe/gpu_probe.py --scratch /tmp/gpu-probe
apptainer exec --nv --cleanenv \
  --bind "$PWD/tests/qualification/providers:/probe:ro" IMAGE.sif \
  /opt/provider-probe/.pixi/envs/worker/bin/python /probe/run_gpu.py \
  /opt/provider-probe/.pixi/envs/tool-a/bin/python \
  --script /probe/gpu_probe.py --scratch /tmp/gpu-probe \
  --driver-library-path /.singularity.d/libs
```

Apptainer's `--nv` injects driver libraries into `/.singularity.d/libs`; the
initial clean-environment probe could not load `libcuda.so.1` without this path.
The GPU-only option preserves that explicitly qualified directory. It does not
forward the worker's ambient `LD_LIBRARY_PATH`. Native, Docker and Modal probes
did not require a loader-path override on the tested platforms.

`prepare_slurm_gpu.py` is only for a fresh disposable container based on the
tested provider scheduler image. It requires `/.dockerenv`, an explicit
`QUALIFICATION_DISPOSABLE_CONTAINER=1`, host-matching `ARTISAN_DOCKER_UID` and
`ARTISAN_DOCKER_GID`, and an owned writable `/gpu-control` bind. Mount the already
prepared `/pixi-env` volume read-only. Do not mount a shared cache or host home.
The script registers one `gpu:a10:1`, keeps prepared software read-only, and
limits startup ownership changes to the container home and owned control bind.

Within that configured disposable scheduler, request the actual GPU explicitly:

```bash
sbatch --wait --partition=debug --cpus-per-task=1 --mem=512M \
  --time=00:01:00 --gres=gpu:a10:1 --output=/gpu-control/batch.json \
  --wrap='/pixi-env/probe/.pixi/envs/worker/bin/python /qualification/run_gpu.py /pixi-env/probe/.pixi/envs/tool-a/bin/python --scratch /gpu-control/batch-scratch'
salloc -p debug -n1 --mem=512M --time=00:01:00 --gres=gpu:a10:1 \
  srun --exact --mpi=none -n1 -c1 --mem=512M --gres=gpu:a10:1 \
  /pixi-env/probe/.pixi/envs/worker/bin/python /qualification/run_gpu.py \
  /pixi-env/probe/.pixi/envs/tool-a/bin/python --scratch /gpu-control/step-scratch
```

The Modal supplement reuses the actual image ID from a completed CPU report:

```bash
pixi run --locked -e dev python tests/qualification/providers/run_modal_gpu.py \
  QUALIFIED_MODAL_IMAGE_ID /tmp/modal-gpu-report.json
```

It creates one ephemeral app, makes one A10G call with one CPU, 512 MiB,
one container, a 30-second input timeout and retries zero, then closes the app.
It does not build an image, deploy an app, install packages or use volumes.

Native A10, Docker, Apptainer, Modal A10G and allocated Slurm batch/step probes
passed the kernel and environment assertions. The Slurm tests observed device 0
in the allocated visibility variables and completed accounting for the exact
batch and step.
The local driver reported API version 12080; the cloud driver reported 13030.
These observations qualify this small single-GPU profile. They do not establish
arbitrary CUDA toolkit, model workload, GPU count or cross-runtime cache
equivalence. The single-GPU scheduler does not prove isolation between multiple
GPU allocations. The container fixtures retain their own OCI/SIF execution
evidence.
