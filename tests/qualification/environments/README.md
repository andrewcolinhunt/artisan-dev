# Canonical native environment preparation

This fixture exercises explicit preparation of one canonical Pixi manifest and
lock. It builds two tiny local Hatch packages from captured source, installs
ordinary wheels at their final prefix, and runs a console command after making
the original source directory unavailable.

Run from a checkout with the development environment installed:

```bash
pixi run -e dev python tests/qualification/environments/run.py \
  --work-root /tmp/artisan-native-example
```

The work directory must not exist. Preparation can download locked packages and
leaves the disposable prefixes, `binding.json`, and `report.json` for inspection.
Remove only that work directory when finished. Earlier Linux x86-64 qualification
used Pixi 0.83.0, Python 3.12.12, Hatchling 1.31.0, and pip 25.3. Other architectures
require their own compatible lock and execution qualification.

The revised fixture checks:

- Both local source packages become noneditable installed wheels whose names,
  versions, dependencies, Python requirements, RECORD contents, and console
  wrappers agree with the captured source and lock.
- The original manifest and lock stay byte-identical. Wheel lowering is derived
  packaging; it does not introduce another authored dependency recipe.
- Literal activation and the requested working directory survive execution.
  Inputs 6 and 4 produce 18 and 12 using the same prepared target.
- Lookup launches no subprocess and never builds, solves, installs, or repairs.
  Declared source, activation, asset, and source file membership changes reject reuse.
- Generated `__pycache__` directories do not invalidate lookup. New excluded
  filenames conservatively do. Unrelated and explicitly excluded synthetic files
  are absent from the captured workspace. An inherited `PIP_TARGET` cannot redirect
  derived-wheel installation.

The private implementation accepts `Environment.from_pixi(absolute_manifest,
name="default")`. The canonical manifest may declare additional exact relative
files in `[tool.dexterity-artisan.environment]` using `assets = ["data/config.json"]`.
That table works in both `pixi.toml` and `pyproject.toml`. Activation scripts and
local wheel references are inferred. Script references in selected features and
the selected inline environment are captured across all their target blocks;
Pixi remains responsible for choosing which scripts run. This conservative
closure avoids duplicating its target selector rules. Extra asset globs,
directories, symbolic links, and escaping paths are rejected. Packaged manifest resources can capture
files, but local source builds require a filesystem package.

The initial local source profile uses static Hatch metadata with explicit
`packages`, `only-include`, `exclude`, and in-root `force-include` selections.
Preparation asks the qualified Hatch backend for actual file selection. Pure
lookup checks selected bytes, selector metadata, bounded tree membership, and
immediate root names/types; it does not implement a second pattern language.
License discovery must stay at the package root. Dynamic metadata, custom
backends/hooks, whole-project selection, and unsupported selectors require an
ordinary prebuilt wheel. Interpreter caches cannot be authored package inputs.

Pixi validates the whole workspace even when installing one environment. Static
metadata for unselected local packages and referenced local wheels are therefore
captured too. Unselected source code is not copied or installed. Only selected
source packages contribute derived wheels and installed import payloads.

This is a bounded software snapshot, not a filesystem sandbox. Activation scripts
that read undeclared files or external paths are unsupported portability inputs;
arbitrary shell reads cannot be inferred. Native target identity binds captured
source, selected platform, and wheel/build identities. Runtime checks require a
completed target and the selected executables; they do not inventory or hash the
installed package files. Manual edits and corruption inside prepared installations
are unsupported and may go undetected. Complete assignment software identity must
also bind selected transport compatibility and the actual launcher/provider target.
The native digest alone does not establish equivalence across providers.

## Prepared containers

The container fixture installs the same captured Pixi source at its final image
prefix. It never copies a host environment. OCI preparation records the base,
build recipe and captured software in immutable image metadata; the prepared
binding selects the image ID and launcher identity.
SIF preparation binds the original OCI ID, original configuration hash and
exported filesystem hash into immutable labels, and records the separately
identified flattened archive used for conversion. Lookup checks captured source,
image/preparation records and target availability. It retains the SIF build digest
without hashing the entire image again. Lookup does not pull, build, install or
repair.

Earlier Linux x86-64 qualification used Docker client/server 28.5.1 and Apptainer
1.5.4 with system `mksquashfs` and `unsquashfs`. Supply an already available Linux
base image by digest and an installed Apptainer executable. Preparation may
download locked packages and create task-owned images and caches:

```bash
pixi run -e dev python tests/qualification/environments/container_prepare.py \
  --cache-root /absolute/owned/container-cache \
  --base-image sha256:YOUR_EXISTING_BASE_IMAGE_DIGEST \
  --apptainer /absolute/apptainer

pixi run -e dev python tests/qualification/environments/container_run.py docker \
  --cache-root /absolute/owned/container-cache \
  --output /tmp/artisan-docker-result

pixi run -e dev python tests/qualification/environments/container_run.py apptainer \
  --cache-root /absolute/owned/container-cache \
  --apptainer /absolute/apptainer \
  --temporary-root /absolute/owned/extraction-scratch \
  --output /tmp/artisan-sif-result
```

Each output directory must be new. In those runs, both launchers produced 18 from
input 6, preserved activation and literal values, rejected writes to the read-only
input, removed ambient Python/loader paths, and stopped a detached command descendant
while an independent sibling completed. The command owner manages Docker by its
exact container identity and Apptainer through the existing process owner.

The SIF profile uses explicit extraction (`--unsquash`) and does not require FUSE.
Its extraction directory needs enough space for the expanded image. Scientific
binds must use ordinary filesystem paths outside `/dev`, `/proc` and `/sys`;
tmpfs under `/dev/shm` can hold images and extraction scratch, but cannot be a
scientific input/output bind in this profile. Commands see the assignment's
scratch and admitted input carriers read-only, with only their execute and
command-scratch directories writable. No host executable fallback is permitted.

Add `--gpu-device 0` to either run command only when device 0 belongs to your outer
allocation. This runs one four-byte driver-API kernel, 41→42, instead of the CPU
ownership cases. Earlier accelerator qualification used NVIDIA A10 with driver
570.195.03. The driver version and `libcuda` bytes participate in the runtime software binding;
physical device identifiers do not. Unset and explicitly empty allocations remain
distinct, and no missing allocation expands to all GPUs. Apptainer passes its
qualified `/.singularity.d/libs` driver path explicitly. This proof does not
qualify a CUDA toolkit, a framework, or another accelerator profile.

For native-worker composition, installed application/schema transport, two
different tool environments and retained file publication, run the
[installed application container probe](../../fixtures/prepared_local_application/README.md#container-tools).
Remove only the recorded disposable directories and exact images when no running
handle uses them; do not prune shared container or package caches.
