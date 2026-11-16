# Container qualification fixtures

These explicit probes run the [shared two-tool sequence](../providers/README.md)
in native Python, a generated OCI image, and a SIF derived from that image. They
use real processes and the candidate persistence controller; ordinary pytest
discovery does not launch them.

The assertions cover read-only inputs, writable output/work binds, cwd, literal
environment values, separate command dependencies and activation, cancellation
with sibling survival, and hydration of a retained external file after worker
scratch disappears. The controller checks cache eligibility and rejects reuse
when the recorded target or launcher version changes.

## Prepare the targets

Use Linux/x86_64, an available Docker daemon, Apptainer, and independently
installed native worker/tool prefixes from the provider fixture. Copy its
canonical `pixi.toml` and `pixi.lock` unchanged into image preparation. Install
with `pixi install --locked --all` at the final container path
`/opt/provider-probe`; do not relocate an installed prefix. Make any local wheel
referenced by the lock available at its locked path during installation.

The helpers separate preparation from execution:

| Helper | Purpose |
| --- | --- |
| `capture_activation.py` | Capture tool activation and the lock receipt after final-path installation. |
| `package_image.py` | Generate an xz build context and Dockerfile from that installed prefix and the qualification source. It does not install dependencies. |
| `export_oci.py` | Stream a stopped image's root filesystem into a separately identified flattened Docker archive, preserving source/config/rootfs/archive hashes. |
| `run.py` | Launch one actual backend and verify completion, cancellation, sibling isolation and accepted retained bytes. |

Use `--help` for each helper's arguments. A base image must supply compatible
Linux libraries; the generated recipe preserves its immutable image ID or the
explicit base reference supplied by the caller. Keep private receipts and raw
runtime IDs outside the repository.

Keep installation caches and conversion data in dedicated disposable storage.
Check disk headroom before building: Docker image-save can stage complete layers
on daemon disk even when its output file is elsewhere. `export_oci.py` avoids
that exporter and records a distinct flattened image identity. It requires an
image without declared volumes and stops if host free space falls below 500 MB.
Preserve its `derivation.json` alongside the resulting SIF digest.

## Run the probes

Set `NATIVE_PREFIX` to the prepared native directory, `OCI_IMAGE` to the actual
generated image ID, `APPTAINER` to the executable, and `SIF` to the derived file.
Each output directory must be new. Freeze the harness source once for all runs:

```bash
QUAL_ROOT=$(mktemp -d)
cp -a tests/qualification "$QUAL_ROOT/qualification"
chmod -R a-w "$QUAL_ROOT/qualification"

pixi run --locked -e dev python tests/qualification/containers/run.py native \
  "$QUAL_ROOT/native-results" --native-prepared "$NATIVE_PREFIX" \
  --harness "$QUAL_ROOT/qualification"

pixi run --locked -e dev python tests/qualification/containers/run.py docker \
  "$QUAL_ROOT/docker-results" --native-prepared "$NATIVE_PREFIX" \
  --image "$OCI_IMAGE" --harness "$QUAL_ROOT/qualification"

pixi run --locked -e dev python tests/qualification/containers/run.py apptainer \
  "$QUAL_ROOT/sif-results" --native-prepared "$NATIVE_PREFIX" \
  --image "$OCI_IMAGE" --apptainer "$APPTAINER" --sif "$SIF" \
  --harness "$QUAL_ROOT/qualification"
```

The Apptainer command needs the original OCI image ID as well as the SIF: both
the source relationship and resulting SIF bytes enter its target identity. The
runner binds the actual launcher version and frozen harness digest too. Native,
OCI and SIF results are not assumed to share cache identity.

A successful `report.json` records value 16, the two tool runtimes, accepted
external bytes, no cancelled success seal and a completed sibling. Accepted
stores remain beneath the result directory; successful acceptance removes the
original candidate delivery. Keep reports, image/SIF hashes and derivation
receipts before removing only the temporary resources you created.

The exercised CPU profiles used Docker 28.5.1 and non-root Apptainer 1.5.4. On
the qualification host, Apptainer automatically extracted SIF to a temporary
sandbox because squashfuse was unavailable on its PATH. Those results establish
that fallback, not direct FUSE mounting. GPU execution and other platforms need
separate evidence.
