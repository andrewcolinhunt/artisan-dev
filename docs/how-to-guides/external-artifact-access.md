# Use External Artifacts Without Copying Their Files

Use an external-file artifact when an operation should read an existing immutable
installation, such as model weights or a reference database. Artisan retains its
file descriptor and ordinary artifact dependencies. The payload stays at its
prepared location.

A model-weights artifact adds model-family semantics to that generic behavior.
It uses the same inputs, provenance and cache rules as other artifacts.

**Prerequisites:** an installed Artisan environment and an existing local
installation you control. Remote execution additionally needs a prepared runner
and its storage/service access; see [Run Remote Workers](running-remote-workers.md).

## Select a checkpoint and prepare its existing files

This complete CPU example uses a synthetic checkpoint: a `scale.txt` file
containing the number two. The installed example command multiplies each input
value by that number. It does not download or copy the checkpoint.

```python
from pathlib import Path
from tempfile import TemporaryDirectory

from artisan.execution import run_operation
from artisan.operations.examples.external_files import scale_with_weights
from artisan.orchestration.runners import LocalRunner
from artisan.schemas import MetricArtifact
from artisan.storage.external_access import prepare_external_files

with TemporaryDirectory() as scratch:
    root = Path(scratch).resolve()
    weights_root = root / "weights"
    weights_root.mkdir()
    # Explicit setup for this synthetic example; real tools supply an existing installation.
    (weights_root / "scale.txt").write_bytes(b"2\n")

    values = [
        MetricArtifact.draft(
            content={"value": value}, original_name=f"value-{value}.json"
        ).finalize()
        for value in (3, 7)
    ]
    selection = scale_with_weights(values)
    assert selection.origin == "preset"
    assert len(selection.inputs["weights"]) == 1
    print(selection.weights.artifact_id)

    prepared = prepare_external_files(
        weights_root, selection.weights.external_descriptor()
    )
    # Explicit command-software preparation; later calls reuse it.
    LocalRunner().prepare(selection.operation)
    result = run_operation(
        selection.operation,
        inputs=selection.inputs,
        external_files={selection.weights.artifact_id: prepared},
        work_dir=root / "calls",
    )
    assert result.success
    assert [item.values["value"] for item in result.artifacts["result"]] == [6.0, 14.0]
    assert not list((root / "calls").rglob("scale.txt"))
```

The temporary directory is removed at the end. For repeated runs, keep the
prepared installation in a persistent location and reuse its `PreparedExternalFiles`
record. `prepare_external_files` hashes the existing files and publishes a small
completion receipt. Calling it again explicitly rechecks their bytes; ordinary
execution checks the receipt, layout and sizes without rehashing payloads.
Interrupted setup can leave an incomplete receipt. Preparation refuses to
overwrite it; inspect the installation and remove incomplete preparation files
explicitly before retrying.

Keep completed installations immutable. Read-only consumer mounts do not stop an
external owner from modifying their source. In particular, same-size byte changes
with an unchanged receipt can escape ordinary checks. Change content by preparing
a new installation; call preparation explicitly when you need full reverification.

## Make defaults visible and overrides explicit

`scale_with_weights` is an example-owned helper. It selects a pinned descriptor
and returns an inspectable operation/input mapping. It performs no file access
and does not execute anything. Its `origin` label is explanatory information,
separate from scientific inputs and cache identity.

Pass one `ModelWeightsArtifact` as `weights=` to replace the default. Passing the
same checkpoint explicitly produces the same effective inputs and results.
The execution engine always receives the selected artifact as an ordinary input;
it never discovers an omitted checkpoint or chooses whichever files are installed.

This example declares `CROSS_PRODUCT` for exactly two input roles. One checkpoint
therefore serves every value. To compare several checkpoints, explicitly supply
multiple artifacts in the operation's `weights` stream, with a prepared binding
for each. That is a requested sweep. For operations with several already paired
input streams, preserve their pairing instead of applying a cross product to all
roles; existing configuration references can carry a shared artifact dependency.

For your own tool, declare its exact file descriptor and compatibility checks in
the tool package. Use `ExternalFilesArtifact.draft(...)` for generic bundles or
`ModelWeightsArtifact.draft_weights(..., model_family=...)` for model semantics.
A checkpoint change changes the artifact identity. A different physical location
for the same descriptor does not.

## Use a retained pipeline

External artifact types are optional. Import `artisan.schemas.external_artifacts`
before creating a new store and include it in the pipeline's trusted
`application_modules`, alongside your own application modules. The example module
imports those types explicitly; the ordinary examples package does not.

Configure a local runner with the same explicit mapping:

```python
from artisan.orchestration.runners import LocalRunner

# Continue with a persistent prepared installation and finalized weights artifact.
runner = LocalRunner(external_files={weights.artifact_id: prepared})
```

Use the selected operation and input mapping in the pipeline as usual. Ordinary
artifact references support sharing the checkpoint across steps. The run retains
the descriptor, exact dependency membership and selected access evidence; it does
not create a second weights payload in managed storage.

Register optional types only when initializing new empty stores. Loading them
makes their tables part of that process's required schema. An older populated
store without those tables must be inspected in a separate process with its
original application-module selection. Do not add tables to its manifest or
rewrite historical evidence to enable the feature.

## Expose the installation to Docker or Modal

Docker uses the local runner's explicit external-file mapping with the prepared
Docker tool backend. The selected installation is mounted read-only through the
existing command adapter. The operation receives its runtime path through the
artifact's `external_root`, without putting host paths into scientific parameters.
The [execution environment guide](execute-environments.md) covers software and
launcher preparation. Installations must use canonical absolute directories
outside image system paths, prepared software, scratch and other mounts.
External binds require recursive read-only support; Docker rejects hosts that
cannot enforce it. Apptainer external access is unsupported.

For Modal, first provision the files and completed receipt into an existing
versioned directory on a named volume. The runner does not create or populate it.
The following configuration fragment assumes that setup and your worker software
are already prepared:

```python
from pathlib import Path

from artisan_modal import ModalRunner, ModalVolumeBinding
from artisan.storage.external_access import PreparedExternalFiles

remote_files = PreparedExternalFiles(
    root=Path("/mnt/artisan-external/reference/v1"),
    descriptor=prepared.descriptor,
    receipt_digest=prepared.receipt_digest,
)
runner = ModalRunner(
    environment=worker_environment,
    app_name="external-files-example",
    volumes=(
        ModalVolumeBinding(
            name="reference-files",
            environment_name="main",
            mount_path=Path("/mnt/artisan-external/reference"),
        ),
    ),
    external_files={weights.artifact_id: remote_files},
)
```

The volume mount and actual volume identity are checked independently of the
artifact's scientific identity. Modal may expose a mount through a platform-owned
path alias; Artisan accepts that alias only after verifying the actual mount,
and rejects symlinks within the installation. Modal mounts a whole volume, so trusted tool code
can see other files in it. Only the selected descriptors become dependencies and
receive artifact accessors. Keep completed version directories unchanged.
Modal retained pipelines still require the usual shared S3-backed roots.

## Inspect or replay a recorded run

Inspection, recovery and descriptor export do not need the payload installation
to remain accessible. The retained descriptor identifies expected content; it
is not a self-contained archive of the external files. Explicit payload
materialization or conversion requests are unsupported for these references.

A cache hit can reuse the recorded dependency identity without a fresh remote
payload probe. That does not establish that the external installation remains
available. A new execution checks the selected installation before science.
Replay retains the recorded checkpoint rather than re-evaluating a tool default;
an explicitly bound replica may supply the same content at a different location.
