# Export a Sanitized Inspection Store

Use a publication export when you want to share recorded results while omitting
selected local paths from execution evidence. The export preserves scientific
artifact content and IDs, provenance, metrics, and historical run identities.
It creates a self-contained inspection manifest and verified content copies.

## Create and inspect a copy

Stop writers before exporting. The source must be a current-format store with
complete logical commits and no active steps. Register your application's
artifact types before opening its store, as you would for ordinary inspection.

This example creates a small CSV pipeline and exports it to a separate directory:

```python
from pathlib import Path
from tempfile import TemporaryDirectory

from artisan.operations.curator import IngestCSV
from artisan.orchestration import PipelineManager
from artisan.storage import ArtifactStore, PublicationExport, export_publication

with TemporaryDirectory() as temporary:
    root = Path(temporary).resolve()
    source = root / "local-workspace"
    source.mkdir()
    incoming = source / "input.csv"
    incoming.write_text("x,y\n1,2\n3,4\n")

    with PipelineManager.create(
        "publication-example",
        delta_root=str(source / "delta"),
        staging_root=str(source / "staging"),
        working_root=str(source / "work"),
        files_root=str(source / "files"),
    ) as pipeline:
        pipeline.run(IngestCSV, inputs=[str(incoming)])

    original = ArtifactStore(str(source / "delta"))
    exported = export_publication(
        original,
        root / "shared-results",
        redact=(str(source),),
    )
    reopened = exported.open_store()
    types = original.provenance.load_type_map()
    assert reopened.provenance.load_type_map() == types
    assert (
        reopened.provenance.load_backward_map()
        == original.provenance.load_backward_map()
    )

    # Move the entire inspection bundle, including its copied external files.
    moved = root / "moved-results"
    exported.directory.rename(moved)
    relocated = PublicationExport(moved).open_store()
    assert relocated.provenance.load_type_map() == types
```

The source files remain unchanged. Choose a new or empty local destination
outside the source roots. The exporter rejects overlapping destinations and
destination paths through symlinks. If it fails, it removes its own incomplete
output and leaves an originally empty destination empty.

Keep the entire exported directory together. Open a moved copy with
`PublicationExport(new_directory).open_store()`. Pass `fs=store.filesystem` to
FileRef reads, verification, and materialization. The mapping uses portable
published locators and cannot fall back to files elsewhere on the host.

## Choose what to omit

`redact` contains explicit, case-sensitive substrings. They are omitted only from
classified operational fields, including supported diagnostic observations and
raw input locators. Scientific identity, software identity, exact occurrence
membership, and declared lineage remain protected. File locators point to
verified copies inside the bundle.

Sanitized replay observations retain explicit replacement requirements. Original
execution and seal IDs remain historical assertions; the bundle's own inspection
digest identifies the copy. An inspection bundle cannot itself authorize replay.
Use the original live store for [recorded execution replay](debugging-executions.md).

The export fails if a requested omission appears in scientific artifact content,
artifact metadata, required filenames, parameters, arbitrary execution metadata,
or other protected evidence. Redaction cannot change the scientific recipe or
software identity and still claim the same source computation.

This operation does not discover secrets or remove arbitrary private scientific
data. It checks supplied substrings in decoded store fields and raw external
file bytes. Inspect nested archives, compressed scientific content, and any
supplemental files separately before publication. Externally backed registered
artifacts must have verifiable complete carriers; copied bytes are checked again
when they are loaded.

## Understand the resulting evidence

Artifact and run identities remain original. File locators and redacted diagnostic
fields may change. The export has a closed file inventory and its own inspection
digest. It carries the original result assertions without creating replacement
worker seals, Delta logs, or acceptance claims.

Open it with `PublicationExport(...).open_store()` for artifact and provenance
queries. It cannot supply pipeline initialization, resume, cache, repair, or
replay authority. Start a fresh live pipeline root to run new work.
