# Export a Sanitized Inspection Store

Use a publication export when you want to share recorded results while omitting
selected local paths from execution evidence. The export preserves scientific
artifact content and IDs, provenance, metrics, and historical run identities.
It creates new storage integrity records for the sanitized copy.

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
    root = Path(temporary)
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
    assert reopened.provenance.load_backward_map() == original.provenance.load_backward_map()

    # Move the entire bundle, including delta/, files/ and staging/.
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
classified operational fields: diagnostic logs, errors, command recordings,
literal recorded input references, and replay context. File locators point to
verified copies inside the bundle; their safe original basenames are preserved.

Matching replay configuration values become `null` with explicit required
replacement pointers. The original execution ID remains historical evidence;
the exporter does not claim that a different configuration produced that ID.
See [recorded execution replay](debugging-executions.md) for replacement handling.

The export fails if a requested omission appears in scientific artifact content,
artifact metadata, required filenames, parameters, arbitrary execution metadata,
or other unclassified fields. Caller-supplied environment overrides can also
appear in protected step configuration or user overrides; they require review
instead of automatic rewriting. Class-declared replay configuration can use the
explicit replacement contract without changing those records.

This operation does not discover secrets or remove arbitrary private scientific
data. It checks supplied substrings in decoded store fields and raw external
file bytes. Inspect nested archives, compressed scientific content, and any
supplemental files separately before publication. Only FileRef-backed external
artifacts are currently supported; populated stores with other external artifact
types are rejected.

## Understand the resulting evidence

Artifact and run identities remain original. File locators and redacted diagnostic
fields change. Delta files, staging seals, commit plans, and their digests are
new and are audited before the bundle is returned. Retained staging belongs to
this inspection export; it does not reproduce the original worker byte layout.

The publication marker labels the copy for inspection. This version of Artisan
rejects pipeline initialization, logical commits, and compaction against it.
Start a fresh pipeline root to run new work. The unchanged format-8 schemas remain
readable by compatible readers, but older writers may not recognize the marker.
