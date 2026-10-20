# Export Complete Evidence Reports

Save a complete outcome assessment or run comparison for later inspection,
including details too large for an inspection response. Exporting returns a
compact receipt; loading the bundle returns the original typed report without
opening its source store.

## Save a report you already computed

This example continues with the `assessment` report from
[assessing recorded metrics](exporting-results.md#assess-recorded-metrics-without-display-rounding).
Create the parent directory yourself and choose an unused destination:

```python
from pathlib import Path

from artisan.inspection import export_evidence, load_evidence_export

parent = Path("reports")
parent.mkdir(exist_ok=True)
receipt = export_evidence(
    assessment,
    parent / "assessment",
    expected_evidence_id=assessment.evidence_id,
)
saved = load_evidence_export(receipt.destination)

assert type(saved) is type(assessment)
assert saved == assessment
assert saved.selection.step_run_id == assessment.selection.step_run_id
assert receipt.collection_counts.observations == len(saved.observations)
print(receipt.model_dump_json(indent=2))
```

Pass a `RunComparison` from `compare_runs` to the same exporter to retain every
pair, unmatched or invalid case, metric summary, and optional nested assessment.
Both nested assessments retain their complete specifications and observations.
Use a different destination for each export. Existing files, directories and
symlinks are rejected, including an existing valid export.

The exporter uses the report already in memory. It does not reread the store or
reevaluate criteria. The report file retains null values, exact JSON integers
and the reader's unrounded numbers. Use a JSON consumer that preserves large
integers; some browser tools lose integer precision.

## Compute and export from the command line

The CLI accepts the same selection and specification fields as the corresponding
[inspection request](../reference/python-api.md). Continuing the Python example,
write a request for the exact resolved attempt:

```python
import json

from artisan.visualization import OutcomeSelection

request = {
    **assessment.selection.model_dump(
        mode="json", include=set(OutcomeSelection.model_fields)
    ),
    "spec": assessment.spec.model_dump(mode="json"),
}
Path("assessment-request.json").write_text(json.dumps(request), encoding="utf-8")
```

Run this command from the directory containing the request and `reports` parent.
Replace the store path with the source of your assessment:

```bash
artisan evidence export assess_outcomes \
  --request assessment-request.json \
  --delta-root runs/delta \
  --output reports/assessment-cli \
  --json
```

The command validates the request and destination before computing one complete
report. It prints one receipt object on success. Use `--request -` to read the
request from standard input. For a comparison, use `compare_runs` with a request
containing `baseline`, `candidate` and `spec`. Add the application's trusted
`--load-module` options when its store requires custom definitions.

Do not include `cursor`, `limit` or `view`, even as null or default values. An
export captures the complete selected report, and these page controls are
rejected. Unknown request fields also fail validation.

Add `--expected-evidence-id` with a previously observed identity to reject a
changed report before writing files. For assessments, retain the same complete
specification and resolved selection too: `evidence_id` identifies the selected
observations and projection, while `spec_id` identifies the criteria. Changing
a threshold can preserve `evidence_id`. Comparison evidence identity includes
its specification and evaluator.

Without `--json`, the command prints the same receipt as indented JSON. Expected
errors produce a sanitized error object with `--json` and exit 1; command syntax
errors go to stderr and exit 2.

## Check what the receipt guarantees

The receipt records the actual evidence and specification identities, resolved
attempts, existing coverage and stored collection counts. Its `report_complete`
flag means every field of the returned report was saved. Missing measurements,
unavailable populations and ambiguous correspondence remain explicit; exporting
does not turn them into successful or complete source evidence.

The receipt fits the shared 48 KiB JSON response budget. It includes the complete
authoritative summary when that fits. Otherwise `summary` is null and
`summary_status` is `omitted_size_limit`; no values are shortened. If the mandatory
receipt itself is too large, the export fails before creating its directory.
The report file has no conversation-size limit.

Each completed bundle contains exactly three files:

| File | What you retain |
| --- | --- |
| `report.json` | The complete assessment or comparison, including all nested details |
| `schema.json` | The writer's generated report schema, retained as documentation |
| `manifest.json` | Completion marker with format/version information, identities, coverage, list lengths and file digests |

`load_evidence_export` verifies the fixed regular files, their lengths and digests,
supported versions, strict JSON and agreement between the manifest and typed
report. It validates against Artisan's installed report models; it does not
execute the archived schema or follow its references. The initial bundle format
and supported reader/evaluator versions are `"1"`. The Artisan package version
is recorded for provenance and need not match exactly.

Capture consistency remains reader-defined. The bundle retains one materialized
result and does not create a transactional snapshot across tables or runs. It
contains the report's evidence, not raw metric artifacts, full logs or the original
compute environment. Exporting and loading materialize complete reports and can
use substantial memory and disk.

## Recover from an interrupted export

The destination must be a local path with an existing parent. URI destinations,
including `file://`, are rejected. Files are published without replacement and
the manifest is written last. An I/O failure can leave an incomplete directory,
or visible completed bytes whose flush failed; the exporter still reports failure.

If `manifest.json` exists, try the passive loader:

```python
from artisan.inspection import load_evidence_export

recovered = load_evidence_export("reports/assessment-cli")
print(recovered.evidence_id)
```

A successful load establishes that the currently visible bundle is consistent.
It does not establish that an earlier failed flush was durable. Digests detect
inconsistent files, and copied evidence IDs retain identity; neither authenticates
a bundle whose files and manifest have all been replaced.

If verification fails, inspect the output and explicitly remove your incomplete
directory or select a new destination. The exporter never deletes partial output,
adopts an existing directory or retries automatically. Extra files, nested
directories, symlinks and interrupted publisher temporary files prevent loading.

These writes require an explicit Python or CLI export request. Inspection calls
and MCP tools remain read-only. A local server path does not provide a download
link to a remote MCP client. You own the files and their retention.

For selected result tables and individual artifact files, see
[Export Pipeline Results](exporting-results.md). For public API inspection, see
[Python API lookup](../reference/python-api.md).
