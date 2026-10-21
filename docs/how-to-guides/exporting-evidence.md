# Export Complete Evidence Reports

Save an outcome assessment or run comparison with all its report details,
including those omitted from a bounded inspection response. You can load the
saved report later without opening its source store. Python and CLI exports
work with the base Artisan installation; no MCP server is required.

## Save a report you already computed

This example continues with the `assessment` report from
[assessing recorded metrics](exporting-results.md#assess-recorded-metrics-without-display-rounding).
Create the parent directory yourself and choose an unused local destination.
Existing files, directories and symlinks are rejected, including valid exports.
URI destinations such as `s3://` and `file://` are not supported.

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
Use a different destination for each export.

The exporter uses the report already in memory. It does not reread the store or
reevaluate criteria. The report file retains null values, exact JSON integers
and the reader's unrounded numbers. Use a JSON consumer that preserves large
integers; some browser tools lose integer precision.

## Compute and export from the command line

Check the installed request schema before preparing a CLI export:

```bash
artisan inspect describe assess_outcomes --json
artisan evidence export --help
```

The CLI accepts the same selection and specification fields as the corresponding
[inspection request](inspecting-evidence.md), without page controls. Continuing
the Python example, write a request for the exact resolved attempt:

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

The command validates the request and destination before reading the store and
computing a complete report. It prints one receipt object on success. Use
`--request -` to read the request from standard input. For a comparison, use
`compare_runs` with a request containing `baseline`, `candidate` and `spec`.
Add the application's trusted
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

The receipt records the evidence and specification identities, resolved
attempts, coverage and saved collection counts. Its `report_complete` flag means
every field of the report was saved. Missing measurements, unavailable
populations and ambiguous matches remain in the report. Keep those limits
alongside any conclusion you share.

The receipt fits the shared 48 KiB JSON response budget. It includes the full
summary when that fits. Otherwise `summary` is null and `summary_status` is
`omitted_size_limit`; no values are shortened. If the mandatory
receipt itself is too large, the export fails before creating its directory.
The report file has no conversation-size limit.

Each completed bundle contains exactly three files:

| File | What you retain |
| --- | --- |
| `report.json` | The complete assessment or comparison, including all nested details |
| `schema.json` | The writer's generated report schema, retained as documentation |
| `manifest.json` | Completion marker with format/version information, identities, coverage, list lengths and file digests |

`load_evidence_export` verifies the files, their lengths and digests, supported
versions, valid JSON and agreement between the manifest and report. It returns
an `OutcomeAssessment` or `RunComparison` validated by the installed Artisan
models. The archived schema documents what the writer saved; loading does not
execute it or follow its references. The recorded package version need not
match the reader's exactly, but unsupported bundle or report versions fail to
load.

The bundle preserves the report as it was read. Reading several tables or runs
does not create an atomic store snapshot. The export contains the report's
selected measurements and diagnostics; raw metric artifacts, full logs and the
original compute environment are outside its scope. Exporting and loading hold
complete reports in memory, so large reports need sufficient memory and disk.

## Recover from an interrupted export

The manifest is written last. A failed export can leave an incomplete directory,
or all three files if the final disk flush failed. Do not infer success from
file existence or retry into that directory.

If `manifest.json` exists, try the passive loader:

```python
from artisan.inspection import load_evidence_export

recovered = load_evidence_export("reports/assessment-cli")
print(recovered.evidence_id)
```

A successful load establishes that the files are currently readable and
consistent. It cannot prove that an earlier failed disk flush succeeded.
Digests detect changed contents; they do not authenticate the report's author.

If verification fails, inspect the output and explicitly remove your incomplete
directory or select a new destination. The exporter never deletes partial output,
adopts an existing directory or retries automatically. Extra files, nested
directories, symlinks and interrupted publisher temporary files prevent loading.

Copy the complete directory when sharing a report. Inspection calls and MCP
tools do not create exports, and a path on a remote MCP server is not a download
link. File transfer and retention are up to you.

For selected result tables and individual artifact files, see
[Export Pipeline Results](exporting-results.md). For public API inspection, see
[Python API lookup](../reference/python-api.md).
