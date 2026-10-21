# Evidence Exports

Export a complete assessment or comparison when the user asks to retain or
share it. First check `list_inspections()` or
`artisan inspect list --json`: `local_interfaces.evidence_export` advertises
format version and supported report kinds. Confirm the installed local API or
CLI help. This interface is separate from read-only inspection capabilities;
its presence does not advertise an MCP export tool.

## Save the selected report

Choose a new local directory with an existing parent. Existing files,
directories and symlinks are rejected; URI destinations are unsupported.
A path returned by a remote MCP server does not mean the client can open it,
and this interface does not transfer remote files. An oversized bounded
response does not automatically authorize file creation.

Use `export_evidence` and `load_evidence_export` from `artisan.inspection` with
a complete `OutcomeAssessment` or `RunComparison` from the public analytical
readers. Continuing the complete assessment example in
[assessment and comparison](assessment-and-comparison.md), choose a destination
that does not already exist:

```python
from artisan.inspection import export_evidence, load_evidence_export

receipt = export_evidence(
    report,
    "assessment-evidence",
    expected_evidence_id=report.evidence_id,
)
saved = load_evidence_export("assessment-evidence")
assert saved.model_dump(mode="json") == report.model_dump(mode="json")
print(receipt.model_dump_json())
```

The writer saves the report in memory without querying the store again. If
starting from a bounded response, obtain the complete report using its exact
resolved selection and specification. Pass the earlier `evidence_id` as
`expected_evidence_id` to reject changed evidence. Retain the assessment's
specification and `spec_id` too: changing a threshold can preserve `evidence_id`.
Comparison evidence identity includes the comparison specification.

For a CLI export, use `artisan evidence export assess_outcomes` or
`artisan evidence export compare_runs` with `--request`, `--delta-root` and
`--output`. The request uses the inspection selection and specification but
must omit `cursor`, `limit` and `view`, even when null. This command reads a new
complete report; use `--expected-evidence-id` to check it against earlier evidence.

## Interpret completeness and identity

`report_complete` means every report field was saved, including observations,
pairs and diagnostics. Missing measurements and unmatched populations remain
missing or unmatched. Preserve coverage, execution state and uncertainty about
whether work has stopped when sharing the report. Exporting cannot turn an
incomplete assessment into a pass.

The directory contains `report.json`, `schema.json` and the completion
`manifest.json`. Publication errors can leave incomplete output; do not infer
success from file existence or retry into that directory. If a manifest exists,
try the passive loader to verify the visible bundle, or inspect the incomplete
output and explicitly choose a new destination.

Keep all three files when sharing or retaining the export. The receipt is bounded;
its summary may be omitted for size even though the full report was saved.
The loader verifies the bundle and returns the complete Python model without
contacting the store or resuming a live query. Additional files or symlinks in
the bundle cause verification to fail. There is no saved-report paging interface,
and exporting or loading a large report can use substantial memory.

Payload digests detect content changes; they do not authenticate the author.
The bundle retains the report's selected evidence, not complete source artifacts
or logs. It does not create an atomic snapshot across store tables or runs.
Handle exported metric data and metadata according to the user's intended
audience and destination.
