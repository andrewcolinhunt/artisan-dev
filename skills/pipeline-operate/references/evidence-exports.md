# Evidence Exports

Use an explicit export when the user needs to retain or share a complete
assessment or comparison. First check `list_inspections()` or
`artisan inspect list --json`: `local_interfaces.evidence_export` advertises
format version and supported report kinds. Confirm the installed local API or
CLI help. This interface is separate from read-only inspection capabilities;
its presence does not advertise an MCP export tool.

## Save the selected report

Choose a new destination directory with an existing parent. Existing entries
are never overwritten. A local destination is accessible only on
that machine/filesystem. A path returned by a remote MCP server does not mean
the client can open it, and this interface does not transfer remote files.
An oversized bounded response does not automatically authorize file creation.

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

The writer preserves the already materialized report without querying the live
store. If starting from a bounded response, obtain the complete report using
its exact resolved selection and spec. Pass the earlier `evidence_id` as
`expected_evidence_id` to reject a changed report. An assessment evidence ID
alone does not identify the criteria, so retain both the selection and spec
when reusing an earlier conclusion.

## Interpret completeness and identity

The export contains the full report model, including observations/pairs and
diagnostics. Full serialization does not create missing measurements, remove
unmatched populations, prove workload closure, or turn an incomplete assessment
into a pass. Preserve coverage and lifecycle qualifications when sharing it.

The directory contains `report.json`, `schema.json` and the completion
`manifest.json`. Publication errors can leave incomplete output; do not infer
success from file existence or retry into that directory. If a manifest exists,
try the passive loader to verify the visible bundle, or inspect the incomplete
output and explicitly choose a new destination.

The bounded receipt is confirmation and attribution, not the complete evidence.
Keep the complete directory for subsequent analysis. The loader validates and returns
the complete saved Python model without contacting the store or reconstructing
live continuations. There is no saved-report paging interface in this release.

Payload digests detect content changes; they are not authenticity signatures.
Saved reports reflect the selected attempts and evidence at creation time,
not later changes to a run. Handle exported metric data and metadata according
to the user's intended audience and destination.
