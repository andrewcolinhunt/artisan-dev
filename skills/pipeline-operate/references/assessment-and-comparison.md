# Assessment and Comparison

Use this reference when the question concerns result quality, differences
between runs, or a local fallback for unavailable MCP reads.

## Define what success means

Resolve the store, run, step and output role before assessing. A successful
execution means the operation completed; accepted artifacts identify its
outputs. Neither alone establishes that the user's goal was met.

If `artisan_assess_outcomes` is exposed, use its actual schema to supply the
criterion: literal metric key path, comparison operator and threshold, with
an expected count when the task requires one. A key path selects JSON object
keys; it is not an expression or an array index. Preserve numeric types and
use the supported reader's raw values instead of rounded display tables.
Units and evaluator labels are caller declarations, not verified provenance.

Select the current step with an unambiguous name or a known step number, or
select an explicitly requested historical attempt. Retain the resolved
`step_run_id` for detail pages. Summaries assess the full selected population;
a detail page is only part of its supporting evidence.

Read coverage as well as verdict. Missing measurements, invalid values and
evaluation errors cannot silently become zero, failure or success. An
incomplete assessment can still contain known failed checks. No criterion,
an unavailable population or an empty population cannot establish quality
success. A passing count-only criterion establishes the count alone.

The population consists of unique accepted metric artifacts for the selected
attempt and role, not upstream subjects or execution occurrences. Cached and
passthrough artifacts retain their origin while belonging to the selected
step. Report the counting basis when it affects the conclusion.

Running/pending attempts are incomplete. Failed, cancelled or skipped attempts
do not expose an accepted metric population. An unavailable population is
different from an available empty population; preserve `null` versus zero.
Recorded disposition and cancellation status add context, not a cleanup
guarantee. Do not replace unavailable current results with an older success.

If the assessment tool is absent, use a supported public reader or established
application code only when it supplies the necessary exact evidence. Otherwise
explain what is known and label quality unassessed. Do not guess a tool name
or build a private storage query to simulate a missing contract.

## Make a defensible comparison

If `artisan_compare_runs` is exposed, retain both selected run/step/role scopes
and their resolved attempt IDs. Supply the actual schema's measurement
identity/version/unit declarations, key paths, direction and tolerance.
The caller must establish that the measurements mean the same thing; arithmetic
alone does not verify a common scorer, subject or experimental treatment.

Use a declared metadata key or explicit artifact pairs to establish
correspondence. Display order, filenames, equal counts and similar step names
do not establish matching. Metadata key types matter: an integer key differs
from its string spelling. Invalid keys and duplicate keys must remain visible;
ambiguity prevents a numerical comparison even if some unique pairs exist.

Report matched, unmatched, invalid and ambiguous populations. Per-metric
coverage must also account for pairs lacking comparable measurements. Do not
present a mean over the comparable subset as a result for every artifact.
Respect the reported rounding and overflow flags; do not recompute differences
by first coercing large integers to floats. A neutral measurement has changes,
not an implied improvement direction.

Optional assessments cover each side's complete selected population, not just
the matched subset. A better paired metric does not establish an overall
winner, causality, improved reliability or a cost advantage. Equal redacted
configuration does not prove equal hidden configuration.

Without the comparison tool, describe supported observations separately or
use an already available exact application analysis. Ask for correspondence
or criteria only when needed to answer the question, while continuing useful
independent reads.

## Use a limited local fallback

A read-only Python script is appropriate when workspace execution is available
and the exact store is accessible from that workspace. A remote MCP connection
does not imply local access. Use the project's established environment; in an
Artisan checkout use `~/.pixi/bin/pixi run --locked`.

For custom artifact types, first call the public
`artisan.registry.bootstrap_applications` with explicit trusted installed
registration modules from user/project configuration. This helper does not
merge environment module names automatically. Correct failed imports in a
fresh interpreter; do not trust a module name merely because stored evidence
mentions it. A missing definition is a configuration gap, not proof of corrupt
data.

| Public import | Useful evidence | Limit |
| --- | --- | --- |
| `list_runs` from `artisan.orchestration` | Run IDs and rollups | A rollup's `last_status` is not pipeline success. |
| `inspect_pipeline`, `inspect_failures` from `artisan.visualization` | Step lifecycle and scoped failures | Failed-execution rows can be absent for a failed step. |
| `inspect_step` from `artisan.visualization` | Accepted-artifact display | Shortened IDs/names are not exact artifact references or role mapping. |
| `inspect_metrics`, `inspect_data` from `artisan.visualization` | Scoped metric display or CSV data | Metric floats round to three decimals by default; display rows do not establish exact thresholds, correspondence or complete criterion coverage. |
| `read_metric_observations`, `assess_outcomes` from `artisan.visualization`, when installed | Exact selected metric values, artifact references and declared criteria | Requires an explicit `OutcomeSelection`; available evidence covers the accepted role, not missing upstream subjects. |
| `compare_runs` from `artisan.visualization`, when installed | Paired changes and population coverage | Requires `ComparisonSpec` and explicit correspondence; caller declarations do not prove scientific comparability. |

Always supply `pipeline_run_id`; several display readers otherwise include
origin-wide results across runs. Use `step_run_id` only on readers whose
documented signature accepts it, consistently with the selected run and step.
Use public facades, not internal status, inspection or artifact-query modules.
Do not create or resume `PipelineManager` to read a store: those entry points
participate in execution and recovery.

For example, save this as `inspect_existing.py` and supply the accessible store
and exact run ID as its two arguments. Load trusted application definitions
first if the store requires them.

```python
import argparse

from artisan.visualization import inspect_pipeline

parser = argparse.ArgumentParser()
parser.add_argument("delta_root")
parser.add_argument("pipeline_run_id")
args = parser.parse_args()

steps = inspect_pipeline(
    args.delta_root,
    pipeline_run_id=args.pipeline_run_id,
)
shown = steps.head(20)
print(f"Run: {args.pipeline_run_id}")
print(shown)
print(f"Displayed {shown.height} of {steps.height} step rows.")
print(f"Omitted {steps.height - shown.height} rows from this display.")
```

This bounds printed rows, not underlying scanning or every cell's size.
An empty table does not independently establish that the requested run exists.
The example provides lifecycle evidence, not an exact assessment or comparison.
