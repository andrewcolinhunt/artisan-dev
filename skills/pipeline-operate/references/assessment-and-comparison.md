# Assessment and Comparison

Assess results against explicit criteria and compare runs using declared
correspondence. Use the selected store and the established environment.
Use `artisan inspect describe assess_outcomes --json` or
`artisan inspect describe compare_runs --json` to check the installed bounded
request and response schemas. Artisan MCP tools use the `artisan_` prefix;
use the names and schemas advertised by your host when using a remote target.

## Define what success means

Resolve the store, run, step and output role before assessing. A successful
execution means the operation completed; accepted artifacts identify its
outputs. Neither alone establishes that the user's goal was met.

For the `assess_outcomes` capability, use its actual schema to supply the
criterion: literal metric key path, comparison operator and threshold, with
an expected count when the task requires one. A key path selects JSON object
keys; it is not an expression or an array index. Preserve numeric types and
use the supported reader's raw values instead of rounded display tables.
Units and evaluator labels are caller declarations, not verified provenance.

Select the current step by name, adding its step number when the name repeats.
Add `step_run_id` to select a historical attempt. Retain the resolved attempt
ID for detail pages. Summaries assess the full selected population; a detail
page contains only part of its supporting evidence.

Read coverage as well as verdict. Missing measurements, invalid values and
evaluation errors cannot silently become zero, failure or success. An
incomplete assessment can still contain known failed checks. Without criteria
or an available, nonempty population, quality remains unassessed.

The population consists of unique accepted metric artifacts for the selected
attempt and role, not upstream subjects or execution occurrences. Cached and
passthrough artifacts retain their origin while belonging to the selected
step. Report the counting basis when it affects the conclusion.

Running and pending attempts have unavailable populations and remain
`unassessed`. Failed, cancelled or skipped attempts also have no accepted metric
population. Preserve the difference between an unavailable count (`null`) and
an available empty population (zero). A partially successful step can pass
criteria over its accepted outputs without establishing that all intended work
finished. Recorded disposition and cancellation status do not prove that
external work stopped. Do not replace unavailable current results with an older
success.

If a remote assessment tool is absent, use a supported public reader or established
application code only when it supplies the necessary exact evidence. Otherwise
explain what is known and label quality unassessed. Do not guess a tool name
or build a private storage query to simulate a missing contract.

## Make a defensible comparison

The `compare_runs` reader uses two selections in the same store. Retain both
run/step/role selections and their resolved attempt IDs. Supply the schema's
measurement identity/version/unit declarations, key paths, direction and tolerance.
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

## Use full reports and bounded local evidence

A read-only Python script can compose complete analytical reports when the
exact selected store is accessible. A remote MCP connection does not imply local
access. Use the project's established environment; in an Artisan checkout use
`~/.pixi/bin/pixi run --locked`.

For custom artifact types, first call the public
`artisan.registry.bootstrap_applications` with explicit trusted installed
registration modules from user/project configuration. This helper does not
merge environment module names automatically. Correct failed imports in a
fresh interpreter; do not trust a module name merely because stored evidence
mentions it. A missing definition is a configuration gap, not proof of corrupt
data.

| Public import | Useful evidence | Limit |
| --- | --- | --- |
| `inspect_evidence` from `artisan.inspection` | Validated bounded projections with explicit omissions and continuation | Display limits do not reduce the population assessed by the underlying reader. |
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

For example, save this as `inspect_existing.py` and supply the selected store,
run ID, step name and output role. This example uses an explicit count criterion;
choose the expected count for your task. Repeat `--load-module` only for trusted
installed registration modules required by that store.

```python
import argparse

from artisan.inspection import inspect_evidence
from artisan.registry import bootstrap_applications
from artisan.visualization import AssessmentSpec, OutcomeSelection, assess_outcomes

parser = argparse.ArgumentParser()
parser.add_argument("delta_root")
parser.add_argument("pipeline_run_id")
parser.add_argument("step_name")
parser.add_argument("role")
parser.add_argument("--expected-count", type=int, required=True)
parser.add_argument("--step-number", type=int)
parser.add_argument("--step-run-id")
parser.add_argument("--load-module", action="append", default=[])
args = parser.parse_args()

bootstrap_applications(args.load_module)
selection = OutcomeSelection(
    pipeline_run_id=args.pipeline_run_id,
    step_name=args.step_name,
    role=args.role,
    step_number=args.step_number,
    step_run_id=args.step_run_id,
)
spec = AssessmentSpec(
    label="Expected accepted count", expected_count=args.expected_count
)
report = assess_outcomes(args.delta_root, selection, spec)
print(report.selection)
print(report.assessment_status)

# Fix the follow-up selection to the attempt just assessed.
exact_selection = OutcomeSelection.model_validate(
    report.selection.model_dump(include=set(OutcomeSelection.model_fields))
)
summary = inspect_evidence(
    "assess_outcomes",
    {**exact_selection.model_dump(mode="json"), "spec": spec.model_dump(mode="json")},
    delta_root=args.delta_root,
)
assert summary.evidence_id == report.evidence_id
print(summary.model_dump_json())
```

The complete `OutcomeAssessment` is an in-process model. Its observations are
not clipped to fit a response. The bounded summary performs another live read;
the identity assertion detects a change between these two reads. It does not
make them an atomic snapshot. Retain the resolved selection and exact spec
alongside the `evidence_id`; assessment evidence identity does not identify the
criteria. A count-only pass establishes the accepted artifact count; it does not
establish metric quality or prove that every intended upstream case was processed.

For comparison, construct `ComparisonSpec` with `ComparisonMetric` declarations
and `MetadataKeyMatch` or `ExplicitPairsMatch`, all from `artisan.visualization`,
then use its public `compare_runs` reader. The complete `RunComparison` preserves
pair results, diagnostics, unmatched populations and optional assessments.
Use the bounded `compare_runs` capability for presentation, with both exact
attempts retained in follow-up requests. Do not recreate its statistics from
pages or rounded tables.

To preserve an already materialized report without another live query, read
[evidence exports](evidence-exports.md). Loading that export returns the complete
Python model; it does not refresh evidence from the store.
