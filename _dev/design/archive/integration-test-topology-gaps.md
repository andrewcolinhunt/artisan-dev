# Design: Integration Test Topology Gaps

**Date:** 2026-03-10 **Status:** Approved **Author:** Claude + ach94

---

## Problem

The integration test suite covers individual pipeline primitives (linear chain,
fan-out, merge, filter, chaining, etc.) but does not cover many of the
**composed topologies** that tutorials teach users to build. Several
high-value patterns — filter-then-continue, iterative refinement, diamond
DAGs, chain-into-downstream — have zero focused integration test coverage.

Bugs in these compositions would reach users before CI catches them.

---

## Method

Every tutorial notebook in `docs/tutorials/` was inventoried for its pipeline
topology (graph shape) and operation-type sequence. Each topology was then
checked against every test in `tests/integration/`. The table below lists
topologies present in tutorials but absent (or only incidentally covered) in
integration tests.

---

## Gaps

### High risk

**Filter -> downstream creator**

- **Tutorial source:** 01-first-pipeline, 02-exploring-results,
  01-provenance-graphs, 04-timing-analysis
- **Pattern:** `... -> MetricCalculator -> Filter -> DataTransformer`
- **Current coverage:** `test_passthrough` wires filter output into a
  downstream `DataTransformer` and verifies artifact counts, but does not
  assert provenance ancestry through the filter. `test_comprehensive` has a
  creator after a filter but it is buried in a 9-step mega-test with many
  moving parts.
- **Bug surface:** Provenance correctness through the filter boundary. The
  wiring works (verified by `test_passthrough`), but if the resolver
  mishandles passthrough provenance (e.g. wrong step attribution, missing
  edges), downstream artifacts silently lose their ancestry chain.

**Empty filter cascade (skip propagation)**

- **Tutorial source:** 06-error-visibility
- **Pattern:** `DataGeneratorWithMetrics -> Filter(impossible) ->
  DataTransformer(SKIPPED) -> MetricCalculator(SKIPPED)`
- **Current coverage:** `test_all_executions_fail_continue` tests skip from
  execution failure, not from a filter legitimately passing zero artifacts.
- **Bug surface:** Skip reason tagging, correct `skipped=True` metadata, and
  ensuring skipped steps are not cached. When a step resolves zero inputs, the
  step executor marks it `skip_reason=empty_inputs` and the pipeline manager
  sets `_stopped=True`, causing all subsequent steps to be skipped at the
  pipeline level with `skip_reason=pipeline_stopped`. A regression here causes
  confusing step statuses or stale cache entries.

**Iterative refinement loop**

- **Tutorial source:** 05-advanced-patterns
- **Pattern:** `DataGenerator -> [MetricCalculator -> Filter -> DataTransformer]
  x 2` via Python for-loop with `OutputReference` reassignment
- **Current coverage:** None.
- **Bug surface:** `OutputReference` objects being reassigned in a loop and
  consumed by the next round's `pipeline.run()`. If step numbering,
  provenance, or output resolution have off-by-one issues across rounds, the
  loop silently passes wrong data between iterations. Artifact counts should
  shrink across rounds as the filter drops candidates.

### Medium-high risk

**Focused diamond DAG**

- **Tutorial source:** 02-branching-and-merging, 05-advanced-patterns
- **Pattern:** `DataGenerator -> TransformA + TransformB -> Merge -> downstream`
- **Current coverage:** `test_comprehensive` contains a diamond sub-graph but
  it is not isolated. No test checks diamond-specific provenance properties
  (e.g. that the merge output has ancestors from both branches, that the
  downstream step receives the full merged set).
- **Bug surface:** Merge provenance correctness after parallel branches.
  Branch-specific metadata or lineage could bleed or be lost at the merge
  boundary.

**Chain -> downstream non-chain step**

- **Tutorial source:** 07-composable-operations (pattern 4)
- **Pattern:** `DataGenerator -> chain(Transform, Transform) ->
  MetricCalculator`
- **Current coverage:** All chain tests (`test_chain_basic`,
  `test_chain_intermediates_*`, `test_chain_three_stage_provenance`) test the
  chain in isolation. None wire chain output into a subsequent `pipeline.run()`
  step.
- **Bug surface:** Chain output `OutputReference` resolution when consumed by a
  regular (non-chain) step. If the chain step's output role naming or
  provenance recording differs subtly from a regular step, the downstream step
  may fail to resolve its inputs.

**Co-produced metrics + filter auto-discovery**

- **Tutorial source:** 03-metrics-and-filtering, 06-error-visibility
- **Pattern:** `DataGeneratorWithMetrics -> Filter(auto-discover co-produced
  metrics)`
- **Current coverage:** None. All filter tests use a separate
  `MetricCalculator` step to produce metrics. The co-production path (metrics
  emitted by the same step that produces data, with output-to-output lineage)
  is untested.
- **Bug surface:** The forward provenance walk that auto-discovers metrics for
  filter criteria may not handle same-step co-produced metrics correctly,
  since the walk normally looks forward to a *later* step.

**1:N LINEAGE expansion (parameter sweep)**

- **Tutorial source:** 04-multi-input-operations (patterns 2 and 3)
- **Pattern:** `DataGenerator(1 dataset) -> ConfigGenerator(N configs) ->
  MultiInputOp(group_by=LINEAGE)` where 1 ancestor fans out to N configs
  that all pair back correctly.
- **Current coverage:** `test_lineage_grouping` tests 1:1 LINEAGE. The 1:N
  expansion case (one ancestor, multiple descendants in one role) is untested.
- **Bug surface:** The LINEAGE grouping algorithm explicitly supports 1:N
  (via `product(*role_lists)` in `_match_primary_by_ancestry`), but no
  integration test exercises this path. A regression in the expansion logic
  would go undetected.

### Medium risk

**passthrough_failures + downstream processing**

- **Tutorial source:** 06-error-visibility (pattern 2)
- **Pattern:** `DataGeneratorWithMetrics -> Filter(passthrough_failures=True) ->
  DataTransformer -> MetricCalculator`
- **Current coverage:** `test_filter_passthrough_failures` verifies all
  artifacts pass through but has no downstream steps.
- **Bug surface:** `passthrough_failures` may set different metadata on the
  passed artifacts (e.g. a failure flag) that confuses downstream input
  resolution.

**Resume and extend with new steps**

- **Tutorial source:** 02-resume-and-caching
- **Pattern:** Run steps 0-3, `PipelineManager.resume()`, then
  `pipeline.run()` step 4 consuming output of step 3.
- **Current coverage:** `test_resume` verifies state reconstruction and output
  chaining but does not actually run a new step after resuming.
  `test_resume_from_failed_pipeline` replaces a failed step and extends with
  a new step, but starts from a failure state — not from a successful run.
- **Bug surface:** Step numbering after resume, output reference resolution
  from restored step results, provenance continuity across the resume
  boundary.

---

## Decisions

- **Keep `test_comprehensive`.** It serves as a smoke test that a complex
  multi-pattern pipeline runs end-to-end. The new focused tests provide
  precise diagnosis when a specific topology breaks; `test_comprehensive`
  confirms they compose without interference.
- **2 rounds for iterative refinement.** The interesting boundary is round 1 ->
  round 2 where the `OutputReference` reassignment first happens. A 3rd round
  adds runtime without new coverage.
- **No separate heterogeneous merge test.** `test_comprehensive` already
  covers `IngestData + DataGenerator -> Merge`. The diamond test exercises
  merge correctness in a more targeted way.

---

## Proposed Tests

Each test targets one topology gap. Tests use existing example operations
(`DataGenerator`, `DataTransformer`, `MetricCalculator`, `Filter`, `Merge`,
`DataGeneratorWithMetrics`) — no new operations needed unless noted.

### Focused topology tests

| Test name | Topology | Key assertions |
|-----------|----------|----------------|
| `test_filter_then_creator` | Gen -> Transform -> Score -> Filter -> Transform | Downstream creator receives only passed IDs; output artifacts have correct provenance ancestry back through the filter (extends `test_passthrough` which verifies wiring but not provenance) |
| `test_empty_filter_cascade` | GenWithMetrics -> Filter(impossible) -> Transform -> Score | All post-filter steps have `skipped=True`; first skipped has `skip_reason=empty_inputs` (step executor: zero resolved inputs), rest have `skip_reason=pipeline_stopped` (pipeline manager: `_stopped` flag set); skipped steps not cached |
| `test_iterative_refinement` | Gen -> [Score -> Filter -> Transform] x 2 rounds | Artifact count is non-increasing across rounds; each round's provenance traces back to the original generator; final round's output has correct step number |
| `test_diamond_dag` | Gen -> TransformA + TransformB -> Merge -> Score | Merge output contains artifacts from both branches; downstream score step processes full merged set; provenance has paths through both branches |
| `test_chain_then_downstream` | Gen -> chain(Transform, Transform) -> Score | MetricCalculator successfully resolves chain output; metric artifacts have provenance back to the chain's final op |
| `test_co_produced_metrics_filter` | GenWithMetrics -> Filter(criteria on co-produced metrics) | Filter auto-discovers co-produced metrics; correct artifacts pass/fail |
| `test_lineage_grouping_one_to_many` | Gen(1) -> ConfigGen(N configs) -> MultiInputOp(LINEAGE) | N execution units; each unit pairs the single ancestor's descendants correctly |
| `test_passthrough_failures_downstream` | GenWithMetrics -> Filter(impossible, passthrough_failures) -> Transform | All artifacts reach downstream; downstream outputs have correct provenance |
| `test_resume_and_extend` | Gen -> Transform -> Score (run), resume, Filter -> Transform (extend) | Resumed steps cached; new steps execute; provenance spans the resume boundary (unlike `test_resume_from_failed_pipeline`, resumes from a successful run) |

### Comprehensive composition test

A single `test_composed_topology` that chains multiple gap patterns into one
pipeline, confirming they compose without interference:

```
DataGenerator
  -> DataTransformer
  -> MetricCalculator
  -> Filter (selective)
  -> DataTransformer ("refine")     # filter-then-creator
  -> [Score -> Filter -> Transform] x 2 rounds  # iterative refinement
```

With a parallel branch from the initial generator through a diamond:

```
DataGenerator
  -> TransformA + TransformB -> Merge -> MetricCalculator  # diamond
```

This test asserts pipeline-level success and correct total artifact/step
counts, but defers detailed provenance checks to the focused tests. Unlike
`test_comprehensive` (which covers a different pattern set and predates the
gap analysis), this test specifically combines the gap patterns identified
above to verify they compose without interference.

---

## Scope & Constraints

- All new tests go in `tests/integration/`.
- Tests use the standard `tmp_path` fixture for Delta Lake storage.
- Tests that need a multi-input operation with LINEAGE grouping will reuse the
  existing `DualInputLineage` test helper or create a minimal one.
- Tests that need `DataGeneratorWithMetrics` will use the existing example
  operation.
- Each test is independent and does not rely on other tests' state.
- Mark all tests with `@pytest.mark.slow` per project convention.
