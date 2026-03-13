# Design: Error Visibility & Storage Tutorial Rework

## Problem

The current tutorials have two issues:

**04-error-visibility.ipynb** covers only one scenario (empty filter
cascade + passthrough_failures). The title "Error Visibility" promises
coverage of the framework's error handling model, but the content is
exclusively about filter behavior. Real error scenarios — execution
failures, partial failures, FailurePolicy, failure logs, validation
errors — are not demonstrated.

**05-storage-and-logging.ipynb** tries to cover too many topics (three-path
model, logging, failure inspection) and the failure demo doesn't work
because Pydantic catches bad params before execution starts.

## Proposal

Split into three focused tutorials:

| Tutorial | Scope |
|----------|-------|
| `04-error-visibility.ipynb` | Rewrite — execution failures, partial failures, FailurePolicy, failure logs |
| `05-storage-and-logging.ipynb` | Rewrite — runs directory layout, Delta Lake tables, configure_logging |
| Keep empty filter content | Absorb into 04 as one section, not the whole tutorial |

---

## Tutorial: 04-error-visibility (rewrite)

### Title

"Error Handling in Practice"

### What you'll learn

- How the framework catches and records execution failures
- Reading failure logs and the executions table
- Partial failures: CONTINUE vs FAIL_FAST policy
- Empty input cascades (filter scenario)
- Validation errors vs runtime errors

### Outline

**Section: A step that fails**

- Run a pipeline where step 1 raises during `execute()`.
- Need an operation that actually fails at runtime, not at validation.
  Options:
  - Use `DataTransformer` with a crafted input CSV that has non-numeric
    values in numeric columns — `float(row[col])` raises `ValueError`
    at `data_transformer.py:152`. This requires ingesting a bad CSV via
    `IngestData`.
  - Define a minimal inline operation that raises in `execute()`.
    Complex due to OperationDefinition validation (per-operation
    OutputRole StrEnum, infer_lineage_from, etc.), but possible.
  - **Recommended:** Ingest a hand-crafted bad CSV. This is realistic
    (real pipelines hit bad data) and uses only existing operations.
    Create the CSV inline in the notebook (no fixture file needed).
- Show `inspect_pipeline` — step shows `failed` status.
- Show `StepResult.failed_count` and `StepResult.metadata`.

**Section: Finding and reading failure logs**

- Navigate to `<runs_dir>/logs/failures/step_N_op/`.
- Read the `.log` file — show run ID, operation, traceback, tool output.
- Cross-reference: the run ID matches the executions Delta table.
- Show how to query the executions table for the error string.

**Section: Partial failures with FailurePolicy**

- Run a pipeline with 5 inputs where 2 fail (mix of good and bad CSVs).
- Default `CONTINUE` policy: step completes with
  `succeeded_count=3, failed_count=2`.
- Downstream steps receive only the 3 successful outputs.
- Show `inspect_pipeline` with partial failure status.
- Re-run with `failure_policy=FailurePolicy.FAIL_FAST`:
  step aborts on first failure. Show the different `StepResult`.

**Section: Empty input cascades**

- Move the current 04 filter cascade demo here (condensed).
- Strict filter → downstream skip. Show `skip_reason` metadata.
- `passthrough_failures` option (brief — one code cell + explanation).

**Section: Validation errors**

- Show that bad params raise `ValidationError` at `pipeline.run()` time.
- Wrap in try/except, show the Pydantic error.
- Explain: this is intentional — config mistakes fail fast before wasting
  compute.
- Contrast with runtime errors which happen inside workers.

**Summary table**

| Error type | When caught | Where recorded | Failure log? |
|------------|-------------|----------------|------------|
| Validation | At `pipeline.run()` | Not recorded — exception raised | No |
| Runtime (execute) | In worker | Executions table + failure log | Yes |
| Infrastructure (OOM, crash) | At dispatch | StepResult metadata | Yes |
| Empty inputs | At step start | Steps table (skipped) | No |

### Key implementation detail: triggering a real runtime failure

The cleanest approach is to ingest a CSV with bad data:

```python
# Create a CSV with a non-numeric value in the 'score' column
bad_csv = tmp_dir / "bad_data.csv"
bad_csv.write_text("id,x,y,z,score\n0,1.0,2.0,3.0,not_a_number\n")
```

Then `IngestData` ingests it successfully (it just stores bytes), and
`DataTransformer.execute()` fails at `float(row[col])` when it hits
`"not_a_number"`. This triggers the full failure recording path:
`_ExecuteFailure` → `record_execution_failure` → failure log written.

The bad CSV is small enough to create inline — no fixture file needed.

---

## Tutorial: 05-storage-and-logging (rewrite)

### Title

"Storage Layout and Logging"

### What you'll learn

- The three-path model: delta, staging, working
- What's inside the runs directory after a pipeline completes
- Delta Lake table structure
- Configuring logging: level, file handler, noise suppression

### Outline

**Section: The three-path model**

- Table: delta_root (permanent), staging_root (temporary), working_root
  (ephemeral).
- Why they're separate: delta is the permanent store, staging is scratch
  for workers, working is per-execution sandboxes.

**Section: Running a pipeline and exploring the output**

- Run a simple 3-step pipeline (generate → transform → score).
- Use `tutorial_setup` which puts everything under `runs/<name>/`.
- Show the directory tree with a `show_tree()` helper.
- Walk through each directory:
  - `delta/` — the Delta Lake tables
  - `staging/` — empty after commit (explain why)
  - `working/` — empty after execution (explain why)
  - `logs/` — pipeline.log location

**Section: What's in the Delta Lake tables**

- List the core tables (from `TablePath` enum) and their purpose:
  - `artifacts/index/` — global registry (artifact_id → type + step)
  - `provenance/artifact_edges/` — derivation edges
  - `provenance/execution_edges/` — execution-level provenance
  - `orchestration/executions/` — one row per execution attempt
  - `orchestration/steps/` — step state transitions
- Content tables (`artifacts/data/`, `artifacts/metrics/`, etc.) are
  managed by the artifact type registry (`ArtifactTypeDef`), not
  `TablePath`. Mention them as "one table per artifact type".
- Show `inspect_pipeline` and `inspect_step` as the primary read tools.
- Brief code cell: read the artifact_index directly with polars to show
  it's just a Delta table.

**Section: Working directory defaults**

- Default `working_root` is `tempfile.gettempdir()`.
- Why: sandboxes are ephemeral scratch, cleaned after each execution.
- `preserve_working=True` to keep them for debugging.
- `preserve_staging=True` to inspect staged Parquet before commit.

**Section: Configuring logging**

- `configure_logging(level, suppress_noise, loggers, logs_root)`.
- PipelineManager calls this automatically.
- Console handler: always on, Rich-colored.
- File handler: added when `level="DEBUG"` + `logs_root` provided.
  PipelineManager passes `logs_root=delta_root.parent / "logs"`.
- `suppress_noise=True` (default) quiets Prefect, httpx, asyncio.
- Code cell: set DEBUG, run a step, show verbose output.

**Section: Crash recovery**

- Brief mention of the `recover_staging` config flag on
  `PipelineManager.create()`: when `True` (the default), leftover
  staging files from a crashed run are committed automatically on
  pipeline init.

**Summary + Next steps**

---

## File numbering

No renumbering needed — 04 and 05 are already in place. The rewrites
are in-place replacements.

## Cross-reference updates

- `docs/concepts/error-handling.md` links to 04-error-visibility →
  update link text if title changes
- `docs/tutorials/index.md` → update descriptions for 04 and 05
- Other tutorials linking to 04 or 05 → check and update descriptions

## Verification

```bash
~/.pixi/bin/pixi run -e docs docs-build
```

Manually verify:
- The bad-CSV failure demo produces an actual failure log on disk
- `inspect_pipeline` shows the failure correctly
- All cross-references resolve
