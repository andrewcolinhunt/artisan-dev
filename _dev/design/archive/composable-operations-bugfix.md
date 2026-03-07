# Bug Fix: Chain Staging Path Mismatch

## Problem

Chain steps silently produced empty commits. All 26 demo checks that
inspected Delta Lake output (artifact counts, provenance edges) failed
because no artifacts were ever committed.

## Root Cause

The staging directory layout is `staging/{step_number}_{operation_name}/...`.
Two independent components must agree on `operation_name`:

1. **Chain executor** (`run_creator_chain` in `execution/executors/chain.py`)
   stages files using the **final operation's name** — e.g.,
   `staging/1_metric_calculator/ab/cd/...`. This is correct: the chain
   delegates to the standard creator lifecycle, which uses `operation.name`
   for staging paths.

2. **Step executor** (`execute_chain_step` in `orchestration/engine/step_executor.py`)
   calls `DeltaCommitter.commit_all_tables()` and `await_staging_files()`
   with `operation_name`, which tells the committer where to look for
   staged files.

The bug: `execute_chain_step` passed `operation_name="chain"` (a hardcoded
string), so the committer looked for `staging/1_chain/...` — a directory
that didn't exist. The staged files sat in `staging/1_metric_calculator/...`
and were never found. The commit succeeded with zero rows, silently.

```
Chain executor stages to:    staging/1_metric_calculator/...
Committer looked for:        staging/1_chain/...          <-- MISMATCH
```

## Fix

In `execute_chain_step`, derive the operation name from the final
operation instance instead of hardcoding it:

```python
# Before (broken)
operation_name="chain"

# After (fixed)
final_op_instance = units[-1].operation
chain_op_name = final_op_instance.name
# ... used in both await_staging_files() and commit_all_tables()
```

This is applied at lines 937-939 of `step_executor.py`, and `chain_op_name`
is referenced in the verify_staging phase (line 986) and commit phase
(line 1004).

## Why It Was Silent

`DeltaCommitter.commit_all_tables()` iterates over subdirectories matching
the pattern `{step_number}_{operation_name}` in the staging root. When no
matching directory exists, it commits zero rows across all tables — which
is not treated as an error. The commit "succeeds" with empty deltas.

The pipeline logs showed `Step 1 commit: metrics=0` but this was easy to
miss among other log lines, and `StepResult.success` was still `True`
because the execution units themselves succeeded (they staged files
correctly).

## Affected Components

| File | Change |
|------|--------|
| `orchestration/engine/step_executor.py` | Replace `"chain"` with `chain_op_name` in verify_staging and commit |

## Verification

The end-to-end demo (`_dev/demos/composable-operations/demo_chain.py`)
passes all 26 checks after this fix, including artifact count verification
and provenance edge inspection across all intermediates modes.

## Lesson

When two components communicate through the filesystem (staging directory
names), the naming convention must be derived from the same source. The
regular `_execute_creator_step` gets this right because it has a single
`operation` object. The chain path introduced a second code path that
hardcoded the name instead of deriving it from the operation.
