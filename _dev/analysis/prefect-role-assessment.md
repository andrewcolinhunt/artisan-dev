# Analysis: Prefect's Role in Artisan

**Date:** 2026-04-05
**Context:** As artisan expands toward cloud backends (K8s, Modal, AWS
Batch), we evaluated whether to lean into Prefect more deeply, maintain
the current light integration, or decouple entirely.

---

## Background

Artisan was designed with a clear separation of concerns: Prefect handles
execution orchestration (dispatching work to compute backends), artisan
handles data artifact management (content-addressed storage, provenance,
caching, atomic commits to Delta Lake). The `prefect-submitit` package
was written specifically to bridge Prefect's task runner model to SLURM.

As we build cloud backends, the question is whether Prefect's abstraction
is the right one to carry forward, or whether `DispatchHandle` (already
implemented) is a better fit.

---

## Current Prefect Usage

The entire Prefect API surface in artisan is ~30 lines of actual calls
spread across 3 backend files, plus ~300 lines of server management:

| API | Where | Purpose |
|-----|-------|---------|
| `@task` | `dispatch.py` | Marks `execute_unit_task` as dispatchable |
| `@flow(task_runner=...)` | `local.py`, `slurm.py` | Wraps step dispatch |
| `.map()` + `unmapped()` | `local.py`, `slurm.py` | Fan-out to workers |
| `ProcessPoolTaskRunner` | `local.py` | Local multiprocess (subclassed) |
| `SlurmTaskRunner` | `slurm.py` | SLURM via prefect-submitit |
| Server discovery | `prefect_server.py` | Find/validate Prefect server |

**Not used:** retries, result caching, result persistence, deployments,
events, automations, blocks, schedules, state hooks, Prefect artifacts,
concurrency limits, Prefect Cloud features.

---

## Feature Overlap Assessment

Several artisan features appear to overlap with Prefect capabilities.
On closer inspection, they solve different problems at different layers.

### Caching

| | Prefect | Artisan |
|---|---|---|
| **What** | Task result memoization | Content-addressed execution deduplication |
| **Key** | Task inputs + source code hash | `execution_spec_id` = hash(operation + artifact_ids + params + config) |
| **Storage** | Serialized Python objects (pickle/JSON) | No separate cache — queries the Delta Lake executions table for a prior successful run |
| **Granularity** | Per-task call | Per-execution (one artifact batch through one operation) |
| **Scope** | Within a flow run (cross-run with persistence) | Global across all pipeline runs, forever |

Prefect's caching is memoization: "I called `f(x)` before, here's the
pickled return value." Artisan's caching asks: "Has this operation already
run on these exact artifacts with these exact params? If so, the output
artifacts already exist in Delta Lake — skip it." The outputs aren't
cached separately — they are the committed artifacts themselves.

**Verdict: Different problem. Artisan's caching is data-layer
deduplication. Prefect's is task-level memoization. Not interchangeable.**

### Step Tracking / Resume

Prefect tracks flow/task runs as state machines (Scheduled → Running →
Completed/Failed). Artisan's `StepTracker` records step state into a
Delta Lake table with full metadata: `step_spec_id`, `operation_class`,
`params_json`, `input_refs_json`, counts, durations.

Prefect's resume re-runs the flow and skips completed tasks. Artisan's
resume queries committed state in Delta Lake — "which steps already have
committed artifacts?" — and picks up from there.

**Verdict: Different data model. Artisan's resume is tied to artifact
persistence, not orchestrator state. Prefect doesn't know which steps
successfully committed data to Delta Lake.**

### Failure Handling

Prefect has retries (re-run a failed task N times with backoff). Artisan
has `FailurePolicy.CONTINUE` vs `FAIL_FAST` — when a batch of 1000
artifacts is being processed and one fails, do you commit the 999
successes, or abort the whole step?

Prefect has no concept of partial-batch success. These are orthogonal —
you could use both together.

**Verdict: Complementary, not overlapping. Artisan's failure policies
operate at the batch level. Prefect's retries operate at the task level.**

### Provenance

Artisan tracks structured lineage: artifact X was produced from artifacts
Y and Z by operation O, in execution E, at step S. This is a graph of
data relationships stored in Delta Lake (`artifact_edges`,
`execution_edges`).

Prefect has task dependency visualization ("task A ran before task B"),
but no data lineage. It doesn't know about artifacts, only task runs.

**Verdict: No overlap. Prefect doesn't attempt data provenance.**

### Content-Addressed Artifact Storage

Artisan computes deterministic artifact IDs from content hashes, stages
artifacts from workers, and atomically commits to Delta Lake. Prefect
stores task results as opaque serialized blobs with no content
addressing, no structured schema, and no atomic commit.

**Verdict: No overlap. Fundamentally different storage models.**

### Observability

Prefect provides a live web UI: running flows, task timelines, durations,
logs, failure states. Artisan has post-hoc analysis (provenance graphs,
timing) but no live dashboard during execution.

**Verdict: Genuine gap. This is the one area where Prefect provides
something artisan doesn't replicate.**

---

## Prefect's Abstraction vs DispatchHandle

The central architectural question: does Prefect's task runner model
fit artisan's needs going forward?

### Task runners: where Prefect fits

Prefect's model is: decorate a function with `@task`, fan out with
`.map()`, collect results. Task runners determine *where* tasks execute:

| Task runner | Backend | Status |
|-------------|---------|--------|
| `ProcessPoolTaskRunner` | Local multiprocess | Works today |
| `SlurmTaskRunner` (prefect-submitit) | SLURM sbatch/srun | Works today |
| `DaskTaskRunner` | Dask cluster | Available (unused) |
| `RayTaskRunner` | Ray cluster | Available (unused) |

This maps cleanly to local and SLURM — the two backends artisan has
today.

### Task runners: where Prefect doesn't fit

For planned cloud backends, no Prefect task runner exists:

| Backend | Native API | Prefect task runner? |
|---------|-----------|---------------------|
| Kubernetes Jobs | `batch/v1` API | No |
| Modal | `modal.Function.map()` | No |
| AWS Batch | `SubmitJob` | No |

Prefect's answer for cloud execution is **work pools**, which operate at
a different scope — they launch *entire flows* on remote infrastructure,
not individual tasks within a flow. This is a scope mismatch: artisan
needs intra-flow fan-out (dispatch N units to N workers), not per-flow
infrastructure provisioning.

For each cloud backend, leaning into Prefect means one of:

- **Write a custom Prefect task runner** (as with prefect-submitit) —
  wrapping each native API in Prefect's task runner protocol so the
  `@flow` + `.map()` pattern still works. Nontrivial effort per backend,
  coupled to Prefect internals (`PrefectFuture`, `FlowRunContext`,
  `resolve_inputs_sync`).

- **Use Prefect work pools** — restructure artisan so each step is a
  deployed Prefect flow on remote infra. Major architectural change,
  pushes toward Prefect Cloud dependency.

### DispatchHandle: the simpler path

`DispatchHandle` is already implemented and provides the exact interface
artisan needs: `dispatch()` / `is_done()` / `collect()` / `cancel()`.

Each cloud backend implements this directly against native APIs:

```python
# Modal example (~60 lines)
class ModalDispatchHandle(DispatchHandle):
    def dispatch(self, units, runtime_env):
        self._call = modal_function.map.aio(units, kwargs={"env": runtime_env})

    def is_done(self):
        return self._call.done()

    def collect(self):
        return list(self._call)
```

No Prefect layer in between. No task runner to write and maintain. No
coupling to Prefect's internal APIs.

Local and SLURM handles currently use Prefect internally — this works
and doesn't need to change. The DispatchHandle boundary means Prefect
is an implementation detail of two backends, not an architectural
commitment.

---

## Cost of Keeping Prefect (Status Quo)

- PostgreSQL (or SQLite) for Prefect server
- Server lifecycle management (`prefect-start` / `prefect-stop`)
- Version compatibility checks between client and server
- ~100+ transitive dependencies
- Log suppression in two files to quiet Prefect's verbose output
- Integration tests require `prefect_test_harness` (ephemeral server)
- `SIGINTSafeProcessPoolTaskRunner` subclasses Prefect internals

These costs are manageable but not zero.

---

## Cost of Removing Prefect

Concentrated in 4 files:

| File | Replacement |
|------|-------------|
| `dispatch.py` | Remove `@task`, make it a plain function |
| `local.py` | `ProcessPoolExecutor` directly (~50 LOC) |
| `slurm.py` | `submitit` directly (~80 LOC) |
| `prefect_server.py` | Delete entirely |

Plus cleanup: remove `prefect_server` param from `PipelineManager`,
remove log suppression, rewrite integration test fixtures.

The work is small but not urgent — nothing is blocked by Prefect being
present.

---

## Options

### Option A: Lean into Prefect

Write custom Prefect task runners for each cloud backend (K8s, Modal,
AWS Batch), similar to how prefect-submitit bridges Prefect to SLURM.
Structure steps as Prefect flows to benefit from the dashboard.

**Pros:**
- Observability UI for free
- Consistent dispatch model across all backends
- Prefect community and ecosystem

**Cons:**
- Each task runner is a nontrivial package to write and maintain
- Coupled to Prefect's internal APIs, which change across versions
- Work pools (Prefect's cloud answer) are the wrong scope for artisan
- Prefect wants to be THE orchestrator — tension with artisan's role

### Option B: Maintain status quo

Keep Prefect for local and SLURM. New cloud backends implement
DispatchHandle directly.

**Pros:**
- No migration work
- Local and SLURM continue working as-is
- Cloud backends are unencumbered by Prefect
- DispatchHandle boundary keeps Prefect contained

**Cons:**
- Two dispatch patterns (Prefect-based and direct) coexist
- Still paying Prefect's dependency and server costs
- Integration tests still need Prefect test harness

### Option C: Decouple entirely

Replace local and SLURM handles with direct `ProcessPoolExecutor` and
`submitit` implementations. Remove Prefect dependency.

**Pros:**
- Eliminates server, PostgreSQL, ~100+ transitive deps
- One dispatch pattern across all backends (DispatchHandle)
- Simpler test infrastructure
- No coupling to Prefect's roadmap or breaking changes

**Cons:**
- Migration effort (small but nonzero)
- Lose the option to use Prefect's dashboard later
- prefect-submitit becomes orphaned (or maintained independently)

---

## Recommendation

**Option B (maintain status quo) now. Option C (decouple) when
convenient.**

Prefect works for local and SLURM today. There's no urgency to rip it
out. But there's also no reason to invest deeper — writing custom Prefect
task runners for cloud backends is more work than implementing
DispatchHandle directly, with less flexibility.

As cloud backends land and prove out the DispatchHandle pattern, the
value of keeping Prefect for just two backends diminishes. At that point,
rewriting local and SLURM handles against native APIs is a contained
cleanup (~130 LOC of new code, deletion of `prefect_server.py` and test
fixtures).

The observability gap is real but solvable independently. The
`StepTracker` already persists run state to Delta Lake — a lightweight
dashboard (Streamlit, Panel, or a notebook widget) over that table would
provide live pipeline monitoring without an orchestration framework
dependency.

---

## Related Docs

- `_dev/analysis/prefect-alternatives.md` — execution layer alternatives
- `_dev/design/0_current/cloud/cloud-deployment.md` — cloud architecture
  (DispatchHandle design, phase plan)
- `_dev/analysis/cloud/other-frameworks/synthesis.md` — cross-framework
  comparison
