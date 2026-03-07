# TODO

## TOP PRIORITIES


- prefect server discovery that isn't tied to one specific git dir, where should it be?
- add integration tests that cover chaining methods

- ctrl c during subrpocess causes things to hang sometimes
- bug in the metrics export, converts all metrics to float.


- add implicit inputs behavior??

- metrics passthrough is needed for structures? how should this work.

- step name required throughout?

- delete old tests in pipelines (artisan, prefect_submitit)

- How do other frameworks handle command, container, etc storage and construction?



- are curator operations invokable on slurm?



---
---

## TO ORGANIZE

- How do other frameworks handle command, container, etc storage and construction?
- Get set up for cloud, evaluate abstractions, do a test of example ops with a docker container, S3 etc.


# Artisan

## Bugs

### Lineage & Artifact Identity

- `original_name` isn't guaranteed unique — materialization and lineage matching should use `artifact_id` instead
- Lineage inference fails when two inputs have identical names from different branches
- Lineage matching breaks on merge with mpnn_fr and mpnn (based on mpnn_fr) — likely same root cause as above

### Execution

- CROSS_PRODUCT pairing broken — needs unique name per paired set; `original_name` can't disambiguate when same structure appears in multiple groups
- Compute backend hardcoded to Slurm in creator context builder — should reflect actual backend
- Caching on failed runs is broken
- Caching based on staging is absent (incomplete steps not yet in delta lake aren't cache-checked)
- Filter fails at 10M artifacts input (works at 1M) — memory bottleneck related to execution units and artifact resolution
- OOM error on orchestrator silently lost
- Execution unit gets serialized with too many references — should be fully lazy
- Check step caching and pipeline stopping on no returned outputs for expected inputs

### Storage

- Delta table partition names have unnecessary quotes (e.g. `'origin=1'`) — strip them

### Logging

- External tool (script) stdout/stderr leaks to console in streaming mode — non-streaming captures correctly, but streaming (e.g. AF3) still prints to console. Design doc exists.

## Features

## Environment
- Environment management strategy (for operation dependencies)
- Faster pixi env startup in notebooks (design doc exists)

### Operations & Filtering

- Fix filter op to be coherent
- Filter is only single-hop provenance — should we change that?
- Expected number of outputs per operation per input (validation config)
- `IngestPipelineStep`: support ingesting all artifacts up through step N (not just from exactly step N)
- Verify cross-pipeline ingest is working — may need to provide artifact field mapping
- Multiple ops inside one op?

### Execution & Orchestration

- Single worker with multiple GPUs/CPUs (bcov use case)
- Instantiate all steps at startup to validate operations, params, inputs/outputs before running
- Record the actual executed command in execution records
- Verify caching end-to-end
- Surface script defaults as operation params (avoid two sources of truth)
- Debug mode: re-run a single execution unit with verbose output and saved intermediate steps
- Data frames throughout as an optimization? Should creators use data frames?
- Things that are materialized that don't need to be — any ops that are not external tools

### Lineage

- Role-based lineage tracing for fan-in branches (choose which role to follow backward)
- Primary vs secondary lineage key
- Name-based grouping for lineage across different trajectories? New group_by mode?

### Analysis & Timings

- Interactive filter plot: wrap histograms after three columns wide

## Refactoring

### API Consistency

- Operation docstrings should display as "role" format (e.g. `type: STRUCTURE`)
- Filter `Criterion.metric` field: consider decomposing into separate `operation`, `field`, and optional `step` fields for disambiguation
- InteractiveFilter: should it require specifying a step explicitly?

### Naming

- `hydrate` -> `load_contents` or similar (less jargony)
- `load_provenance_map` -> make forward/reverse naming symmetric
- `rename tool_output` to something like `execution_log` or `stdout_error`
- Merged and passthrough naming is weird — output roles could be confusing

### Code Organization

- Clean up worker ID usage (hacky throughout)
- Should each package have its own `ArtifactTypes` enum, or is the shared extensible registry sufficient?
- Partition delta lake tables by step
- Code quality pass

## Logging

- Slurm logs should go to project root, not git repo root
- Make step separation more visible in logs (e.g. spacing between steps)
- Better log handling — external tool pollution, default logging too verbose

## Open Questions

- Metrics access with multiple operations — store operation name? Access via `metrics.op.metric_name`?
- Step override/deletion: how to handle re-running or replacing previous steps cleanly

## Documentation

- Comprehensive artisan docs update — double check everything is up to date
- Update tutorials — split them more cleanly, make sure they work ok

---
---

# Pipelines

## Bugs

### Visualization

- Viewer: Toggle Controls Panel and Toggle Selection Mode broken
- Viewer: lazy loading still slow for 1k structures — likely metric loading/sorting at scale
- Viewer: coloring edge cases with ligands/heavy atoms (polymer entity filtering done for mmCIF, may still have edge cases). Coloring should ONLY apply to cartoon. Sticks should be viewer defaults.
- Provenance stepper is broken — slider bar breaks things

### Export

- Bug in pipelines export for nested metrics and metric name collisions

## Features

### Operations

- Structure viewer approve/reject ("tinder") mode: key bindings + buttons to classify designs, save selections and return
- Add Rosetta Relax operation

### Visualization & Export

- Expand export utilities (currently only `export_structures_with_metrics`)
- Export: write files to sharded directories
- Structure viewer: tune lazy load prefetch buffer (e.g. 10 on each side)

## Refactoring

### API Consistency

- Unify API patterns across InteractiveFilter and StructureViewer (currently inconsistent)

### Naming

- `display_provenance_stepper` -> `interactive_micro_graph` or similar

### Code Organization

- Remove hardcoded comment numbers (unmaintainable)
- Reorganize test and demo files — demos that need input files should be self-contained in their demo dir

## Logging

- Better logging setup — should PipelineManager configure it?
- AtomWorks log pollution

## Open Questions

- AF3 config: preprocess vs config generator — where's the boundary?

## Documentation

- Comprehensive pipelines docs update — double check everything is up to date
- Tutorials: review and verify all domain notebooks
- Docs: compute-backends, external-tool-integration
- Motif scaffolding demo with public enzyme (not internal)
- Create a script that precomputes all delta tables for tutorials — tutorials should output to their own runs dir, not create a shared precomputed dir
- Fix the docs site header (too big) — smaller icons, relocate search bar

---
---

# Prefect-Submitit

## Bugs

- Prefect logging weird since switching to process pool task runner — not being suppressed, seeing task outputs

## Features

- Thundering herd update for many concurrent slurm jobs
- Propagation of keyboard interrupt to Slurm jobs
- Improve Slurm polling efficiency

## Refactoring

- Is the prefect server configured globally for the machine or locally for the directory? Maybe should be global?
- Incorporate pixi into prefect server start message

## Logging

- Prefect server link should be accessible from SSH
- Resolve Prefect dir so UI link points through the correct SSH port
- Prefect logs and postgres logs should be more accessible — put all logs in runs

## Open Questions

- What happened to the slurm submission logging?

## Documentation

- Docs: deploying-with-prefect

---
---

