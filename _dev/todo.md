# TODO

## TOP PRIORITIES

re read the updated docs

update for prefect cloud
personally I do not find the prefect web ui particularly useful.
we need to update the how to doc to describe the different ways of using prefect.

- handholding installation

- IPD specific getting started
do not do anything on the head node.
work on your linux box.
work on jojo.
work on a long running jupyter hub node.
default working root.
backfill jobs.

- add a claude code tutorial for protein design

---
---

## TO ORGANIZE

- ingested files vs externally pointing artifact references is still confusingly named.
- propogation of slurm job cancellation to the pipeline still seems slow, but tolerable.

- artifact creation claude skill

- timings should also measure subprocess startup
- interactive filter provenance edges??

- Cache policy should be able to be set per step

- step name required throughout? weirdness with chaining and the step name. should require a step name there.

- add implicit inputs behavior??
- are curator operations invokable on slurm?
- metrics passthrough is needed for structures? how should this work. for example, ligandize? what is the case where it matters? I think this was fixed with the new filter.

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
- Caching based on staging is absent (incomplete steps not yet in delta lake aren't cache-checked)
- Filter fails at 10M artifacts input (works at 1M) — memory bottleneck related to execution units and artifact resolution
- Execution unit gets serialized with too many references — should be fully lazy
- Check step caching and pipeline stopping on no returned outputs for expected inputs

### Storage

- Delta table partition names have unnecessary quotes (e.g. `'origin=1'`) — strip them

### Visualization

- Provenance stepper is broken — slider bar breaks things

### Export

- Bug in export for nested metrics and metric name collisions

### Logging

- External tool (script) stdout/stderr leaks to console in streaming mode — non-streaming captures correctly, but streaming (e.g. AF3) still prints to console. Design doc exists.

## Features

### Environment

- Environment management strategy (for operation dependencies)

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

### Visualization & Export

- Expand export utilities (currently only `export_structures_with_metrics`)
- Export: write files to sharded directories
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
- `display_provenance_stepper` -> `interactive_micro_graph` or similar

### Code Organization

- Clean up worker ID usage (hacky throughout)
- Should each package have its own `ArtifactTypes` enum, or is the shared extensible registry sufficient?
- Partition delta lake tables by step
- Code quality pass
- Remove hardcoded comment numbers (unmaintainable)
- Reorganize test and demo files — demos that need input files should be self-contained in their demo dir

## Logging

- Slurm logs should go to project root, not git repo root
- Make step separation more visible in logs (e.g. spacing between steps)
- Better log handling — external tool pollution, default logging too verbose
- Better logging setup — should PipelineManager configure it?
- AtomWorks log pollution

## Open Questions

- AF3 config: preprocess vs config generator — where's the boundary?

## Documentation

- Create a script that precomputes all delta tables for tutorials — tutorials should output to their own runs dir, not create a shared precomputed dir
- Fix the docs site header (too big) — smaller icons, relocate search bar
