# TODO

## Top Priorities

- Update the dev workflow
- Clean commit history
- Update the changelog
- Write up PR message as an md file
- Write up release message as an md file

---

## Bugs

### Lineage & Artifact Identity

- `original_name` isn't guaranteed unique — materialization and lineage matching should use `artifact_id` instead
- Lineage matching breaks on merge with mpnn_fr and mpnn (based on mpnn_fr) — likely same root cause as above

### Execution

- CROSS_PRODUCT pairing broken — needs unique name per paired set; `original_name` can't disambiguate when same structure appears in multiple groups
- Compute backend hardcoded to Slurm in creator context builder — should reflect actual backend
- Check step caching and pipeline stopping on no returned outputs for expected inputs
- Filter fails at 10M artifacts input (works at 1M) — memory bottleneck related to execution units and artifact resolution
- Execution unit gets serialized with too many references — should be fully lazy
- Slurm job cancellation propagation to the pipeline is slow (tolerable but not ideal)

### Storage

- Delta table partition names have unnecessary quotes (e.g. `'origin=1'`) — strip them

### Visualization

- Provenance stepper is broken — slider bar breaks things

### Export

- Bug in export for nested metrics and metric name collisions TODO: i think we addressed this

### Logging

- External tool stdout/stderr leaks to console in streaming mode — non-streaming captures correctly, but streaming (e.g. AF3) still prints to console. Design doc exists. This is the root cause of AtomWorks log pollution and similar issues.

---

## Features

### Environment & Cloud

- Environment management strategy (how do you handle multiple envs per op?)
- Cloud setup: evaluate abstractions, test example ops with Docker container, S3, etc.

### Operations & Filtering

- Filter is only single-hop provenance — should we change that?
- Interactive filter provenance edges
- Expected number of outputs per operation per input (validation config)
- `IngestPipeline`: support ingesting all artifacts up through step N (not just from exactly step N)
- Verify cross-pipeline ingest is working — may need to provide artifact field mapping

### Execution & Orchestration

- Cache policy should be settable per step
- Verify caching end-to-end
- Instantiate all steps at startup to validate operations, params, inputs/outputs before running
- Record the actual executed command in execution records
- Surface script defaults as operation params (avoid two sources of truth)
- Debug mode: re-run a single execution unit with verbose output and saved intermediate steps
- Reduce unnecessary materialization — ops that are not external tools shouldn't need it
- Timings should also measure subprocess startup

### Lineage

- Role-based lineage tracing for fan-in branches (choose which role to follow backward)
- Primary vs secondary lineage key
- Name-based grouping for lineage across different trajectories? New group_by mode?

### Visualization & Export

- Interactive filter plot: wrap histograms after three columns wide

---

## Refactoring

### API Consistency

- Operation docstrings should display as "role" format (e.g. `type: STRUCTURE`)
- Should we require step name in every step?

### Naming

- `hydrate` -> `load_contents` or similar (less jargony)
- `load_provenance_map` -> make forward/reverse naming symmetric
- `tool_output` -> `execution_log` or `stdout_error`
- Merged and passthrough naming is weird — output roles could be confusing
- `display_provenance_stepper` -> `interactive_micro_graph` or similar
- Ingested files vs externally-pointing artifact references — confusingly named

### Code Organization

- Clean up worker ID usage (hacky throughout)
- Should each package have its own `ArtifactTypes` enum, or is the shared extensible registry sufficient?
- Partition delta lake tables by step
- Code quality pass
- Remove hardcoded comment numbers (unmaintainable)
- Reorganize test and demo files — demos that need input files should be self-contained in their demo dir

### Logging

- Slurm logs should go to project root, not git repo root
- Better log handling — reduce default verbosity, external tool pollution

---

## Open Questions

- AF3 config: preprocess vs config generator — where's the boundary?
- Implicit inputs behavior — is this needed?
- Are curator operations invokable on Slurm?
- Data frames throughout as an optimization? Should creators use data frames?

---

## Documentation

- Fix the docs site header (too big) — smaller icons, relocate search bar

---

## Dev Tooling

- Artifact creation Claude skill
