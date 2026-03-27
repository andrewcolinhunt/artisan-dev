# TODO

## Top Priorities

- Update the dev workflow
- Clean commit history
- Update the changelog
- Write up PR message as an md file
- Write up release message as an md file


- Slurm intra backend

- in docs, add comparison to ray, dask, flyte
evaluate in comparison to ray data


- cache invalidation, forced execution

if name == main guard error message. it is required for pipeline scripts because of multiprocessing.

how is materialization handled for files that are external files, only references stored in the delta lake?


better pixi handling guide
dexterity-artisan = { path = "/path/to/your/artisan-fork", editable = true }

---

## Bugs

### Lineage & Artifact Identity

- `original_name` isn't guaranteed unique — materialization and lineage matching should use `artifact_id` instead. `validate_stem_match_uniqueness` exists but is dead code (never called). Lineage builder silently overwrites on duplicate names. Materialization can overwrite files too.
- Lineage matching breaks on merge (e.g. mpnn_fr and mpnn) — Merge concatenates artifact IDs with no `original_name` uniqueness check, breaking stem-matching in lineage capture. Same root cause as above.

### Execution

- CROSS_PRODUCT pairing — lineage capture breaks because `primary_id_to_idx` overwrites earlier indices when the same artifact appears in multiple groups. Pairing itself works fine. No fix attempted yet.
- Filter fails at 10M artifacts input (works at 1M) — curators create a single ExecutionUnit with ALL IDs. Phase 1 provenance walk has no chunking. Multiple multi-GB memory pressure points.
- Execution unit serialized with too many references — ID lists eagerly materialized (4-5 GB peak at 10M). Design doc exists at `_dev/design/1_future/lazy_execution_units.md` with phased approach.
- Slurm job cancellation propagation to the pipeline is slow (tolerable but not ideal)

### Storage

- Delta table partition quotes — cosmetic issue caused by upstream `deltalake-rs`/Polars, not Artisan code. Reads work fine (transparent handling).

### Visualization

- Provenance stepper broken — likely widget rendering issue (output clearing + SVG reload causing flickering/blank states), not indexing error. Tests don't cover the interactive callback.

---

## Features

### Environment & Cloud

- Environment management strategy (how do you handle multiple envs per op?)
- Cloud setup: evaluate abstractions, test example ops with Docker container, S3, etc.

### Operations & Filtering

- Interactive filter provenance edges — InteractiveFilter writes `execution_edges` but not `artifact_edges`, making it invisible in the provenance graph
- Expected number of outputs per operation per input (validation config)
- `IngestPipelineStep`: support ingesting all artifacts up through step N (not just from exactly step N) — currently exact single-step only. `load_artifact_ids_by_type` already accepts a list, so straightforward.
- Cross-pipeline ingest field mapping — ingest works but has no field mapping. Key mismatch between output names and artifact types. No cross-pipeline provenance edges.

### Execution & Orchestration

- Cache policy should be settable per step — currently pipeline-level only on `PipelineConfig`
- Instantiate all steps at startup to validate operations, params, inputs/outputs before running — validation is currently lazy/per-step. A typo in step 5 params won't be caught until steps 0-4 have run.
- Record the actual executed command in execution records — `ExecutionRecord` has no command field. Command is constructed at runtime but discarded.
- Surface script defaults as operation params (avoid two sources of truth) — no bridging between argparse defaults and operation params
- Debug mode: re-run a single execution unit with verbose output and saved intermediate steps — `preserve_working` flag is the closest thing
- Reduce unnecessary materialization — all creator ops get full materialization by default (`InputSpec.materialize=True`). The `materialize=False` escape hatch exists but isn't the default. Sandbox directories still created regardless.
- Timings should also measure subprocess startup — `execute` phase timer wraps the entire call with no subprocess startup breakdown

### Lineage

- Role-based lineage tracing for fan-in branches — role info stored on edges but never used in traversal. `walk_backward`/`walk_forward` ignore roles entirely.
- Primary vs secondary lineage key — primary role concept exists at execution time but not persisted on `ArtifactProvenanceEdge`. No way to query "primary lineage chain" post-hoc.
- Name-based grouping for lineage across different trajectories — no `GroupByStrategy.NAME` mode. Stem-matching infrastructure could be reused but hasn't been lifted into grouping layer.

### Visualization & Export

- Interactive filter plot: wrap histograms after three columns wide — all histograms render in a single row. Straightforward fix in `plot()`.

---

## Refactoring

### API Consistency

- Operation docstrings role format — auto-generated role-doc system exists, uses `role_name (type)` format. Cosmetic change needed in `_build_role_docs` if different format desired.
- Step name optional — defaults to `operation.name`. Duplicate default names cause silent ambiguity (last-wins). Consider requiring, auto-generating unique names, or warning on duplicates.

### Naming

None renamed yet. Scope varies:

- `hydrate` -> `load_contents` or similar — very large scope (20+ files, public API)
- `load_provenance_map` -> `load_backward_provenance_map` — small scope. `ProvenanceStore` already has symmetric names internally.
- `tool_output` -> `execution_log` or `stdout_error` — medium scope (6+ source files, 11 test files, Delta schema)
- Merged and passthrough naming — large scope. `Filter` returns `passthrough={"passthrough": [...]}`. Deeply embedded.
- `display_provenance_stepper` -> `interactive_micro_graph` or similar — small scope (3 source files, 1 test, 1 doc)
- Ingested files vs artifact references — medium scope. `IngestFiles` vs `FileRefArtifact` naming confusing.

### Code Organization

- Clean up worker ID usage — threaded as a loose `int` through 5+ function signatures. Should be resolved once onto `RuntimeEnvironment`.
- Partition delta lake tables by step — partially done. Most tables partitioned by `origin_step_number`. `artifact_index` and `artifact_edges` are not.
- Reorganize test and demo files — demos that need input files should be self-contained in their demo dir

### Logging

- Slurm logs should go to project root, not git repo root — `SlurmTaskRunner` created without `log_folder` parameter. Fix: pass explicit path.

---

## Open Questions

- AF3 config: preprocess vs config generator — where's the boundary?

---

## Documentation

- Fix the docs site header (too big) — smaller icons, relocate search bar

---

## Dev Tooling

- Artifact creation Claude skill
