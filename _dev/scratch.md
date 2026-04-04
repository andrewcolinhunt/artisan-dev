Things to resolve now vs later

Resolved:
- Update streaming_pipeline_execution.md to use the current dispatch(units, runtime_env) signature — done (active doc already updated)
- Decide whether SLURM_INTRA waits for DispatchHandle or lands first and gets migrated — SLURM_INTRA already implemented, DispatchHandle migrates it
- Where resource_budget() lives on BackendBase — `resource_budget() -> ResourceBudget | None`, default None (queue-managed), defined in streaming_pipeline_execution.md
- Whether the result contract stays as list[dict] or becomes typed — `UnitResult` frozen dataclass, ships as separate prep PR before DispatchHandle (see dispatch-handle.md Sequencing section)

Resolve later:
- Return-value staging optimization (future, after both tracks have v1)
- Step executor subsumption (after the scheduler is operational)
- Commit batching + remote staging interaction (after cloud storage lands)
