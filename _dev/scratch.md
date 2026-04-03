  Things to resolve now vs later
                                                                                                                                                                                               
  Resolve now (affects both tracks):
  - Update streaming_pipeline_execution.md to use the current dispatch(units, runtime_env) signature          

  Do this please

  - Decide whether SLURM_INTRA waits for DispatchHandle or lands first and gets migrated                                                                                                       
  
slurm intra is already implemented

  Resolve before step scheduler:                                                                                                                                                               
  - Where resource_budget() lives on BackendBase (return type, default behavior)
  
  What is the most logical solution?

  - Whether the result contract stays as list[dict] or becomes typed (affects both the scheduler and cloud handles)                                                                            

What is the most logical solution?

  Resolve later:                                                                                                                                                                               
  - Return-value staging optimization (future, after both tracks have v1)
  - Step executor subsumption (after the scheduler is operational)                                                                                                                             
  - Commit batching + remote staging interaction (after cloud storage lands)