# Comparison to Alternatives

Artisan targets batch scientific computation on HPC clusters with per-artifact
provenance, queryable structured results, and content-addressed caching — all on
a shared filesystem with no services to deploy. This page places Artisan
alongside the major workflow frameworks so you can decide which fits your
problem.

For the design rationale behind the differences highlighted here, see
[Design Principles](../concepts/design-principles.md).

---

## Choosing a framework

| If your work looks like this... | Consider |
|---|---|
| Chaining CLI bioinformatics tools, nf-core pipelines available | **Nextflow** |
| File-in/file-out transformations with wildcard naming patterns | **Snakemake** |
| Scheduled recurring ETL/ELT with enterprise system integrations | **Airflow** |
| General-purpose Python workflow orchestration and observability | **Prefect** |
| Batch scientific computation needing per-artifact lineage and queryable results on HPC | **Artisan** |

These are not mutually exclusive. Artisan uses Prefect internally as its
dispatch layer (see [below](#comparison-prefect-relationship)), and a team could use
Airflow to trigger Artisan pipelines on a schedule.

---

## Comparison matrix

| Dimension | Nextflow | Snakemake | Airflow | Prefect | Artisan |
|---|---|---|---|---|---|
| **Fundamental unit** | Process (channel-connected) | Rule (file-matched) | Task (operator-based) | Task (decorator-based) | Artifact (content-addressed) |
| **Workflow language** | Groovy DSL | Python-embedded DSL | Python | Python | Python |
| **Data model** | File channels between processes | Files matched by wildcards | XComs (small JSON) | Opaque task returns | Typed artifacts in Delta Lake tables |
| **Provenance** | Execution-level (file checksums, task lineage) | File-level metadata + HTML reports | External (OpenLineage) | Flow/task run history | Dual: execution + per-artifact derivation chains |
| **Caching** | Hash of inputs + command, automatic | Timestamp + Merkle tree | None built-in | Opt-in per-task (`cache_key_fn`) | Content-addressed hashes, automatic (configurable via `CachePolicy`) |
| **Result storage** | Files in `work/` dirs | Files on filesystem | External (user-managed) | External (opt-in persistence) | Delta Lake tables (queryable, ACID) |
| **Result querying** | Parse files or use Seqera Platform | Parse files | External tools | External tools | Direct SQL-like queries via Polars/DuckDB |
| **HPC / SLURM** | Native (+ PBS, LSF, SGE) | Native (plugin-based) | None | Indirect (Dask + SLURMCluster) | Native (`SlurmBackend` via job arrays) |
| **Other executors** | Kubernetes, AWS Batch, Google Cloud | Kubernetes, cloud via plugins | Extensive operator ecosystem | Work pools (K8s, ECS, etc.) | Extensible `BackendBase` architecture (LOCAL, SLURM built-in) |
| **Infrastructure** | None (file-based) | None (file-based) | Scheduler + DB + web server | Server or Prefect Cloud | None (Delta Lake on filesystem) |
| **Error model** | Per-process retry with resource escalation | Delete incomplete, retry with escalation | Task retry + SLA alerts | Task retry + state machine | Per-item containment with configurable policy (CONTINUE or FAIL_FAST) |
| **Ecosystem** | nf-core (100+ pipelines) | Workflow Catalog, Bioconda | 1,000+ provider operators | Growing integrations | Domain-extensible artifact type registry |

---

## Mapping concepts across frameworks

If you are coming from another framework, this table maps its core abstractions
to the closest Artisan equivalents.

| Concept in other frameworks | Artisan equivalent |
|---|---|
| Nextflow **channel** / Snakemake **wildcard rule** | `OutputReference` — a typed, resolvable pointer to a step's output artifacts |
| Nextflow **process** / Snakemake **rule** / Airflow **operator** / Prefect **task** | `OperationDefinition` — a computation with declared inputs and outputs |
| Nextflow **workflow** / Snakemake **Snakefile** / Airflow **DAG** / Prefect **flow** | `PipelineManager` — step sequencer with automatic caching and provenance |
| Nextflow `publishDir` / Snakemake output files | Delta Lake commit — artifacts are stored as table rows, not scattered files |
| Nextflow `-resume` / Snakemake timestamp check / Prefect `cache_key_fn` | Content-addressed cache — automatic, no flags or per-task configuration |
| Nextflow `work/` directory | Staging directory → atomic Delta Lake commit |
| Airflow XCom | Artifact — content-addressed, typed, and queryable |
| Nextflow **operator chain** / Snakemake **rule dependencies** | `CompositeDefinition` — compose multiple operations into a reusable unit with collapsed or expanded execution |

---

## Detailed comparisons

### vs. Nextflow

Nextflow is the closest peer. Both target HPC, both wrap external tools, both
support SLURM natively, and both have content-based caching.

**Where Nextflow is stronger:**

- nf-core provides 100+ production-ready bioinformatics pipelines
- Native multi-executor support: PBS, LSF, SGE, Kubernetes, AWS Batch
- Channel model fits naturally when chaining CLI tools via stdin/stdout
- Larger community, Seqera Platform for managed deployment

**Where Artisan is stronger:**

- Per-artifact lineage within batches, not only per-task
- Results are queryable Delta Lake tables — "all metrics from step 3" is a
  Polars scan, not a directory walk
- Content stored in table rows prevents filesystem bloat from millions of
  small output files
- Typed artifact system extensible by domain layers without framework changes
- Composites compose multiple operations within a single step, with collapsed or expanded execution
- Pure Python — no Groovy DSL

### vs. Snakemake

Snakemake is file-centric and rule-based, inspired by GNU Make. It excels at
reproducible file transformation chains.

**Where Snakemake is stronger:**

- Wildcard/rule model is simpler for straightforward file transformations
- Self-contained HTML provenance reports with embedded results
- Multi-executor plugins: SLURM, PBS, LSF, Kubernetes, cloud
- Established community in computational biology

**Where Artisan is stronger:**

- Content-addressed caching is deterministic — no timestamp drift or clock skew
- Per-artifact lineage within batch operations
- Results are queryable without parsing files
- Table-based storage prevents filesystem bloat at scale

### vs. Airflow

Airflow is an enterprise task scheduler for recurring data pipelines. It solves
a different problem.

**Where Airflow is stronger:**

- Time-based scheduling, cron triggers, event-driven orchestration
- Massive operator ecosystem: AWS, GCP, Snowflake, dbt, Spark
- Enterprise features: RBAC, audit logs, connection management, SLAs
- The standard for data engineering team workflows

**Where Artisan is stronger:**

- No infrastructure to deploy or maintain
- Automatic content-addressed caching (Airflow re-runs by default)
- Native HPC/SLURM support
- Built-in per-artifact provenance (Airflow requires external OpenLineage)
- Designed for batch computation, not scheduled job orchestration

### vs. Prefect

Prefect is a Python-native orchestration framework. Artisan uses Prefect
internally as its dispatch backend, so this comparison describes what Artisan
adds on top.

**What Prefect gives you that Artisan does not:**

- Scheduled deployments and event-driven triggers
- Rich UI for monitoring flow/task runs and inspecting logs
- Work pools and agents for heterogeneous infrastructure
- Transactions with commit/rollback semantics across tasks
- Managed cloud offering (Prefect Cloud)

**What Artisan adds on top of Prefect:**

- Typed, immutable, content-addressed artifact data model
- Automatic provenance tracking at the artifact level, not only task level
- Deterministic content-addressed caching without per-task configuration
- Operation model (preprocess/execute/postprocess) for wrapping external tools
- Delta Lake storage with ACID commits and direct queryability
- Staging-commit pattern for safe concurrent writes from thousands of workers
- Composites that compose multiple operations with collapsed or expanded execution for tightly coupled computations
- Backend abstraction (`BackendBase`) that decouples operation logic from
  compute dispatch — swap LOCAL for SLURM without changing operations
- Extensible type system where domain layers add artifact types and get full
  infrastructure for free

---

(comparison-prefect-relationship)=
## How Artisan uses Prefect

Artisan does not compete with Prefect. It uses Prefect as a transport layer
for dispatching work to workers, wrapped behind a `BackendBase` abstraction.
Understanding this relationship clarifies every comparison above.

```
PipelineManager                          (Artisan: step sequencing, caching, provenance)
  └─ execute_step()
       └─ BackendBase.create_flow()      (Artisan: backend abstraction)
            └─ @flow(task_runner=...)     (Prefect: parallel dispatch + observability)
                 └─ execute_unit_task.map(units)
                      ├─ run_creator_flow()    (Artisan: single operation lifecycle)
                      └─ run_composite()        (Artisan: composite operations lifecycle)
```

Two built-in backends control which Prefect `task_runner` is used:

| Backend | Task runner | Dispatch mechanism |
|---|---|---|
| `LocalBackend` | `ProcessPoolTaskRunner` | Process pool on the orchestrator machine |
| `SlurmBackend` | `SlurmTaskRunner` (from `prefect_submitit`) | SLURM job arrays via `submitit` |

| Responsibility | Handled by |
|---|---|
| Pipeline definition, step sequencing | Artisan (`PipelineManager`) |
| Input resolution, cache lookup | Artisan (orchestration layer) |
| Backend selection and flow creation | Artisan (`BackendBase`) |
| Parallel dispatch to workers | Prefect (via backend-selected `task_runner`) |
| Operation lifecycle (preprocess/execute/postprocess) | Artisan (execution layer) |
| Composite routing (single ops vs. composed composites) | Artisan (`execute_unit_task`) |
| Lineage capture, staging | Artisan (execution layer) |
| Atomic commit to Delta Lake | Artisan (orchestration layer) |
| Flow/task run observability UI | Prefect |

Workers run the same execution code regardless of backend — Prefect is the
transport, not the brain. Curator operations bypass Prefect dispatch and
execute locally in a subprocess on the orchestrator. Custom backends can be
created by subclassing
`BackendBase` and implementing `create_flow()` and `capture_logs()`.

---

## See also

- [Design Principles](../concepts/design-principles.md) — rationale for the
  decisions behind these differences
- [Architecture Overview](../concepts/architecture-overview.md) — system
  structure, five layers, and the orchestrator-worker split
- [Operations Model](../concepts/operations-model.md) — two operation types,
  the three-phase lifecycle, and the spec system
- [Execution Flow](../concepts/execution-flow.md) — how Prefect integrates
  into the dispatch-execute-commit lifecycle
- [Storage and Delta Lake](../concepts/storage-and-delta-lake.md) — why Delta
  Lake and the staging-commit pattern
