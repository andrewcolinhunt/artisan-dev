# Comparison to Alternatives

Artisan targets batch scientific computation with per-artifact provenance,
queryable structured results, and content-addressed caching — with no services
to deploy. Its extensible backend architecture supports HPC clusters (SLURM)
and cloud environments from the same pipeline code. This page places Artisan
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
| Asset-centric data platform with built-in UI, scheduling, and cloud-native deployment | **Dagster** |
| Batch scientific computation needing per-artifact lineage and queryable results | **Artisan** |

These are not mutually exclusive. Artisan uses Prefect internally as its
dispatch layer (see [below](#comparison-prefect-relationship)), and a team could use
Airflow to trigger Artisan pipelines on a schedule.

---

## Comparison matrix

| Dimension | Nextflow | Snakemake | Airflow | Prefect | Dagster | Artisan |
|---|---|---|---|---|---|---|
| **Fundamental unit** | Process (channel-connected) | Rule (file-matched) | Task (operator-based) | Task (decorator-based) | Asset (software-defined) | Artifact (content-addressed) |
| **Data model** | File channels between processes | Files matched by wildcards | XComs (JSON; pluggable backend) | Untyped task returns | Assets with IO Managers | Typed artifacts in Delta Lake tables |
| **Provenance** | Execution-level (file checksums, task lineage) | File-level metadata + HTML reports | External (OpenLineage) | Flow/task run history | Asset-level lineage graph | Dual: execution + per-artifact derivation chains |
| **Caching** | Hash of inputs + command, automatic | Provenance-based triggers + Merkle-tree cross-workflow cache | None built-in | Opt-in per-task (`cache_policy`) | Code + data version hashing, opt-in | Content-addressed hashes, automatic (configurable via `CachePolicy`) |
| **Result storage** | Files in `work/` dirs | Files on filesystem | External (user-managed) | External (opt-in persistence) | IO Manager-dependent (S3, Snowflake, local, etc.) | Delta Lake tables (queryable, ACID) |
| **HPC / SLURM** | Native (+ PBS, LSF, SGE) | Native (plugin-based) | Community plugins | Via integrations (Dask, prefect-submitit) | Community plugin (`dagster-slurm`) | Native (`SlurmBackend` via job arrays) |
| **Infrastructure** | None (file-based) | None (file-based) | Scheduler + DB + web server | Server or Prefect Cloud | Dagster UI + daemon + database (or Dagster+) | None (Delta Lake on filesystem) |
| **Error model** | Per-process retry with resource escalation | Delete incomplete, retry with escalation | Task retry + deadline alerts | Task retry + state machine | Op-level retry with run-level policies | Per-item containment with configurable policy (CONTINUE or FAIL_FAST) |
| **Ecosystem** | nf-core (100+ pipelines) | Workflow Catalog, Bioconda | 1,000+ provider operators | Growing integrations | Growing integrations (dbt, Spark, Fivetran, etc.) | Domain-extensible artifact type registry |

---

## Detailed comparisons

### vs. Nextflow

Nextflow is the closest peer. Both target HPC, both wrap external tools, both
support SLURM natively, and both have content-based caching.

**Where Nextflow is stronger:**

- nf-core provides 100+ production-ready bioinformatics pipelines
- Native multi-executor support: PBS, LSF, SGE, Kubernetes, AWS Batch
- Channel model fits naturally when chaining CLI tools
- Larger community, Seqera Platform for managed deployment

**Where Artisan is stronger:**

- Per-artifact lineage within batches, not only per-task
- Results are queryable Delta Lake tables — "all metrics from step 3" is a
  Polars scan, not a directory walk
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

- Content-addressed caching is fully deterministic — Snakemake's mtime-based triggers can be affected by clock skew on distributed filesystems
- Per-artifact lineage within batch operations
- Results are queryable without parsing files
- Table-based storage prevents filesystem bloat at scale

### vs. Airflow

Airflow is an enterprise task scheduler for recurring data pipelines. It solves
a different problem.

**Where Airflow is stronger:**

- Time-based scheduling, cron triggers, event-driven orchestration
- Massive operator ecosystem: AWS, GCP, Snowflake, dbt, Spark
- Enterprise features: RBAC, audit logs, connection management, deadline alerts
- The standard for data engineering team workflows

**Where Artisan is stronger:**

- No infrastructure to deploy or maintain
- Automatic content-addressed caching (Airflow re-runs by default)
- Native HPC/SLURM support
- Built-in per-artifact provenance (Airflow requires external OpenLineage)
- Designed for batch computation, not scheduled job orchestration

### vs. Dagster

Dagster is the closest peer on the data-awareness axis. Both treat data as a
first-class citizen with built-in lineage and intelligent caching. The key
differences are in lineage granularity, infrastructure model, and compute
targets.

**Where Dagster is stronger:**

- Polished web UI with asset graph visualization, run monitoring, and alerting
- Built-in scheduling, sensors, and freshness policies
- Large integration ecosystem: dbt, Spark, Fivetran, Snowflake, and more
- Managed cloud offering (Dagster+) with serverless execution
- Mature and well-funded — battle-tested in production data teams

**Where Artisan is stronger:**

- Per-artifact lineage within batches, not only per-asset
- Zero infrastructure — no database, no daemon, no web server. Delta Lake on
  the filesystem.
- Native HPC/SLURM support via `SlurmBackend` with extensible `BackendBase`
  for cloud environments
- Content-addressed identity is universal and automatic. Dagster's asset
  versioning is opt-in and based on code version hashing, not content hashing.
- All results are queryable Delta Lake tables. Dagster delegates storage to IO
  Managers, which may write to opaque formats.
- Composites with collapsed or expanded execution modes

### vs. Prefect

Prefect is a Python-native orchestration framework. Artisan uses Prefect
internally as its dispatch backend, so this comparison describes what Artisan
adds on top.

**What Prefect gives you that Artisan does not:**

- Scheduled deployments and event-driven triggers
- Rich UI for monitoring flow/task runs and inspecting logs
- Work pools and workers for heterogeneous infrastructure
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

Workers run the same execution code regardless of backend — Prefect is the
transport, not the brain. Custom backends can be created by subclassing
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
