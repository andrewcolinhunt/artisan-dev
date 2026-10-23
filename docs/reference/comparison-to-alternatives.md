# Comparison to Alternatives

Choose Artisan when a Python pipeline needs reusable tool integrations, typed
artifacts, explicit per-artifact lineage, and queryable results.
Artisan runs locally without an orchestration server. Cluster execution uses a
separate runner provider; remote tools require deployed endpoints.

Use the linked framework documentation to check behavior for the version you
plan to use.

## Choosing a framework

| Workflow need | Framework to evaluate |
| --- | --- |
| Connect command-line processes through data channels and resume prior tasks | [Nextflow](https://docs.seqera.io/nextflow/cache-and-resume) |
| Describe file-producing rules and their dependencies in a Python-based workflow language | [Snakemake](https://snakemake.readthedocs.io/en/stable/) |
| Schedule and monitor task DAGs through a scheduler and metadata database | [Airflow](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/overview.html) |
| Add run tracking, retries, deployments, and schedules to Python functions | [Prefect](https://docs.prefect.io/v3/concepts/flows) |
| Store typed results and their declared parents, then query them across pipeline runs | Artisan |

An external scheduler can launch an Artisan script. Artisan then owns that
script's step execution, result storage, and provenance. It does not provide a
scheduling service or a live orchestration dashboard.

## Caching and result storage

- **Nextflow** resumes tasks with `-resume` when their task hashes match and the
  required outputs remain in the work directory. Its default file hash uses
  path, size, and modification time; other cache modes change this behavior.
  See [caching and resuming](https://docs.seqera.io/nextflow/cache-and-resume).
- **Snakemake** offers an opt-in cache shared across workflows. It hashes the
  steps, parameters, inputs, and software environment that determine an output.
  See [between-workflow caching](https://snakemake.readthedocs.io/en/stable/executing/caching.html).
- **Prefect** provides configurable task cache policies and result persistence.
  Reusing persisted results requires a matching cache key and accessible result
  storage. See [caching](https://docs.prefect.io/v3/concepts/caching).
- **Artisan** hashes prepared operation configuration and ordered input
  occurrences, then reuses eligible committed results. `CachePolicy` controls
  which terminal outcomes qualify. Operations that read changing external
  state can opt out of reuse. See
  [cache configuration](../how-to-guides/configuring-execution.md#set-cache-policy).

Artisan stores embedded artifact payloads, metadata, execution records, and
provenance in Delta Lake tables. Externally backed artifacts retain verified
file locations. Each table transaction is atomic; Artisan's completion records
control visibility across related tables. Raw Delta queries bypass that
cross-table boundary. See [storage](../concepts/storage-and-delta-lake.md).

## Mapping concepts

These are approximate correspondences, not interchangeable APIs.

| Other framework concept | Artisan interface |
| --- | --- |
| Nextflow process, Snakemake rule, Airflow task, Prefect task | `OperationDefinition`, with declared input and output roles |
| Nextflow workflow, Snakefile, Airflow DAG, Prefect flow | `PipelineManager`, which submits and records pipeline steps |
| Dependency between tasks or rules | `OutputReference`, connecting a step's output role to a later input |
| Reusable group of workflow steps | `CompositeDefinition`, which expands into ordinary Artisan steps |
| Produced file or task return value | An artifact with a type, content identity, and declared parents |

An operation may produce many artifacts within one execution. It declares the
parents of each output, allowing provenance queries within a batch. Artisan
validates and records those declarations; it does not infer scientific lineage
from filenames or command arguments.

(comparison-prefect-relationship)=
## Artisan's native orchestration boundary

Artisan is independent of Prefect. `PipelineManager` owns step sequencing,
input resolution, cache lookup, and persistence. Its native local runner uses
worker processes; optional packages implement the public `RunnerBase` and
`LifecycleRouter` contracts for other infrastructure.

The separate `artisan-submitit` package supplies SLURM job-array and
intra-allocation runners. Users pass configured runner instances to Artisan.
Curators remain in local subprocesses, while creator workers use the selected
runner. A tool endpoint can independently place a creator's execute phase on
remote compute.

See [configuring execution](../how-to-guides/configuring-execution.md) for
placement choices and [runner providers](../contributing/coding-conventions.md#external-step-runner-providers)
for the integration contract.
