# Artisan vs Pachyderm: Provenance & Artifact Tracking Comparison

## Executive Summary

Pachyderm and artisan represent two architecturally distinct approaches to the same fundamental problem: making data pipelines reproducible through content-addressed storage and automatic provenance tracking. Pachyderm is a Kubernetes-native, language-agnostic platform that applies "git for data" semantics at the repository/commit level, using containerized execution and object-store backends to version and process data at petabyte scale. Artisan is a Python-native batch pipeline framework that content-addresses individual artifacts (artifact_id = xxh3_128(content)), providing per-artifact provenance granularity with typed artifact schemas and HPC-native execution. Both systems achieve automatic provenance and deterministic caching through content addressing, but they diverge sharply in granularity (commit-level vs artifact-level), execution model (containers on Kubernetes vs Python processes on HPC/local), and infrastructure requirements.

---

## Provenance & Lineage

### Pachyderm's Approach

Pachyderm provides automatic, structural provenance at two levels:

- **Commit provenance**: Tracks relationships between commits across repositories. When a commit in one repo produces derived data in another, the derived commit is recorded as "provenant on" the source commit. This happens automatically whenever a pipeline processes input commits and produces output commits.
- **Branch provenance**: A more general assertion that future commits on a downstream branch will be derived from the head commit of an upstream branch. This captures the structural relationship between pipeline stages independent of any specific commit.
- **Global IDs**: When a new commit triggers downstream processing across a DAG, all resulting commits and jobs share a single Global Identifier. Running `pachctl list commit <globalID>` returns every commit across every repo that was created as part of that logical unit of work. This provides a "fan-out" view of provenance: one input change, traced to all its downstream effects.

Pachyderm's provenance is entirely automatic and transparent to the user. Every commit generates provenance edges. The `pachctl inspect commit` and `pachctl flush commit` commands allow upstream tracing (where did this data come from?) and downstream tracing (what did this input produce?).

### Artisan's Approach

Artisan maintains two complementary provenance systems:

- **Execution provenance**: What ran, when, with what parameters. Captured as ExecutionRecords (aligned to W3C PROV Activity).
- **Artifact provenance**: Individual derivation chains between artifacts. If an operation takes artifacts A, B, C and produces D, E, F, the system records individual edges A->D, B->E, C->F (aligned to W3C PROV wasDerivedFrom). This enables both micro provenance (tracing a single artifact's lineage) and macro provenance (the step-level DAG of the pipeline).

### Where They Diverge

The critical difference is **granularity**. Pachyderm's provenance operates at the commit level: "this output commit was derived from these input commits." Within a commit, there may be thousands of files, and provenance does not track which specific input file produced which specific output file. Artisan's provenance operates at the individual artifact level: each artifact has its own derivation chain.

This means artisan can answer "which specific input artifact produced this specific output artifact?" while Pachyderm answers "which input commit(s) contributed to this output commit?" Pachyderm's Global ID system is powerful for tracing the full DAG impact of a single change, but it does not provide the per-item granularity that artisan's dual provenance model offers.

Both systems make provenance structural rather than opt-in, which is a significant shared design principle that distinguishes them from tools where lineage is manually annotated.

---

## Data Model

### Pachyderm

Pachyderm's data model mirrors Git semantics applied to data:

- **Repository**: A named collection of versioned data. Each pipeline has an output repo; users create input repos.
- **Branch**: A mutable pointer to the head commit of a lineage within a repo.
- **Commit**: An immutable snapshot of the state of a repository at a point in time. Commits are content-addressed via hash trees (Merkle trees) where keys are filenames and values are file content plus metadata.
- **File**: The atomic unit of storage within a commit. Files are chunked, content-addressed, and stored in the backing object store. The hash tree provides versioning semantics across the entire file system state.

Pachyderm is **untyped** at the data model level. A repo contains files, and those files can be anything: CSVs, images, Parquet files, binary blobs. There is no schema enforcement or typed artifact system. The meaning of data is determined entirely by the pipeline code that reads and writes it.

### Artisan

Artisan's data model is artifact-centric:

- **Artifact**: The fundamental unit. Each artifact has a content-addressed ID (xxh3_128 hash of content), a type (Data, Metric, FileRef, Config, ExecutionConfig), and a role (named input/output slot on an operation).
- **ArtifactTypeDef**: Extensible type system allowing custom artifact types with defined schemas.
- **Batch-native**: Operations produce and consume lists of artifacts, not single values. This is a first-class concept, not an afterthought.

### Comparison

| Aspect | Pachyderm | Artisan |
|--------|-----------|---------|
| Unit of identity | File (within commit) | Individual artifact |
| Content addressing | Object hash tree (Merkle tree over file chunks) | xxh3_128 hash of artifact content |
| Type system | Untyped (files are opaque) | Typed (Data, Metric, FileRef, Config, etc.) |
| Schema enforcement | None at platform level | Via ArtifactTypeDef subclasses |
| Versioning granularity | Commit-level snapshots of entire repos | Per-artifact immutable identity |
| Named roles | No (files identified by path) | Yes (named input/output slots on operations) |
| Batch semantics | Via datum partitioning of files | First-class list-of-artifacts model |

Pachyderm's model excels at handling large, unstructured datasets (images, videos, genomic data) where the file system metaphor is natural. Artisan's model excels at structured, typed scientific workflows where individual artifacts have semantic meaning and relationships matter at the item level.

---

## Storage & Persistence

### Pachyderm

Pachyderm separates metadata from data:

- **Data storage**: Backed by any S3-compatible object store (AWS S3, GCS, Azure Blob Storage, MinIO for on-prem). Files are content-chunked and deduplicated before storage. Content-based chunking means that unchanged portions of modified files are not re-stored.
- **Metadata storage**: Stored in PostgreSQL and etcd. Hash trees (representing file system snapshots) are stored as metadata, with the format redesigned to support external sorting at scale (billions of files).
- **Deduplication**: Applied at the chunk level using content-addressed fixed-size chunks. This provides automatic deduplication across repos and commits. A known limitation is that modifications to the middle of large files can invalidate all subsequent chunks, causing re-storage of unchanged data.
- **Versioning**: Git-like commit history per repo. Every mutation creates a new commit. Immutable once committed.

### Artisan

- **Data storage**: Delta Lake-backed persistent storage with ACID transactions.
- **Metadata storage**: Integrated into the Delta Lake tables alongside artifact data.
- **Deduplication**: Automatic via content addressing. Same content = same artifact_id = stored once. Deduplication is at the artifact level, not the chunk level.
- **Concurrency**: Staging-commit pattern specifically designed to prevent concurrent write corruption on shared NFS/HPC filesystems, which is a common deployment scenario.
- **Versioning**: Artifacts are immutable by construction (identity IS the content hash). No explicit versioning layer needed; the provenance graph captures how artifacts relate over time.

### Comparison

Pachyderm's object-store backend is designed for cloud-scale data lakes with petabytes of unstructured data. It handles massive files well via chunked deduplication, though the fixed-chunk approach has known edge cases with in-place modifications. Artisan's Delta Lake approach is optimized for structured, tabular workflows common in scientific computing, with ACID transactions providing safety on shared filesystems (HPC/NFS) where Pachyderm's Kubernetes-based concurrency model does not apply.

---

## Pipeline & Execution

### Pachyderm

Pipelines are defined declaratively in JSON or YAML specifications:

```json
{
  "pipeline": {"name": "my-pipeline"},
  "transform": {
    "image": "my-docker-image:v1",
    "cmd": ["python", "/code/process.py"]
  },
  "input": {
    "pfs": {"repo": "input-data", "glob": "/*"}
  }
}
```

Key characteristics:
- **Containerized execution**: Every pipeline step runs in a Docker container. The user specifies the image and command; Pachyderm handles scheduling.
- **Language-agnostic**: Any language that can read/write files works. R, Python, Java, Go, bash scripts, compiled binaries.
- **Automatic triggering**: Pipelines trigger automatically when new commits appear on input repos. No manual scheduling needed.
- **Datum-based parallelism**: Glob patterns partition input data into datums. Each datum is processed independently by a worker pod. Results are combined into the output commit.
- **Input combinators**: Cross (cartesian product), Union, Join, and Group operations combine datums from multiple input repos.
- **Worker pods**: Kubernetes pods that run continuously, waiting for new data. Parallelism is configurable (default: 1 worker per node).

### Artisan

Pipelines are defined in Python:
- **Python-native**: Operations are Python classes. Pipelines are composed programmatically.
- **Operation-centric**: Each operation defines typed input/output specs with named roles.
- **Two-level batching**: Artifacts-per-unit and units-per-worker, giving fine-grained control over batch sizes.
- **HPC-native execution**: SLURM integration via submitit, plus local process pool.
- **Full sandbox isolation**: Each execution gets an isolated sandbox.
- **Deterministic caching**: Based on content hashes of inputs plus parameters.

### Comparison

| Aspect | Pachyderm | Artisan |
|--------|-----------|---------|
| Pipeline definition | JSON/YAML spec | Python code |
| Execution unit | Docker container | Python process |
| Triggering | Automatic on data change | Explicit run/submit |
| Parallelism model | Datum-level on Kubernetes pods | Two-level batching on HPC/local |
| Language support | Any (via containers) | Python |
| Input partitioning | Glob patterns + combinators | Operation input specs with roles |
| Scheduling | Kubernetes-managed | SLURM or local process pool |

Pachyderm's model is "describe what to run and how to partition; the platform handles when and where." Artisan's model is "compose operations in Python with explicit control over batching and execution targets." Pachyderm's automatic triggering on data changes is a powerful feature for always-on pipelines; artisan's explicit execution model suits batch scientific workflows where runs are deliberate.

---

## Caching & Incrementality

### Pachyderm

Pachyderm's incrementality operates at the **datum** level:

- The system tracks which datums have been successfully processed via a hash computed from the pipeline definition and the content hash of the datum's data.
- When new commits arrive, Pachyderm identifies which datums are new or changed and which are unchanged.
- Unchanged datums are **skipped entirely**. Their previous output is carried forward and combined with new results to form the output commit.
- This is automatic and transparent. Users do not opt in to caching; it is the default behavior.
- The `--reprocess` flag can force full reprocessing when needed.

This means that if a commit adds one file to a repo containing 10,000 files, and the glob pattern maps each file to a datum, only the one new datum is processed. The 10,000 previous results are reused.

### Artisan

Artisan's caching operates at the **artifact** level:

- Cache keys are derived from content hashes of input artifacts plus operation parameters.
- If the same inputs (by content hash) have been processed by the same operation with the same configuration before, the cached output is returned without re-execution.
- Because artifact identity IS the content hash, deduplication is intrinsic: identical content is never stored twice, and identical computations are never run twice.

### Comparison

Both systems achieve "don't recompute what hasn't changed" through content addressing, but at different granularities:

- **Pachyderm**: Datum-level. A datum may contain one or many files. Caching granularity depends on how glob patterns partition the data.
- **Artisan**: Artifact-level. Each individual artifact's processing is independently cacheable.

Pachyderm's approach is more flexible in what constitutes a "unit of caching" (the glob pattern is user-configurable), while artisan's approach is more granular by default (each artifact is independently tracked). Pachyderm's caching is more natural for file-based workflows; artisan's for structured artifact workflows.

---

## Infrastructure Model

### Pachyderm

- **Hard requirement**: Kubernetes cluster (any provider: AWS EKS, GKE, Azure AKS, on-prem, or local via Minikube/kind).
- **Storage requirement**: S3-compatible object store (or cloud-native: GCS, Azure Blob).
- **Metadata**: PostgreSQL + etcd persistent volumes.
- **Networking**: gRPC between pachd daemon, worker pods, and client CLI.
- **Scaling**: Kubernetes-native horizontal scaling. Add nodes to the cluster, increase parallelism in pipeline specs.
- **Deployment**: Helm charts, with images available from Iron Bank (DoD-hardened) for high-security environments.

The Kubernetes requirement is both Pachyderm's greatest strength (cloud-native, auto-scaling, container isolation) and its greatest barrier to entry (significant operational complexity for teams without Kubernetes expertise).

### Artisan

- **No infrastructure requirement beyond Python**: Runs locally with no external services.
- **HPC integration**: SLURM submission via submitit for cluster computing.
- **Storage**: Delta Lake on local or shared filesystem (NFS).
- **Scaling**: Process pool locally; SLURM job arrays on HPC.
- **Deployment**: `pip install` (or equivalent). No container orchestration needed.

### Trade-offs

Pachyderm's infrastructure model assumes a cloud-native or Kubernetes-managed environment. This gives it container isolation, language agnosticism, and cloud-scale storage, but it requires operational expertise in Kubernetes, Helm, object stores, and distributed systems debugging. Artisan's model assumes a Python/scientific computing environment, possibly with HPC access. This gives it zero-infrastructure startup, HPC integration, and simplicity, but it is Python-only and does not provide container-level isolation.

For a research lab with a SLURM cluster, artisan is the natural fit. For a cloud-native ML team with Kubernetes infrastructure already in place, Pachyderm integrates cleanly.

---

## Current State

### HPE Acquisition and Product Evolution

HPE acquired Pachyderm in January 2023 to expand its AI-at-scale capabilities. Pachyderm was rebranded as **HPE Machine Learning Data Management (MLDM)** and integrated alongside **HPE Machine Learning Development Environment (MLDE)** (formerly Determined AI, acquired by HPE 18 months prior). The two products now form a combined platform via the **PDK (Platform Development Kit)**.

### Open Source Status

The Pachyderm GitHub repository remains under the Apache-2.0 license as of 2025. The codebase is public at `github.com/pachyderm/pachyderm`. However, the Community Edition imposes scaling limits:
- Maximum 16 user-created pipelines
- Maximum 8 workers (constant parallelism)
- Enterprise license required to exceed these limits

These limits did not exist in earlier versions, effectively creating a freemium model where the open-source code is available but production-scale usage requires an enterprise license.

### Development Activity

As of late 2024, the repository showed continued development with version 2.12.0-rc.1 released in October 2024. The project has 6,287 commits and 231 contributors. However, development pace appears to have shifted toward the HPE enterprise product rather than community-driven open-source development. The `python-pachyderm` client library was archived in January 2025, with the SDK now maintained as part of the main repository.

### Community

The acquisition has created uncertainty in the open-source community. HPE's focus is enterprise customers, and the Community Edition scaling limits signal that serious usage is expected to be licensed. The project's identity is increasingly that of an HPE enterprise product rather than an independent open-source project.

---

## Where Pachyderm is Stronger

- **Container isolation and language agnosticism**: Any code that reads/writes files can be a pipeline step. R, Julia, C++, bash scripts, pre-compiled binaries. Artisan is Python-only.
- **Cloud-native scalability**: Kubernetes-based horizontal scaling to petabytes of data and large clusters. Automatic worker pod management, autoscaling, and resource orchestration.
- **Automatic pipeline triggering**: Pipelines fire automatically when input data changes. No explicit execution step needed. This is powerful for always-on, event-driven data processing.
- **Data versioning at scale**: The "git for data" model with repos, branches, and commits provides a familiar and powerful metaphor for versioning large datasets. Branching enables experimentation without duplicating data.
- **Input combinators**: Cross, Union, Join, and Group operations for combining data from multiple repos provide flexible datum construction that has no direct equivalent in artisan.
- **Unstructured data handling**: The file-based model naturally handles images, videos, genomic data, and other unstructured formats at scale.
- **Global ID for DAG-wide tracing**: A single identifier connects all commits and jobs resulting from one input change across the entire DAG. This provides an immediate answer to "what was the full impact of this data change?"
- **Enterprise features**: RBAC, multi-cluster management, JupyterHub integration, DoD-hardened images (Iron Bank).

---

## Where Artisan is Stronger

- **Per-artifact provenance granularity**: Individual derivation chains (A->D, B->E, C->F) rather than commit-level provenance. This is critical for scientific workflows where understanding which specific input produced which specific output matters.
- **Typed artifact system**: Data, Metric, FileRef, Config, ExecutionConfig with extensible type definitions. Artifacts carry semantic meaning at the platform level, not just as opaque files.
- **Python-native simplicity**: No Kubernetes, no Docker, no Helm charts, no object store configuration. `pip install` and run. The barrier to entry is orders of magnitude lower.
- **HPC integration**: Native SLURM support via submitit. This is the standard execution environment for academic and scientific computing. Pachyderm has no HPC story.
- **Named roles and batch semantics**: Operations define typed input/output slots with roles. Batch processing of artifact lists is first-class, not bolted on via glob patterns.
- **W3C PROV alignment**: Explicit alignment with the W3C provenance standard (Activity, Entity, wasDerivedFrom). This matters for scientific reproducibility and regulatory compliance.
- **Dual provenance model**: Separate execution provenance (what ran) and artifact provenance (what derived from what) provide complementary views. Pachyderm conflates these into a single commit-based model.
- **No infrastructure overhead**: No Kubernetes cluster to maintain, no object store to provision, no persistent volumes to configure. Runs on a laptop or an HPC login node.
- **Shared filesystem safety**: The staging-commit pattern with ACID transactions on Delta Lake is specifically designed for shared NFS filesystems common in HPC environments. Pachyderm assumes Kubernetes-managed storage.
- **No scaling limits in open-source**: No artificial pipeline or parallelism caps.

---

## Key Differentiators Summary Table

| Dimension | Pachyderm | Artisan |
|-----------|-----------|---------|
| **Core metaphor** | "Git for data" (repos, branches, commits) | Content-addressed artifact graph |
| **Content addressing** | Object hash tree (Merkle tree over file chunks) | xxh3_128 hash per artifact |
| **Provenance granularity** | Per-commit (which commits derived from which) | Per-artifact (which artifact derived from which) |
| **Provenance model** | Single: commit/branch provenance + Global IDs | Dual: execution provenance + artifact provenance |
| **Standards alignment** | Custom (commit provenance, Global IDs) | W3C PROV (Activity, Entity, wasDerivedFrom) |
| **Type system** | Untyped (files are opaque bytes) | Typed (Data, Metric, FileRef, Config, etc.) |
| **Pipeline definition** | Declarative JSON/YAML | Programmatic Python |
| **Execution model** | Docker containers on Kubernetes pods | Python processes on local/SLURM |
| **Language support** | Any (via containers) | Python only |
| **Triggering** | Automatic on data commit | Explicit run/submit |
| **Caching granularity** | Datum-level (glob-configurable) | Artifact-level |
| **Storage backend** | S3-compatible object store | Delta Lake on local/shared filesystem |
| **Deduplication** | Chunk-level (content-based chunking) | Artifact-level (same hash = same artifact) |
| **Infrastructure requirement** | Kubernetes + object store + PostgreSQL + etcd | Python environment (+ optional SLURM) |
| **Scaling model** | Kubernetes horizontal scaling | HPC job arrays / local process pool |
| **Target environment** | Cloud-native / enterprise Kubernetes | Scientific computing / HPC / local |
| **License** | Apache-2.0 (with Community Edition scaling limits) | Open source (no scaling limits) |
| **Governance** | HPE enterprise product (acquired Jan 2023) | Independent project |
| **Batch semantics** | Datum partitioning via glob patterns | First-class list-of-artifacts with named roles |
| **Data versioning** | Full git-like history (branches, commits, diffs) | Immutable artifacts (identity = content hash) |
