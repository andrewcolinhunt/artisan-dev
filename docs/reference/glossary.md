# Glossary

Key terms used throughout the Artisan documentation.

---

(glossary-artifact)=
## Artifact

An immutable, content-addressed data node identified by the xxh3\_128 hash of
its content. Artifacts are the fundamental data units flowing through pipelines.
Once finalized, an artifact's ID is a permanent commitment to its exact content.

---

(glossary-content-addressing)=
## Content addressing

A storage strategy where data is identified by the hash of its content rather
than by location or name. In Artisan, `artifact_id = xxh3_128(content)`, meaning
identical content always produces the same ID. This enables automatic
deduplication and deterministic caching.

---

(glossary-delta-lake)=
## Delta Lake

An open-source storage layer that brings ACID transactions to Parquet files.
Artisan uses Delta Lake as its backing store for all artifacts, provenance, and
execution records. It provides transactional writes, time travel, and partition
pruning without requiring an external database server.

---

(glossary-parquet)=
## Parquet

A columnar file format optimized for analytical queries. Delta Lake tables are
composed of Parquet files. Artisan stages worker results as Parquet files before
committing them atomically to Delta Lake.

---

(glossary-provenance)=
## Provenance

The record of where data came from and how it was produced. Artisan maintains
dual provenance: *execution provenance* (what computation happened) and
*artifact provenance* (which specific input produced which specific output).

---

(glossary-execution-unit)=
## Execution unit

The work package dispatched to a worker. An `ExecutionUnit` carries a fully
configured operation instance, a batch of input artifact IDs, the cache key, and
the step number. The number of artifacts per unit is controlled by
`execution.artifacts_per_unit`.

---

(glossary-nfs)=
## NFS

Network File System, a distributed filesystem protocol common on HPC clusters.
When workers run on different cluster nodes, Artisan calls `fsync()` on staged
files to ensure NFS close-to-open consistency before the orchestrator reads them.

---

(glossary-acid)=
## ACID

Atomicity, Consistency, Isolation, Durability — the four properties that
guarantee reliable database transactions. Delta Lake provides ACID transactions
over Parquet files, ensuring that concurrent worker writes and partial failures
never corrupt the shared artifact store.

---

(glossary-hydration)=
## Hydration

The process of loading an artifact's full content from storage. `hydrate=True`
(default) loads all fields including content bytes. `hydrate=False` loads only
the artifact ID and type, which is sufficient for passthrough operations like
Filter and Merge that route artifacts without reading their content.

---

(glossary-stem-matching)=
## Stem matching

The default algorithm for inferring artifact provenance (parent–child
relationships). It strips file extensions from input and output filenames, then
checks if the output stem starts with the input stem. Digit boundary protection
prevents `design_1` from matching `design_10`. Exactly one match is required;
zero or multiple matches result in an orphan artifact.
