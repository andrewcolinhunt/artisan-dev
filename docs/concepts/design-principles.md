# Design Principles

Artisan separates computation, declared lineage, and persistence so you can
inspect what ran and reuse accepted results. These boundaries also define what
operation authors and pipeline owners must provide.

## Content is identity

An artifact ID hashes its registered type, canonical content, and semantic
metadata. Changing a name or semantic metadata creates a new identity even when
the bytes stay the same. Moving verified external bytes to another location
does not change identity.

Drafts remain mutable while an operation builds them. Finalization validates the
content and descriptors, assigns the ID, and protects durable fields. To change
an artifact, create another draft. Identical typed semantic artifacts share a
stored content row, while executions remain separate historical events.

Cache keys identify a declared invocation: the operation and behavior version,
parameters, effective configuration, and typed input occurrences. Input roles,
positions, grouping, and duplicates all matter. A key match makes work eligible
for reuse; cache policy and the prior result still determine whether it is used.

Operation authors must increment `version` when code or dependency changes can
change results for the same declared invocation. Operations that read mutable
external state must set `cacheable=False`. Artifact hashing does not make
undeclared inputs or nondeterministic computations reproducible.

See [Artifacts and Content Addressing](artifacts-and-content-addressing.md) and
[cache policy](../how-to-guides/configuring-execution.md#set-cache-policy).

## Provenance is always captured, never reconstructed

Execution provenance records which artifacts an execution consumed and produced.
Artifact provenance records the exact parents of each new output. A batch that
consumes A and B and produces C and D does not reveal whether C came from A,
B, or both. The operation supplies that relationship explicitly.

Every emitted artifact role has a lineage entry. Derived outputs name exact
input IDs or co-produced output indices; roots have an empty lineage list.
Artisan validates these declarations before accepting the result. It does not
infer missing parents from names, config references, or dispatch positions.

For an output with several jointly necessary parents, Artisan labels the declared
parent set with a group ID. That group describes the derivation. Input pairing
has its own grouping for dispatch and cache identity, which does not supply
missing lineage.

The framework can verify that a declared parent belongs to the allowed input
role. It cannot determine whether that parent is scientifically correct. Test
that relationship in the operation's tests.

See [Provenance System](provenance-system.md).

(creator-operations-are-pure-computation)=
## Creators separate computation from coordination

A creator's preprocess phase adapts artifacts to prepared inputs. Execute runs
the computation. Postprocess constructs draft artifacts and declares their
parents. The framework owns dispatch, working directories, staging, and commits.

This separation lets you test each phase directly. It does not make arbitrary
Python code side-effect-free: creators can read files and invoke external tools.
Keep undeclared external state out of cacheable operations, and use managed
command execution when Artisan needs to own command cancellation.

Curators receive an `ArtifactStore` because filtering, merging, and ingestion
need collection queries. They run in isolated local subprocesses even when the
pipeline uses an external runner for creators. Curators return result objects;
the orchestrator owns their commits.

Pipeline code chooses runner instances and per-step placement. Operations
provide provider-neutral resource and batching defaults. Composites reuse
wiring over ordinary steps, each with its own persistence boundary.

See [Operations Model](operations-model.md) and
[Configure Execution](../how-to-guides/configuring-execution.md).

(scale-is-transparent)=
## Runners share the execution contract

The built-in local runner and external providers dispatch execution units and
return ordered results through the same public runner contract. Operation
phases, lineage validation, and logical commits remain framework responsibilities.

Providers still differ in scheduling, filesystem access, credentials, and
cancellation evidence. Install the application and required tools in each
execution environment. A local success does not by itself qualify a remote
provider or container image.

The shared execution path has overhead: working directories, input verification,
and staged files can dominate short computations. Batching amortizes that work
across several artifacts.

See [Execution Flow](execution-flow.md) and
[Architecture Overview](architecture-overview.md#the-orchestrator-worker-split).

(shared-state-is-never-mutated-concurrently)=
## One writer coordinates shared state

Workers write isolated staging files. One orchestrator or repair process at a
time writes a store. Each logical commit has an immutable plan, a fixed order
of table effects, and a completion marker published last. Supported readers
expose the result only after that completion boundary.

This coordinates visibility across Delta tables; it does not turn several
physical table writes into one Delta transaction. Raw table reads can expose
partial effects. A crash may leave an exact plan available for retry or a sealed
execution available for recovery after verification.

Accepted outputs remain durable independently of later work. Cancellation
prevents unfinished work from being published as successful output, while a
result committed before cancellation remains available. Recovering execution
files does not silently replace a run's latest failed or unfinished attempt
with an earlier success.

See [Storage and Delta Lake](storage-and-delta-lake.md#the-staging-commit-pattern),
[Error Handling](error-handling.md), and
[run selection](../how-to-guides/inspecting-provenance.md#select-the-run-you-want-to-inspect).

(layers-depend-downward-only)=
## Keep public boundaries small

Use public package imports for application code. Schemas define data contracts,
operations define computation, storage supplies supported readers, execution
runs workers, and orchestration sequences steps. Internal modules can change
without becoming an application compatibility contract.

Custom artifact types and operations live in their application package. Trusted
application loading imports their definitions before workers or readers need
them. The framework remains domain-agnostic; registering a domain type does not
require adding domain logic to core.

See [Python API](../reference/python-api.md) and
[application loading](../how-to-guides/configuring-execution.md#load-application-definitions-in-every-process).

## Fail fast, fail loud

Artisan checks operation declarations when their classes are defined, validates
call shape before accepting a step attempt, and validates stored input content
before cache lookup or dispatch. These checks catch different errors at the
point where the relevant evidence becomes available.

For example, creator output contracts must declare `derives_from`, role enums
must match the declared roles, and algorithm configuration must use the matched
nested `Params` model. Runtime lineage validation then checks the actual parent
IDs and output indices, which are not known at class definition time.

An accepted attempt that fails records the available error and phase context.
Whether the pipeline continues depends on its failure policy. Cleanup reports
uncertainty when Artisan cannot prove termination; it does not convert a
cancellation request into evidence that a remote process has stopped.

See [Error Handling](error-handling.md).
