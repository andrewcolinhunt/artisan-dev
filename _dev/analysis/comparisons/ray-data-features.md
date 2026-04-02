# Ray Data: Comprehensive Feature Analysis

Research date: 2026-03-25. Based on Ray 2.54.0 documentation, Anyscale blog
posts, DeepWiki analysis, and source code review.

---

## Streaming Execution Model

Ray Data switched its default execution strategy from bulk synchronous to
streaming in Ray 2.4. The streaming executor does not wait for one operator to
complete before starting the next -- data flows continuously through the
pipeline.

### Architecture

The execution follows a three-stage pipeline:

- **Logical planning** -- user API calls (`read_parquet`, `map_batches`, etc.)
  construct a `LogicalPlan` DAG of logical operators.
- **Physical planning** -- the planner converts logical operators into physical
  operators (e.g., `ReadOp` becomes `TaskPoolMapOperator`), applying
  optimization passes at both levels.
- **Streaming execution** -- `StreamingExecutor` continuously schedules tasks
  to physical operators with backpressure control.

### StreamingExecutor

`StreamingExecutor` inherits from both `Executor` and `threading.Thread`. It
runs on a dedicated background thread (typically on the driver process; for
training jobs via `streaming_split()`, on the `SplitCoordinator` actor).

Key attributes:

| Attribute | Purpose |
|---|---|
| `_topology` | Dict mapping each `PhysicalOperator` to its `OpState` |
| `_resource_manager` | Manages CPU, GPU, memory, object store budgets |
| `_backpressure_policies` | Controls flow rates across operators |
| `_progress_manager` | Tracks execution progress |
| `_output_node` | Terminal operator producing results |

### Execution Loop

The main loop in `run()`:

```
while True:
    t_start = time.perf_counter()
    continue_sched = self._scheduling_loop_step(self._topology)
    sched_loop_duration = time.perf_counter() - t_start
    self.update_metrics(sched_loop_duration)
    if not continue_sched or self._shutdown:
        break
```

Each `_scheduling_loop_step()` performs:

- **Process completed tasks** -- calls `ray.wait()` on active task refs with
  `timeout=0`, pulls finished outputs into operator output queues.
- **Dispatch new tasks** -- iteratively calls `select_operator_to_run()` then
  `dispatch_next_task()` until no operator is eligible.
- **Update state** -- refreshes operator states, progress tracking.
- **Trigger scaling** -- invokes cluster and actor autoscalers.

Returns `bool` indicating whether scheduling should continue. Loop terminates
when all operators complete or `_shutdown` is set.

### Topology and OpState

The `Topology` is a dictionary mapping each `PhysicalOperator` to its
`OpState`. Each `OpState` tracks:

| Field | Purpose |
|---|---|
| `input_queues` | FIFO buffers from upstream operators |
| `output_queue` | `OpBufferQueue` to downstream operators |
| `op` | The physical operator instance |
| `num_completed_tasks` | Finished task count |
| `_scheduling_status` | Runnable status and resource availability |
| `op_display_metrics` | CPU, GPU, memory, task metrics for UI |

The `OpBufferQueue` is thread-safe, buffering `RefBundle` objects and tracking
block counts and memory usage estimates.

### RefBundle

The unit of data transfer between operators:

- `blocks` -- list of `(ObjectRef, BlockMetadata)` tuples
- `owns_blocks` -- ownership flag for lifetime management
- `output_split_idx` -- index for streaming splits
- Methods: `size_bytes()`, `num_rows()`

---

## Backpressure and Flow Control

Ray Data manages backpressure across the streaming topology to bound memory
usage and avoid object store spilling. The system only schedules new tasks if it
would keep the streaming execution under configured resource limits.

### Backpressure Policies

**ConcurrencyCapBackpressurePolicy** -- limits concurrent tasks per operator
via `max_concurrency`. Useful for controlling memory consumption of expensive or
stateful operations.

**ResourceBudgetBackpressurePolicy** -- prevents exceeding resource budgets by
checking:

- Internal queue memory: `op.internal_input_queue_num_bytes()` vs configured
  limit
- Incremental resource usage: `op.incremental_resource_usage()` vs remaining
  budget
- Output queue memory: dynamic limit based on downstream consumption rate

When `OpResourceAllocator` is enabled, output queue limits adjust dynamically
-- fast-consuming downstreams get higher limits for pipelining, slow ones get
reduced limits.

### Backpressure Metrics

Operators track:

- `_in_task_submission_backpressure` -- task submission blocked
- `_in_task_output_backpressure` -- output production blocked
- `task_submission_backpressure_time` -- total time in submission backpressure
- `task_output_backpressure_time` -- total time in output backpressure

The `[backpressure]` label appears in progress bars when an operator is
backpressured, meaning it won't submit more tasks until the downstream operator
is ready.

### Downstream Capacity Backpressure

Configured via `DataContext.downstream_capacity_backpressure_ratio` (default:
10.0). Controls how much output an operator can buffer relative to downstream
consumption rate.
`DataContext.enable_dynamic_output_queue_size_backpressure` (default: False)
enables dynamic adjustment of queue sizes.

---

## Parallelism and Resource Management

### ResourceManager

The `ResourceManager` tracks and allocates cluster resources across four types:

| Resource | Measurement |
|---|---|
| `cpu` | Logical cores from `ray.remote` args |
| `gpu` | GPU cores from `ray.remote` args |
| `memory` | Heap memory in bytes |
| `object_store_memory` | Block sizes and queue depths |

### Resource Budgeting Strategies

**Simple allocation** -- distributes global limits without per-operator budgets.

**Reservation-based allocation** (`ReservationOpResourceAllocator`, default
enabled):

- Each operator reserves minimum resources (e.g., `min_actors * cpu_per_actor`)
- Remaining resources form a shared pool
- Shared pool distributed by current operator usage ratios
- Operators can exceed reservation by borrowing from shared pool

Configured via:

- `DataContext.op_resource_reservation_enabled` (default: True)
- `DataContext.op_resource_reservation_ratio` (default: 0.5)

Object store memory limit defaults to 50% with reservation enabled, 25% when
disabled.

### ExecutionOptions Resource Controls

- `resource_limits` -- soft caps on resource consumption during execution
  (auto-detected by default)
- `exclude_resources` -- resources reserved for external processes
  (auto-configured with Ray Train)

### Operator Selection Algorithm

`select_operator_to_run()` filtering:

- Get eligible operators (has inputs, not complete, can accept input)
- Filter based on resource availability
- Apply backpressure policies
- Rank eligible operators (downstream prioritized over upstream)
- Return highest-priority operator; prioritize operator with smallest out queue

When multiple operators qualify, the executor prioritizes the one with the
smallest output queue to avoid buffering.

### Scheduling Strategies

- **SPREAD** -- applied to read operations and map operations with <50 MB
  argument sizes. Distributes blocks across cluster nodes.
- **DEFAULT** -- used for shuffle, sort, split operations and map operations
  with large arguments (>50 MB, configurable via
  `DataContext.large_args_threshold = 52428800`).

Configurable via `DataContext.scheduling_strategy` (default: `"SPREAD"`) and
`DataContext.scheduling_strategy_large_args` (default: `"DEFAULT"`).

### Parallelism for Reads

The number of output blocks for reads is determined by:

- Default baseline: 200 blocks (`DataContext.read_op_min_num_blocks`)
- Minimum block threshold: 1 MiB (`DataContext.target_min_block_size`)
- Maximum block threshold: 128 MiB (`DataContext.target_max_block_size`)
- CPU utilization: at least 2x available CPUs

`override_num_blocks` allows manual control. When input files are fewer than
`override_num_blocks`, Ray splits read task outputs into the desired number of
blocks.

### Actor Pool Autoscaling

`ActorPoolStrategy` controls actor-based execution:

- Fixed pool: `ActorPoolStrategy(size=n)`
- Autoscaling: `ActorPoolStrategy(min_size=m, max_size=n)`
- With initial size: `ActorPoolStrategy(min_size=m, max_size=n, initial_size=initial)`

Default for callable classes: `ActorPoolStrategy(min_size=1, max_size=None)`
(autoscaling from 1 to unlimited).

The `ActorAutoscaler` scales pools dynamically based on queue depth and task
completion rate metrics.

### Cluster Autoscaling

The `ClusterAutoscaler` requests cluster scaling based on
`pending_processor_usage()` across operators, triggered each scheduling loop
iteration.

---

## Fault Tolerance

### Task-Level Retries

Ray provides automatic retries controlled by `max_retries` (default: 3 for
non-actor tasks). Set to -1 for infinite retries, 0 to disable.

- **System-level failures** (worker crashes, node failures) automatically
  trigger retries.
- **Application-level failures** (Python exceptions) require explicit
  `retry_exceptions=True` or a list of specific exception types.
- Global override: `RAY_TASK_MAX_RETRIES` environment variable.

### Actor Task Retries

Actor method calls are at-most-once by default. Enable retries via:

- `max_task_retries > 0` for bounded retries
- `max_task_retries = -1` for unlimited retries (at-least-once semantics)

For Ray Data specifically:

- `DataContext.actor_task_retry_on_errors` (default: False) -- whether to retry
  actor tasks on specific errors
- `DataContext.actor_init_retry_on_errors` (default: False)
- `DataContext.actor_init_max_retries` (default: 3)

### Object Fault Tolerance / Lineage Reconstruction

When an object value is lost from the object store (e.g., node failure):

- Ray first attempts to find copies on other nodes.
- If no copies exist, Ray re-executes the task that created the value (lineage
  reconstruction).
- Requires the object to have been generated by a task (not `ray.put`).
- Tasks are assumed deterministic and idempotent.
- Disable via `RAY_TASK_MAX_RETRIES=0`.

Limitations:

- Objects from `ray.put` are not recoverable.
- Actor-created objects are not reconstructable by default.
- No recovery from owner (driver) failure.

### Error Tolerance

`DataContext.max_errored_blocks` (default: 0) -- allows a specified number of
blocks to fail without failing the entire job. Failed block data is discarded.
Negative values indicate unlimited tolerance. Useful for long-running jobs
resilient to corrupted samples.

`DataContext.write_file_retry_on_errors` -- list of error substrings that
trigger automatic write retries (defaults to common AWS S3 transient errors).

### Error Wrapping

When a task fails due to a Python exception, Ray wraps the original in
`RayTaskError`. If the user's exception type supports subclassing, the raised
exception is an instance of both `RayTaskError` and the original type. Unserializable
exceptions convert to `RayError`.

---

## Memory Management

### Block Size Management

Blocks are the fundamental unit of data -- each contains a disjoint subset of
rows, formatted as Arrow tables or pandas DataFrames.

| Parameter | Default | Purpose |
|---|---|---|
| `target_min_block_size` | 1 MiB | Minimum block threshold |
| `target_max_block_size` | 128 MiB | Maximum block size for reads/transforms |
| `target_shuffle_max_block_size` | 1 GiB | Block size limit for shuffle operations |
| `MAX_SAFE_BLOCK_SIZE_FACTOR` | 1.5 | Dynamic splitting trigger (192 MiB) |

Workers automatically split outputs exceeding `target_max_block_size` into
smaller blocks.

### Heap Memory Bounds

Ray Data attempts to cap heap memory usage at:

```
num_execution_slots * max_block_size
```

where execution slots default to CPU count (or custom resources if specified).

### Object Store

- Blocks are stored in Ray's shared-memory object store.
- Automatic spilling to disk when memory capacity is exceeded.
- Locality scheduling prioritizes task placement on nodes with local block
  copies.
- Reference counting maintains blocks while any Dataset references them.
- `DataContext.override_object_store_memory_limit_fraction` allows overriding
  the default limit.

### Spilling

Object store spilling is expected in:

- All-to-all shuffle operations (`random_shuffle()`, `sort()`)
- `ds.materialize()` calls with large datasets
- Reading 1+ GiB binary files

Metrics: `obj_store_mem_spilled`, `obj_store_mem_freed`, `obj_store_mem_used`.

### Memory Recommendations

- Individual rows should be <10 MB.
- `map_batches()` batch sizes should fit comfortably in heap memory.
- Example impact: default batch size (1024 rows) on 1 GB dataset with 1 MB rows
  produced 4.2 GB peak heap memory; reducing to 32 rows decreased peak to
  1.6 GB.

### Eager Free

`DataContext.eager_free` (default: False) -- when enabled, eagerly frees memory
after blocks are consumed. `DataContext.trace_allocations` (default: False)
enables allocation tracing for debugging.

---

## Scheduling

### Operator Selection

The streaming executor selects which operator to dispatch next through:

- **Eligibility check** -- has inputs, not complete, can accept input
  (`should_add_input()` returns True)
- **Resource check** -- sufficient CPU/GPU/memory available
- **Backpressure check** -- not blocked by downstream
- **Priority ranking** -- downstream operators prioritized over upstream;
  among equals, smallest output queue wins

This ensures a pull-based approach: work flows toward the output, minimizing
buffering.

### Task Dispatch

`dispatch_next_task()`:

- Pulls next input bundle from input queue
- Creates remote task(s) via operator's execution logic
- Enqueues returned object references for tracking
- Respects backpressure by not exceeding output queue limits

### Placement Groups

By default, Ray Data tasks ignore placement groups. Override with
`scheduling_strategy = None` to respect existing placement groups.

---

## Batching

### map_batches()

The primary batching API with full parameter set:

```python
Dataset.map_batches(
    fn,
    batch_size=None,           # rows per batch; None = entire block
    batch_format='default',    # 'default'/'numpy'/'pandas'/'pyarrow'/None
    zero_copy_batch=True,      # read-only zero-copy view
    compute=None,              # TaskPoolStrategy or ActorPoolStrategy
    num_cpus=None,             # CPU cores per task/actor
    num_gpus=None,             # GPU units per task/actor
    concurrency=None,          # int or (min, max) or (min, max, initial)
    ...
)
```

### Batch Format Options

| Format | Type |
|---|---|
| `"default"` / `"numpy"` | `Dict[str, numpy.ndarray]` |
| `"pandas"` | `pandas.DataFrame` |
| `"pyarrow"` | `pyarrow.Table` |
| `None` | Preserves input block format |

### Zero-Copy Batches

When `zero_copy_batch=True` (default), batches are read-only views on data in
Ray's object store. This decreases memory utilization and improves performance.
Set to `False` if the UDF needs to modify data in-place (copies entire batch).

### Task Bundling (BlockRefBundler)

The `BlockRefBundler` accumulates input blocks until reaching
`min_rows_per_bundle`, creating bundled tasks to amortize overhead. When
`batch_size` is set and each input block is smaller than `batch_size`, Ray Data
bundles multiple blocks as input for one task until their total size meets or
exceeds the batch size.

### Compute Strategies

- **TaskPoolStrategy** (default for functions) -- stateless, ephemeral tasks.
  `TaskPoolStrategy(size=n)` limits to `n` concurrent tasks.
- **ActorPoolStrategy** (default for callable classes) -- stateful, long-lived
  actors. Amortizes expensive initialization (e.g., loading GPU models).

---

## Progress Tracking and Observability

### Progress Bars

Real-time console progress bars updated every second showing:

- Row counts (completed/remaining)
- Resource usage (CPU and GPU allocation across operators)
- Task/actor status (running, pending, completed)
- Backpressure indicators (`[backpressure]` label)

Configuration:

| Setting | Default |
|---|---|
| `enable_progress_bars` | True |
| `enable_operator_progress_bars` | True |
| `enable_progress_bar_name_truncation` | True (100-char threshold) |
| `enable_rich_progress_bars` | False (experimental rich UI) |
| `use_ray_tqdm` | True (distributed tqdm) |

### Dashboard

The Ray Data dashboard (introduced in Ray 2.9) provides:

- **Jobs View** -- execution progress (blocks), state (running/failed/finished),
  timestamps, aggregated metrics, expandable per-operator details.
- **Metrics View** -- time-series graphs with operator-specific metrics grouped
  by dataset and operator.

### Prometheus Metrics

Tagged with `dataset` and `operator` labels. Categories:

- **Overview** -- `data_spilled_bytes`, `data_freed_bytes`,
  `data_current_bytes`, `data_cpu_usage_cores`, `data_gpu_usage_cores`,
  `data_output_bytes`, `data_output_rows`
- **Input** -- `num_inputs_received`, `bytes_inputs_received`,
  `num_task_inputs_processed`, `bytes_task_inputs_processed`
- **Output** -- `num_task_outputs_generated`, `bytes_task_outputs_generated`,
  `rows_task_outputs_generated`, queue depth metrics
- **Task** -- `num_tasks_submitted/running/finished/failed`,
  `block_generation_time`, `task_submission_backpressure_time`. Histograms:
  `task_completion_time`, `block_size_bytes`, `block_size_rows`
- **Actor** -- `num_alive_actors`, `num_restarting_actors`,
  `num_pending_actors`
- **Object Store** -- internal queue occupancy, `obj_store_mem_freed/spilled/used`,
  `obj_store_mem_pending_task_inputs`
- **Iterator** -- `data_iter_total_blocked_seconds`

Metrics update interval: 5 seconds (`UPDATE_METRICS_INTERVAL_S`).

### Logging

Execution progress logged to `/tmp/ray/{SESSION_NAME}/logs/ray-data/ray-data.log`
every 5 seconds.

Enable detailed scheduling traces via `RAY_DATA_TRACE_SCHEDULING=1`.

### Stats Method

`ds.stats()` provides operator-level execution summaries with min/max/mean/sum
aggregations across all blocks.

Configuration:

- `DataContext.enable_auto_log_stats` (default: False)
- `DataContext.verbose_stats_logs` (default: False)
- `DataContext.log_internal_stack_trace_to_stdout` (default: False)

---

## Operator Fusion

### Mechanism

The `OperatorFusionRule` merges consecutive map operators into a single map
operator, eliminating serialization overhead between them. Fusion occurs during
plan optimization, before physical operators are created.

### Planning Architecture

Both logical and physical plans go through optimization passes:

- Logical rules: `DEFAULT_LOGICAL_RULES` -- high-level optimizations
- Physical rules: `DEFAULT_PHYSICAL_RULES` including `OperatorFusionRule`
- Custom rules extend the `Rule` class

### Fusion Criteria

Operations fuse when they have equivalent resource requirements. Steps with
different specifications (CPU vs GPU, task vs actor) become distinct stages,
while compatible steps combine into single execution units.

### Interaction with Block Sizes

When an AllToAll op gets fused with a Map op, the upstream op inherits the
larger target block size. AllToAll ops use a larger default block size
(`target_shuffle_max_block_size` = 1 GiB) compared to Map ops (128 MiB).

### Interaction with Parallelism

When `override_num_blocks` is specified and differs from the input file count,
Ray Data prevents fusion with downstream transforms. This materializes read
outputs to the object store before the consuming map task executes, ensuring the
desired parallelism.

### Source Location

`python/ray/data/_internal/logical/rules/operator_fusion.py`

---

## Data Locality

### Locality-Aware Scheduling

Ray preferentially schedules compute tasks on nodes that already have a local
copy of the object, reducing network transfer.

### SPREAD Strategy

Read operations and small-argument map operations (<50 MB) use SPREAD
scheduling to distribute work evenly across the cluster.

### Actor Locality

`ExecutionOptions.actor_locality_enabled` (default: False) enables
locality-aware dispatch for stateful `map()` operations. The
`_ActorTaskSelector` assigns bundles to actors based on data locality.

### Output Locality

`ExecutionOptions.locality_with_output` (default: disabled) directs tasks
toward the output driver node or specific node IDs. Particularly beneficial for
ML training data ingest scenarios.

---

## Dynamic Repartitioning

### repartition()

Three modes:

- **With `num_blocks` + `shuffle=True`** -- full distributed shuffle producing
  exactly `num_blocks` blocks. Expensive: requires full materialization and
  acts as synchronization barrier.
- **With `num_blocks` + `shuffle=False`** -- splits and combines blocks to
  minimize data movement without full shuffle.
- **With `target_num_rows_per_block`** (exclusive with `num_blocks` and
  `shuffle`) -- streaming repartitioning where blocks carry no more than the
  target rows. Smaller blocks are combined up to the target.

### Key-Based Repartitioning (Ray 2.46+)

`repartition(keys=...)` with hash-shuffle strategy deterministically co-locates
rows based on column value hashes. Requires:

```python
DataContext.get_current().shuffle_strategy = ShuffleStrategy.HASH_SHUFFLE
```

### Shuffle Strategies

**Hash-Shuffling** -- partitions rows via `hash(key-values) % N`, routes
shards to `HashShuffleAggregator` actors, then recombines via reduce phase.
Ideal for joins and group-by.

**Range-Partitioning Shuffle** (default) -- samples 10 rows per block, derives
range boundaries from sorted samples, partitions blocks accordingly.

**Push-Based Shuffle** -- for large-scale (>1000 blocks or >1 TB). Enable via
`RAY_DATA_PUSH_BASED_SHUFFLE=1` or
`DataContext.get_current().use_push_based_shuffle = True`.

### Coalescing Without Materialization

`ds.map_batches(lambda batch: batch, batch_size=X)` performs streaming
coalescing without triggering a full materialization barrier, unlike
`ds.repartition()`.

---

## Execution Strategies

### Streaming (Default since Ray 2.4)

Operators execute concurrently in a pipeline. Data flows continuously through
connected operator queues without waiting for upstream completion. Memory-bounded
via backpressure.

### Bulk Synchronous (Legacy)

Each operator completes fully before the next starts. All intermediate data
materialized. No longer the default but behavior emerges for all-to-all
operations (shuffle, sort) which act as synchronization barriers.

### Lazy Execution

`ds = ds.lazy()` defers all subsequent operations until the dataset is consumed
or `ds.fully_executed()` is called. Enables additional fusion optimizations.

### Ordering

`ExecutionOptions.preserve_order` (default: False) maintains block processing
order through the streaming executor. Ensures deterministic output at the cost
of some performance.

---

## Error Handling

### Error Propagation

Exceptions in the `StreamingExecutor.run()` thread are caught and stored:

```python
except Exception as e:
    exc = e
finally:
    _, state = self._output_node
    state.mark_finished(exc)
```

The output iterator receives the exception and propagates it to the consumer.

### Error Wrapping

- Application errors: wrapped in `RayTaskError` (inherits from both
  `RayTaskError` and the original exception type when possible)
- Unserializable exceptions: converted to `RayError`
- `DataContext.raise_original_map_exception` (default: False) -- when True,
  raises original UDF exceptions rather than wrapped `UserCodeException`

### Error Tolerance

`DataContext.max_errored_blocks` (default: 0) -- number of blocks allowed to
fail without aborting the job. Failed block data is discarded silently. Set to
negative for unlimited tolerance.

### Retry Configuration

- Task retries: `max_retries` (default: 3)
- Actor task retries: `DataContext.actor_task_retry_on_errors` (default: False)
- Write retries: `DataContext.write_file_retry_on_errors` (default: AWS S3
  transient errors)

---

## Cancellation

### Task Cancellation

`ray.cancel(object_ref)`:

- Default: sends `KeyboardInterrupt` to the task's worker
- `force=True`: force-exits the worker
- Cancelled tasks are not automatically retried

### Executor Shutdown

`StreamingExecutor.shutdown()`:

- Acquires `_shutdown_lock` (threading.RLock) to prevent race conditions
- Sets `_shutdown = True` flag
- Waits for thread via `join(timeout=2.0)`
- Updates final metrics
- Shuts down operators hierarchically
- Invokes post-execution callbacks
- Unregisters dataset logger

Force parameter allows asynchronous task termination. The destructor calls
`shutdown(force=False)` automatically.

### Executor Validation

On completion, `_validate_operator_queues_empty()` verifies all operator queues
are drained.

---

## Result Consumption

### Lazy Evaluation

Dataset transformations are lazy. Execution is triggered only by consumption
operations.

### Iteration Methods

| Method | Returns | Use Case |
|---|---|---|
| `iter_batches(batch_format=...)` | Iterator of batches | Generic batch consumption |
| `iter_rows()` | Iterator of dicts | Row-by-row processing |
| `iter_torch_batches()` | Iterator of torch tensors | PyTorch training |
| `iter_tf_batches()` / `to_tf()` | TensorFlow datasets | TensorFlow training |

### Local Shuffle Buffer

`local_shuffle_buffer_size` parameter on iteration methods shuffles a subset of
rows up to the buffer size during consumption. More randomness at the cost of
slower iteration. Recommended over expensive global `random_shuffle()`.

### Materialization

- `ds.materialize()` -- executes and caches all blocks in Ray's object store.
  Subsequent operations read from cache.
- `take(n)` / `take_all()` -- blocking collection of rows
- `show(n)` -- displays rows (triggers execution)
- `take_batch(n)` -- returns a single batch

### Streaming Splits

**`streaming_split(n, equal=False, locality_hints=None)`** -- divides dataset
into `n` disjoint `DataIterator` objects for parallel consumption.

- Coordinator actor manages block distribution
- No materialization in memory (unlike `split()`)
- Iterators are Ray-serializable, passable to any task/actor
- Implicit barrier at start of each iteration epoch
- If one iterator falls behind, others may be stalled
- Each iteration triggers a new execution of the Dataset

**`split(n)`** -- materializes dataset then splits. Requires fitting in object
store memory.

### Prefetching

`DataContext.actor_prefetcher_enabled` (default: False) -- enables actor-based
block prefetching for consumption.

---

## Physical Operators

### Operator Lifecycle

```
start() -> add_input() -> has_next() -> get_next() -> completed() -> shutdown()
```

### MapOperator Hierarchy

- **TaskPoolMapOperator** -- ephemeral cached task submissions, no persistent
  workers. Each task is independent. Default for stateless functions.
- **ActorPoolMapOperator** -- manages long-lived actor workers via `_ActorPool`
  (handles actor lifecycle: pending, running, restarting). Uses `_bundle_queue`
  for input queuing and `_ActorTaskSelector` for locality-based assignment.
  `should_add_input()` returns True only when free actor slots exist.

### Non-Map Operators

- **InputDataBuffer** -- data source entry point
- **OutputSplitter** -- supports streaming `.split()` for parallel consumption
- **LimitOperator** -- manipulates block references without compute
- **UnionOperator** / **ZipOperator** -- combining operators

### Task Execution

Map tasks execute via `_map_task()`:

- Receives `DataContext`, `TaskContext`, blocks, and slices
- Applies `MapTransformer` (chain of transform functions)
- Yields blocks and metadata via streaming generator
- Tracks execution statistics (wall time, CPU time, memory)

### Internal Queue Interface

`InternalQueueOperatorMixin` exposes:

- `internal_input_queue_num_blocks()` / `internal_input_queue_num_bytes()`
- `internal_output_queue_num_blocks()` / `internal_output_queue_num_bytes()`

These enable accurate memory tracking for resource budgeting.

---

## Key Configuration Reference

### DataContext Parameters

| Parameter | Default | Purpose |
|---|---|---|
| `target_max_block_size` | 128 MiB | Max block size for reads/transforms |
| `target_min_block_size` | 1 MiB | Min block size on read |
| `target_shuffle_max_block_size` | 1 GiB | Shuffle operation block limit |
| `read_op_min_num_blocks` | 200 | Minimum read output blocks |
| `streaming_read_buffer_size` | 32 MiB | Buffer size for streaming reads |
| `scheduling_strategy` | `"SPREAD"` | Global scheduling strategy |
| `scheduling_strategy_large_args` | `"DEFAULT"` | Strategy for large-arg tasks |
| `large_args_threshold` | 50 MiB | Large args size threshold |
| `op_resource_reservation_enabled` | True | Per-operator resource reservation |
| `op_resource_reservation_ratio` | 0.5 | Ratio of resources to reserve |
| `max_errored_blocks` | 0 | Allowed block failures |
| `use_push_based_shuffle` | False | Push-based shuffle for large data |
| `use_polars` | False | Use Polars for sort/groupby/agg |
| `eager_free` | False | Eagerly free consumed blocks |
| `enable_pandas_block` | True | Enable pandas block format |
| `actor_prefetcher_enabled` | False | Actor-based block prefetching |
| `wait_for_min_actors_s` | -1 | Wait time for min actors (-1=indefinite) |
| `max_tasks_in_flight_per_actor` | None | Concurrent tasks per actor |
| `downstream_capacity_backpressure_ratio` | 10.0 | Downstream backpressure ratio |
| `warn_on_driver_memory_usage_bytes` | 2 GiB | Driver memory warning threshold |

### ExecutionOptions Parameters

| Parameter | Default | Purpose |
|---|---|---|
| `resource_limits` | Auto | Soft caps on resource consumption |
| `exclude_resources` | Disabled | Resources reserved for externals |
| `preserve_order` | False | Maintain block ordering |
| `verbose_progress` | True | Per-operator progress metrics |
| `locality_with_output` | Disabled | Direct tasks toward output node |
| `actor_locality_enabled` | False | Locality-aware actor dispatch |

---

## Performance Benchmarks

On 500-machine clusters processing 20 TiB datasets:

- Single-stage pipelines: >1 TiB/s throughput
- Multi-stage pipelines (1-4 stages): 100-200 GiB/s end-to-end throughput

Real-world video processing: ~350 GB frame data in 5 minutes across
heterogeneous CPU/GPU clusters with ~500 MiB/s network utilization to GPU nodes.

---

## Sources

- [Ray Data Internals](https://docs.ray.io/en/latest/data/data-internals.html)
- [Ray Data Key Concepts](https://docs.ray.io/en/latest/data/key-concepts.html)
- [Performance Tips and Tuning](https://docs.ray.io/en/latest/data/performance-tips.html)
- [Execution Configurations](https://docs.ray.io/en/latest/data/execution-configurations.html)
- [Monitoring Your Workload](https://docs.ray.io/en/latest/data/monitoring-your-workload.html)
- [Iterating over Data](https://docs.ray.io/en/latest/data/iterating-over-data.html)
- [Shuffling Data](https://docs.ray.io/en/latest/data/shuffling-data.html)
- [DataContext API](https://docs.ray.io/en/latest/data/api/data_context.html)
- [map_batches API](https://docs.ray.io/en/latest/data/api/doc/ray.data.Dataset.map_batches.html)
- [streaming_split API](https://docs.ray.io/en/latest/data/api/doc/ray.data.Dataset.streaming_split.html)
- [ActorPoolStrategy API](https://docs.ray.io/en/latest/data/api/doc/ray.data.ActorPoolStrategy.html)
- [Task Fault Tolerance](https://docs.ray.io/en/latest/ray-core/fault_tolerance/tasks.html)
- [Object Fault Tolerance](https://docs.ray.io/en/latest/ray-core/fault_tolerance/objects.html)
- [Streaming Distributed Execution (Anyscale Blog)](https://www.anyscale.com/blog/streaming-distributed-execution-across-cpus-and-gpus)
- [Ray Data GA (Anyscale Blog)](https://www.anyscale.com/blog/ray-data-ga)
- [Streaming Execution and Resource Management (DeepWiki)](https://deepwiki.com/ray-project/ray/3.3-data-io-and-formats)
- [Ray Data Architecture (DeepWiki)](https://deepwiki.com/ray-project/ray/3-ray-data)
- [StreamingExecutor Source](https://github.com/ray-project/ray/blob/master/python/ray/data/_internal/execution/streaming_executor.py)
