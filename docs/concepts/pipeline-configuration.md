# Pipeline Configuration

`PipelineConfig` is the frozen Pydantic model that captures every
orchestrator-level setting for a pipeline run. It is set once when you
call `PipelineManager.create(...)` (or constructed directly) and is
propagated to every worker via `RuntimeEnvironment`.

```python
from artisan.orchestration import PipelineConfig, PipelineManager

config = PipelineConfig(
    name="my-pipeline",
    delta_root="/data/delta",
    staging_root="/data/staging",
    default_step_runner="local",
    default_compute_provider="local",
)
pipeline = PipelineManager(config)
```

After creation, the config is exposed read-only on `pipeline.config`:

```python
print(pipeline.config.delta_root)
print(pipeline.config.default_step_runner)
```

## Fields

| Field                       | Type             | Description                                                                                |
| --------------------------- | ---------------- | ------------------------------------------------------------------------------------------ |
| `name`                      | `str`            | Pipeline identifier used for logging and Prefect.                                          |
| `pipeline_run_id`           | `str`            | Unique ID generated for this run session (auto-derived from `name` when blank).            |
| `delta_root`                | `str`            | Root URI for the Delta Lake tables that hold artifacts and provenance.                     |
| `staging_root`              | `str`            | Root URI where workers stage Parquet files before commit.                                  |
| `working_root`              | `str`            | Root path for worker sandboxes. Always local, defaults to `tempfile.gettempdir()`.         |
| `failure_policy`            | `FailurePolicy`  | Default behavior on step failure: `CONTINUE` (default) or `FAIL_FAST`.                     |
| `cache_policy`              | `CachePolicy`    | When completed steps qualify as cache hits. `ALL_SUCCEEDED` (default).                     |
| `default_step_runner`       | `str`            | Default runner for step dispatch (`"local"`, `"slurm"`, `"slurm_intra"`).                  |
| `default_compute_provider`  | `str`            | Default compute provider for `execute()` routing (`"local"` or `"modal"`).                 |
| `preserve_staging`          | `bool`           | Debug flag — keep staging files after commit.                                              |
| `preserve_working`          | `bool`           | Debug flag — keep worker sandboxes after execution.                                        |
| `recover_staging`           | `bool`           | At pipeline init, commit leftover staging files from a prior crashed run. Default `True`.  |
| `skip_cache`                | `bool`           | Bypass all cache lookups (step-level and execution-level).                                 |
| `files_root`                | `str \| None`    | Root for Artisan-managed external files. Auto-derived for local; required for cloud.      |
| `storage`                   | `StorageConfig`  | Storage backend configuration (S3, GCS, local).                                            |

## Public surface

- `from artisan.orchestration import PipelineConfig` — re-exported from the
  top-level package since it is part of the stable user-facing API.
- The model is `frozen` (Pydantic `model_config = {"frozen": True}`); all
  fields are immutable after construction. To change a field, build a new
  config and a new `PipelineManager`.

## Related

- [PipelineManager.create](../how-to-guides/building-a-pipeline.md) — the
  factory that constructs a config from kwargs.
- [Configuring S3](../how-to-guides/configuring-s3.md) — using
  `PipelineConfig` directly for cloud deployments.
- [List previous runs](../how-to-guides/building-a-pipeline.md#list-previous-runs)
  — `list_runs(delta_root)` reads runs from `PipelineConfig.delta_root`.
