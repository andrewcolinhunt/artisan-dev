# Connect to Prefect Cloud

How to connect Artisan pipelines to Prefect Cloud for flow tracking and
monitoring.

**Prerequisites:** [Configuring Execution](configuring-execution.md), a
Prefect Cloud account with a workspace and API key.

---

## Minimal working example

The most common path — log in with the Prefect CLI, then run a pipeline:

```bash
prefect cloud login
```

NOTE: should be pixi run prefect cloud login

```python
from artisan.orchestration import PipelineManager
from myops import PreprocessOp

pipeline = PipelineManager.create(
    name="example",
    delta_root="runs/delta",
    staging_root="runs/staging",
)

pipeline.run(operation=PreprocessOp, name="preprocess", params={"count": 10})
```

Expected log output:

```
Prefect Cloud: https://api.prefect.cloud/api/accounts/.../workspaces/... (source: prefect_profile)
```

The flow run appears in the Prefect Cloud UI automatically.

---

## Connect with `prefect cloud login`

This is the recommended approach. It stores your API URL and key in
`~/.prefect/profiles.toml`, which Artisan reads as a fallback when no
environment variables are set.

```bash
prefect cloud login
# Select your workspace when prompted
```

Run your pipeline. Artisan discovers the Cloud URL from your Prefect profile
and skips the self-hosted health check (Cloud is managed infrastructure).

---

## Connect with environment variables

Set `PREFECT_API_URL` and `PREFECT_API_KEY` directly:

```bash
export PREFECT_API_URL="https://api.prefect.cloud/api/accounts/<account-id>/workspaces/<workspace-id>"
export PREFECT_API_KEY="pnu_..."
```

This takes priority over profile-based configuration. Expected log:

```
Prefect Cloud: https://api.prefect.cloud/api/accounts/.../workspaces/... (source: env:PREFECT_API_URL)
```

---

## Connect with an explicit URL

Pass the Cloud workspace URL directly to `PipelineManager.create()` or
`resume()`:

```python
pipeline = PipelineManager.create(
    name="example",
    delta_root="runs/delta",
    staging_root="runs/staging",
    prefect_server="https://api.prefect.cloud/api/accounts/<account-id>/workspaces/<workspace-id>",
)
```

The API key must still be set via environment variable or Prefect profile —
Artisan does not accept the key as a parameter.

---

## Use SLURM with Cloud

Artisan propagates `PREFECT_API_URL` and `PREFECT_API_KEY` to SLURM workers
automatically via `get_current_settings().to_environment_variables()`. No
extra configuration is needed.

```python
from artisan.orchestration import Backend

pipeline.run(
    operation=MyOp,
    inputs=...,
    backend=Backend.SLURM,
    resources={"gpus": 1, "memory_gb": 32},
)
```

SLURM compute nodes need outbound HTTPS access to `api.prefect.cloud`. Many
HPC clusters restrict outbound internet — check with your cluster
administrator if workers fail to connect.

---

## Common pitfalls

| Problem | Cause | Fix |
|---------|-------|-----|
| `PrefectServerNotFound` | No profile, env var, or explicit URL | Run `prefect cloud login` or set `PREFECT_API_URL` |
| `Unauthorized` at flow submission | Missing or expired API key | Re-run `prefect cloud login` or check `PREFECT_API_KEY` |
| SLURM workers can't reach Cloud | No outbound HTTPS on compute nodes | Contact cluster admin to allow `api.prefect.cloud:443` |

---

## Verify

Run a single-step pipeline and confirm the flow run appears in the Prefect
Cloud UI at `https://app.prefect.cloud`. The pipeline log should show
`Prefect Cloud:` with your workspace URL.

---

## Cross-references

- [Configuring Execution](configuring-execution.md) -- Resource allocation, batching, and SLURM configuration
- [Execution Flow](../concepts/execution-flow.md) -- Dispatch, execute, commit lifecycle
- [SLURM Execution Tutorial](../tutorials/execution/07-slurm-execution.ipynb) -- Interactive SLURM walkthrough

TODO: lets simplify this more. point it to the first tutorial in getting started.