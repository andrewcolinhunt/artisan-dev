# Connect to Prefect

How to connect Artisan pipelines to a Prefect server for flow tracking and
monitoring — local, remote, or Prefect Cloud.

**Prerequisites:** [Installation](../getting-started/installation.md),
[Configuring Execution](configuring-execution.md).

---

## Minimal working example

The most common path — start a local Prefect server and run a pipeline:

```bash
pixi run prefect-start
```

```python
from artisan.operations.examples import DataGenerator
from artisan.orchestration import PipelineManager

pipeline = PipelineManager.create(
    name="example",
    delta_root="runs/delta",
    staging_root="runs/staging",
)

pipeline.run(operation=DataGenerator, name="generate", params={"count": 5})
```

Expected log output:

```
Prefect self-hosted: http://localhost:<port>/api (source: discovery_file)
```

The flow run appears in the Prefect UI at `http://localhost:<port>`.

---

## How Artisan discovers a server

Artisan checks the following sources in order and uses the first match:

| Priority | Source | How to set |
|----------|--------|------------|
| 1 | `prefect_server=` argument | Pass to `PipelineManager.create()` or `resume()` |
| 2 | `PREFECT_SUBMITIT_SERVER` env var | `export PREFECT_SUBMITIT_SERVER=http://host:port/api` |
| 3 | `PREFECT_API_URL` env var | `export PREFECT_API_URL=http://host:port/api` |
| 4 | Discovery file | Written automatically by `pixi run prefect-start` |
| 5 | Prefect profile | Written by `pixi run prefect cloud login` |

A local discovery file (priority 4) beats a Cloud profile (priority 5). If
you have a running local server and a Cloud profile, Artisan connects to the
local server. To override this, set `PREFECT_API_URL` or pass
`prefect_server=` explicitly.

---

## Start a local server

```bash
pixi run prefect-start   # Start in background
pixi run prefect-stop    # Stop the server
```

`prefect-start` writes a discovery file to
`~/.prefect-submitit/server.json` containing the server URL. Artisan reads
this file automatically (priority 4 in the table above).

The server binds to a UID-based port to avoid collisions when multiple users
share a machine. On HPC clusters, run the server on a login or utility node —
not the head node.

---

## Connect to an existing server

If a Prefect server is already running (started by another user, a shared
service, or a container), point Artisan to it with an environment variable:

```bash
export PREFECT_SUBMITIT_SERVER="http://shared-host:4200/api"
```

Or pass the URL directly:

```python
pipeline = PipelineManager.create(
    name="example",
    delta_root="runs/delta",
    staging_root="runs/staging",
    prefect_server="http://shared-host:4200/api",
)
```

---

## Connect to Prefect Cloud

### Log in with the Prefect CLI (recommended)

```bash
pixi run prefect cloud login
# Select your workspace when prompted
```

This stores your API URL and key in `~/.prefect/profiles.toml`. Artisan
reads it as a fallback (priority 5) and skips the self-hosted health check
since Cloud is managed infrastructure.

Expected log output:

```
Prefect Cloud: https://api.prefect.cloud/api/accounts/.../workspaces/... (source: prefect_profile)
```

### Use environment variables

```bash
export PREFECT_API_URL="https://api.prefect.cloud/api/accounts/<account-id>/workspaces/<workspace-id>"
export PREFECT_API_KEY="pnu_..."
```

Environment variables take priority over the profile and the discovery file.
Expected log:

```
Prefect Cloud: https://api.prefect.cloud/api/accounts/.../workspaces/... (source: env:PREFECT_API_URL)
```

### Pass the URL as an argument

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

## Use SLURM with Prefect

Artisan propagates Prefect connection settings to SLURM workers
automatically. No extra configuration is needed:

```python
from artisan.orchestration import Backend

pipeline.run(
    operation=DataGenerator,
    name="generate",
    params={"count": 5},
    backend=Backend.SLURM,
    resources={"gpus": 1, "memory_gb": 32},
)
```

Network requirements differ by connection type:

- **Local server:** SLURM compute nodes must be able to reach the server
  host and port.
- **Prefect Cloud:** Compute nodes need outbound HTTPS access to
  `api.prefect.cloud`. Many HPC clusters restrict outbound internet —
  check with your cluster administrator.

---

## Common pitfalls

| Problem | Cause | Fix |
|---------|-------|-----|
| `PrefectServerNotFound` | No server argument, env var, discovery file, or profile | Run `pixi run prefect-start` or `pixi run prefect cloud login` |
| `PrefectServerUnreachable` | Server URL found but health check fails | Check the server is running (`pixi run prefect-start`) |
| `Unauthorized` at flow submission | Missing or expired Cloud API key | Re-run `pixi run prefect cloud login` or check `PREFECT_API_KEY` |
| Cloud ignored when local server running | Discovery file (priority 4) beats profile (priority 5) | Set `PREFECT_API_URL` or pass `prefect_server=` to force Cloud |
| SLURM workers can't connect | Network restrictions on compute nodes | For Cloud: allow `api.prefect.cloud:443`; for local: ensure route to server host |

---

## Verify

Run a single-step pipeline and check the log output. You should see either:

- `Prefect self-hosted: http://...` — local or remote server
- `Prefect Cloud: https://api.prefect.cloud/...` — Prefect Cloud

Confirm the flow run appears in the Prefect UI (local at
`http://localhost:<port>` or Cloud at `https://app.prefect.cloud`).

---

## Cross-references

- [Installation](../getting-started/installation.md) — Server discovery and
  Prefect setup
- [Configuring Execution](configuring-execution.md) — Resource allocation,
  batching, and SLURM
- [Execution Flow](../concepts/execution-flow.md) — How Artisan orchestrates
  pipeline execution
- [SLURM Execution Tutorial](../tutorials/execution/07-slurm-execution.ipynb) —
  Interactive SLURM walkthrough
