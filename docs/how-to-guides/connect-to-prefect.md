# Connect to Prefect

How to connect Artisan pipelines to a Prefect server for parallel dispatch and
monitoring.

**Prerequisites:** [Installation](../getting-started/installation.md),
[Configuring Execution](configuring-execution.md).

---

## Prefect's role in Artisan

Artisan uses Prefect as a dispatch layer for parallel task execution — not as a
workflow engine. Artisan owns pipeline definition, step sequencing, caching, and
provenance. Prefect dispatches work to local processes or SLURM nodes and
provides a monitoring UI.

The Prefect server coordinates workers and tracks run state. It is backed by
PostgreSQL to handle concurrent connections from parallel SLURM jobs. Pixi
installs PostgreSQL automatically — no system packages needed.

:::{important}
Artisan stores all durable state (artifacts, provenance, cache) independently
of Prefect. The Prefect server holds only transient monitoring data — run
status, logs, and timing. You can safely treat the server as ephemeral: start
one at the beginning of a session and let it go when you're done. Nothing of
lasting value is lost.
:::

---

## Start a local server

```bash
pixi run prefect-start   # Start in background
pixi run prefect-stop    # Stop the server
```

`prefect-start` launches a PostgreSQL-backed Prefect server and writes a
discovery file to `~/.prefect-submitit/server.json`. Artisan reads this file
automatically when you create or resume a pipeline.

The server binds to a UID-based port to avoid collisions when multiple users
share a machine. Verify it's running by opening `http://localhost:<port>` in
your browser.

### Quick test

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

## How Artisan discovers a server

Artisan checks the following sources in order and uses the first match:

| Priority | Source | How to set |
|----------|--------|------------|
| Highest | `prefect_server=` argument | Pass to `PipelineManager.create()` or `resume()` |
| | `PREFECT_SUBMITIT_SERVER` env var | `export PREFECT_SUBMITIT_SERVER=http://host:port/api` |
| | `PREFECT_API_URL` env var | `export PREFECT_API_URL=http://host:port/api` |
| | Discovery file | Written automatically by `pixi run prefect-start` |
| Lowest | Prefect profile | Written by `pixi run prefect cloud login` |

A local discovery file beats a Cloud profile. If you have both a running local
server and a Cloud profile, Artisan connects to the local server. To override
this, set `PREFECT_API_URL` or pass `prefect_server=` explicitly.

---

## Use SLURM with Prefect

Artisan propagates Prefect connection settings to SLURM workers
automatically. No extra configuration is needed:

```python
from artisan.operations.examples import DataGenerator
from artisan.orchestration import Backend

pipeline.run(
    operation=DataGenerator,
    name="generate",
    params={"count": 5},
    backend=Backend.SLURM,
    resources={"gpus": 1, "memory_gb": 32},
)
```

SLURM compute nodes must be able to reach the Prefect server host and port
over the cluster network. Since the self-hosted server runs on the same
cluster, this works out of the box.

---

## Common pitfalls

| Problem | Cause | Fix |
|---------|-------|-----|
| `PrefectServerNotFound` | No server argument, env var, discovery file, or profile | Run `pixi run prefect-start` |
| `PrefectServerUnreachable` | Server URL found but health check fails | Check the server is running (`pixi run prefect-start`) |
| SLURM workers can't connect | Compute nodes can't reach the server host | Ensure the server runs on a node reachable from compute nodes |
| Cloud ignored when local server running | Discovery file beats profile in priority | Set `PREFECT_API_URL` or pass `prefect_server=` to force Cloud |

---

## Verify

Run a single-step pipeline and check the log output. You should see:

```
Prefect self-hosted: http://localhost:<port>/api (source: discovery_file)
```

Confirm the flow run appears in the Prefect UI at `http://localhost:<port>`.

---

## Alternative: Prefect Cloud

Artisan also supports [Prefect Cloud](https://www.prefect.io/cloud) as an
alternative to running your own server. Cloud is managed infrastructure — no
server to start or maintain.

:::{note}
The self-hosted server is the recommended approach for HPC workloads. Prefect
Cloud's free tier has limitations that make it a poor fit for most research
use cases:

- **Network access:** SLURM compute nodes need outbound HTTPS to
  `api.prefect.cloud`. Most HPC clusters block this.
- **Run retention:** Only 7 days on the free plan — shorter than many
  experiment cycles.
- **Rate limits:** 500 requests/minute on the free plan, which can throttle
  high-parallelism jobs.
- **Users:** Limited to 2 users per workspace on the free plan.

Cloud can be useful for local development on a laptop or workstation with
internet access. If you'd like to experiment with it, the setup is
straightforward.
:::

### Log in with the Prefect CLI

```bash
pixi run prefect cloud login
# Select your workspace when prompted
```

This stores your API URL and key in `~/.prefect/profiles.toml`. Artisan
reads it as a fallback (lowest priority in the discovery table above) and
skips the health check since Cloud is managed infrastructure.

### Use environment variables

```bash
export PREFECT_API_URL="https://api.prefect.cloud/api/accounts/<account-id>/workspaces/<workspace-id>"
export PREFECT_API_KEY="pnu_..."
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

Expected log output when connected to Cloud:

```
Prefect Cloud: https://api.prefect.cloud/api/accounts/.../workspaces/... (source: prefect_profile)
```

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
