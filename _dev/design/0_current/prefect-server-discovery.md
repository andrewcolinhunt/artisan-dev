# Design: Prefect Server Discovery & Multi-Node Resilience

## Problem

Running a pipeline from a different node than where the Prefect server is
running should be seamless. Today it isn't — users hit opaque connectivity
failures that require manual debugging and environment variable exports.

## Context: What Happened

A user working across two nodes (g2117, jojo) encountered a cascade of failures:

1. **`pixi run prefect-start` on g2117 failed** — a stale Prefect server process
   (PID 3841055) was already bound to port 4296. The startup script reported
   "did not become healthy within 30s" with no guidance about the existing
   process.

2. **Existing server was reachable on localhost but not on hostname** — `curl
   http://localhost:4296/api/health` returned 200, but `curl
   http://g2117.ipd:4296/api/health` returned connection refused. The discovery
   file stored the hostname URL, so `PipelineManager.create()` failed with
   `PrefectServerUnreachable`.

3. **Setting `PREFECT_API_URL=http://localhost:4296/api` didn't help** — same
   `PrefectServerUnreachable` error. The env var was read (error showed
   `source: env:PREFECT_API_URL`) but the health check still failed from within
   the pixi-managed Python process.

4. **The server on g2117 turned out to be from a different environment** —
   started from `prefect-submitit-dev/.pixi/envs/default/bin/prefect`, not the
   `pipelines` pixi env. Different Prefect versions between client and server.

5. **Pointing to a healthy server on jojo (`PREFECT_API_URL=http://jojo.ipd:4296/api`)
   fixed discovery** — `PipelineManager.create()` succeeded. But the pipeline
   then failed at step dispatch with `500 Internal Server Error` on
   `POST /api/flows/`, again likely due to Prefect version mismatch between the
   client in `pipelines` and the server on jojo.

6. **Resolution required killing the jojo server and restarting from the correct
   environment** — manual, error-prone, and required understanding the
   architecture.

## Current Discovery Flow

`PipelineManager.create()` calls `discover_server()` which delegates to
`prefect_submitit.server.discovery.resolve_api_url()`:

```
Priority order:
1. Explicit argument to PipelineManager.create(prefect_server=...)
2. PREFECT_SUBMITIT_SERVER env var
3. PREFECT_API_URL env var
4. Discovery file (written by prefect-start)
5. Prefect profile settings
```

Once a URL is resolved, `_validate_health()` calls
`prefect_submitit.server.discovery.health_check(url)` — a simple HTTP GET to
`/api/health`. If it returns False, the user gets `PrefectServerUnreachable`.

## What's Missing

### 1. No version compatibility check

The health check only verifies HTTP connectivity. A server running Prefect 3.x
can return healthy to a 3.y client, but dispatch fails at runtime with 500
errors. The user discovers the version mismatch only after the pipeline starts
running and steps fail.

### 2. No stale process detection on startup

`prefect-start` tries to bind the port, fails, and reports a generic timeout.
It doesn't check whether an existing process owns the port, what environment
it's from, or whether it's healthy. The user has to manually run `lsof`,
`ps aux | grep prefect`, etc.

### 3. Discovery file is node-specific but stored in a shared filesystem

The discovery file is written by `prefect-start` with the hostname of the node
where the server was started (e.g., `g2117.ipd:4296`). On a shared NFS
filesystem, another node reads this file and tries to connect to the hostname —
which may be unreachable due to firewall rules, DNS issues, or the server
process having died. There is no fallback to try `localhost` or re-discover.

### 4. No guidance when health check fails

The error message says "not reachable" and suggests `pixi run prefect-start`.
It doesn't:
- Show what URL it tried and why
- Check if the port is in use by a stale process
- Suggest checking whether the server is on a different node
- Recommend version alignment

### 5. No cross-environment version check

Nothing validates that the Prefect server was started from the same (or
compatible) environment as the client. A server from `prefect-submitit-dev` and
a client from `pipelines` can have different Prefect versions, and the user has
no way to know until runtime failures occur.

## Desired Behavior

| Scenario | Current | Desired |
|----------|---------|---------|
| Port already in use by compatible server | Timeout, generic error | Detect existing process, reuse if healthy + compatible |
| Port in use by incompatible server | Timeout, generic error | Detect version mismatch, offer to kill and restart |
| Discovery file points to unreachable host | `PrefectServerUnreachable` | Try localhost fallback, then suggest cross-node options |
| Server healthy but wrong Prefect version | Succeeds discovery, 500s at runtime | Fail fast at discovery with version mismatch error |
| User on different node than server | Manual `export PREFECT_API_URL=...` | Discovery file includes version; cross-node resolution works |
