# Inspect Evidence Locally

Inspect runs, failures, artifacts and outcome assessments from Python or the
CLI. Both interfaces work with the base installation and share their request
and response contracts with the optional MCP server.

Choose the interface your caller can use:

| Caller | Interface |
| --- | --- |
| An agent or script with shell access to the Artisan environment | `artisan inspect`, with JSON requests and responses |
| Python code already running in that environment | `artisan.inspection`, with validated model responses |
| A client that calls MCP tools, or a task naming an existing MCP target | The configured [MCP connection](connecting-mcp.md) |

These interfaces inspect persisted evidence. To work with full result tables in
Python, use the [analytical readers](inspecting-provenance.md).

## Find the installed contract

Start in the environment that can access the intended store:

```bash
artisan inspect list --json
artisan inspect describe list_runs --json
artisan guide list --json
artisan guide show pipeline-operate
```

The catalog identifies the installed version and available inspections. Build
requests from each description's request and response JSON schemas.

The operating guide covers store and attempt selection, inspection, and outcome
assessment. Its version, origin, and digest identify the installed distribution
or editable checkout. Read it before reusing instructions from another version.

These commands do not import application modules or read a store. To discover
registered operations, load the application's trusted modules explicitly:

```bash
artisan inspect run list_operations --load-module artisan.operations.examples --json
artisan inspect run capabilities --load-module artisan.operations.examples --json
```

Check the capabilities response's `error_count` and `name_collision_count` before
using the operation catalog. Inspect `view="errors"` or `view="name_collisions"`
for details.

## Query a store

Supply an existing Delta root, such as the one created by the
[first pipeline tutorial](../tutorials/01-getting-started/01-first-pipeline.ipynb):

```bash
artisan inspect run list_runs --delta-root /absolute/path/to/runs/delta --json
printf '%s\n' '{"limit":1}' | artisan inspect run list_runs \
  --request - --delta-root /absolute/path/to/runs/delta --json
```

Choose a `pipeline_run_id` from the response's `items` before requesting status
or outputs. The one-row request may require
[continuation](#continue-and-interpret-evidence) to reach your run.

`--request` accepts a UTF-8 JSON object file or `-` for stdin. Omitting it supplies
an empty object. Invalid JSON, duplicate keys, non-finite numbers, undeclared
fields and incorrect field types fail before querying the store.

Use repeated `--load-module` flags for trusted installed applications whose
operations or artifact types you need. Explicit flags replace the comma-separated
`ARTISAN_LOAD_MODULES` list. `--delta-root` takes precedence over
`ARTISAN_DELTA_ROOT`.
Application imports can run code; use only modules you trust. Discovery queries
expose import failures, while store queries stop when required application
registration fails.

Local execution can read a configured remote store. Keep the intended store and
credentials consistent when moving between interfaces. If a task specifies an
existing MCP target, use that target rather than guessing an equivalent local
path. See [connect an MCP client](connecting-mcp.md) for that interface.

## Use the same contract from Python

Python callers initialize trusted application definitions explicitly. Replace
the store path before running this example:

```python
from artisan.inspection import inspect_evidence
from artisan.registry import bootstrap_applications

bootstrap_applications(["artisan.operations.examples"], purpose="inspect results")
root = "/absolute/path/to/runs/delta"
page = inspect_evidence("list_runs", {"limit": 1}, delta_root=root)
runs = []
while True:
    payload = page.model_dump(mode="json")
    runs.extend(payload["items"])
    follow_up = payload["follow_up"]
    if follow_up is None:
        break
    page = inspect_evidence(
        follow_up["capability"], follow_up["arguments"], delta_root=root
    )
print([run["pipeline_run_id"] for run in runs])
```

The printed IDs cover every page of the selected run list. Keep one run ID when
moving on to status, outputs or failure evidence.

For Python discovery, `list_inspections()` and `describe_inspection(name)` from
`artisan.inspection` provide the same catalog and schemas as the CLI. They do not
discover application operations. Use `artisan.registry.discover` and pass its
report as `discovery` when querying the `capabilities` inspection.

(continue-and-interpret-evidence)=
## Continue and interpret evidence

Continue with the returned `follow_up.capability` and `follow_up.arguments`, as
the Python example does. For the CLI, save those arguments as a JSON request and
pass it to `artisan inspect run` with the returned capability name. For MCP,
prefix the capability with `artisan_` to obtain its server tool name; your client
may add its own namespace. Use the tool name advertised by that client. A null
`follow_up` means no continuation is available for the selected view.

CLI, Python and MCP use the same responses and cursor rules. Keep the same store
representation and selectors, including exact `step_run_id` values in assessment
or comparison continuations. An equivalent store path spelled differently can
invalidate a cursor. You may change `limit` between pages.

Responses are bounded to 48 KiB of canonical JSON and collections to at most 100
rows per page. A byte limit can return fewer rows than requested. Selected values
remain whole. Check the selected collection's `total_items`, `omitted_fields`
and `has_more` alongside source coverage. Exhausting a page sequence does not
establish that a bounded provenance traversal found every ancestor or that an
assessment has every measurement it needs.

These bounds limit response size, not reader memory or query cost. To retain a
complete assessment or comparison, [export the evidence](exporting-evidence.md).

## Handle errors and changing evidence

Known query errors raise typed `ArtisanError` exceptions in Python. With `--json`,
the CLI writes one JSON object to stdout, including for handled failures (exit 1).
Argument parsing errors exit 2; application diagnostics go to stderr. Unexpected
implementation failures remain visible as exceptions. A report describing a
failed execution can still be a successful inspection. See
[MCP error handling](connecting-mcp.md#handle-query-errors-and-slow-reads) for
protocol validation.

| Error code | Recovery |
| --- | --- |
| `evidence_view_changed` | Discard accumulated pages and restart the query; do not combine pages from different views. |
| `invalid_evidence_cursor` | Restart the intended query without the token. |
| `evidence_item_too_large` | Follow the suggested field or section selection. Lowering `limit` cannot make one oversized row fit. |
| `evidence_response_too_large` | Narrow the request or use a full Python reader for the required evidence. |

Paging over a live store is not an atomic snapshot. If evidence keeps changing,
wait for a stable view or narrow the selection.

## Update clients for projection version 2

Current responses carry `projection_version="2"`. Continuations use
`follow_up.capability` instead of the earlier MCP-specific `follow_up.tool`.
Update consumers to dispatch by capability and discard version-1 cursors. Start
each query again after upgrading; old tokens are rejected.
