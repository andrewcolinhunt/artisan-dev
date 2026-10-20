# Connect an MCP Client

Connect a client to Artisan's read-only Model Context Protocol (MCP) server to
discover operations and inspect persisted pipeline results and failure logs.

## Prerequisites

- Python 3.12 or later and an MCP client that supports stdio servers.
- An Artisan store created with the installed release. Follow the
  [first pipeline tutorial](../tutorials/01-getting-started/01-first-pipeline.ipynb)
  to create one.
- Familiarity with [provenance](../concepts/provenance-system.md) and the
  [artifact and run terminology](../reference/glossary.md).

## Minimal working configuration

After installing the server, add this stdio server entry to your client,
replacing both absolute paths:

```json
{
  "command": "/absolute/path/to/.venv/bin/artisan-mcp",
  "args": [
    "--transport", "stdio",
    "--delta-root", "/absolute/path/to/runs/delta",
    "--load", "artisan.operations.examples"
  ]
}
```

Your client's surrounding configuration syntax may differ. The executable,
argument list, and environment settings are the portable parts of this entry.
The client starts the process and communicates through its stdin and stdout.

---

## Install the server

Install Artisan with its optional MCP dependencies:

```bash
python -m venv .venv
.venv/bin/python -m pip install 'dexterity-artisan[mcp]'
.venv/bin/artisan-mcp --help
```

Use the absolute path to this environment's `artisan-mcp` executable in your
client. The client does not need an activated shell, shell expansion of `~`,
or a working directory inside the repository.

## Select the store and operation modules

Set `--delta-root` to the Delta directory itself, such as
`/absolute/path/to/runs/delta`, rather than its parent `runs` directory.
Check the resolved configuration without starting a session:

```bash
/absolute/path/to/.venv/bin/artisan-mcp \
  --delta-root /absolute/path/to/runs/delta \
  --load artisan.operations.examples \
  --print-config
```

This prints JSON and exits. Curator builtins load automatically. Use repeated
`--load` options for additional trusted, installed operation modules; this
option imports modules and does not install packages. Include modules that
register custom artifact types used by the store as well as the operations you
want to discover.

You can provide the same settings through the server entry's environment:

```json
{
  "command": "/absolute/path/to/.venv/bin/artisan-mcp",
  "args": ["--transport", "stdio"],
  "env": {
    "ARTISAN_DELTA_ROOT": "/absolute/path/to/runs/delta",
    "ARTISAN_LOAD_MODULES": "artisan.operations.examples"
  }
}
```

`ARTISAN_LOAD_MODULES` accepts a comma-separated module list. Explicit
`--delta-root` and `--load` flags replace their corresponding environment
settings, so choose one source for each setting when configuring your client.

---

## Verify the connection

Start the server through your client and inspect it in this order:

| Task | Tool and selection | Verify |
|---|---|---|
| Check discovery | `artisan_capabilities` | `read_only: true`; check `error_count` and `name_collision_count`. The configured root is withheld. Use `view="errors"` or `view="name_collisions"` for details. |
| Find an operation | `artisan_list_operations`, `query="data_transformer"` | The example transformer appears when its module was loaded. |
| Inspect its contract | `artisan_describe_operation`, `name="data_transformer"` | Complete input/output and parameter contracts, including `scale_factor`. |
| Choose a run | `artisan_list_runs` | A page of run IDs from your store. |
| Inspect the run | `artisan_get_run_status`, `pipeline_run_id` | Current statuses in `steps.items`. `last_status` describes the latest updated step, not overall quality. |
| Inspect outputs | `artisan_get_step_result`, run ID and step name | A flat `items` page of accepted artifact references. |
| Diagnose failures | `artisan_diagnose_run`, run ID | A summary of execution failures and coverage; use `view="failures"` for their details. |

For agent instructions that guide this inspection, install the
[`pipeline-operate` skill](../getting-started/using-claude-code.md#artisan-skills)
in your client. Installing the MCP Python extra does not install client skills.
The server provides read-only evidence; the skill helps interpret it.

## Continue a result page

A collection appears as `items`, `steps.items`, or `edges.items`, depending on
the tool. Its page reports the selected collection's `total_items`, the number
returned, `has_more`, and `next_cursor`. A byte limit can return fewer rows than
the requested `limit`. Follow the response's `follow_up.tool` and
`follow_up.arguments` to continue with the same selectors.

For example, this FastMCP client starts from the `artisan://runs` JSON resource
and follows its tool continuations. Replace the executable and store paths:

```python
import asyncio
import json

from fastmcp import Client
from fastmcp.client.transports import StdioTransport


async def inspect_runs():
    transport = StdioTransport(
        command="/absolute/path/to/.venv/bin/artisan-mcp",
        args=["--delta-root", "/absolute/path/to/runs/delta"],
    )
    async with Client(transport) as client:
        resource = await client.read_resource("artisan://runs")
        page = json.loads(resource[0].text)
        runs = []
        while True:
            if "code" in page:
                raise RuntimeError(f"Run inspection failed: {page['code']}")
            runs.extend(page["items"])
            follow_up = page["follow_up"]
            if follow_up is None:
                break
            result = await client.call_tool(
                follow_up["tool"], follow_up["arguments"], raise_on_error=False
            )
            page = result.structured_content
            if result.is_error:
                raise RuntimeError(f"Run inspection failed: {page['code']}")
        print([run["pipeline_run_id"] for run in runs])


asyncio.run(inspect_runs())
```

Cursors are opaque and bound to the query, selected fields, store and observed
view. You may change `limit` between pages; keep the other selectors unchanged.
Tokens remain usable after a server restart if the query and evidence are
unchanged. On `evidence_view_changed`, discard collected pages and restart
without a cursor. On `invalid_evidence_cursor`, restart the intended query
without reusing that token. Active runs can change repeatedly; wait for a stable
view or narrow the selection. Paging is not an atomic snapshot of the store.

JSON resources use the same default projections as their corresponding tools.
Their returned continuations target real tools; do not construct additional
resource URIs to retrieve later pages. Summary views and indivisible sections
do not accept a cursor.

## Request the evidence you need

Use selectors to avoid transporting unrelated or oversized values:

- Artifact queries and step results omit metadata by default. Pass
  `metadata_keys=["case"]` to include that exact top-level key. Selected values
  are returned whole; missing keys are not synthesized. Check `omitted_fields`
  to distinguish omitted metadata from present empty values.
- For an operation, use `section="summary"` to orient yourself, then select
  `inputs`, `outputs`, `params`, `examples`, or `tags`. The `params` section
  preserves the complete schema, including definitions needed by references.
  `section="all"` returns the complete description only when it fits.
- Use `detail="identity"` for operation lists or a diagnosis's `failures` view
  when descriptions or error text are too large. This selection explicitly
  omits detailed fields; it does not shorten their values.
- Use capabilities `view="sources"`, `"errors"`, or `"name_collisions"` to
  inspect one discovery collection at a time.

For clients upgrading from earlier MCP responses, update step-result consumers
from type-grouped lists to flat `items`; read run steps and graph edges through
their named pages; request diagnosis detail views explicitly; and request
artifact metadata keys explicitly. These MCP projections carry
`projection_version="1"`; core Python reader results are not paginated.

Artisan-owned JSON responses fit 48 KiB of serialized UTF-8, including page and
continuation data. The final tool result fits 128 KiB, including matching JSON
text and structured content, but excluding the outer JSON-RPC envelope.
Discovery schemas and native framework errors raised before a tool runs are
outside these limits. The limits bound returned evidence, not store-read memory
or graph traversal cost.

An `evidence_item_too_large` error means one whole row cannot fit; lowering
`limit` will not fix it. Follow the error's supported field/section guidance or
inspect that value through Python. `evidence_response_too_large` means the
required response cannot fit. Values and schemas are never recursively clipped.
Prompts also fit 48 KiB including their instructions and evidence framing.
The macro-lineage resource returns complete DOT or an explicit valid-DOT
placeholder when unavailable or too large; it has no pagination.

## Check coverage before drawing conclusions

Exhausting pages exhausts the **selected collection**. Diagnosis's recent-run
view selects at most five other failed runs; compare `matching_failed_run_count`
with `similar_run_count` and use `artisan_list_runs` for older rollups. These
runs share a status predicate, not an established failure cause.

Diagnosis traces at most ten distinct failed-execution output artifacts to a
depth of two. Check `upstream_scope` for seed counts, omitted seeds and traversal
truncation. Null completeness flags mean seed evidence was unavailable. Graph
queries separately expose `traversal_truncated`; `edges.has_more=false` only
means the selected edge list is exhausted.

A failed step can have no failed-execution row, for example after unconfirmed
cleanup. Empty failures or edges do not establish that external work stopped,
that retry is safe, or that no upstream cause exists. Status inspection also
omits separate cancellation evidence.

Use `artisan_assess_outcomes` for explicit criteria over one accepted metric
role, and `artisan_compare_runs` for two explicitly selected populations with
declared correspondence. Start with their summary views, then request
observations or comparison detail pages. Conclusions are computed over the full
selected populations before paging. Continue with the exact `step_run_id`
selectors in returned arguments. Execution status, assessment, unmatched cases,
and numeric coverage remain separate; an inspection that reports a failed
criterion is still a successful tool call. See
[assessment and comparison examples](exporting-results.md#assess-recorded-metrics-without-display-rounding)
for criteria, matching and exporting the complete reports.

## Inspect failure-log excerpts

For a failed example, follow the
[error visibility tutorial](../tutorials/05-errors-and-control/02-error-visibility.ipynb).
Call `artisan_get_step_logs` with the run ID, step name and `tail_lines`.
The tool serves local failure logs beneath `<runs_dir>/logs/failures`, where
`runs_dir` is the parent of the configured Delta root. Successful steps and
unavailable local logs can both yield an empty tail.

Requests accept 1–1,000 lines and read at most 100 files and 256 KiB in aggregate.
The response must also fit the 48 KiB JSON budget, so it may omit more old lines
or return only the suffix of a long line. Check `truncated`,
`truncation_reasons` and `partial_first_line`. This is an excerpt with no cursor
for older content; use the original local file when you need the full log.

## Handle query errors and slow reads

Handled Artisan query failures set MCP `isError=true` and preserve structured
error details, including `code`, `field` and recovery guidance. FastMCP's client
raises on these by default. With `raise_on_error=False`, check `result.is_error`
and read `result.structured_content`; `.data` is `None` for an error. JSON
resources instead contain an error object in their normal JSON format. Prompt
evidence retains its untrusted-evidence framing, and lineage resources retain
DOT format. Unexpected internal failures are masked.

Store reads run away from the server's event loop so lightweight requests can
remain responsive. Cancelling a request, timing out, or disconnecting does not
prove that its read thread stopped and never cancels a pipeline run.

## Troubleshooting

| Symptom | Check |
|---|---|
| The executable is missing or reports missing MCP dependencies | Install `'dexterity-artisan[mcp]'` into the environment named in `command`. |
| The server waits when launched manually | Stdio waits for protocol messages from a client. Use `--print-config` for a check that exits. |
| Store calls return `delta_root_unset` | Supply `--delta-root` or `ARTISAN_DELTA_ROOT`. Catalog discovery remains available without a root. |
| An operation is absent | Inspect the capabilities discovery views, install its package into the server environment, and add its trusted module with `--load`. |
| Failure logs are empty | Confirm the step failed and its local log files remain under the standard runs directory. Successful-step output is not served by this tool. |

## Cross-references

- [Inspecting Provenance](inspecting-provenance.md) — inspect results through Python.
- [Provenance System](../concepts/provenance-system.md) — how runs, executions, and artifacts relate.
- [Glossary](../reference/glossary.md) — framework terminology.
