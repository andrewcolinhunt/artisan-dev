"""Three preloaded prompts: diagnose-failure, explain-run, walk-lineage.

Each prompt fetches its context from the core readers and returns a single
instruction with that context inlined as JSON — the agent starts with the
data already in hand. Store errors degrade to a short note rather than
failing the prompt.
"""

from __future__ import annotations

import json
from typing import Any

from fastmcp import Context, FastMCP

from artisan_mcp._boundary import require_delta_root


def register(mcp: FastMCP) -> None:
    """Attach the prompts to ``mcp``."""

    @mcp.prompt(name="artisan/diagnose-failure")
    async def diagnose_failure(pipeline_run_id: str, ctx: Context) -> str:
        """Preload a run's failure diagnosis and ask for an explanation + fix."""
        config = ctx.lifespan_context["config"]

        def fetch() -> Any:
            from artisan.visualization.inspect import diagnose_run

            return diagnose_run(
                require_delta_root(config), pipeline_run_id
            ).model_dump()

        diagnosis = _safe(fetch)
        return (
            f"You are diagnosing pipeline run {pipeline_run_id!r}. Here is its "
            f"failure diagnosis (failed steps with error envelopes, similar "
            f"recent failed runs, backward provenance, and suggested "
            f"actions):\n\n{_dump(diagnosis)}\n\n"
            "Explain what failed and why, then recommend a concrete fix. Use "
            "artisan_get_step_logs for a failed step's full log if you need more."
        )

    @mcp.prompt(name="artisan/explain-run")
    async def explain_run(pipeline_run_id: str, ctx: Context) -> str:
        """Preload a run's status and metrics and ask for a plain-language summary."""
        config = ctx.lifespan_context["config"]

        def fetch_status() -> Any:
            from artisan.orchestration.run_status import run_status

            return run_status(require_delta_root(config), pipeline_run_id).model_dump()

        def fetch_metrics() -> Any:
            from artisan.schemas.execution.storage_config import StorageConfig
            from artisan.visualization.inspect import inspect_metrics

            storage = StorageConfig()
            return inspect_metrics(
                require_delta_root(config),
                storage_options=storage.delta_storage_options(),
                fs=storage.filesystem(),
            ).to_dicts()

        status = _safe(fetch_status)
        metrics = _safe(fetch_metrics)
        return (
            f"Summarize pipeline run {pipeline_run_id!r} for a colleague. Run "
            f"status (per-step terminal states and rollup):\n\n{_dump(status)}\n\n"
            f"Metrics:\n\n{_dump(metrics)}\n\n"
            "Explain in plain language what the run did, how far it got, and "
            "what the metrics say."
        )

    @mcp.prompt(name="artisan/walk-lineage")
    async def walk_lineage(artifact_id: str, ctx: Context) -> str:
        """Preload an artifact's backward lineage and ask for an explanation."""
        config = ctx.lifespan_context["config"]

        def fetch() -> Any:
            from artisan.provenance import provenance_edges

            return provenance_edges(
                require_delta_root(config),
                artifact_id,
                direction="backward",
                depth=3,
            ).model_dump()

        edges = _safe(fetch)
        return (
            f"Trace the lineage of artifact {artifact_id!r}. Here are its "
            f"backward provenance edges (depth 3):\n\n{_dump(edges)}\n\n"
            "Explain where this artifact came from — the chain of upstream "
            "artifacts that produced it. Walk further with "
            "artisan_get_provenance_graph if the trace was truncated."
        )


def _safe(fetch: Any) -> Any:
    """Run a reader, returning its result or an ``{"error": ...}`` note."""
    from artisan.errors import ArtisanError

    try:
        return fetch()
    except (FileNotFoundError, ArtisanError) as exc:
        return {"error": str(exc)}


def _dump(value: Any) -> str:
    """Pretty-print a payload as JSON for prompt embedding."""
    return json.dumps(value, indent=2, default=str)
