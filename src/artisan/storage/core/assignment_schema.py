"""Private physical schema for directly committed assignment decisions."""

from __future__ import annotations

import polars as pl

ASSIGNMENT_DECISIONS_PATH = "provenance/assignment_decisions"
ASSIGNMENT_DECISIONS_SCHEMA = {
    "assignment_id": pl.String,
    "sequence": pl.Int64,
    "action": pl.String,
    "step_run_id": pl.String,
    "unit_index": pl.Int64,
    "attempt": pl.Int64,
    "generation": pl.Int64,
    "admission_digest": pl.String,
    "recipe_digest": pl.String,
    "software_digest": pl.String,
    "software_cacheable": pl.Boolean,
    "input_context_digest": pl.String,
    "replay_of_execution_run_id": pl.String,
    "delivery_id": pl.String,
    "seal_uri": pl.String,
    "seal_digest": pl.String,
    "source_step_run_id": pl.String,
    "source_unit_index": pl.Int64,
    "source_attempt": pl.Int64,
    "source_generation": pl.Int64,
    "source_recipe_digest": pl.String,
    "source_software_digest": pl.String,
    "source_software_cacheable": pl.Boolean,
    "source_input_context_digest": pl.String,
    "source_replay_of_execution_run_id": pl.String,
    "source_delivery_id": pl.String,
    "source_seal_uri": pl.String,
    "source_seal_digest": pl.String,
}
