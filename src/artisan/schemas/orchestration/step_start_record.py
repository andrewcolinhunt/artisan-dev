"""Step start record for pipeline step persistence."""

from __future__ import annotations

from pydantic import BaseModel


class StepStartRecord(BaseModel):
    """Metadata for a step's initial 'running' row in the steps table.

    Attributes:
        step_run_id: Unique ID for this step execution attempt.
        step_spec_id: Deterministic ID for cache lookup.
        step_number: Sequential pipeline step index.
        step_name: Human-readable step label.
        operation_class: Fully qualified operation class name.
        params_json: JSON-encoded operation parameters.
        input_refs_json: JSON-encoded input references.
        compute_backend: Backend name (e.g. "local", "slurm").
        compute_options_json: JSON-encoded backend options.
        output_roles_json: JSON-encoded output role names.
        output_types_json: JSON-encoded output role-to-type mapping.
    """

    step_run_id: str
    step_spec_id: str
    step_number: int
    step_name: str
    operation_class: str
    params_json: str
    input_refs_json: str
    compute_backend: str
    compute_options_json: str
    output_roles_json: str
    output_types_json: str

    model_config = {"frozen": True}
