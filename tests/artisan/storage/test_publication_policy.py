"""Protected semantic values remain private when JSON escapes their spelling."""

from __future__ import annotations

import json

import pytest

from artisan.schemas.enums import TablePath
from artisan.storage._publication_policy import PublicationPolicy


@pytest.mark.parametrize(
    "private_value", [r"C:\Users\example", 'example "workspace"', "\u2603-workspace"]
)
def test_protected_metadata_rejects_json_escaped_value(private_value: str) -> None:
    policy = PublicationPolicy.create((private_value,))
    row = {"metadata": json.dumps({"location": private_value})}
    with pytest.raises(ValueError, match="protected evidence"):
        policy.row(TablePath.ARTIFACT_INDEX, row, {})


def test_redaction_values_are_captured_before_generator_validation() -> None:
    policy = PublicationPolicy.create(iter(["example-workspace"]))
    assert policy.text("/example-workspace/input.csv") == "/[redacted]/input.csv"


def test_redaction_policy_rejects_a_single_string() -> None:
    with pytest.raises(ValueError, match="nonempty strings"):
        PublicationPolicy.create("example-workspace")


def test_publication_preserves_software_identity_and_rejects_overlap() -> None:
    from artisan.schemas.execution.replay import (
        OperationIdentity,
        ReplayDiagnostic,
        ReplaySnapshot,
    )

    identity = OperationIdentity(
        module="example.operations",
        qualname="Example",
        name="example",
        version="1",
        module_digest="b" * 32,
    )
    diagnostic = ReplayDiagnostic(
        source_execution_run_id="source",
        source_execution_spec_id="spec",
        source_identity=identity,
        selected_identity=identity,
        source_software_digest="a" * 64,
        selected_software_digest="a" * 64,
        allow_code_change=False,
        runner="local",
        roots={},
    )
    snapshot = ReplaySnapshot.unavailable("fixture").model_copy(
        update={"diagnostic": diagnostic},
    )
    published = PublicationPolicy.create(["private-path"]).replay(
        snapshot.model_dump_json()
    )
    assert ReplaySnapshot.model_validate_json(published).diagnostic == diagnostic
    with pytest.raises(ValueError, match="protected evidence"):
        PublicationPolicy.create(["a" * 64]).replay(snapshot.model_dump_json())


def test_current_raw_input_paths_are_the_only_new_redactable_descriptor() -> None:
    policy = PublicationPolicy.create(("private",))
    row = {
        "error": "private failed",
        "input_refs_json": json.dumps(
            {"raw_paths": ["/private/a", "/private/a", "/private/b"]}
        ),
    }
    copied = policy.row(TablePath.STEPS, row, {})
    assert copied["error"] == "[redacted] failed"
    assert json.loads(copied["input_refs_json"]) == {
        "raw_paths": ["/[redacted]/a", "/[redacted]/a", "/[redacted]/b"]
    }


@pytest.mark.parametrize(
    "inputs",
    [
        {"data": [{"artifact_id": "private"}]},
        {"data": [{"step_run_id": "private", "role": "data"}]},
        {"unknown": "/private/input"},
        {"raw_paths": ["/private/input"], "extra": True},
    ],
)
def test_typed_or_unknown_input_descriptors_remain_protected(inputs: object) -> None:
    with pytest.raises(ValueError, match="protected evidence"):
        PublicationPolicy.create(("private",)).row(
            TablePath.STEPS,
            {
                "error": None,
                "input_refs_json": json.dumps(inputs),
            },
            {},
        )
