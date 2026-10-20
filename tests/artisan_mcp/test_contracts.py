"""Native MCP results honor advertised schemas, error identity and byte limits."""

from __future__ import annotations

import asyncio
import json

import jsonschema
import pytest
from fastmcp import Client, FastMCP
from fastmcp.exceptions import ToolError
from mcp.types import CallToolResult
from pydantic import BaseModel

from artisan.errors import ArtisanError, CommitError, ErrorCode
from artisan.inspection._evidence import (
    MAX_PAYLOAD_BYTES,
    encode_json,
)
from artisan.inspection.models import output_schema
from artisan.orchestration.run_status import RunStatus
from artisan_mcp._boundary import MAX_TOOL_RESULT_BYTES, render_tool


def _handled_error() -> ArtisanError:
    return ArtisanError(
        ErrorCode.PARAM_TYPE_MISMATCH,
        "Choose a valid field.",
        error_type="validation",
        field="selection",
        recovery_hint="CHECK_INPUT",
    )


def _commit_error() -> CommitError:
    return CommitError(
        logical_commit_id="commit-7",
        table="executions",
        plan_key="plans/commit-7.json",
        verified_tables=["steps", "artifact_index"],
        staging_objects=["staging/one.parquet", "staging/two.parquet"],
        message="An exact commit requires repair.",
    )


def _wire(result) -> bytes:
    """Measure the native result representation, not a Python ToolResult."""
    return (
        CallToolResult(
            content=result.content,
            structuredContent=result.structured_content,
            isError=result.is_error,
        )
        .model_dump_json(by_alias=True)
        .encode("utf-8")
    )


def test_all_advertised_schemas_include_typed_success_and_error_branches(make_app):
    async def check():
        async with Client(make_app()) as client:
            tools = await client.list_tools()
            assert len(tools) == 12
            for tool in tools:
                schema = tool.outputSchema
                assert schema["type"] == "object", tool.name
                assert "anyOf" in schema, tool.name
                assert "ToolResult" not in schema.get("$defs", {}), tool.name
                jsonschema.Draft202012Validator.check_schema(schema)
                validator = jsonschema.Draft202012Validator(schema)
                validator.validate(_handled_error().to_dict(include_cause=False))
                validator.validate(_commit_error().to_dict(include_cause=False))
                assert not validator.is_valid({"arbitrary": "dictionary"}), tool.name
                assert not validator.is_valid(
                    {**_handled_error().to_dict(), "cause": "private"}
                ), tool.name

    asyncio.run(check())


def test_advertised_schemas_validate_existing_tool_views(make_app, seeded_run):
    requests = (
        [
            ("artisan_capabilities", {"view": view})
            for view in ("summary", "sources", "errors", "name_collisions")
        ]
        + [
            ("artisan_list_operations", {"detail": detail})
            for detail in ("summary", "identity")
        ]
        + [
            (
                "artisan_describe_operation",
                {"name": "data_generator", "section": section},
            )
            for section in (
                "all",
                "summary",
                "params",
                "inputs",
                "outputs",
                "examples",
                "tags",
            )
        ]
        + [
            ("artisan_list_runs", {}),
            ("artisan_get_run_status", {"pipeline_run_id": seeded_run.run_id}),
            (
                "artisan_query_artifacts",
                {"pipeline_run_id": seeded_run.run_id, "metadata_keys": ["filename"]},
            ),
            (
                "artisan_get_step_result",
                {"pipeline_run_id": seeded_run.run_id, "step_name": "generate"},
            ),
            (
                "artisan_get_step_logs",
                {"pipeline_run_id": seeded_run.run_id, "step_name": "transform"},
            ),
            ("artisan_get_provenance_graph", {"artifact_id": "a" * 32}),
        ]
        + [
            (
                "artisan_diagnose_run",
                {"pipeline_run_id": seeded_run.run_id, "view": view},
            )
            for view in ("summary", "failures", "similar_runs", "upstream_edges")
        ]
    )

    async def check():
        async with Client(make_app(delta_root=seeded_run.delta_root)) as client:
            schemas = {
                tool.name: tool.outputSchema for tool in await client.list_tools()
            }
            for name, arguments in requests:
                result = await client.call_tool(name, arguments)
                assert result.is_error is False
                payload = result.structured_content
                jsonschema.validate(payload, schemas[name])
                assert json.loads(result.content[0].text) == payload
                assert len(encode_json(payload)) <= MAX_PAYLOAD_BYTES
                assert len(_wire(result)) <= MAX_TOOL_RESULT_BYTES
                assert not jsonschema.Draft202012Validator(schemas[name]).is_valid(
                    {**payload, "projection_version": "invalid"}
                )

    asyncio.run(check())


def test_handled_error_raises_by_default_and_preserves_recovery_opt_in(make_app):
    async def check():
        async with Client(make_app()) as client:
            arguments = {"pipeline_run_id": "missing"}
            with pytest.raises(ToolError, match="delta_root_unset"):
                await client.call_tool("artisan_get_run_status", arguments)
            result = await client.call_tool(
                "artisan_get_run_status", arguments, raise_on_error=False
            )
            assert result.is_error is True
            assert result.data is None
            assert result.structured_content["code"] == "delta_root_unset"
            assert result.structured_content["recovery_hint"] == "CHECK_INPUT"
            assert json.loads(result.content[0].text) == result.structured_content

    asyncio.run(check())


def test_commit_error_preserves_repair_fields_and_suppresses_cause(
    make_app, invoke_result, monkeypatch, tmp_path
):
    from artisan.inspection import runs

    error = _commit_error()
    marker = "PRIVATE-CAUSE-DO-NOT-EMIT"

    def fail(*args, **kwargs):
        raise error from RuntimeError(marker)

    monkeypatch.setattr(runs, "runs_payload", fail)
    result = invoke_result(make_app(delta_root=tmp_path), "artisan_list_runs")
    assert result.is_error is True
    assert result.data is None
    assert result.structured_content == error.to_dict(include_cause=False)
    assert "cause" not in result.structured_content
    assert marker not in _wire(result).decode("utf-8")


@pytest.mark.parametrize("failure", ["reader_validation", "projection", "unexpected"])
def test_internal_defects_are_masked_in_native_results(
    make_app, invoke_result, monkeypatch, tmp_path, caplog, capfd, failure
):
    from artisan.inspection import runs

    marker = "PRIVATE-INVALID-PROJECTION"

    def fail(*args, **kwargs):
        if failure == "reader_validation":
            return RunStatus.model_validate({"step_count": marker})
        if failure == "projection":
            return {"items": marker}
        raise RuntimeError(marker)

    monkeypatch.setattr(runs, "runs_payload", fail)
    result = invoke_result(make_app(delta_root=tmp_path), "artisan_list_runs")
    assert result.is_error is True
    assert result.structured_content is None
    assert marker not in _wire(result).decode("utf-8")
    assert marker in caplog.text + capfd.readouterr().err


def test_success_cannot_pass_validation_as_an_error_branch(
    make_app, invoke_result, monkeypatch, tmp_path
):
    from artisan.inspection import runs

    monkeypatch.setattr(
        runs, "runs_payload", lambda *args, **kwargs: _handled_error().to_dict()
    )
    result = invoke_result(make_app(delta_root=tmp_path), "artisan_list_runs")
    assert result.is_error is True
    assert result.structured_content is None
    assert "Choose a valid field." not in _wire(result).decode("utf-8")


class _TextPayload(BaseModel):
    code: str
    text: str


def _text_app(value: str) -> FastMCP:
    app = FastMCP("boundary-contract", mask_error_details=True)

    @app.tool(output_schema=output_schema(_TextPayload))
    def inspect_text():
        return render_tool(lambda: {"code": "evidence", "text": value}, _TextPayload)

    return app


def test_success_with_code_field_remains_successful(invoke_result):
    result = invoke_result(_text_app('原文 🧬\n"quoted"'), "inspect_text")
    assert result.is_error is False
    assert result.structured_content == {
        "code": "evidence",
        "text": '原文 🧬\n"quoted"',
    }
    assert json.loads(result.content[0].text) == result.structured_content


@pytest.mark.parametrize("value", ["🧬" * 13_000, "\\" * 24_000])
def test_domain_and_wire_limits_replace_oversized_results(invoke_result, value):
    original = {"code": "evidence", "text": value}
    if value.startswith("\\"):
        # Escaping the text fallback can exceed the wire cap below the domain cap.
        assert len(encode_json(original)) < MAX_PAYLOAD_BYTES
        unbounded = CallToolResult(
            content=[{"type": "text", "text": encode_json(original).decode()}],
            structuredContent=original,
            isError=False,
        )
        assert len(unbounded.model_dump_json().encode()) > MAX_TOOL_RESULT_BYTES
    result = invoke_result(_text_app(value), "inspect_text")
    assert result.is_error is True
    assert result.structured_content["code"] == "evidence_response_too_large"
    assert len(encode_json(result.structured_content)) <= MAX_PAYLOAD_BYTES
    assert len(_wire(result)) <= MAX_TOOL_RESULT_BYTES
    assert json.loads(result.content[0].text) == result.structured_content


def test_oversized_handled_error_uses_fixed_bounded_fallback(
    make_app, invoke_result, monkeypatch, tmp_path
):
    from artisan.inspection import runs

    marker = "PRIVATE-LARGE-ERROR-"

    def fail(*args, **kwargs):
        error = _commit_error()
        error.staging_objects = (marker * 10_000,)
        raise error

    monkeypatch.setattr(runs, "runs_payload", fail)
    result = invoke_result(make_app(delta_root=tmp_path), "artisan_list_runs")
    assert result.is_error is True
    assert result.structured_content["code"] == "evidence_response_too_large"
    assert marker not in _wire(result).decode("utf-8")
    assert len(_wire(result)) <= MAX_TOOL_RESULT_BYTES


def _expanded_contract(value, root, active=frozenset()):
    """Ignore presentation metadata while preserving constraints and defaults."""
    if isinstance(value, list):
        return [_expanded_contract(item, root, active) for item in value]
    if not isinstance(value, dict):
        return value
    if "$ref" in value:
        ref = value["$ref"]
        if ref in active:
            return {"$ref": ref}
        target = root
        for segment in ref.removeprefix("#/").split("/"):
            target = target[segment]
        return _expanded_contract(target, root, active | {ref})
    # FastMCP drops the OpenAPI discriminator hint; oneOf and const still validate it.
    return {
        key: _expanded_contract(item, root, active)
        for key, item in value.items()
        if key not in {"title", "description", "$defs", "discriminator"}
    }


def test_mcp_effective_schemas_and_defaults_match_core(make_app):
    from artisan.inspection.catalog import describe_inspection

    async def check():
        async with Client(make_app()) as client:
            for tool in await client.list_tools():
                core = describe_inspection(tool.name.removeprefix("artisan_"))
                assert _expanded_contract(
                    tool.inputSchema, tool.inputSchema
                ) == _expanded_contract(core.request_schema, core.request_schema), (
                    tool.name
                )
                assert _expanded_contract(
                    tool.outputSchema, tool.outputSchema
                ) == _expanded_contract(core.response_schema, core.response_schema), (
                    tool.name
                )

    asyncio.run(check())
