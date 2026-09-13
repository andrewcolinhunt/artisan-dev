"""The MCP surface is honestly and unconditionally read-only."""

from __future__ import annotations

_READ_TOOLS = {
    "artisan_capabilities",
    "artisan_list_operations",
    "artisan_describe_operation",
    "artisan_list_runs",
    "artisan_get_run_status",
    "artisan_get_step_result",
    "artisan_query_artifacts",
    "artisan_get_step_logs",
    "artisan_get_provenance_graph",
    "artisan_diagnose_run",
}


class TestReadOnlySurface:
    def test_read_only_registers_ten_tools(self, make_app, tool_names) -> None:
        assert tool_names(make_app()) == _READ_TOOLS

    def test_capabilities_report_read_only(self, make_app, invoke) -> None:
        assert invoke(make_app(), "artisan_capabilities")["read_only"] is True
