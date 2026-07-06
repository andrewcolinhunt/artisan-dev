"""Write-gate tests: the read-only surface is exactly ten tools.

Phase 2 write tools are unbuilt, so ``ARTISAN_WRITE`` changes only the
``read_only`` capability flag today — the tool set stays the ten read tools
either way. When write tools land, this test grows to assert their
appearance under the flag.
"""

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


class TestWriteGate:
    def test_read_only_registers_ten_tools(self, make_app, tool_names) -> None:
        assert tool_names(make_app(write=False)) == _READ_TOOLS

    def test_write_enabled_registers_same_ten(self, make_app, tool_names) -> None:
        # No write tools exist yet (Phase 2); the surface is unchanged.
        assert tool_names(make_app(write=True)) == _READ_TOOLS

    def test_flag_flips_read_only_in_capabilities(self, make_app, invoke) -> None:
        assert invoke(make_app(write=False), "artisan_capabilities")["read_only"]
        assert not invoke(make_app(write=True), "artisan_capabilities")["read_only"]
