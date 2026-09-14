"""Error-envelope round-trips through the MCP wire.

Every fallible tool returns ``ArtisanError.to_dict()`` on failure rather
than raising, so the client receives a structured envelope, not a tool
error.
"""

from __future__ import annotations


class TestEnvelopeBoundary:
    def test_unknown_operation(self, make_app, invoke) -> None:
        env = invoke(
            make_app(), "artisan_describe_operation", {"name": "no_such_op_xyz"}
        )
        assert env["code"] == "unknown_operation"
        assert env["error_type"] == "validation"
        assert env["recovery_hint"] == "CHECK_INPUT"

    def test_delta_root_unset(self, make_app, invoke) -> None:
        env = invoke(
            make_app(delta_root=None),
            "artisan_get_run_status",
            {"pipeline_run_id": "run-x"},
        )
        assert env["code"] == "delta_root_unset"
        assert env["error_type"] == "config"
        assert env["recovery_hint"] == "CHECK_INPUT"

    def test_incompatible_store_from_missing_manifest(
        self, make_app, invoke, tmp_path
    ) -> None:
        # A root without the exact format-2 contract fails before table reads.
        env = invoke(
            make_app(delta_root=tmp_path),
            "artisan_get_run_status",
            {"pipeline_run_id": "run-x"},
        )
        assert env["code"] == "incompatible_store"
        assert env["error_type"] == "config"
        assert env["recovery_hint"] == "CHECK_INPUT"
        assert "cause" not in env
        assert str(tmp_path) not in str(env)

    def test_diagnose_incompatible_store(self, make_app, invoke, tmp_path) -> None:
        env = invoke(
            make_app(delta_root=tmp_path),
            "artisan_diagnose_run",
            {"pipeline_run_id": "run-x"},
        )
        assert env["code"] == "incompatible_store"
