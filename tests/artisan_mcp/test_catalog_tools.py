"""Catalog tool tests: capabilities, list_operations, describe_operation."""

from __future__ import annotations


class TestCapabilities:
    def test_reports_versions_and_read_only(self, make_app, invoke) -> None:
        payload = invoke(make_app(), "artisan_capabilities")
        assert payload["artisan_version"]
        assert payload["server_version"]
        assert payload["read_only"] is True
        assert payload["delta_root"] is None
        assert payload["discovery"]["operations_count"] >= 1

    def test_write_flag_flips_read_only(self, make_app, invoke) -> None:
        payload = invoke(make_app(write=True), "artisan_capabilities")
        assert payload["read_only"] is False

    def test_delta_root_surfaced(self, make_app, invoke, tmp_path) -> None:
        payload = invoke(make_app(delta_root=tmp_path), "artisan_capabilities")
        assert payload["delta_root"] == str(tmp_path)


class TestListOperations:
    def test_lists_examples_when_loaded(self, make_app, invoke) -> None:
        page = invoke(make_app(), "artisan_list_operations")
        names = [item["name"] for item in page["items"]]
        assert "data_transformer" in names  # from artisan.operations.examples
        assert "filter" in names  # curator builtin

    def test_load_modules_attributed_in_discovery(self, make_app, invoke) -> None:
        # The op registry is a process-global side effect, so a bare
        # list_operations can be polluted by earlier tests. The discovery
        # report, attributed per-lifespan, is the honest signal that the
        # examples module was (or was not) loaded by this server.
        loaded = invoke(make_app(), "artisan_capabilities")["discovery"]
        modules = {s["module"] for s in loaded["sources"]}
        assert "artisan.operations.examples" in modules

        bare = invoke(make_app(load_modules=None), "artisan_capabilities")["discovery"]
        assert "artisan.operations.examples" not in {
            s["module"] for s in bare["sources"]
        }

    def test_kind_and_query_filter_and_together(self, make_app, invoke) -> None:
        page = invoke(
            make_app(),
            "artisan_list_operations",
            {"kind": "creator", "query": "transformer"},
        )
        names = [item["name"] for item in page["items"]]
        assert "data_transformer" in names
        assert all("transformer" in n for n in names)
        assert all(item["kind"] == "creator" for item in page["items"])

    def test_pagination_has_more_and_cursor(self, make_app, invoke) -> None:
        app = make_app()
        first = invoke(app, "artisan_list_operations", {"limit": 1})
        assert len(first["items"]) == 1
        assert first["has_more"] is True
        assert first["next_cursor"] == "1"

        second = invoke(app, "artisan_list_operations", {"limit": 1, "cursor": "1"})
        assert len(second["items"]) == 1
        assert second["items"][0]["name"] != first["items"][0]["name"]

    def test_last_page_has_no_cursor(self, make_app, invoke) -> None:
        page = invoke(make_app(), "artisan_list_operations", {"limit": 1000})
        assert page["has_more"] is False
        assert page["next_cursor"] is None


class TestDescribeOperation:
    def test_describes_known_op(self, make_app, invoke) -> None:
        meta = invoke(
            make_app(), "artisan_describe_operation", {"name": "data_transformer"}
        )
        assert meta["name"] == "data_transformer"
        assert "params_schema" in meta
        assert "examples" in meta

    def test_unknown_op_returns_envelope(self, make_app, invoke) -> None:
        env = invoke(
            make_app(), "artisan_describe_operation", {"name": "data_transformr"}
        )
        assert env["code"] == "unknown_operation"
        assert "data_transformer" in env["suggestions"]


class TestOperationResource:
    def test_reads_operation_metadata(self, make_app, read_resource) -> None:
        import json

        content = read_resource(make_app(), "artisan://operations/data_transformer")
        meta = json.loads(content.text)
        assert meta["name"] == "data_transformer"
