"""Real-Modal integration: pipelines and plain HTTP over a deployed tool endpoint.

Prerequisites:
- ``artisan modal deploy wait_tool`` has been run against the workspace
- Modal credentials (``MODAL_TOKEN_ID``/``MODAL_TOKEN_SECRET`` or ``~/.modal.toml``)
- A proxy-auth token pair (Modal dashboard → Proxy Auth Tokens) in
  ``MODAL_PROXY_TOKEN_ID`` / ``MODAL_PROXY_TOKEN_SECRET``
"""

from __future__ import annotations

import io
import json
import tarfile
import time

import pytest

pytestmark = pytest.mark.modal


@pytest.fixture(autouse=True)
def _require_proxy_auth_tokens() -> None:
    from artisan.utils.env_file import env_or_dotenv

    if not (
        env_or_dotenv("MODAL_PROXY_TOKEN_ID")
        and env_or_dotenv("MODAL_PROXY_TOKEN_SECRET")
    ):
        pytest.skip(
            "MODAL_PROXY_TOKEN_ID/MODAL_PROXY_TOKEN_SECRET not found in the "
            "environment or a .env file (copy .env.example to .env and fill "
            "in a dashboard-created proxy-auth token)"
        )


def test_pipeline_commits_artifacts_via_endpoint(tmp_path):
    """Two batched inputs retain exact parents and human names locally and on Modal."""
    import polars as pl

    from artisan.operations.examples import DataGenerator, WaitTool
    from artisan.orchestration import PipelineManager

    from .conftest import get_execution_outputs, load_artifact_edges, read_table

    pipeline = PipelineManager.create(
        name="tool_endpoint_smoke",
        delta_root=str(tmp_path / "delta"),
        staging_root=str(tmp_path / "staging"),
        working_root=str(tmp_path / "working"),
    )
    generated = pipeline.run(
        operation=DataGenerator,
        name="generate",
        params={"count": 2, "seed": 42},
    )
    for provider in ("local", "modal"):
        pipeline.run(
            operation=WaitTool,
            name=f"wait_{provider}",
            inputs={"dataset": generated.output("datasets")},
            params={"seconds": 1},
            batch_strategy={"artifacts_per_unit": 2},
            compute_provider=provider,
        )
    assert pipeline.finalize()["overall_success"]
    delta_root = pipeline.config.delta_root
    data = read_table(delta_root, "artifacts/data")
    source_ids = get_execution_outputs(delta_root, 0, "datasets")
    sources = data.filter(pl.col("artifact_id").is_in(source_ids))
    expected = {
        (
            f"{row['original_name']}_waited",
            row["artifact_id"],
            "dataset",
            "output",
            "data",
            "data",
            None,
        )
        for row in sources.iter_rows(named=True)
    }
    assert len(expected) == 2
    for step_number in (1, 2):
        output_ids = get_execution_outputs(delta_root, step_number, "output")
        assert len(output_ids) == 2
        outputs = data.filter(pl.col("artifact_id").is_in(output_ids))
        names = dict(zip(outputs["artifact_id"], outputs["original_name"], strict=True))
        execution_ids = (
            read_table(delta_root, "orchestration/executions")
            .filter(pl.col("origin_step_number") == step_number)["execution_run_id"]
            .to_list()
        )
        edges = load_artifact_edges(delta_root, output_ids).filter(
            pl.col("execution_run_id").is_in(execution_ids)
        )
        assert edges.height == 2
        actual = {
            (
                names[row["target_artifact_id"]],
                row["source_artifact_id"],
                row["source_role"],
                row["target_role"],
                row["source_artifact_type"],
                row["target_artifact_type"],
                row["group_id"],
            )
            for row in edges.iter_rows(named=True)
        }
        assert actual == expected


def test_endpoint_serves_non_artisan_clients():
    """The same endpoint answers plain HTTP — no artisan imports needed."""
    import httpx
    import modal

    from artisan.utils.env_file import env_or_dotenv

    url = modal.Function.from_name("artisan-tool-wait_tool", "endpoint").get_web_url()
    headers = {
        "Modal-Key": env_or_dotenv("MODAL_PROXY_TOKEN_ID") or "",
        "Modal-Secret": env_or_dotenv("MODAL_PROXY_TOKEN_SECRET") or "",
    }
    with httpx.Client(base_url=url, headers=headers, timeout=120) as client:
        # Multipart contract: each part's filename is the input ROLE;
        # input_filenames maps role -> real file name so the worker
        # materializes the input under its original stem.
        submitted = client.post(
            "/submit",
            data={
                "params": json.dumps({"seconds": 1}),
                "input_filenames": json.dumps({"dataset": "sample.csv"}),
            },
            files=[("files", ("dataset", b"a,b\n1,2\n"))],
        )
        submitted.raise_for_status()
        call_id = submitted.json()["call_id"]

        while True:
            polled = client.get("/result", params={"call_id": call_id})
            polled.raise_for_status()
            body = polled.json()
            if body["status"] != "pending":
                break
            time.sleep(2)

        assert body["status"] == "done"
        assert "sample_waited.csv" in body["manifest"]["output_names"]

        download = client.get("/download", params={"call_id": call_id})
        download.raise_for_status()
        with tarfile.open(fileobj=io.BytesIO(download.content)) as tar:
            extracted = tar.extractfile("sample_waited.csv")
            assert extracted is not None
            marker = extracted.read().decode()
            assert marker.startswith("seconds,host,source\n1,")
            assert marker.rstrip().endswith("sample.csv")
