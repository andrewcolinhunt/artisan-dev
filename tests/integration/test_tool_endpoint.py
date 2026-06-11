"""Real-Modal integration: pipelines and plain HTTP over a deployed tool endpoint.

Prerequisites:
- ``artisan modal deploy echo_tool`` has been run against the workspace
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
    """A pipeline step with compute_provider='modal' commits artifacts."""
    from artisan.operations.examples import EchoTool
    from artisan.orchestration import PipelineManager

    pipeline = PipelineManager.create(
        name="tool_endpoint_smoke",
        delta_root=str(tmp_path / "delta"),
        staging_root=str(tmp_path / "staging"),
        working_root=str(tmp_path / "working"),
    )
    pipeline.run(
        operation=EchoTool,
        name="echo",
        params={"text": "from modal", "filename": "echo.txt"},
        compute_provider="modal",
    )
    result = pipeline.finalize()
    assert result["overall_success"]


def test_endpoint_serves_non_artisan_clients():
    """The same endpoint answers plain HTTP — no artisan imports needed."""
    import httpx
    import modal

    from artisan.utils.env_file import env_or_dotenv

    url = modal.Function.from_name("artisan-tool-echo_tool", "endpoint").get_web_url()
    headers = {
        "Modal-Key": env_or_dotenv("MODAL_PROXY_TOKEN_ID") or "",
        "Modal-Secret": env_or_dotenv("MODAL_PROXY_TOKEN_SECRET") or "",
    }
    with httpx.Client(base_url=url, headers=headers, timeout=120) as client:
        submitted = client.post(
            "/submit",
            data={"params": json.dumps({"text": "curl", "filename": "c.txt"})},
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
        assert "c.txt" in body["manifest"]["output_names"]

        download = client.get("/download", params={"call_id": call_id})
        download.raise_for_status()
        with tarfile.open(fileobj=io.BytesIO(download.content)) as tar:
            extracted = tar.extractfile("c.txt")
            assert extracted is not None
            assert extracted.read() == b"curl\n"
