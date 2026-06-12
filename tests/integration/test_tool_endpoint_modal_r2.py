"""Live end-to-end: a tool endpoint on Modal delivering outputs to R2.

Deploys ``artisan-tool-wait_tool`` from the working branch (WaitTool's
source overlay is on by design), then exercises both stored-output modes
of ``endpoint-s3-outputs`` against the real bucket:

- **prefix mode** through the artisan client (``call_endpoint``): the
  worker uploads with the ``r2-artisan`` Modal Secret's credentials and
  the client fetches via the manifest's presigned GET;
- **capability mode** as an external consumer (raw httpx, no artisan
  machinery): a caller-minted SigV4 presigned PUT carries the outputs to
  the caller's own bucket, ``/download`` answers 409, and the caller
  fetches with its own credentials.

Excluded from every default suite (``modal`` marker, deliberately not
``integration``); also picked up by the broader ``test-modal`` task.
Unlike the sibling ``test_tool_endpoint.py`` (which assumes a prior
deploy), this module deploys in its fixture — the deployment must carry
the R2 secret. Run explicitly:

    ~/.pixi/bin/pixi run -e dev test-modal-endpoint

Requirements (each missing one skips with a reason):

- Modal credentials (``~/.modal.toml`` or ``MODAL_TOKEN_ID``/``SECRET``)
- proxy-auth tokens ``MODAL_PROXY_TOKEN_ID``/``SECRET`` (repo-root .env)
- R2 credentials: ``AWS_ACCESS_KEY_ID``, ``AWS_SECRET_ACCESS_KEY``,
  ``ARTISAN_S3_ENDPOINT_URL``, ``ARTISAN_S3_BUCKET`` (repo-root .env,
  falling back to ``_dev/demos/s3_real/.env``)
- the ``r2-artisan`` Modal Secret carrying ``AWS_ACCESS_KEY_ID``,
  ``AWS_SECRET_ACCESS_KEY``, ``AWS_REGION``, ``AWS_ENDPOINT_URL``
- a worker image carrying s3fs and ca-certificates (any
  ``artisan-worker:latest`` from the ``fix/s3fs-default-env`` lock
  onward); ``ARTISAN_TEST_WORKER_IMAGE`` pins a different ref —
  remember Modal caches registry refs, so a re-pushed tag needs one
  run with ``MODAL_FORCE_BUILD=1``
"""

from __future__ import annotations

import contextlib
import json
import os
import time
import uuid
from pathlib import Path

import httpx
import pytest

pytestmark = pytest.mark.modal

_REPO_ROOT = Path(__file__).resolve().parents[2]
_ENV_FILES = (
    _REPO_ROOT / ".env",
    _REPO_ROOT / "_dev" / "demos" / "s3_real" / ".env",
)

R2_SECRET_NAME = "r2-artisan"
POLL_DEADLINE_S = 900  # first call pulls the worker image — minutes, not seconds


@pytest.fixture(scope="module")
def r2() -> dict[str, str]:
    """R2 endpoint + bucket from env (.env fallbacks); skip when absent."""
    from dotenv import load_dotenv

    for env_file in _ENV_FILES:
        if env_file.exists():
            load_dotenv(env_file, override=False)
    required = (
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "ARTISAN_S3_ENDPOINT_URL",
        "ARTISAN_S3_BUCKET",
    )
    missing = [key for key in required if not os.environ.get(key)]
    if missing:
        pytest.skip(f"R2 credentials missing: {', '.join(missing)}")
    # test-side verification resolves filesystems the same ambient way
    # the worker does (the Modal Secret injects the same variables)
    os.environ.setdefault("AWS_ENDPOINT_URL", os.environ["ARTISAN_S3_ENDPOINT_URL"])
    return {
        "endpoint": os.environ["ARTISAN_S3_ENDPOINT_URL"],
        "bucket": os.environ["ARTISAN_S3_BUCKET"],
    }


@pytest.fixture(scope="module")
def proxy_headers() -> dict[str, str]:
    """Proxy-auth headers for raw-httpx consumers; skip when tokens absent."""
    from artisan.utils.env_file import env_or_dotenv

    token_id = env_or_dotenv("MODAL_PROXY_TOKEN_ID")
    token_secret = env_or_dotenv("MODAL_PROXY_TOKEN_SECRET")
    if not (token_id and token_secret):
        pytest.skip("no MODAL_PROXY_TOKEN_ID/SECRET (repo-root .env)")
    return {"Modal-Key": token_id, "Modal-Secret": token_secret}


@pytest.fixture(scope="module")
def endpoint_url(r2: dict[str, str]) -> str:
    """Deploy artisan-tool-wait_tool from the working branch; return its URL.

    Mirrors ``artisan modal deploy wait_tool`` exactly (``build_app`` +
    ``app.deploy()``), with the R2 secret — and optionally a test image —
    swapped into the class-level config ``endpoint_spec`` reads.
    """
    if not ((Path.home() / ".modal.toml").exists() or os.environ.get("MODAL_TOKEN_ID")):
        pytest.skip("no Modal credentials (~/.modal.toml or MODAL_TOKEN_ID)")
    import modal

    from artisan.execution.tool_endpoint.deploy import build_app
    from artisan.operations.examples import WaitTool
    from artisan.schemas.operation_config.compute import ComputeProvider

    field = WaitTool.model_fields["compute_provider"]
    original = field.default
    deploy_cfg = original.modal.model_copy(
        update={
            "secrets": [R2_SECRET_NAME],
            "image": os.environ.get("ARTISAN_TEST_WORKER_IMAGE", original.modal.image),
        }
    )
    field.default = ComputeProvider(modal=deploy_cfg)
    try:
        build_app(WaitTool).deploy()  # the same call the CLI makes
    finally:
        field.default = original
    url = modal.Function.from_name("artisan-tool-wait_tool", "endpoint").get_web_url()
    assert url, "deployed endpoint has no web URL"
    return str(url)


def test_prefix_mode_via_artisan_client(
    endpoint_url: str, r2: dict[str, str], tmp_path: Path
) -> None:
    """Worker-credential upload to a caller-chosen prefix, presigned fetch."""
    import s3fs

    from artisan.execution.tool_endpoint.client import call_endpoint
    from artisan.operations.examples import WaitTool
    from artisan.schemas.operation_config.compute import (
        ComputeProvider,
        ModalComputeConfig,
    )
    from artisan.schemas.specs.input_models import ExecuteInput

    run_prefix = f"integration/endpoint-{uuid.uuid4().hex[:8]}"
    op = WaitTool(
        params=WaitTool.Params(seconds=2),
        compute_provider=ComputeProvider(
            active="modal",
            modal=ModalComputeConfig(
                output_store=f"s3://{r2['bucket']}/{run_prefix}",
                poll_interval=2.0,
            ),
        ),
    )
    source = tmp_path / "in.csv"
    source.write_text("a,b\n1,2\n")
    execute_dir = tmp_path / "execute"
    execute_dir.mkdir()
    log_path = tmp_path / "tool_output.log"

    fs = s3fs.S3FileSystem()
    try:
        call_endpoint(
            op,
            ExecuteInput(
                inputs={"dataset": str(source)},
                execute_dir=str(execute_dir),
                log_path=str(log_path),
            ),
        )
        # outputs materialized exactly as the inline mode would lay them out
        assert (execute_dir / "in_waited.csv").exists()
        assert "tick" in log_path.read_text()
        # and the bytes really live in R2 under the caller's prefix
        keys = fs.ls(f"{r2['bucket']}/{run_prefix}/wait_tool")
        assert len(keys) == 1
        assert keys[0].endswith(".tar.gz")
    finally:
        with contextlib.suppress(FileNotFoundError):
            fs.rm(f"{r2['bucket']}/{run_prefix}", recursive=True)


def test_capability_mode_external_consumer(
    endpoint_url: str,
    r2: dict[str, str],
    proxy_headers: dict[str, str],
    tmp_path: Path,
) -> None:
    """No artisan, no shared credentials: presigned PUT in, own-bucket fetch."""
    import botocore.session
    from botocore.config import Config

    from artisan.execution.tool_endpoint.transport import InlineTransport

    key = f"integration/capability-{uuid.uuid4().hex[:8]}/run.tar.gz"
    s3 = botocore.session.Session().create_client(
        "s3",
        region_name=os.environ.get("AWS_REGION", "auto"),
        endpoint_url=r2["endpoint"],
        config=Config(signature_version="s3v4"),  # the documented minting rule
    )
    put_url = s3.generate_presigned_url(
        "put_object", Params={"Bucket": r2["bucket"], "Key": key}, ExpiresIn=3600
    )

    try:
        with httpx.Client(
            base_url=endpoint_url, headers=proxy_headers, timeout=120.0
        ) as client:
            response = client.post(
                "/submit",
                data={
                    "params": json.dumps({"seconds": 1}),
                    "input_filenames": json.dumps({"dataset": "in.csv"}),
                    "output_store": put_url,
                },
                files=[("files", ("dataset", b"a,b\n1,2\n"))],
            )
            assert response.status_code == 200, response.text
            call_id = response.json()["call_id"]

            deadline = time.monotonic() + POLL_DEADLINE_S
            while True:
                result = client.get("/result", params={"call_id": call_id})
                assert result.status_code == 200, result.text
                body = result.json()
                if body["status"] != "pending":
                    break
                assert time.monotonic() < deadline, "worker did not finish in time"
                time.sleep(3)

            assert body["status"] == "done", body
            stored = body["manifest"]["stored"]
            assert stored["presigned_url"] is None
            assert stored["uri"] == put_url.split("?", 1)[0]
            # the endpoint cannot serve what it never held
            download = client.get(
                "/download", params={"call_id": call_id}, follow_redirects=False
            )
            assert download.status_code == 409

        # fetch from our own bucket with our own credentials
        payload = s3.get_object(Bucket=r2["bucket"], Key=key)["Body"].read()
        InlineTransport().unpack_outputs(payload, str(tmp_path / "fetched"))
        assert (tmp_path / "fetched" / "in_waited.csv").exists()
    finally:
        with contextlib.suppress(Exception):
            s3.delete_object(Bucket=r2["bucket"], Key=key)
