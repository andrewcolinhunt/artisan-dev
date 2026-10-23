"""WaitTool deployment for the object-storage output tutorial.

Set ARTISAN_S3_BUCKET and ARTISAN_S3_ENDPOINT_URL before importing this module.
The deployed endpoint uses the r2-artisan Modal secret for storage credentials.
"""

from __future__ import annotations

from artisan.operations.examples import WaitTool
from artisan.schemas import (
    ComputeProvider,
    ModalComputeConfig,
    ToolEndpointDataPolicy,
)
from artisan.utils import env_or_dotenv

BUCKET = env_or_dotenv("ARTISAN_S3_BUCKET")
ENDPOINT = env_or_dotenv("ARTISAN_S3_ENDPOINT_URL")
if not BUCKET or not ENDPOINT:
    message = "Set ARTISAN_S3_BUCKET and ARTISAN_S3_ENDPOINT_URL before deployment"
    raise ValueError(message)


class R2WaitTool(WaitTool):
    """Run WaitTool with an allowed destination for tutorial output tarballs."""

    name = "r2_wait_tool"
    compute_provider: ComputeProvider = ComputeProvider(
        modal=ModalComputeConfig(
            secrets=["r2-artisan"],
            env={
                "ARTISAN_S3_BUCKET": BUCKET,
                "ARTISAN_S3_ENDPOINT_URL": ENDPOINT,
            },
            local_python_sources=["artisan", "r2_wait"],
            data_policy=ToolEndpointDataPolicy(
                output_allowlist=(f"s3://{BUCKET}/artisan-tutorial", ENDPOINT),
            ),
        )
    )
