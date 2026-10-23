"""Exercise the synthetic probe through the production worker without Modal."""

from __future__ import annotations

from managed_cancellation_probe import ManagedCancellationProbe

from artisan.execution.tool_endpoint.protocol import InputRef, ToolRequest
from artisan.execution.tool_endpoint.server import run_tool_request
from artisan.execution.tool_endpoint.spec import command_contract
from artisan.schemas.operation_config.endpoint_policy import ToolEndpointDataPolicy

result = run_tool_request(
    ManagedCancellationProbe,
    ToolRequest(
        expected_command_contract=command_contract(ManagedCancellationProbe),
        params={"seconds": 1, "token": "a" * 32, "mode": "normal"},
        inputs=[InputRef(name="dataset", filename="synthetic.csv", data=b"a,b\n1,2\n")],
    ),
    data_policy=ToolEndpointDataPolicy(),
)
assert result.manifest.error is None, result.manifest.error
assert result.manifest.output_names == ["synthetic_waited.csv"]
assert result.output_tar
