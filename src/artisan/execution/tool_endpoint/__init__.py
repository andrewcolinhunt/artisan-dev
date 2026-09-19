"""Tool endpoints: per-tool Modal apps serving a typed, tool-native HTTP API.

``protocol`` holds the wire models, ``transport`` the bulk-file data plane,
``server`` the worker-side execution, and ``deploy`` the Modal app builder.
The package and deployment module import without Modal; invoking deployment or
default endpoint discovery requires the ``modal`` extra.
"""

from __future__ import annotations

from artisan.execution.tool_endpoint.client import call_endpoint, cancel_scope
from artisan.execution.tool_endpoint.protocol import (
    InputRef,
    ResultResponse,
    SchemaResponse,
    SubmitResponse,
    ToolManifest,
    ToolRequest,
    WorkerResult,
)
from artisan.execution.tool_endpoint.server import run_tool_request
from artisan.execution.tool_endpoint.transport import (
    MAX_INLINE_BYTES,
    InlineTransport,
)

__all__ = [
    "MAX_INLINE_BYTES",
    "InlineTransport",
    "InputRef",
    "ResultResponse",
    "SchemaResponse",
    "SubmitResponse",
    "ToolManifest",
    "ToolRequest",
    "WorkerResult",
    "call_endpoint",
    "cancel_scope",
    "run_tool_request",
]
