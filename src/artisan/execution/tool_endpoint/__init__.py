"""Tool endpoints: per-tool Modal apps serving a typed, tool-native HTTP API.

``protocol`` holds the wire models, ``transport`` the bulk-file data plane,
``server`` the worker-side execution, and ``deploy`` the Modal app builder
(import ``deploy`` directly — it requires the ``modal`` SDK).
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
from artisan.execution.tool_endpoint.server import resolve_op, run_tool_request
from artisan.execution.tool_endpoint.transport import (
    MAX_INLINE_BYTES,
    DataTransport,
    InlineTransport,
)

__all__ = [
    "MAX_INLINE_BYTES",
    "DataTransport",
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
    "resolve_op",
    "run_tool_request",
]
