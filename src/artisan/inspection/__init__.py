"""Bounded inspection, installed operating guidance, and complete report exports.

Exports load on access so retrieving guidance does not import store readers or
optional agent-protocol dependencies. Full analytical readers remain available
from ``artisan.visualization``.
"""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from artisan.inspection.api import inspect_evidence
    from artisan.inspection.catalog import (
        InspectionCatalog,
        InspectionDescription,
        describe_inspection,
        list_inspections,
    )
    from artisan.inspection.export_models import EvidenceExportReceipt
    from artisan.inspection.exports import export_evidence, load_evidence_export
    from artisan.inspection.guidance import (
        OperatingGuideCatalog,
        OperatingGuideTopic,
        list_operating_guides,
        read_operating_guide,
    )

__all__ = [
    "EvidenceExportReceipt",
    "InspectionCatalog",
    "InspectionDescription",
    "OperatingGuideCatalog",
    "OperatingGuideTopic",
    "describe_inspection",
    "export_evidence",
    "inspect_evidence",
    "list_inspections",
    "list_operating_guides",
    "load_evidence_export",
    "read_operating_guide",
]

_EXPORT_MODULES = {
    "EvidenceExportReceipt": "export_models",
    "InspectionCatalog": "catalog",
    "InspectionDescription": "catalog",
    "OperatingGuideCatalog": "guidance",
    "OperatingGuideTopic": "guidance",
    "describe_inspection": "catalog",
    "export_evidence": "exports",
    "inspect_evidence": "api",
    "list_inspections": "catalog",
    "list_operating_guides": "guidance",
    "load_evidence_export": "exports",
    "read_operating_guide": "guidance",
}


def __getattr__(name: str) -> Any:
    """Load only the implementation needed by the requested public symbol."""
    module_name = _EXPORT_MODULES.get(name)
    if module_name is None:
        message = f"module {__name__!r} has no attribute {name!r}"
        raise AttributeError(message)
    value = getattr(import_module(f"{__name__}.{module_name}"), name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    """Include discoverable public names without importing their implementations."""
    return sorted(set(globals()) | set(__all__))
