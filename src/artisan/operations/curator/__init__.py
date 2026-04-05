"""Artisan curator operations: Filter, Merge, Ingest, InteractiveFilter."""

from __future__ import annotations

from artisan.operations.curator.consolidate_record_bundles import (
    ConsolidateRecordBundles,
)
from artisan.operations.curator.filter import Filter
from artisan.operations.curator.ingest_data import IngestData
from artisan.operations.curator.ingest_files import IngestFiles
from artisan.operations.curator.ingest_pipeline_step import IngestPipelineStep
from artisan.operations.curator.interactive_filter import InteractiveFilter
from artisan.operations.curator.merge import Merge

__all__ = [
    "ConsolidateRecordBundles",
    "Filter",
    "IngestData",
    "IngestFiles",
    "IngestPipelineStep",
    "InteractiveFilter",
    "Merge",
]
