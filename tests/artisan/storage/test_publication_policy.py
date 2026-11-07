"""Protected semantic values remain private when JSON escapes their spelling."""

from __future__ import annotations

import json

import pytest

from artisan.schemas.enums import TablePath
from artisan.storage._publication_policy import PublicationPolicy


@pytest.mark.parametrize(
    "private_value", [r"C:\Users\example", 'example "workspace"', "caf\u00e9-workspace"]
)
def test_protected_metadata_rejects_json_escaped_value(private_value: str) -> None:
    policy = PublicationPolicy.create((private_value,))
    row = {"metadata": json.dumps({"location": private_value})}
    with pytest.raises(ValueError, match="protected evidence"):
        policy.row(TablePath.ARTIFACT_INDEX, row, {})
