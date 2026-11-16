"""Curator reuse identity independent of fresh step and query-view ownership."""

from __future__ import annotations

import json

from artisan.schemas.execution.curator_admission import CuratorAdmission
from artisan.utils.hashing import canonical_json_bytes, compute_content_digest


def curator_cache_key(admission: CuratorAdmission) -> str | None:
    """Bind trusted resolved science/software and exact ordered source membership.

    The definition stamp must include effective parameters before redaction.
    Persisted parameter text is inspection data, not a substitute for that stamp.
    """
    if admission.definition_digest is None or admission.software_digest is None:
        return None
    inputs = [
        {
            "role": role.role,
            "members": [
                {
                    "artifact_id": member.artifact_id,
                    "artifact_type": member.artifact_type,
                    "producers": sorted(
                        (item.model_dump(mode="json") for item in member.producers),
                        key=canonical_json_bytes,
                    ),
                }
                for member in role.members
            ],
        }
        for role in admission.inputs
    ]
    descriptor = {
        "definition": admission.definition_digest,
        "software": admission.software_digest,
        "software_cacheable": admission.software_cacheable,
        "operation": admission.operation_name,
        "version": admission.operation_version,
        "cacheable": admission.cacheable,
        "inputs": inputs,
        "associations": [
            item.model_dump(mode="json") for item in admission.associations
        ],
        "output_roles": json.loads(admission.record.output_roles_json),
        "output_types": json.loads(admission.record.output_types_json),
    }
    return compute_content_digest(
        b"artisan.curator-cache-v1\0" + canonical_json_bytes(descriptor)
    )
