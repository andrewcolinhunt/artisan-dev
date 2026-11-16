"""Fresh-process recovery fixture for an exact curator cache source."""

from __future__ import annotations

import json
import os
import sys

from artisan.orchestration.engine.retained_curator import CuratorResults
from artisan.orchestration.engine.retained_step_tracker import RetainedStepTracker
from artisan.schemas.execution.curator_admission import CuratorAdmissionReference
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.storage.cache.curator_lookup import lookup_curator
from artisan.storage.core.curator_reader import CuratorReader
from fixtures.retained_acceptance import open_committer


def main() -> None:
    request = json.load(sys.stdin)
    storage = StorageConfig.model_validate(request["storage"])
    committer = open_committer(request["root"], storage.filesystem(), storage)

    def checkpoint(event: str) -> None:
        if event == request["boundary"]:
            os._exit(41)

    coordinator = CuratorResults(
        RetainedStepTracker(committer, "synthetic-pipeline"),
        request["staging"],
        checkpoint=checkpoint,
    )
    hit = lookup_curator(
        CuratorAdmissionReference.model_validate(request["reference"]),
        reader=CuratorReader(committer),
    )
    assert hit is not None
    coordinator.reuse(hit)
    msg = "Curator reuse did not reach the selected interruption"
    raise RuntimeError(msg)


if __name__ == "__main__":
    main()
