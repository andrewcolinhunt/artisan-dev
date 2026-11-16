"""Terminate a real controller between terminal publication effects."""

from __future__ import annotations

import json
import os
import sys

from fsspec import AbstractFileSystem

from artisan.orchestration.engine.retained_step_results import RetainedStepResults
from artisan.orchestration.engine.retained_step_tracker import RetainedStepTracker
from artisan.schemas.execution.storage_config import StorageConfig
from artisan.storage.io import step_result_plan
from artisan.storage.io.step_result_plan import StepUnitFailure
from fixtures.retained_acceptance import open_committer


def main() -> None:
    request = json.load(sys.stdin)
    storage = StorageConfig.model_validate(request["storage"])
    committer = open_committer(request["root"], storage.filesystem(), storage)
    boundary = request["boundary"]

    def checkpoint(event: str) -> None:
        if event == boundary:
            os._exit(41)

    original_publish = step_result_plan.publish_immutable_bytes

    def interrupt_plan(fs: AbstractFileSystem, path: str, payload: bytes) -> None:
        if boundary == "terminal_file" and path.endswith("/plan.json"):
            os._exit(41)
        original_publish(fs, path, payload)

    step_result_plan.publish_immutable_bytes = interrupt_plan
    RetainedStepResults(
        RetainedStepTracker(committer, "synthetic-pipeline"), checkpoint=checkpoint
    ).finish(
        request["step_run_id"],
        failures={
            int(index): StepUnitFailure.model_validate(failure)
            for index, failure in request.get("failures", {}).items()
        },
    )
    msg = "Requested step publication crash was not reached"
    raise RuntimeError(msg)


if __name__ == "__main__":
    main()
