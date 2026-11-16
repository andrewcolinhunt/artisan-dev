"""Separate-process worker/controller entrypoint for crash qualification."""

from __future__ import annotations

import base64
import json
import os
import shutil
import sys
from pathlib import Path

import fsspec
from qualification.lineage.projection import BoundaryProjection
from qualification.persistence.prototype import CandidateStore, Envelope, stage_delivery


def main() -> None:
    """Read a portable request on stdin, then stage or accept one delivery."""
    request = json.load(sys.stdin)
    fs = fsspec.filesystem(request["protocol"], **request["options"])
    envelope = Envelope.model_validate(request["envelope"])
    projection = BoundaryProjection.model_validate(request["projection"])

    def checkpoint(name: str) -> None:
        if name == request.get("boundary"):
            os._exit(73)

    if request["action"] == "worker":
        scratch = Path(request["scratch"])
        scratch.mkdir(parents=True)
        for filename, encoded in request.get("external_files", {}).items():
            (scratch / filename).write_bytes(base64.b64decode(encoded))
        artifacts = {}
        for artifact_id, encoded in request["artifacts"].items():
            path = scratch / artifact_id
            path.write_bytes(base64.b64decode(encoded))
            artifacts[artifact_id] = path.read_bytes()
        stage_delivery(
            request["directory"],
            envelope,
            projection,
            artifacts,
            fs=fs,
            checkpoint=checkpoint,
        )
        shutil.rmtree(scratch)
        return

    store = CandidateStore(
        request["root"],
        fs=fs,
        storage_options=request["delta_options"],
        checkpoint=checkpoint,
    )
    store.accept(request["directory"])
    msg = "The requested process-death boundary was not reached"
    raise AssertionError(msg)


if __name__ == "__main__":
    main()
