"""Measure existing curator worker software without installing or executing science."""

from __future__ import annotations

import argparse
import hashlib
import json
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory

from artisan.execution.curator_definition import capture_curator_definition
from artisan.execution.environments.observed_worker import ObservedWorker
from artisan.execution.recording.commands import capture_commands
from artisan.execution.worker_requirements import observed_definition_sources
from artisan.operations.curator.merge import Merge
from artisan.orchestration.runners.local import LocalRunner
from artisan.schemas.operation_config.runner_resources import RunnerResources
from artisan.utils.command_supervisor import managed_invocation
from artisan.utils.external_tools import run_command
from artisan.utils.hashing import canonical_json_bytes

PROGRAM = """
import json,sys
from artisan.execution.curator_definition import capture_curator_definition
from artisan.operations.curator.merge import Merge
print(json.dumps({'executable':sys.executable,'definition':capture_curator_definition(Merge()).digest}))
"""


def _source_hashes() -> dict[str, str]:
    _, sources = observed_definition_sources((Merge,), (LocalRunner,))
    return {
        f"{name}/{logical}": hashlib.sha256(path.read_bytes()).hexdigest()
        for name, files in sources.items()
        for logical, path in files.items()
    }


def run() -> dict[str, object]:
    """Qualify controller/child identity, launch that worker, and reject tampering."""
    before = _source_hashes()
    operation = Merge()
    with TemporaryDirectory(prefix="artisan-curator-software-") as directory:
        scratch = Path(directory)
        runner = LocalRunner(cache_root=scratch / "unprovisioned-cache")
        bound = runner.resolve_curator(operation, RunnerResources(gpus=0))
        assert isinstance(bound.worker, ObservedWorker)
        assert bound.tools == ()
        assert not bound.software_cacheable
        assert not runner.cache_root.exists()
        with managed_invocation(), capture_commands():
            result = run_command(
                bound.worker_environment(scratch / "worker", {}),
                ["python", "-I", "-c", PROGRAM],
                cwd=directory,
            )
        measured = json.loads(result.stdout)
        assert measured == {
            "executable": str(bound.worker.executable),
            "definition": capture_curator_definition(operation).digest,
        }
        changed = replace(
            bound, worker=replace(bound.worker, executable_sha256="0" * 64)
        )
        try:
            changed.verify_worker()
        except ValueError as error:
            assert "interpreter changed" in str(error)
        else:
            msg = "Changed worker bytes were accepted"
            raise AssertionError(msg)
        bound.verify_worker()
        assert _source_hashes() == before
        report = {
            "definition_digest": measured["definition"],
            "software_digest": bound.software_digest,
            "software_cacheable": bound.software_cacheable,
            "source_digest": hashlib.sha256(canonical_json_bytes(before)).hexdigest(),
            "source_files": len(before),
            "source_inventory": before,
            "source_unchanged": True,
            "changed_worker_rejected": True,
            "packages": [item.name for item in bound.compatibility.packages],
            "worker_executable": measured["executable"],
            "prepared_execution": False,
        }
    assert not scratch.exists()
    return {**report, "scratch_closed": True}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    args.report.write_text(json.dumps(run(), indent=2) + "\n")


if __name__ == "__main__":
    main()
