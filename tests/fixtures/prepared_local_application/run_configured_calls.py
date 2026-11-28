"""Verify selected calls from a separately prepared callback interpreter.

Run with the worker environment created by create_profile.py after locking it.
Pass --profile, --cache, and --output as absolute paths.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from artisan.composites.base.compiler import compile_definition
from artisan.execution import CalledOperation
from artisan.execution.compute.prepared import PreparedCommandEnvironment
from artisan.execution.configured_calls import ConfiguredCommandEnvironment
from artisan.execution.local_assignment import PreparedLocalBindings
from artisan.operations.examples import (
    ConfiguredGenerator,
    DataGenerator,
    DataTransformerScript,
)
from artisan.orchestration import PipelineManager, StepDisposition, StepStatus
from artisan.orchestration.runners import LocalRunner
from artisan.schemas import Environment, RunnerResources
from artisan.schemas.execution.prepared_environment import PreparedNativeEnvironment
from artisan.utils.external_tools import run_command

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--profile", type=Path, required=True)
parser.add_argument("--cache", type=Path, required=True)
parser.add_argument("--output", type=Path, required=True)
args = parser.parse_args()
root = args.profile.resolve()
declaration = Environment.from_pixi(root / "source/pixi.toml", name="worker")
DataTransformerScript.model_fields["environment"].default = declaration
DataTransformerScript.model_rebuild(force=True)
selection = {"generator": CalledOperation(DataGenerator, {"count": 1})}
runner = LocalRunner(cache_root=args.cache.resolve())
runner.prepare(DataTransformerScript, called_operations=selection)
plan = compile_definition(DataTransformerScript, called_operations=selection)
bindings = runner.resolve(plan, RunnerResources())
assert isinstance(bindings, PreparedLocalBindings)
target = bindings.tools[0].target
assert isinstance(target, PreparedNativeEnvironment)
assert target.prefix != Path(sys.prefix)
scope = bindings._call_scope(target, {})
scratch = args.output.resolve()
scratch.mkdir(parents=True, exist_ok=True)
environment = ConfiguredCommandEnvironment(
    PreparedCommandEnvironment(target, runner.pixi, runner.system_env, scratch, {}),
    scope.model_dump(mode="json"),
    scratch,
)
program = """
import json,sys
from pathlib import Path
from artisan.execution import run_operation
from artisan.execution.executor_runtime import ExecutorRuntime
from artisan.operations.examples import DataGenerator

def reject(*args, **kwargs):
    raise AssertionError('Callback looked up software')
ExecutorRuntime._software = reject
counts = []
for seed in (1,2):
    result = run_operation(DataGenerator, selection_key='generator', params={'seed': seed}, work_dir=Path.cwd())
    counts.append(len(result.artifacts['datasets']))
Path('proof.json').write_text(json.dumps({'prefix':sys.prefix, 'counts': counts}))
"""
run_command(environment, ["python", "-I", "-c", program], cwd=str(scratch))
report = json.loads((scratch / "proof.json").read_text())
assert report["counts"] == [1, 1], report
assert report["prefix"] == str(target.prefix), report
print(json.dumps(report), flush=True)

worker_runner = LocalRunner(environment=declaration, cache_root=args.cache.resolve())
worker_runner.prepare(ConfiguredGenerator, called_operations=selection)
with PipelineManager.create(
    "configured-proof",
    delta_root=str(scratch / "delta"),
    staging_root=str(scratch / "staging"),
    working_root=str(scratch / "work"),
    default_step_runner=worker_runner,
) as pipeline:
    first = pipeline.run(ConfiguredGenerator, called_operations=selection)
    assert first.status == StepStatus.SUCCEEDED, first.error
    repeated = pipeline.run(ConfiguredGenerator, called_operations=selection)
    assert repeated.disposition == StepDisposition.CACHE_HIT, repeated
    changed = pipeline.run(
        ConfiguredGenerator,
        called_operations={"generator": CalledOperation(DataGenerator, {"count": 2})},
    )
    assert changed.status == StepStatus.SUCCEEDED, changed.error
    assert changed.disposition == StepDisposition.EXECUTED, changed
    assert len(pipeline) == 3
print("Prepared parent cache reused; changed selected setting missed", flush=True)
