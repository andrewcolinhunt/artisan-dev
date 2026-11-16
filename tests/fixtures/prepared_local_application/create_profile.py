"""Capture an owned immutable source fixture before explicit native preparation."""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import shutil
import tomllib
from pathlib import Path

from packaging.requirements import Requirement
from packaging.utils import canonicalize_name

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--destination", type=Path, required=True)
args = parser.parse_args()
checkout = Path(__file__).resolve().parents[3]
root = args.destination.resolve()
source = root / "source"
source.mkdir(parents=True, exist_ok=False)
core = source / "core"
core.mkdir()
for name in ("pyproject.toml", "README.md", "LICENSE"):
    path = checkout / name
    if path.exists():
        shutil.copy2(path, core / name)
for name in ("src/artisan", "src/artisan_mcp", "skills/pipeline-operate"):
    path = checkout / name
    if path.exists():
        shutil.copytree(
            path, core / name, ignore=shutil.ignore_patterns("__pycache__", "*.pyc")
        )
shutil.copytree(
    checkout / "tests/fixtures/prepared_local_application",
    source / "application",
    ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
)
requirements = tomllib.loads((core / "pyproject.toml").read_text())["project"][
    "dependencies"
]
pending = [Requirement(item) for item in requirements]
versions = {}
while pending:
    requirement = pending.pop()
    if requirement.marker and not requirement.marker.evaluate({"extra": ""}):
        continue
    name = canonicalize_name(requirement.name)
    if name in versions:
        continue
    versions[name] = importlib.metadata.version(name)
    pending.extend(
        Requirement(item) for item in importlib.metadata.requires(name) or []
    )
lines = [
    "[workspace]",
    'name = "prepared-local-proof"',
    'channels = ["conda-forge"]',
    'platforms = ["linux-64"]',
    'requires-pixi = "==0.83.0"',
    "",
    "[environments]",
    'worker = {features = ["worker"], no-default-feature = true}',
    'tool-a = {features = ["tool-a"], no-default-feature = true}',
    'tool-b = {features = ["tool-b"], no-default-feature = true}',
    "",
    "[feature.worker.dependencies]",
    'python = "==3.12.12"',
    "",
    "[feature.worker.pypi-dependencies]",
    'dexterity-artisan = {path = "core"}',
    'prepared-local-application = {path = "application"}',
]
lines += [
    f'{json.dumps(name)} = "=={version}"' for name, version in sorted(versions.items())
]
lines += ["", "[feature.worker.activation]", 'scripts = ["worker-activation.sh"]', ""]
for role, version in [("a", "24.2"), ("b", "25.0")]:
    lines += [
        f"[feature.tool-{role}.dependencies]",
        'python = "==3.12.12"',
        "",
        f"[feature.tool-{role}.pypi-dependencies]",
        f'packaging = "=={version}"',
        "",
        f"[feature.tool-{role}.activation.env]",
        f'PROBE_ACTIVATION = "{role}"',
        'PROBE_LITERAL = "from canonical activation"',
        "",
    ]
(source / "pixi.toml").write_text("\n".join(lines))
(source / "worker-activation.sh").write_text('echo "prepared worker activation"\n')
inventory = {
    str(path.relative_to(source)): hashlib.sha256(path.read_bytes()).hexdigest()
    for path in sorted(source.rglob("*"))
    if path.is_file()
}
(root / "source-snapshot.json").write_text(json.dumps(inventory, indent=2) + "\n")
(root / "package-versions.json").write_text(json.dumps(versions, indent=2) + "\n")
summary = {"root": str(root), "files": len(inventory), "pinned_packages": len(versions)}
(root / "snapshot-summary.json").write_text(json.dumps(summary, indent=2) + "\n")
