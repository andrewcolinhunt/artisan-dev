"""Prepare disposable Pixi environments without modifying the repository lock."""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from pathlib import Path

MANIFEST = """[workspace]
name = "provider-qualification"
channels = ["conda-forge"]
platforms = ["linux-64"]

[dependencies]
python = "==3.12.12"

[feature.worker.dependencies]
cloudpickle = "==3.1.2"
pydantic = "==2.12.5"

[feature.tool_a.dependencies]
packaging = "==24.2"

[feature.tool_a.activation.env]
QUALIFICATION_TOOL = "a"

[feature.tool_b.dependencies]
packaging = "==25.0"

[feature.tool_b.activation.env]
QUALIFICATION_TOOL = "b"

[environments]
worker = ["worker"]
tool-a = ["tool_a"]
tool-b = ["tool_b"]
"""


def prepare(
    root: Path,
    pixi: Path,
    *,
    artisan_wheel: Path | None = None,
    provider_root: Path | None = None,
) -> None:
    """Install each prefix in its final location and capture activation values."""
    root.mkdir(parents=True, exist_ok=True)
    manifest = root / "pixi.toml"
    content = MANIFEST
    if artisan_wheel is not None:
        content += "\n[feature.worker.pypi-dependencies]\n"
        content += (
            "dexterity-artisan = { path = " + json.dumps(str(artisan_wheel)) + " }\n"
        )
        content += 'polars = "==1.37.1"\ndeltalake = "==1.4.0"\npyarrow = "==26.0.0"\nfsspec = "==2026.2.0"\n'
        if provider_root is not None:
            content += (
                "artisan-submitit = { path = " + json.dumps(str(provider_root)) + " }\n"
            )
            content += 'pytest = "*"\npytest-timeout = "*"\n'
    manifest.write_text(content)
    subprocess.run(
        [str(pixi), "install", "--manifest-path", str(manifest), "--all"], check=True
    )
    tools = []
    for role in "ab":
        environment = f"tool-{role}"
        activation = subprocess.check_output(
            [
                str(pixi),
                "shell-hook",
                "--manifest-path",
                str(manifest),
                "-e",
                environment,
                "--json",
            ],
            text=True,
        )
        activated = json.loads(activation)["environment_variables"]
        tools.append(
            {
                "PATH": str(root / ".pixi/envs" / environment / "bin")
                + ":/usr/bin:/bin",
                "PYTHON": str(root / ".pixi/envs" / environment / "bin/python"),
                "QUALIFICATION_TOOL": activated["QUALIFICATION_TOOL"],
                "QUALIFICATION_LITERAL": "a literal $value with spaces",
            }
        )
    (root / "tool-environments.json").write_text(json.dumps(tools, indent=2) + "\n")
    receipt = {
        "lock_digest": hashlib.sha256((root / "pixi.lock").read_bytes()).hexdigest(),
        "tools": ["a", "b"],
    }
    (root / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    parser.add_argument("--pixi", type=Path, default=Path.home() / ".pixi/bin/pixi")
    parser.add_argument("--artisan-wheel", type=Path)
    parser.add_argument("--provider-root", type=Path)
    arguments = parser.parse_args()
    prepare(
        arguments.root.resolve(),
        arguments.pixi,
        artisan_wheel=arguments.artisan_wheel,
        provider_root=arguments.provider_root,
    )
