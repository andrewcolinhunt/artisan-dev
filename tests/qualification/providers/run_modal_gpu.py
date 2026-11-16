"""Execute one bounded GPU command in an already qualified ephemeral image."""

from __future__ import annotations

import argparse
import json
import uuid
from pathlib import Path

import modal


def qualify(image_id: str, output: Path) -> None:
    """Reuse prepared software for one A10G kernel without rebuilding an image."""
    image = modal.Image.from_id(image_id)
    app = modal.App(f"artisan-provider-gpu-{uuid.uuid4().hex[:10]}")

    @app.function(
        image=image,
        gpu="A10G",
        cpu=1,
        memory=512,
        max_containers=1,
        timeout=30,
        startup_timeout=120,
        retries=0,
        serialized=True,
        include_source=False,
    )
    def invoke(files: dict[str, str]) -> dict[str, object]:
        import sys
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory(prefix="provider-gpu-") as directory:
            root = Path(directory)
            for name, content in files.items():
                (root / name).write_text(content)
            sys.path.insert(0, str(root))
            from run_gpu import run

            return run(
                Path("/opt/provider-probe/.pixi/envs/tool-a/bin/python"),
                root / "gpu_probe.py",
                root / "scratch",
            )

    files = {
        name: Path(__file__).with_name(name).read_text()
        for name in ("gpu_probe.py", "run_gpu.py", "sequence.py")
    }
    with modal.enable_output(), app.run():
        report = {
            "app_id": app.app_id,
            "image_id": image_id,
            "sdk": modal.__version__,
            "requested_gpu": "A10G",
            "result": invoke.remote(files),
        }
    report["app_context_closed"] = True
    output.write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("image_id")
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    qualify(args.image_id, args.output)
